<?php

namespace Scf\Server;

use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Core\Table\SocketConnectionTable;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Root;
use Scf\Server\LinuxCrontab\LinuxCrontabManager;
use Scf\Server\Struct\Node;
use Scf\Server\Task\CrontabManager;
use Scf\Server\Task\RQueue;
use Scf\Server\Proxy\ConsoleRelay;
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Coroutine\Http\Client;
use Swoole\Event;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Throwable;
use function Co\run;

class SubProcessManager {
    protected const PROCESS_EXIT_GRACE_SECONDS = 8;
    protected const PROCESS_EXIT_WARN_AFTER_SECONDS = 60;
    protected const PROCESS_EXIT_WARN_INTERVAL_SECONDS = 60;
    protected const REMOTE_APPOINT_UPDATE_TIMEOUT_SECONDS = 300;

    /**
     * Server 托管的总控子进程。
     *
     * 这层进程必须在 server->start() 之前通过 addProcess 注册，
     * 避免在 server worker 的协程运行时里再直接 new Process()->start()。
     */
    protected ?Process $managerProcess = null;
    protected array $processList = [];
    protected array $pidList = [];
    protected ?array $includedProcesses = null;
    protected array $excludedProcesses = [];
    protected array $serverConfig;
    protected Server $server;
    protected ?Process $consolePushProcess = null;
    protected $shutdownHandler = null;
    protected $reloadHandler = null;
    protected $restartHandler = null;
    protected $commandHandler = null;
    protected $nodeStatusBuilder = null;
    protected $gatewayBusinessTickHandler = null;
    protected $gatewayHealthTickHandler = null;
    protected $gatewayBusinessCommandHandler = null;

    public function __construct(Server $server, $serverConfig, array $options = []) {
        $this->server = $server;
        $this->serverConfig = $serverConfig;
        $this->shutdownHandler = $options['shutdown_handler'] ?? null;
        $this->reloadHandler = $options['reload_handler'] ?? null;
        $this->restartHandler = $options['restart_handler'] ?? null;
        $this->commandHandler = $options['command_handler'] ?? null;
        $this->nodeStatusBuilder = $options['node_status_builder'] ?? null;
        $this->gatewayBusinessTickHandler = $options['gateway_business_tick_handler'] ?? null;
        $this->gatewayHealthTickHandler = $options['gateway_health_tick_handler'] ?? null;
        $this->gatewayBusinessCommandHandler = $options['gateway_business_command_handler'] ?? null;
        $this->includedProcesses = isset($options['include_processes']) ? array_fill_keys((array)$options['include_processes'], true) : null;
        $this->excludedProcesses = isset($options['exclude_processes']) ? array_fill_keys((array)$options['exclude_processes'], true) : [];
        $runQueueInMaster = $serverConfig['redis_queue_in_master'] ?? true;
        $runQueueInSlave = $serverConfig['redis_queue_in_slave'] ?? false;
        if ($this->isProxyGatewayMode() && $this->processEnabled('GatewayClusterCoordinator')) {
            $this->processList['GatewayClusterCoordinator'] = $this->createGatewayClusterCoordinatorProcess();
        }
        if ($this->isProxyGatewayMode() && $this->processEnabled('GatewayBusinessCoordinator') && is_callable($this->gatewayBusinessTickHandler) && is_callable($this->gatewayBusinessCommandHandler)) {
            $this->processList['GatewayBusinessCoordinator'] = $this->createGatewayBusinessCoordinatorProcess();
        }
        if ($this->isProxyGatewayMode() && $this->processEnabled('GatewayHealthMonitor') && is_callable($this->gatewayHealthTickHandler)) {
            $this->processList['GatewayHealthMonitor'] = $this->createGatewayHealthMonitorProcess();
        }
        //内存使用情况统计
        if ($this->processEnabled('MemoryUsageCount')) {
            $this->processList['MemoryUsageCount'] = $this->createMemoryUsageCountProcess();
        }
        //心跳检测
        if ($this->processEnabled('Heartbeat')) {
            $this->processList['Heartbeat'] = $this->createHeartbeatProcess();
        }
        //日志备份
        if ($this->processEnabled('LogBackup')) {
            $this->processList['LogBackup'] = $this->createLogBackupProcess();
        }
        //排程任务
        if ($this->processEnabled('CrontabManager')) {
            $this->processList['CrontabManager'] = $this->createCrontabManagerProcess();
        }
        // 控制台日志只需要从子节点推给 master，本机 master 不必自连
        if (
            $this->processEnabled('ConsolePush')
            && !App::isMaster()
            && !(defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true)
            && !(defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true)
        ) {
            $this->consolePushProcess = $this->createConsolePushProcess();
        }
        //redis队列
        if ($this->processEnabled('RedisQueue') && ((App::isMaster() && $runQueueInMaster) || (!App::isMaster() && $runQueueInSlave))) {
            $this->processList['RedisQueue'] = $this->createRedisQueueProcess();
        }
        //文件变更监听
        if ($this->processEnabled('FileWatch') && Env::isDev() && APP_SRC_TYPE == 'dir') {
            $this->processList['FileWatch'] = $this->createFileWatchProcess();
        }
    }

    /**
     * 将子进程管理器注册为 Swoole server 托管进程。
     *
     * 注册动作必须发生在 server->start() 之前，这样总控进程由 manager
     * 统一拉起；后续 CrontabManager/MemoryMonitor 等子进程再由该总控进程继续派生。
     *
     * @return void
     */
    public function start(): void {
        if ($this->managerProcess instanceof Process) {
            return;
        }
        if ((int)($this->server->master_pid ?? 0) > 0 || (int)($this->server->manager_pid ?? 0) > 0) {
            throw new \RuntimeException('SubProcessManager 必须在 server->start() 之前通过 addProcess 注册');
        }
        $this->managerProcess = $this->createManagerProcess();
        $this->server->addProcess($this->managerProcess);
    }

    /**
     * 创建 server 托管的总控子进程。
     *
     * 这个进程是 gateway/http 的子进程根节点，负责在运行态就绪后继续拉起
     * MemoryUsageCount、CrontabManager、RedisQueue 等业务辅助子进程。
     *
     * @return Process
     */
    protected function createManagerProcess(): Process {
        return new Process(function (Process $process) {
            $process->setBlocking(false);
            while (true) {
                if (Runtime::instance()->serverIsReady() && (!(defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true) || App::isReady())) {
                    break;
                }
                usleep(100000);
            }
            $this->run($process);
        });
    }

    /**
     * 运行 SubProcessManager 总控循环。
     *
     * 这层进程维护“当前生效”的业务子进程句柄，因此 worker 侧后续所有
     * 重启/迭代命令都必须先投递给这里，再由这里转给真正仍然存活的子进程。
     * 否则一旦子进程在 manager 内部被重拉，worker 手里的旧句柄就会立刻失效。
     *
     * @param Process $managerProcess server 托管的总控进程句柄
     * @return void
     */
    private function run(Process $managerProcess): void {
        $shutdownRequested = false;
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
        if ($this->consolePushProcess) {
            $this->consolePushProcess->start();
        }
        foreach ($this->processList as $name => $process) {
            /** @var Process $process */
            $process->start();
            $this->pidList[$process->pid] = $name;
        }
        while (true) {
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT, time());
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, count($this->aliveManagedProcesses()));
            // 先消费 manager 自己的控制命令，确保 shutdown/restart 指令能在回收退出子进程前生效，
            // 避免同一轮里刚收到 shutdown，又把刚退出的子进程重新拉起。
            $message = @$managerProcess->read();
            if ($message && StringHelper::isJson($message)) {
                $payload = JsonHelper::recover($message);
                $command = (string)($payload['command'] ?? '');
                $params = (array)($payload['params'] ?? []);
                if ($this->handleManagerCommand($command, $params)) {
                    $shutdownRequested = true;
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, true);
                }
            }

            if ($shutdownRequested && !$this->aliveManagedProcesses()) {
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                break;
            }

            if (!Runtime::instance()->serverIsAlive()) {
                $this->drainManagedProcesses($this->managedProcessDrainGraceSeconds());
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, count($this->aliveManagedProcesses()));
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                break;
            }
            while ($ret = Process::wait(false)) {
                if (!Runtime::instance()->serverIsAlive()) {
                    $this->drainManagedProcesses($this->managedProcessDrainGraceSeconds());
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, count($this->aliveManagedProcesses()));
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                    break;
                }
                $pid = $ret['pid'];
                if (isset($this->pidList[$pid])) {
                    $oldProcessName = $this->pidList[$pid];
                    unset($this->pidList[$pid]);
                    $currentProcess = $this->processList[$oldProcessName] ?? null;
                    $currentPid = $currentProcess instanceof Process ? (int)($currentProcess->pid ?? 0) : 0;
                    if ($currentPid > 0 && $currentPid !== $pid) {
                        Console::warning("【{$oldProcessName}】旧子进程#{$pid}已退出，当前接管PID:{$currentPid}");
                        continue;
                    }
                    if ($shutdownRequested) {
                        continue;
                    }
                    Console::warning("【{$oldProcessName}】子进程#{$pid}退出，准备重启");
                    switch ($oldProcessName) {
                        case 'GatewayClusterCoordinator':
                            $newProcess = $this->createGatewayClusterCoordinatorProcess();
                            $this->processList['GatewayClusterCoordinator'] = $newProcess;
                            break;
                        case 'MemoryUsageCount':
                            $newProcess = $this->createMemoryUsageCountProcess();
                            $this->processList['MemoryUsageCount'] = $newProcess;
                            break;
                        case 'GatewayBusinessCoordinator':
                            $newProcess = $this->createGatewayBusinessCoordinatorProcess();
                            $this->processList['GatewayBusinessCoordinator'] = $newProcess;
                            break;
                        case 'GatewayHealthMonitor':
                            $newProcess = $this->createGatewayHealthMonitorProcess();
                            $this->processList['GatewayHealthMonitor'] = $newProcess;
                            break;
                        case 'Heartbeat':
                            $newProcess = $this->createHeartbeatProcess();
                            $this->processList['Heartbeat'] = $newProcess;
                            break;
                        case 'LogBackup':
                            $newProcess = $this->createLogBackupProcess();
                            $this->processList['LogBackup'] = $newProcess;
                            break;
                        case 'CrontabManager':
                            $newProcess = $this->createCrontabManagerProcess();
                            $this->processList['CrontabManager'] = $newProcess;
                            break;
                        case 'FileWatch':
                            $newProcess = $this->createFileWatchProcess();
                            $this->processList['FileWatch'] = $newProcess;
                            break;
                        case 'RedisQueue':
                            $newProcess = $this->createRedisQueueProcess();
                            $this->processList['RedisQueue'] = $newProcess;
                            break;
                        default:
                            Console::warning("子进程 {$pid} 退出，未知进程");
                    }
                    if (!empty($newProcess)) {
                        $newProcess->start();
                        $this->pidList[$newProcess->pid] = $oldProcessName;
                    }
                }
            }
            usleep(200000);
        }
    }

    /**
     * 处理 worker 发往 SubProcessManager 总控进程的命令。
     *
     * manager 持有的是当前最新一代子进程句柄，因此所有需要命中“当前活着的”
     * Crontab/RedisQueue manager 的操作都应该经由这里转发。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return bool 是否进入“等待所有托管子进程退出后结束 manager 自身”的关停流程
     */
    protected function handleManagerCommand(string $command, array $params = []): bool {
        switch ($command) {
            case 'iterate_processes':
                $targets = array_values(array_filter((array)($params['targets'] ?? []), 'is_string'));
                $targets = array_values(array_filter($targets, fn(string $target): bool => $this->hasProcess($target)));
                if (!$targets) {
                    return false;
                }
                $this->bumpProcessGenerations($targets);
                $this->sendCommandToProcesses('upgrade', [], $targets);
                return false;
            case 'forward_process_command':
                $forwardCommand = (string)($params['command'] ?? '');
                if ($forwardCommand === '') {
                    return false;
                }
                $forwardParams = (array)($params['payload'] ?? []);
                $targets = array_values(array_filter((array)($params['targets'] ?? []), 'is_string'));
                $targets = $targets ? array_values(array_filter($targets, fn(string $target): bool => $this->hasProcess($target))) : null;
                $this->sendCommandToProcesses($forwardCommand, $forwardParams, $targets);
                return false;
            case 'shutdown_processes':
                $this->shutdownManagedProcessesDirect((bool)($params['graceful_business'] ?? false));
                return true;
            default:
                return false;
        }
    }

    protected function drainManagedProcesses(int $graceSeconds = self::PROCESS_EXIT_GRACE_SECONDS): void {
        $startedAt = microtime(true);
        $deadline = $startedAt + max(1, $graceSeconds);
        $nextWarnAt = $startedAt + self::PROCESS_EXIT_WARN_AFTER_SECONDS;
        do {
            while ($ret = Process::wait(false)) {
                $pid = (int)($ret['pid'] ?? 0);
                if ($pid > 0) {
                    unset($this->pidList[$pid]);
                }
            }

            $remaining = $this->aliveManagedProcesses();
            if (!$remaining) {
                return;
            }

            $now = microtime(true);
            if ($now >= $nextWarnAt) {
                $elapsed = max(1, (int)floor($now - $startedAt));
                foreach ($remaining as $name => $pid) {
                    Console::warning("【{$name}】子进程#{$pid}仍在等待平滑退出({$elapsed}s)");
                }
                $nextWarnAt += self::PROCESS_EXIT_WARN_INTERVAL_SECONDS;
            }

            usleep(200000);
        } while (microtime(true) < $deadline);

        foreach ($this->aliveManagedProcesses() as $name => $pid) {
            Console::warning("【{$name}】子进程#{$pid}超时未退出，发送 SIGTERM");
            @Process::kill($pid, SIGTERM);
        }

        $termDeadline = microtime(true) + 2;
        do {
            while ($ret = Process::wait(false)) {
                $pid = (int)($ret['pid'] ?? 0);
                if ($pid > 0) {
                    unset($this->pidList[$pid]);
                }
            }

            if (!$this->aliveManagedProcesses()) {
                return;
            }

            usleep(200000);
        } while (microtime(true) < $termDeadline);

        foreach ($this->aliveManagedProcesses() as $name => $pid) {
            Console::warning("【{$name}】子进程#{$pid}强制退出");
            @Process::kill($pid, SIGKILL);
        }
    }

    protected function aliveManagedProcesses(): array {
        $alive = [];
        if ($this->consolePushProcess instanceof Process) {
            $pid = (int)($this->consolePushProcess->pid ?? 0);
            if ($pid > 0 && @Process::kill($pid, 0)) {
                $alive['ConsolePush'] = $pid;
            }
        }

        foreach ($this->processList as $name => $process) {
            if (!$process instanceof Process) {
                continue;
            }
            $pid = (int)($process->pid ?? 0);
            if ($pid > 0 && @Process::kill($pid, 0)) {
                $alive[$name] = $pid;
            }
        }

        return $alive;
    }

    protected function managedProcessDrainGraceSeconds(): int {
        if (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true) {
            return max(
                \Scf\Server\Proxy\AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS,
                (int)(Config::server()['proxy_upstream_shutdown_timeout'] ?? \Scf\Server\Proxy\AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS)
            );
        }
        return self::PROCESS_EXIT_GRACE_SECONDS;
    }

    protected function triggerShutdown(): void {
        if ($this->isProxyGatewayMode() && $this->dispatchGatewayInternalCommand('shutdown')) {
            return;
        }
        if (is_callable($this->shutdownHandler)) {
            ($this->shutdownHandler)();
            return;
        }
        Http::instance()->shutdown();
    }

    protected function triggerReload(): void {
        if ($this->isProxyGatewayMode() && $this->dispatchGatewayInternalCommand('reload')) {
            return;
        }
        if (is_callable($this->reloadHandler)) {
            ($this->reloadHandler)();
            return;
        }
        Http::instance()->reload();
    }

    /**
     * 触发 gateway 控制面与业务面的联合 reload。
     *
     * 这个入口专门给 gateway 模式下的 FileWatcher 使用。普通 `reload`
     * 语义仍然保留为“业务面 reload”，避免影响现有 dashboard 命令含义；
     * 但文件变动属于代码热更新场景，需要让 gateway worker 也重新载入代码。
     *
     * @return void
     */
    protected function triggerGatewayCodeReload(): void {
        if ($this->isProxyGatewayMode() && $this->dispatchGatewayInternalCommand('reload_gateway')) {
            return;
        }
        $this->triggerReload();
    }

    protected function triggerRestart(): void {
        if ($this->isProxyGatewayMode() && $this->dispatchGatewayInternalCommand('restart')) {
            return;
        }
        if (is_callable($this->restartHandler)) {
            ($this->restartHandler)();
            return;
        }
        $this->triggerShutdown();
    }

    protected function handleRemoteCommand(string $command, array $params, object $socket): bool {
        if (is_callable($this->commandHandler)) {
            $handled = (bool)($this->commandHandler)($command, $params, $socket);
            if ($handled) {
                return true;
            }
        }

        switch ($command) {
            case 'shutdown':
                $socket->push("【" . SERVER_HOST . "】start shutdown");
                $this->triggerShutdown();
                return true;
            case 'reload':
                $socket->push("【" . SERVER_HOST . "】start reload");
                $this->triggerReload();
                return true;
            case 'restart':
                $socket->push("【" . SERVER_HOST . "】start reload");
                $this->triggerReload();
                return true;
            case 'restart_crontab':
                if ($this->isProxyGatewayMode() && $this->dispatchGatewayInternalCommand('restart_crontab')) {
                    $socket->push("【" . SERVER_HOST . "】start crontab restart");
                    return true;
                }
                return false;
            case 'restart_redisqueue':
                if ($this->isProxyGatewayMode() && $this->dispatchGatewayInternalCommand('restart_redisqueue')) {
                    $socket->push("【" . SERVER_HOST . "】start redisqueue restart");
                    return true;
                }
                return false;
            case 'linux_crontab_sync':
                try {
                    $result = LinuxCrontabManager::applyReplicationPayload((array)($params['config'] ?? []));
                    Console::success("【LinuxCrontab】已同步本地排程配置: items=" . (int)($result['item_count'] ?? 0), false);
                    $this->reportLinuxCrontabSyncState($socket, 'success', [
                        'message' => "【" . SERVER_HOST . "】Linux 排程已同步: items=" . (int)($result['item_count'] ?? 0),
                        'item_count' => (int)($result['item_count'] ?? 0),
                        'sync' => (array)($result['sync'] ?? []),
                    ]);
                } catch (Throwable $throwable) {
                    Console::warning("【LinuxCrontab】同步排程配置失败:" . $throwable->getMessage(), false);
                    $this->reportLinuxCrontabSyncState($socket, 'failed', [
                        'message' => "【" . SERVER_HOST . "】Linux 排程同步失败:" . $throwable->getMessage(),
                        'error' => $throwable->getMessage(),
                    ]);
                }
                return true;
            case 'appoint_update':
                $taskId = (string)($params['task_id'] ?? '');
                $type = (string)($params['type'] ?? '');
                $version = (string)($params['version'] ?? '');
                $statePayload = [
                    'event' => 'node_update_state',
                    'data' => [
                        'task_id' => $taskId,
                        // 升级状态回报必须和 slave_node_report / heartbeat 使用同一 host 标识，
                        // 否则 master 汇总 task 状态时会把这个 slave 误判成一直 pending。
                        'host' => APP_NODE_ID,
                        'type' => $type,
                        'version' => $version,
                        'updated_at' => time(),
                    ]
                ];
                $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                    'data' => [
                        'state' => 'running',
                        'message' => "【" . SERVER_HOST . "】开始更新 {$type} => {$version}",
                    ]
                ])));
                $result = $this->executeRemoteAppointUpdate($type, $version, $taskId);
                $reportedState = (string)(($result['data'] ?? [])['master']['state'] ?? '');
                if (!empty($result['ok']) && $reportedState !== 'failed') {
                    $finalState = $reportedState === 'pending' ? 'pending' : 'success';
                    $finalMessage = $finalState === 'pending'
                        ? "【" . SERVER_HOST . "】版本更新已完成，等待重启生效:{$type} => {$version}"
                        : "【" . SERVER_HOST . "】版本更新成功:{$type} => {$version}";
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => $finalState,
                            'message' => $finalMessage,
                            'updated_at' => time(),
                        ]
                    ])));
                    $socket->push($finalMessage);
                } else {
                    $error = (string)($result['message'] ?? '') ?: App::getLastUpdateError() ?: '未知原因';
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => 'failed',
                            'error' => $error,
                            'message' => "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}",
                            'updated_at' => time(),
                        ]
                    ])));
                    $socket->push("【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}");
                }
                return true;
            default:
                return false;
        }
    }

    /**
     * 在 slave 节点上执行远端下发的指定版本升级。
     *
     * slave 不能再直接在 cluster 协调进程里调用 `App::appointUpdateTo()`，否则会绕开
     * gateway 业务编排子进程那层统一升级链，导致本地包替换、rolling upstream、
     * 业务子进程迭代与 master 行为不一致。这里改为与 master 复用同一条业务编排命令，
     * 只在完成后额外把结果回报给 master。
     *
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @param string $taskId 集群升级任务 id
     * @return array<string, mixed>
     */
    protected function executeRemoteAppointUpdate(string $type, string $version, string $taskId): array {
        if (
            $this->isProxyGatewayMode()
            && $this->hasProcess('GatewayBusinessCoordinator')
            && is_callable($this->gatewayBusinessCommandHandler)
        ) {
            $requestId = uniqid('gateway_business_', true);
            $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
            Runtime::instance()->delete($resultKey);
            Console::info("【GatewayCluster】转交业务编排执行升级: task={$taskId}, type={$type}, version={$version}, request_id={$requestId}", false);
            if (!$this->sendCommand('appoint_update', [
                'task_id' => $taskId,
                'type' => $type,
                'version' => $version,
                'request_id' => $requestId,
            ], ['GatewayBusinessCoordinator'])) {
                Runtime::instance()->delete($resultKey);
                return [
                    'ok' => false,
                    'message' => 'Gateway 业务编排命令投递失败',
                    'data' => [],
                    'updated_at' => time(),
                ];
            }
            $result = $this->waitForGatewayBusinessCommandResult($requestId, self::REMOTE_APPOINT_UPDATE_TIMEOUT_SECONDS);
            if (!empty(($result['data'] ?? [])['iterate_business_processes']) && !empty($result['ok'])) {
                // 这一轮升级已经完成包替换和 upstream 切换，随后再补齐 gateway 业务子进程迭代，
                // 保持 slave 与 master 在 app 升级后的行为一致。
                $this->iterateBusinessProcesses();
            }
            return $result;
        }

        if (App::appointUpdateTo($type, $version)) {
            return [
                'ok' => true,
                'message' => 'success',
                'data' => [],
                'updated_at' => time(),
            ];
        }

        return [
            'ok' => false,
            'message' => App::getLastUpdateError() ?: '未知原因',
            'data' => [],
            'updated_at' => time(),
        ];
    }

    /**
     * 等待 gateway 业务编排命令把执行结果写回 Runtime。
     *
     * cluster 协调进程和 worker 都不直接执行升级重活，只负责等待
     * `GatewayBusinessCoordinator` 的统一结果；这里复用相同的 request_id 回写协议，
     * 让 slave 也能和 master 走同一套升级收口。
     *
     * @param string $requestId 业务编排请求 id
     * @param int $timeoutSeconds 最长等待秒数
     * @return array<string, mixed>
     */
    protected function waitForGatewayBusinessCommandResult(string $requestId, int $timeoutSeconds): array {
        $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        do {
            $result = Runtime::instance()->get($resultKey);
            if (is_array($result) && $result) {
                Runtime::instance()->delete($resultKey);
                $result['ok'] = (bool)($result['ok'] ?? false);
                $result['message'] = (string)($result['message'] ?? ($result['ok'] ? 'success' : 'failed'));
                $result['data'] = (array)($result['data'] ?? []);
                $result['updated_at'] = (int)($result['updated_at'] ?? time());
                return $result;
            }
            Coroutine::sleep(0.2);
        } while (microtime(true) < $deadline);

        Runtime::instance()->delete($resultKey);
        return [
            'ok' => false,
            'message' => "Gateway 业务编排执行超时: request_id={$requestId}",
            'data' => [],
            'updated_at' => time(),
        ];
    }

    /**
     * 将 Linux 排程同步结果即时回报给 master gateway。
     *
     * slave 收到配置后不仅要落地本地文件并同步系统 crontab，还要尽快把
     * “这轮同步是否成功 + 最新节点状态”回推给 master。这样 dashboard
     * 不必等待下一次心跳，就能立即刷新节点排程抽屉里的系统安装状态。
     *
     * @param object $socket 当前 slave -> master 的 websocket 连接
     * @param string $state success / failed
     * @param array<string, mixed> $payload 额外状态字段
     * @return void
     */
    protected function reportLinuxCrontabSyncState(object $socket, string $state, array $payload = []): void {
        $status = $this->buildGatewayClusterStatusPayload();
        $status['linux_crontab_sync'] = [
            'state' => $state,
            'message' => (string)($payload['message'] ?? ''),
            'error' => (string)($payload['error'] ?? ''),
            'updated_at' => time(),
            'item_count' => (int)($payload['item_count'] ?? 0),
            'sync' => (array)($payload['sync'] ?? []),
        ];

        try {
            $socket->push(JsonHelper::toJson([
                'event' => 'linux_crontab_sync_state',
                'data' => [
                    'host' => APP_NODE_ID,
                    'state' => $state,
                    'message' => (string)($payload['message'] ?? ''),
                    'error' => (string)($payload['error'] ?? ''),
                    'item_count' => (int)($payload['item_count'] ?? 0),
                    'sync' => (array)($payload['sync'] ?? []),
                    'updated_at' => time(),
                    'status' => $status,
                ],
            ]));
        } catch (Throwable) {
        }
    }

    protected function buildNodeStatusPayload(Node $node): array {
        $status = $node->asArray();
        if (is_callable($this->nodeStatusBuilder)) {
            $customized = ($this->nodeStatusBuilder)($status, $node);
            if (is_array($customized) && $customized) {
                $status = array_replace_recursive($status, $customized);
            }
        }
        return $status;
    }

    /**
     * 生成 gateway 业务编排命令的共享内存结果 key。
     *
     * worker 只能把命令通过 pipe 发给编排子进程，真正的执行结果则写回 Runtime，
     * 这样 dashboard / 本地 IPC / 集群转发都能用同一条等待链路收口。
     *
     * @param string $requestId 命令请求 id
     * @return string
     */
    protected function gatewayBusinessCommandResultKey(string $requestId): string {
        // 结果 key 必须和 Gateway worker 侧保持同一套定长编码，否则高精度 request id
        // 在写入 Runtime(Table) 时会超过 key 长度限制，导致结果实际没有写回成功。
        return 'gateway_business_result:' . md5($requestId);
    }

    /**
     * 显式结束当前子进程的 Swoole 事件循环。
     *
     * 这批 addProcess 子进程大量通过 `run(function () { ... })` 持有 coroutine scheduler。
     * 如果 shutdown 分支只 `return`，事件循环会在 PHP rshutdown 阶段被 Swoole 兜底
     * `Event::wait()`，从而触发 deprecated warning。这里统一在明确退出点主动结束 loop。
     *
     * @return void
     */
    protected function exitCoroutineRuntime(): void {
        Event::exit();
    }

    /**
     * 判断心跳子进程当前是否应该暂停构建节点状态。
     *
     * gateway 进入 shutdown/restart 时，会先把 ready 拉低并标记 draining，
     * 但 Heatbeat 的定时器可能比 shutdown 命令更早触发。如果此时继续构建
     * 节点状态，就会在 gateway 侧去补采 upstream memory rows，进而在
     * upstream 已经开始退出时撞上本地 IPC 死锁。
     *
     * 因此只要控制面已经进入 draining / not-ready，就先停止这轮状态构建，
     * 等待总控进程显式下发 shutdown，再由子进程收口退出。
     *
     * @return bool 需要暂停心跳状态构建时返回 true。
     */
    protected function shouldSkipHeartbeatStatusBuild(): bool {
        return !Runtime::instance()->serverIsAlive()
            || Runtime::instance()->serverIsDraining()
            || !Runtime::instance()->serverIsReady();
    }

    /**
     * 周期执行 gateway 业务编排逻辑。
     *
     * 这里承接的是 bootstrapManagedUpstreams 一类“需要在子进程里跑”的编排职责，
     * 统一包一层异常保护，避免单轮失败拉崩整个编排进程。
     *
     * @return void
     */
    protected function runGatewayBusinessTick(): void {
        if (!is_callable($this->gatewayBusinessTickHandler)) {
            return;
        }
        try {
            ($this->gatewayBusinessTickHandler)();
        } catch (Throwable $throwable) {
            Console::error('【GatewayBusiness】业务编排执行失败: ' . $throwable->getMessage());
        }
    }

    /**
     * 周期执行 gateway 健康检查逻辑。
     *
     * active upstream 健康探测需要与 worker 解耦，否则控制面 worker 会直接承担
     * 状态机推进职责。这里将它固定收口在单独的健康检查子进程里。
     *
     * @return void
     */
    protected function runGatewayHealthTick(): void {
        if (!is_callable($this->gatewayHealthTickHandler)) {
            return;
        }
        try {
            ($this->gatewayHealthTickHandler)();
        } catch (Throwable $throwable) {
            Console::error('【GatewayHealth】健康检查执行失败: ' . $throwable->getMessage());
        }
    }

    /**
     * 执行一条 gateway 业务编排命令。
     *
     * 只有本地业务编排命令会落到这里，例如 reload / appoint_update。
     * 执行结果会被标准化后写回共享内存。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return array<string, mixed>
     */
    protected function executeGatewayBusinessCommand(string $command, array $params): array {
        if (!is_callable($this->gatewayBusinessCommandHandler)) {
            return [
                'ok' => false,
                'message' => 'gateway business command handler unavailable',
                'data' => [],
                'updated_at' => time(),
            ];
        }
        try {
            $result = ($this->gatewayBusinessCommandHandler)($command, $params);
            if (is_array($result)) {
                $result['ok'] = (bool)($result['ok'] ?? false);
                $result['message'] = (string)($result['message'] ?? ($result['ok'] ? 'success' : 'failed'));
                $result['data'] = (array)($result['data'] ?? []);
                $result['updated_at'] = (int)($result['updated_at'] ?? time());
                return $result;
            }
            return [
                'ok' => true,
                'message' => 'success',
                'data' => ['result' => $result],
                'updated_at' => time(),
            ];
        } catch (Throwable $throwable) {
            return [
                'ok' => false,
                'message' => $throwable->getMessage(),
                'data' => [],
                'updated_at' => time(),
            ];
        }
    }

    /**
     * 创建 gateway 集群协调子进程。
     *
     * 这个子进程承接两类 cluster 职责：
     * 1. master 节点的周期 cluster tick，由子进程通过 pipe message 通知 worker 推送状态；
     * 2. slave 节点到 master 的 websocket 桥接与控制台日志回传。
     *
     * 这样 worker 只保留 dashboard/socket 入口和 fd 级推送出口，不再自己起 cluster 定时器
     * 或维护到 master 的长连接。
     *
     * @return Process
     */
    protected function createGatewayClusterCoordinatorProcess(): Process {
        return new Process(function (Process $process) {
            run(function () use ($process) {
                if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                    define('IS_GATEWAY_SUB_PROCESS', true);
                }
                App::mount();
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT, time());
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【GatewayCluster】集群协调PID:" . $process->pid, false);
                }
                if (App::isMaster()) {
                    $this->runGatewayMasterClusterLoop($process);
                    return;
                }
                $this->runGatewaySlaveClusterLoop($process);
            });
        });
    }

    /**
     * gateway master 的 cluster 周期循环。
     *
     * master 不再在 worker 里跑 dashboard 状态 tick，而是由 cluster 子进程
     * 通过 pipe message 驱动 worker 做“剪掉离线节点 + 推送 dashboard 状态”。
     *
     * @param Process $process 当前 cluster 协调子进程
     * @return void
     */
    protected function runGatewayMasterClusterLoop(Process $process): void {
        MemoryMonitor::start('GatewayCluster');
        $socket = $process->exportSocket();
        $lastTickAt = 0;
        while (true) {
            if (!Runtime::instance()->serverIsAlive()) {
                Console::warning('【GatewayCluster】服务器已关闭,结束运行', false);
                MemoryMonitor::stop();
                $this->exitCoroutineRuntime();
                return;
            }
            $now = time();
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT, $now);
            if ($now !== $lastTickAt && Runtime::instance()->serverIsReady()) {
                $lastTickAt = $now;
                $this->sendGatewayPipeMessage('gateway_cluster_tick');
            }
            $message = $socket->recv(timeout: 1);
            if ($message === 'shutdown') {
                Console::warning('【GatewayCluster】收到shutdown,安全退出', false);
                MemoryMonitor::stop();
                $this->exitCoroutineRuntime();
                return;
            }
        }
    }

    /**
     * gateway slave 的 cluster 桥接循环。
     *
     * slave 节点在这里完成到 master gateway 的 websocket 长连接、节点心跳、
     * 远端命令接收，以及本地控制台日志上送。worker 不再自己维护这条桥接链路。
     *
     * @param Process $process 当前 cluster 协调子进程
     * @return void
     */
    protected function runGatewaySlaveClusterLoop(Process $process): void {
        MemoryMonitor::start('GatewayCluster');
        $processSocket = $process->exportSocket();
        while (true) {
            if (!Runtime::instance()->serverIsAlive()) {
                Console::warning('【GatewayCluster】服务器已关闭,结束运行', false);
                MemoryMonitor::stop();
                return;
            }
            try {
                $socket = Manager::instance()->getMasterSocketConnection();
                $socket->push(JsonHelper::toJson([
                    'event' => 'slave_node_report',
                    'data' => [
                        'host' => APP_NODE_ID,
                        'ip' => SERVER_HOST,
                        'role' => SERVER_ROLE,
                    ]
                ]));
                $socket->push(JsonHelper::toJson([
                    'event' => 'node_heart_beat',
                    'data' => [
                        'host' => APP_NODE_ID,
                        'status' => $this->buildGatewayClusterStatusPayload(),
                    ]
                ]));
                while (true) {
                    if (!Runtime::instance()->serverIsAlive()) {
                        try {
                            $socket->close();
                        } catch (Throwable) {
                        }
                        Console::warning('【GatewayCluster】服务器已关闭,结束运行', false);
                        MemoryMonitor::stop();
                        $this->exitCoroutineRuntime();
                        return;
                    }
                    Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT, time());
                    $message = $processSocket->recv(timeout: 0.1);
                    if ($message === 'shutdown') {
                        try {
                            $socket->close();
                        } catch (Throwable) {
                        }
                        Console::warning('【GatewayCluster】收到shutdown,安全退出', false);
                        MemoryMonitor::stop();
                        $this->exitCoroutineRuntime();
                        return;
                    }
                    if (is_string($message) && $message !== '' && StringHelper::isJson($message)) {
                        $payload = JsonHelper::recover($message);
                        $command = (string)($payload['command'] ?? '');
                        $params = (array)($payload['params'] ?? []);
                        if ($command === 'console_log') {
                            $socket->push(JsonHelper::toJson([
                                'event' => 'console_log',
                                'data' => [
                                    'host' => APP_NODE_ID,
                                    ...$params,
                                ]
                            ]));
                        }
                    }
                    $reply = $socket->recv(1.0);
                    // Saber websocket 在“这一秒内没有收到任何帧”时也可能返回 false，
                    // 这不等于连接已断开。之前这里把 timeout 直接当成断线，slave
                    // 会每隔几秒就主动 close 并重新握手，所以日志里不断出现新的
                    // “已与master gateway建立连接,客户端ID:xx”。
                    //
                    // 这里改成与 Heartbeat 子进程同样的处理：timeout 只代表当前
                    // 没有上行消息，本轮继续主动上送一次 node_heart_beat 保活。
                    if ($reply === false || !$reply || $reply->data === '' || $reply->data === '::pong') {
                        $socket->push(JsonHelper::toJson([
                            'event' => 'node_heart_beat',
                            'data' => [
                                'host' => APP_NODE_ID,
                                'status' => $this->buildGatewayClusterStatusPayload(),
                            ]
                        ]));
                        MemoryMonitor::updateUsage('GatewayCluster');
                        continue;
                    }
                    if (JsonHelper::is($reply->data)) {
                        $data = JsonHelper::recover($reply->data);
                        $event = (string)($data['event'] ?? '');
                        if ($event === 'command') {
                            $command = (string)($data['data']['command'] ?? '');
                            $params = (array)($data['data']['params'] ?? []);
                            if (!$this->handleRemoteCommand($command, $params, $socket)) {
                                Console::warning("【GatewayCluster】Command '$command' is not supported", false);
                            }
                        } elseif ($event === 'slave_node_report_response') {
                            Console::success('【GatewayCluster】已与master gateway建立连接,客户端ID:' . ($data['data'] ?? ''), false);
                        } elseif ($event === 'console_subscription') {
                            ConsoleRelay::setRemoteSubscribed((bool)($data['data']['enabled'] ?? false));
                        }
                    }
                    MemoryMonitor::updateUsage('GatewayCluster');
                }
            } catch (Throwable $throwable) {
                if (!Runtime::instance()->serverIsAlive()) {
                    MemoryMonitor::stop();
                    $this->exitCoroutineRuntime();
                    return;
                }
                Console::warning("【GatewayCluster】与master gateway连接失败:" . $throwable->getMessage(), false);
            }
            Coroutine::sleep(1);
        }
    }

    /**
     * 构建 gateway 节点上报给 master 的 cluster 状态。
     *
     * @return array<string, mixed>
     */
    protected function buildGatewayClusterStatusPayload(): array {
        $node = Node::factory();
        $node->appid = APP_ID;
        $node->id = APP_NODE_ID;
        $node->name = APP_DIR_NAME;
        $node->ip = SERVER_HOST;
        $node->env = SERVER_RUN_ENV ?: 'production';
        $node->fingerprint = APP_FINGERPRINT;
        $node->port = Runtime::instance()->httpPort();
        $node->socketPort = Runtime::instance()->dashboardPort() ?: Runtime::instance()->httpPort();
        $node->started = $this->serverStartedAt();
        $node->restart_times = Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0;
        $node->master_pid = $this->server->master_pid;
        $node->manager_pid = $this->server->manager_pid;
        $node->swoole_version = swoole_version();
        $node->cpu_num = swoole_cpu_num();
        $node->stack_useage = memory_get_usage(true);
        $node->scf_version = SCF_COMPOSER_VERSION;
        $node->server_run_mode = APP_SRC_TYPE;
        $node->role = SERVER_ROLE;
        $node->app_version = App::version() ?: (App::info()?->toArray()['version'] ?? App::profile()->version);
        $node->public_version = App::publicVersion() ?: (App::info()?->toArray()['public_version'] ?? (App::profile()->public_version ?: '--'));
        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
        $node->heart_beat = time();
        $node->framework_update_ready = function_exists('scf_framework_update_ready') && scf_framework_update_ready();
        $node->tables = ATable::list();
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->server_stats = Runtime::instance()->get('SERVER_STATS') ?: [];
        $node->server_stats['long_connection_num'] = SocketConnectionTable::instance()->count();
        $node->mysql_execute_count = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
        $node->http_request_reject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
        $node->http_request_count = Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0;
        $node->http_request_count_current = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
        $node->http_request_count_today = Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0;
        $node->http_request_processing = Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0;
        $node->memory_usage = MemoryMonitor::sum();
        // gateway 模式下节点详情页需要直接看到当前节点本地 Linux 排程；
        // 非 gateway 模式继续保持原有常驻 CrontabManager 状态。
        $node->tasks = $this->isProxyGatewayMode()
            ? LinuxCrontabManager::nodeTasks()
            : CrontabManager::allStatus();
        return $this->buildNodeStatusPayload($node);
    }

    /**
     * 返回当前节点稳定的启动时间。
     *
     * `started` 字段表示服务本轮启动时间，不能随着每次心跳刷新。这里优先复用
     * server bootstrap 时写入 Runtime 的固定值，缺失时只补一次。
     *
     * @return int
     */
    protected function serverStartedAt(): int {
        $startedAt = (int)(Runtime::instance()->get(Key::RUNTIME_SERVER_STARTED_AT) ?? 0);
        if ($startedAt > 0) {
            return $startedAt;
        }
        $startedAt = time();
        Runtime::instance()->set(Key::RUNTIME_SERVER_STARTED_AT, $startedAt);
        return $startedAt;
    }

    /**
     * 向 worker 投递一条 gateway 内部 pipe 消息。
     *
     * cluster 子进程只能通过 IPC 通知 worker 处理 fd 级动作，例如 dashboard 推送；
     * 不能在子进程里直接操作 worker 私有的 websocket 连接集合。
     *
     * @param string $event 事件名
     * @param array<string, mixed> $data 事件数据
     * @return void
     */
    protected function sendGatewayPipeMessage(string $event, array $data = []): void {
        try {
            $this->server->sendMessage(JsonHelper::toJson([
                'event' => $event,
                'data' => $data,
            ]), 0);
        } catch (Throwable) {
        }
    }

    /**
     * 创建 gateway 业务编排子进程。
     *
     * 该进程负责承接 worker 转发过来的业务编排命令，并持续推进
     * bootstrapManagedUpstreams 等周期型控制逻辑。执行结果统一写入 Runtime。
     *
     * @return Process
     */
    protected function createGatewayBusinessCoordinatorProcess(): Process {
        return new Process(function (Process $process) {
            run(function () use ($process) {
                if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                    define('IS_GATEWAY_SUB_PROCESS', true);
                }
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT, time());
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【GatewayBusiness】业务编排PID:" . $process->pid, false);
                }
                $lastTickAt = 0;
                $socket = $process->exportSocket();
                while (true) {
                    if (!Runtime::instance()->serverIsAlive()) {
                        Console::warning('【GatewayBusiness】服务器已关闭,结束运行');
                        $this->exitCoroutineRuntime();
                        break;
                    }
                    $now = time();
                    Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT, $now);
                    if ($now !== $lastTickAt && Runtime::instance()->serverIsReady()) {
                        $lastTickAt = $now;
                        $this->runGatewayBusinessTick();
                    }

                    // 在单个协程事件循环里复用同一个进程 socket，避免反复 run()
                    // 触发新的 Scheduler，造成 eventLoop 已存在的运行时告警。
                    $message = $socket->recv(timeout: 1);
                    if ($message === false || $message === null || $message === '') {
                        continue;
                    }
                    if ($message === 'shutdown') {
                        Console::warning('【GatewayBusiness】收到shutdown,安全退出');
                        $this->exitCoroutineRuntime();
                        break;
                    }
                    if (!StringHelper::isJson($message)) {
                        continue;
                    }
                    $payload = JsonHelper::recover($message);
                    $command = (string)($payload['command'] ?? '');
                    $params = (array)($payload['params'] ?? []);
                    $requestId = (string)($params['request_id'] ?? '');
                    unset($params['request_id']);
                    Console::info("【GatewayBusiness】收到业务编排命令: command={$command}" . ($requestId !== '' ? ", request_id={$requestId}" : ''), false);
                    $result = $this->executeGatewayBusinessCommand($command, $params);
                    Console::info("【GatewayBusiness】业务编排命令执行完成: command={$command}" . ($requestId !== '' ? ", request_id={$requestId}" : '') . ", ok=" . (!empty($result['ok']) ? 'yes' : 'no') . ", message=" . (string)($result['message'] ?? ''), false);
                    if ($requestId !== '') {
                        $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
                        Runtime::instance()->delete($resultKey);
                        Runtime::instance()->set($resultKey, $result);
                        Console::info("【GatewayBusiness】业务编排结果已写回: command={$command}, request_id={$requestId}, result_key={$resultKey}", false);
                    }
                }
            });
        });
    }

    /**
     * 创建 gateway active upstream 健康检查子进程。
     *
     * 健康检查不再由 worker 定时器直接推进，而是固定收口到单独子进程，
     * 避免 worker 同时承担 socket 入口与自愈状态机。
     *
     * @return Process
     */
    protected function createGatewayHealthMonitorProcess(): Process {
        return new Process(function (Process $process) {
            run(function () use ($process) {
                if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                    define('IS_GATEWAY_SUB_PROCESS', true);
                }
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT, time());
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【GatewayHealth】健康检查PID:" . $process->pid, false);
                }
                $socket = $process->exportSocket();
                while (true) {
                    if (!Runtime::instance()->serverIsAlive()) {
                        Console::warning('【GatewayHealth】服务器已关闭,结束运行');
                        $this->exitCoroutineRuntime();
                        break;
                    }
                    Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT, time());
                    if (Runtime::instance()->serverIsReady() && App::isReady()) {
                        $this->runGatewayHealthTick();
                    }
                    $message = $socket->recv(timeout: 1);
                    if ($message === 'shutdown') {
                        Console::warning('【GatewayHealth】收到shutdown,安全退出');
                        $this->exitCoroutineRuntime();
                        break;
                    }
                }
            });
        });
    }

    /**
     * 判断当前是否运行在 gateway 控制面模式。
     *
     * gateway 下的 shutdown/reload/restart 不能在子进程里直接调用捕获的
     * GatewayServer 闭包，否则会在 fork 出来的对象副本里操作 server，造成
     * 控制面进入半关闭状态却没有真正释放监听端口。
     *
     * @return bool
     */
    protected function isProxyGatewayMode(): bool {
        return defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true;
    }

    /**
     * 将控制命令回投给 gateway 控制面自身执行。
     *
     * SubProcessManager 作为 addProcess 托管进程后，FileWatcher/Heartbeat 等
     * 触发的控制动作需要回到 gateway 的 HTTP 控制面处理，不能继续直接调用
     * fork 过来的 GatewayServer 闭包。
     *
     * @param string $command
     * @return bool
     */
    protected function dispatchGatewayInternalCommand(string $command): bool {
        $port = $this->gatewayInternalControlPort();
        if ($port <= 0) {
            return false;
        }
        $payload = json_encode(['command' => $command], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($payload === false) {
            return false;
        }
        if (Coroutine::getCid() > 0) {
            return $this->dispatchGatewayInternalCommandByCoroutine($port, $payload);
        }
        return $this->dispatchGatewayInternalCommandByStream($port, $payload);
    }

    /**
     * 解析 gateway 内部控制命令端口。
     *
     * Gateway 已不再承接 HTTP 业务转发，因此内部控制命令始终走独立控制面端口。
     * 这里与 GatewayServer::controlPort() 的计算规则保持一致。
     *
     * @return int
     */
    protected function gatewayInternalControlPort(): int {
        $businessPort = (int)(Runtime::instance()->httpPort() ?: 0);
        if ($businessPort <= 0) {
            $businessPort = (int)($this->serverConfig['port'] ?? 0);
        }
        if ($businessPort <= 0) {
            return 0;
        }
        $configPort = (int)($this->serverConfig['port'] ?? $businessPort);
        $configuredControlPort = (int)($this->serverConfig['gateway_control_port'] ?? 0);
        if ($configuredControlPort > 0) {
            $offset = max(1, $configuredControlPort - $configPort);
            return $businessPort + $offset;
        }
        return $businessPort + 1000;
    }

    /**
     * 协程场景下通过 HTTP client 回投内部命令。
     *
     * @param int $port
     * @param string $payload
     * @return bool
     */
    protected function dispatchGatewayInternalCommandByCoroutine(int $port, string $payload): bool {
        $client = new Client('127.0.0.1', $port);
        $client->setHeaders([
            'Host' => '127.0.0.1:' . $port,
            'Content-Type' => 'application/json',
        ]);
        $client->set(['timeout' => 1.0]);
        $ok = $client->post('/_gateway/internal/command', $payload);
        $statusCode = (int)$client->statusCode;
        $client->close();
        return $ok && $statusCode === 200;
    }

    /**
     * 非协程场景下通过 stream socket 回投内部命令。
     *
     * @param int $port
     * @param string $payload
     * @return bool
     */
    protected function dispatchGatewayInternalCommandByStream(int $port, string $payload): bool {
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            1.0,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return false;
        }
        stream_set_timeout($socket, 1);
        $request = "POST /_gateway/internal/command HTTP/1.1\r\n"
            . "Host: 127.0.0.1:{$port}\r\n"
            . "Content-Type: application/json\r\n"
            . "Connection: close\r\n"
            . "Content-Length: " . strlen($payload) . "\r\n\r\n"
            . $payload;
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);
        return is_string($response) && str_contains($response, ' 200 ');
    }

    public function shutdown(bool $gracefulBusiness = false): void {
        if ($this->dispatchManagerProcessCommand('shutdown_processes', [
            'graceful_business' => $gracefulBusiness,
        ])) {
            return;
        }
        $this->shutdownManagedProcessesDirect($gracefulBusiness);
    }

    /**
     * 直接向当前 manager 所持有的子进程句柄发送 shutdown。
     *
     * 这条路径只允许在 manager 自身上下文执行，避免再通过 manager pipe 回投给自己。
     *
     * @return void
     */
    protected function shutdownManagedProcessesDirect(bool $gracefulBusiness = false): void {
        $this->bumpProcessGenerations(['CrontabManager', 'RedisQueue']);
        $this->consolePushProcess?->write('shutdown');
        $businessProcessNames = ['CrontabManager', 'RedisQueue'];
        if ($gracefulBusiness) {
            $businessTargets = array_values(array_filter($businessProcessNames, fn(string $target): bool => $this->hasProcess($target)));
            if ($businessTargets) {
                $this->sendCommand('upgrade', [], $businessTargets);
            }
        }
        foreach ($this->processList as $name => $process) {
            /** @var Process $process */
            if ($gracefulBusiness && in_array($name, $businessProcessNames, true)) {
                continue;
            }
            // gateway 控制面 restart 只要求 Crontab/RedisQueue 平滑排空；其余附属
            // addProcess 子进程并不承载用户业务。对这批协程型辅助进程继续走 PHP
            // shutdown 路径，会在 full restart 时触发 Swoole rshutdown 的
            // Event::wait() deprecated warning。这里直接 SIGKILL 收口，既不拖住
            // 控制面监听 FD，也避免它们进入各自的协程 shutdown 尾声。
            if ($this->isProxyGatewayMode()) {
                $pid = (int)($process->pid ?? 0);
                if ($pid > 0) {
                    @Process::kill($pid, SIGKILL);
                    continue;
                }
            }
            $process->write('shutdown');
        }
    }

    public function quiesceBusinessProcesses(): void {
        $this->iterateProcesses(['CrontabManager', 'RedisQueue']);
    }

    /**
     * 向受管子进程发送控制命令。
     *
     * worker 持有的子进程对象不会随着 manager 内部重拉自动更新，因此这里默认先把
     * 命令投递给 manager 进程，再由 manager 用“当前活着的”进程句柄转发。只有在
     * manager 不可用时，才回退到当前进程内保存的本地句柄。
     *
     * @param string $cmd 命令名
     * @param array<string, mixed> $params 命令参数
     * @param array<int, string>|null $targets 目标子进程名列表，null 表示广播
     * @return bool 至少有一条命令成功投递时返回 true
     */
    public function sendCommand($cmd, array $params = [], ?array $targets = null): bool {
        if ($this->dispatchManagerProcessCommand('forward_process_command', [
            'command' => (string)$cmd,
            'payload' => $params,
            'targets' => $targets ?? [],
        ])) {
            return true;
        }
        return $this->sendCommandToProcesses((string)$cmd, $params, $targets);
    }

    /**
     * 使用当前进程里保存的子进程句柄直接发送命令。
     *
     * 这条路径只作为 manager IPC 不可用时的降级兜底。真正的稳定路径由
     * `sendCommand()` 先交给 manager 转发，避免 worker 拿着旧句柄投递到失效 pipe。
     *
     * @param string $cmd 命令名
     * @param array<string, mixed> $params 命令参数
     * @param array<int, string>|null $targets 目标子进程名列表
     * @return bool 至少有一条命令成功投递时返回 true
     */
    protected function sendCommandToProcesses(string $cmd, array $params = [], ?array $targets = null): bool {
        $targetLookup = $targets ? array_fill_keys($targets, true) : null;
        $sent = false;
        foreach ($this->processList as $name => $process) {
            if ($targetLookup !== null && !isset($targetLookup[$name])) {
                continue;
            }
            /** @var Process $process */
            $socket = $process->exportSocket();
            $sent = $socket->send(JsonHelper::toJson([
                'command' => $cmd,
                'params' => $params,
            ])) || $sent;
        }
        return $sent;
    }

    public function iterateBusinessProcesses(): void {
        $this->iterateProcesses(['CrontabManager', 'RedisQueue']);
    }

    public function iterateCrontabProcess(): void {
        $this->iterateProcesses(['CrontabManager']);
    }

    public function iterateRedisQueueProcess(): void {
        $this->iterateProcesses(['RedisQueue']);
    }

    public function iterateProcesses(array $targets): void {
        $targets = array_values(array_filter($targets, fn(string $target): bool => $this->hasProcess($target)));
        if (!$targets) {
            return;
        }
        if ($this->dispatchManagerProcessCommand('iterate_processes', ['targets' => $targets])) {
            return;
        }
        $this->bumpProcessGenerations($targets);
        $this->sendCommandToProcesses('upgrade', [], $targets);
    }

    /**
     * 向 SubProcessManager 总控进程投递控制命令。
     *
     * worker 侧持有的子进程对象不会随着 manager 内部重拉自动更新，所以需要
     * 优先通过总控进程这条稳定句柄下发控制命令。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return bool 是否已成功投递给 manager 进程
     */
    protected function dispatchManagerProcessCommand(string $command, array $params = []): bool {
        if (!$this->managerProcess instanceof Process) {
            return false;
        }
        return (bool)$this->managerProcess->write(JsonHelper::toJson([
            'command' => $command,
            'params' => $params,
        ]));
    }

    /**
     * 判断 SubProcessManager 总控进程是否仍然存活。
     *
     * Gateway shutdown/restart 需要等待这层根进程退出，才能确认下面派生出的
     * Crontab/RedisQueue/Health 等托管子进程不会继续持有旧 server 继承下来的监听 FD。
     *
     * @return bool
     */
    public function isManagerProcessAlive(): bool {
        $pid = (int)($this->managerProcess?->pid ?? 0);
        return $pid > 0 && @Process::kill($pid, 0);
    }

    public function hasProcess(string $name): bool {
        if ($name === 'ConsolePush') {
            return $this->consolePushProcess instanceof Process;
        }
        return isset($this->processList[$name]);
    }

    protected function processEnabled(string $name): bool {
        if (isset($this->excludedProcesses[$name])) {
            return false;
        }
        if ($this->includedProcesses === null) {
            return true;
        }
        return isset($this->includedProcesses[$name]);
    }

    /**
     * 推送控制台日志
     * @param $time
     * @param $message
     * @return bool|int
     */
    public function pushConsoleLog($time, $message): bool|int {
        if ($this->consolePushProcess && Coroutine::getCid() > 0) {
            $socket = $this->consolePushProcess->exportSocket();
            return $socket->send(JsonHelper::toJson([
                'time' => $time,
                'message' => $message,
            ]));
        }
        return false;
    }

    protected function shouldPushManagedLifecycleLog(): bool {
        return defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true;
    }

    protected function bumpProcessGenerations(array $targets): void {
        foreach ($targets as $target) {
            switch ($target) {
                case 'CrontabManager':
                    Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
                    break;
                case 'RedisQueue':
                    Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 控制台消息推送socket
     * @return Process
     */
    private function createConsolePushProcess(): Process {
        return new Process(function (Process $process) {
            App::mount();
            Console::info("【ConsolePush】控制台消息推送PID:" . $process->pid, false);
            MemoryMonitor::start('ConsolePush');
            run(function () use ($process) {
                while (true) {
                    $masterSocket = Manager::instance()->getMasterSocketConnection();
                    while (true) {
                        $masterSocket->push('::ping');
                        $reply = $masterSocket->recv(5);
                        if ($reply === false || empty($reply->data)) {
                            $masterSocket->close();
                            Console::warning('【ConsolePush】与master节点连接已断开', false);
                            break;
                        }
                        $processSocket = $process->exportSocket();
                        $msg = $processSocket->recv(timeout: 30);
                        if ($msg) {
                            if (StringHelper::isJson($msg)) {
                                $payload = JsonHelper::recover($msg);
                                $masterSocket->push(JsonHelper::toJson(['event' => 'console_log', 'data' => [
                                    'host' => SERVER_ROLE == NODE_ROLE_MASTER ? 'master' : SERVER_HOST,
                                    ...$payload
                                ]]));
                            } elseif ($msg == 'shutdown') {
                                $masterSocket->close();
                                Console::warning('【ConsolePush】管理进程退出,结束推送', false);
                                MemoryMonitor::stop();
                                return;
                            }
                        }
                        MemoryMonitor::updateUsage('ConsolePush');
                    }
                }
            });
        });
    }

    /**
     * 创建 RedisQueue manager 进程。
     *
     * 这里的 manager 只负责 RedisQueue 消费子进程的生命周期编排：
     * 1. 在运行态 ready 后拉起真正的队列消费子进程；
     * 2. 在收到 upgrade/shutdown 后停止继续拉新消费进程；
     * 3. 用非阻塞 wait(false) 监听消费子进程退出，避免 manager 自己被内部 wait() 卡死。
     *
     * 这样 dashboard 连续触发 restart_redisqueue 时，老 manager 仍能及时进入排空态，
     * 不会因为阻塞等待消费子进程退出而把控制链闷住。
     *
     * @return Process
     */
    private function createRedisQueueProcess(): Process {
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
        return new Process(function (Process $process) {
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID, (int)$process->pid);
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【RedisQueue】Redis队列管理PID:" . $process->pid, false);
            }
            define('IS_REDIS_QUEUE_PROCESS', true);
            // 升级/关停时 manager 需要持续回收消费子进程，不能被 pipe read 阻塞住。
            // 这里直接读取 pipe fd 的非阻塞 stream，避免 Process::read() 在 EAGAIN 时持续刷 warning。
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $managerId = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
            $quiescing = false;
            $queueWorkerPid = 0;
            while (true) {
                // manager 自己负责以非阻塞方式回收队列消费子进程，避免内部 wait() 把升级控制链阻塞住。
                while ($ret = Process::wait(false)) {
                    $pid = (int)($ret['pid'] ?? 0);
                    if ($pid > 0 && $pid === $queueWorkerPid) {
                        $queueWorkerPid = 0;
                        Runtime::instance()->redisQueueProcessStatus(false);
                    }
                }

                if (!Runtime::instance()->serverIsAlive()) {
                    Console::warning("【RedisQueue】服务器已关闭,结束运行");
                    break;
                }
                // iterate 时会先 bump generation；即便 upgrade 命令还在 pipe 里，老 manager 也应该立刻停止继续拉新子进程。
                $latestManagerId = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
                if (!$quiescing && $latestManagerId !== $managerId) {
                    $quiescing = true;
                    Console::warning("【RedisQueue】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                }

                // 非排空态下才受 serverIsReady 门控；一旦进入迭代排空，哪怕控制面
                // 已经把 serverIsReady 拉低，也必须继续推进剩余任务收口并退出。
                if (!$quiescing && !Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }

                if (!$quiescing && $queueWorkerPid <= 0 && !Runtime::instance()->redisQueueProcessStatus() && Runtime::instance()->serverIsAlive() && !Runtime::instance()->serverIsDraining()) {
                    Runtime::instance()->redisQueueProcessStatus(true);
                    $queueProcess = RQueue::startProcess();
                    $queueWorkerPid = (int)($queueProcess?->pid ?? 0);
                    if ($queueWorkerPid <= 0) {
                        Runtime::instance()->redisQueueProcessStatus(false);
                    }
                }

                if ($quiescing && $queueWorkerPid <= 0 && (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0) <= 0) {
                    Console::warning("【RedisQueue】#{$managerId} 管理进程排空完成,退出等待拉起");
                    break;
                }
                $cmd = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($cmd === false) {
                    $cmd = '';
                }
                if ($cmd == 'shutdown') {
                    Console::warning("【RedisQueue】#{$managerId} 服务器已关闭,结束运行");
                    break;
                }
                if ($cmd !== '') {
                    if (!$quiescing) {
                        $quiescing = true;
                        Console::warning("【RedisQueue】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                    }
                }
                sleep(1);
            }
            is_resource($commandPipe) and fclose($commandPipe);
        });
    }

    /**
     * 排程任务
     * @return Process
     */
    private function createCrontabManagerProcess(): Process {
        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
        return new Process(function (Process $process) {
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            Console::info("【Crontab】排程任务管理PID:" . $process->pid, false);
            define('IS_CRONTAB_PROCESS', true);
            // 升级/关停时 manager 需要持续 wait(false) 回收任务子进程，不能被 pipe read 闷住。
            // 这里直接读取 pipe fd 的非阻塞 stream，避免 Process::read() 在 EAGAIN 时持续刷 warning。
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $managerId = (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0);
            $quiescing = false;
            while (true) {
                // Crontab manager 需要在整个生命周期里持续回收任务子进程。
                // 否则排空阶段虽然任务进程已经退出，但 CrontabTable 里的旧行还在，
                // manager 会误判“仍有存量任务未收完”而一直不退出，最终拖住 gateway 监听 FD。
                while ($ret = Process::wait(false)) {
                    $pid = (int)($ret['pid'] ?? 0);
                    if ($pid <= 0) {
                        continue;
                    }
                    if ($task = CrontabManager::getTaskTableByPid($pid)) {
                        CrontabManager::removeTaskTable($task['id']);
                    }
                }
                // gateway restart / crontab iterate 都会先 bump generation。即便 upgrade 命令
                // 因关停时序没有及时从 pipe 里读到，老 manager 也必须基于代际变化立即进入排空。
                $latestManagerId = (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0);
                if (!$quiescing && $latestManagerId !== $managerId) {
                    $quiescing = true;
                    Console::warning("【Crontab】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                }
                // 非排空态下才需要等待 server ready。gateway restart 过程中会先把
                // serverIsReady 置为 false；如果这里一刀切 continue，老 manager
                // 会停止推进任务回收，最终自己永远挂着并继续持有旧监听 FD。
                if (!$quiescing && !Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }
                if (!$quiescing && !Runtime::instance()->crontabProcessStatus() && Runtime::instance()->serverIsAlive() && !Runtime::instance()->serverIsDraining()) {
                    $taskList = CrontabManager::start();
                    Runtime::instance()->crontabProcessStatus(true);
                    if ($taskList) {
                        while ($ret = Process::wait(false)) {
                            if ($t = CrontabManager::getTaskTableByPid($ret['pid'])) {
                                CrontabManager::removeTaskTable($t['id']);
                            }
                        }
                    }
                }
                $tasks = CrontabManager::getTaskTable();
                if (!$tasks) {
                    Runtime::instance()->crontabProcessStatus(false);
                    if ($quiescing) {
                        Console::warning("【Crontab】#{$managerId} 管理进程排空完成,退出等待拉起");
                        break;
                    }
                } else {
                    foreach ($tasks as $processTask) {
                        if (!isset($processTask['id'])) {
                            Console::warning("【Crontab】任务ID为空:" . JsonHelper::toJson($processTask));
                            continue;
                        }
                        if (Counter::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR')) {
                            CrontabManager::errorReport($processTask);
                        }
                        $taskInstance = CrontabManager::getTaskTableById($processTask['id']);
                        $taskPid = (int)($taskInstance['pid'] ?? 0);
                        $taskAlive = $taskPid > 0 && Process::kill($taskPid, 0);
                        if (!$taskAlive) {
                            // gateway restart 时老 manager 可能在父进程退出后才继续扫尾。
                            // 这时如果 wait(false) 没及时捞到子进程退出，CrontabTable 会残留旧行，
                            // manager 就会误判“还有任务没排空”而继续持有旧监听 FD。
                            if ($quiescing || $taskInstance['manager_id'] !== $managerId) {
                                CrontabManager::removeTaskTable($processTask['id']);
                            } else {
                                CrontabManager::updateTaskTable($processTask['id'], [
                                    'process_is_alive' => STATUS_OFF,
                                ]);
                            }
                            continue;
                        }
                        if ($quiescing) {
                            // gateway full restart 时，Crontab task 子进程大多只是挂着
                            // Timer/Coroutine 等待下一轮执行；让它们自己走 PHP shutdown
                            // 尾声，会触发 Swoole rshutdown 的 Event::wait() warning。
                            // 这里改由 manager 在“当前轮次已空闲”后直接收掉对应 task pid：
                            // 正在执行中的任务继续跑完，回到 idle 态后再被回收。
                            if ((int)($taskInstance['is_busy'] ?? 0) <= 0) {
                                @Process::kill($taskPid, SIGKILL);
                            }
                            continue;
                        }
                        if ($taskInstance['process_is_alive'] == STATUS_OFF) {
                            sleep($processTask['retry_timeout'] ?? 60);
                            if ($quiescing) {
                                // 排空阶段不再重拉旧任务，让老 manager 只负责把存量任务收干净后退出。
                                CrontabManager::removeTaskTable($processTask['id']);
                            } elseif ($managerId == $processTask['manager_id']) {//重新创建发生致命错误的任务进程
                                CrontabManager::createTaskProcess($processTask, $processTask['restart_num'] + 1);
                            } else {
                                CrontabManager::removeTaskTable($processTask['id']);
                            }
                        } elseif ($taskInstance['manager_id'] !== $managerId) {
                            CrontabManager::removeTaskTable($processTask['id']);
                        }
                    }
                }
                $msg = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($msg === false) {
                    $msg = '';
                }
                if ($msg !== '') {
                    if (StringHelper::isJson($msg)) {
                        $payload = JsonHelper::recover($msg);
                        $command = $payload['command'] ?? 'unknow';
                        Console::log("【Crontab】#{$managerId} 收到命令:" . Color::cyan($command));
                        switch ($command) {
                            case 'upgrade':
                                $quiescing = true;
                                Console::warning("【Crontab】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                            case 'shutdown':
                                break;
                            default:
                                Console::info($command);
                        }
                    } elseif ($msg == 'shutdown') {
                        Console::warning("【Crontab】服务器已关闭,结束运行", $this->shouldPushManagedLifecycleLog());
                        break;
                    }
                }
                if ($msg == 'shutdown') {
                    Console::warning("【Crontab】服务器已关闭,结束运行", $this->shouldPushManagedLifecycleLog());
                    break;
                }
                sleep(1);
            }
            is_resource($commandPipe) and fclose($commandPipe);
        });
    }


    /**
     * 内存占用统计
     * @return Process
     */
    private function createMemoryUsageCountProcess(): Process {
        return new Process(function (Process $process) {
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            Runtime::instance()->set(Key::RUNTIME_MEMORY_MONITOR_PID, (int)$process->pid);
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【MemoryMonitor】内存监控PID:" . $process->pid, false);
            }
            MemoryMonitor::start('MemoryMonitor');
            $nextTickAt = 0.0;
            while (true) {
                $msg = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($msg === false) {
                    $msg = '';
                }
                if ($msg === 'shutdown') {
                    MemoryMonitor::stop();
                    Console::warning("【MemoryMonitor】收到shutdown,安全退出", $this->shouldPushManagedLifecycleLog());
                    break;
                }

                if (microtime(true) >= $nextTickAt) {
                    try {
                        $processList = MemoryMonitorTable::instance()->rows();
                        if ($processList) {
                            foreach ($processList as $processInfo) {
                                $processName = $processInfo['process'];
                                $limitMb = $processInfo['limit_memory_mb'] ?? 300;
                                $pid = (int)$processInfo['pid'];

                                if (PHP_OS_FAMILY !== 'Darwin' && !Process::kill($pid, 0)) {
                                    continue;
                                }

                                $mem = MemoryMonitor::getPssRssByPid($pid);
                                $rss = isset($mem['rss_kb']) ? round($mem['rss_kb'] / 1024, 1) : null;
                                $pss = isset($mem['pss_kb']) ? round($mem['pss_kb'] / 1024, 1) : null;
                                $osActualMb = $pss ?? $rss;

                                $processInfo['rss_mb'] = $rss;
                                $processInfo['pss_mb'] = $pss;
                                $processInfo['os_actual'] = $osActualMb;
                                $processInfo['updated'] = time();

                                $autoRestart = $processInfo['auto_restart'] ?? STATUS_OFF;
                                if (
                                    $autoRestart == STATUS_ON
                                    && PHP_OS_FAMILY !== 'Darwin'
                                    && str_starts_with($processName, 'worker:')
                                    && $osActualMb !== null
                                    && $osActualMb > $limitMb
                                    && time() - ($processInfo['restart_ts'] ?? 0) >= 120
                                ) {
                                    Log::instance()->setModule('system')
                                        ->error("{$processName}[PID:$pid] 内存 {$osActualMb}MB ≥ {$limitMb}MB，强制重启");
                                    Process::kill($pid, SIGTERM);
                                    $processInfo['restart_ts'] = time();
                                    $processInfo['restart_count'] = ($processInfo['restart_count'] ?? 0) + 1;
                                }

                                $curr = MemoryMonitorTable::instance()->get($processName);
                                if ($curr && $curr['pid'] !== $processInfo['pid']) {
                                    $processInfo['pid'] = $curr['pid'];
                                }
                                MemoryMonitorTable::instance()->set($processName, $processInfo);
                            }
                        } else {
                            Console::warning("【MemoryMonitor】暂无待统计进程", false);
                        }
                        MemoryMonitor::updateUsage('MemoryMonitor');
                    } catch (Throwable $e) {
                        Log::instance()->error("【MemoryMonitor】调度异常:" . $e->getMessage());
                        Process::kill($process->pid, SIGTERM);
                    }
                    $nextTickAt = microtime(true) + 5;
                }

                usleep(200000);
            }
            is_resource($commandPipe) and fclose($commandPipe);
        }, false, SOCK_DGRAM);
    }

    /**
     * 心跳和状态推送
     * @return Process
     */
    private function createHeartbeatProcess(): Process {
        return new Process(function (Process $process) {
            run(function () use ($process) {
                if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                    define('IS_GATEWAY_SUB_PROCESS', true);
                }
                App::mount();
                Runtime::instance()->set(Key::RUNTIME_HEARTBEAT_PID, (int)$process->pid);
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【Heatbeat】心跳进程PID:" . $process->pid, false);
                }
                if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !App::isMaster()) {
                    MemoryMonitor::start('Heatbeat');
                    $processSocket = $process->exportSocket();
                    while (true) {
                        $cmd = $processSocket->recv(timeout: 0.1);
                        if ($cmd == 'shutdown') {
                            Timer::clearAll();
                            Console::warning('【Heatbeat】服务器已关闭,终止心跳', false);
                            MemoryMonitor::stop();
                            $this->exitCoroutineRuntime();
                            return;
                        }
                        MemoryMonitor::updateUsage('Heatbeat');
                        Coroutine::sleep(5);
                    }
                }
                MemoryMonitor::start('Heatbeat');
                $node = Node::factory();
                $node->appid = APP_ID;
                $node->id = APP_NODE_ID;
                $node->name = APP_DIR_NAME;
                $node->ip = SERVER_HOST;
                $node->fingerprint = APP_FINGERPRINT;
                $node->port = Runtime::instance()->httpPort();
                $node->socketPort = Runtime::instance()->dashboardPort() ?: Runtime::instance()->httpPort();
                $node->started = $this->serverStartedAt();
                $node->restart_times = 0;
                $node->master_pid = $this->server->master_pid;
                $node->manager_pid = $this->server->manager_pid;
                $node->swoole_version = swoole_version();
                $node->cpu_num = swoole_cpu_num();
                $node->stack_useage = Coroutine::getStackUsage();
                $node->scf_version = SCF_COMPOSER_VERSION;
                $node->server_run_mode = APP_SRC_TYPE;
                while (true) {
                    $socket = Manager::instance()->getMasterSocketConnection();
                    $socket->push(JsonHelper::toJson(['event' => 'slave_node_report', 'data' => [
                        'host' => SERVER_HOST,
                        'role' => SERVER_ROLE
                    ]]));
                    $pingTimerId = Timer::tick(1000 * 5, function () use ($socket, &$node) {
                        // 控制面进入 shutdown/restart 后，心跳进程只等待明确的 shutdown 命令，
                        // 不再继续构建节点状态，避免在退出窗口里触发 gateway->upstream IPC。
                        if ($this->shouldSkipHeartbeatStatusBuild()) {
                            MemoryMonitor::updateUsage('Heatbeat');
                            return;
                        }
                        $node->role = SERVER_ROLE;
                        $node->app_version = App::version() ?: (App::info()?->toArray()['version'] ?? App::profile()->version);
                        $node->public_version = App::publicVersion() ?: (App::info()?->toArray()['public_version'] ?? (App::profile()->public_version ?: '--'));
                        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
                        $node->heart_beat = time();
                        $node->framework_update_ready = function_exists('scf_framework_update_ready') && scf_framework_update_ready();
                        $node->tables = ATable::list();
                        $node->restart_times = Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0;
                        $node->stack_useage = memory_get_usage(true);
                        $node->threads = count(Coroutine::list());
                        $node->thread_status = Coroutine::stats();
                        $node->server_stats = Runtime::instance()->get('SERVER_STATS') ?: [];
                        $node->server_stats['long_connection_num'] = SocketConnectionTable::instance()->count();
                        $node->mysql_execute_count = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
                        $node->http_request_reject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
                        $node->http_request_count = Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0;
                        $node->http_request_count_current = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
                        $node->http_request_count_today = Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0;
                        $node->http_request_processing = Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0;
                        $node->memory_usage = MemoryMonitor::sum();
                        // 与 gateway cluster 状态保持一致：proxy gateway 节点改为上报
                        // 当前节点本地 Linux 排程；非 gateway 模式保留原有常驻排程。
                        $node->tasks = $this->isProxyGatewayMode()
                            ? LinuxCrontabManager::nodeTasks()
                            : CrontabManager::allStatus();
                        $payload = $this->buildNodeStatusPayload($node);
                        if ($node->role == NODE_ROLE_MASTER) {
                            ServerNodeStatusTable::instance()->set('localhost', $payload);
                            $socket->push('::ping');
                        } else {
                            $socket->push(JsonHelper::toJson(['event' => 'node_heart_beat', 'data' => [
                                'host' => SERVER_HOST,
                                'status' => $payload
                            ]]));
                        }
                        MemoryMonitor::updateUsage('Heatbeat');
                    });
                    while (true) {
                        $processSocket = $process->exportSocket();
                        $cmd = $processSocket->recv(timeout: 0.1);
                        if ($cmd == 'shutdown') {
                            Timer::clear($pingTimerId);
                            Console::warning('【Heatbeat】服务器已关闭,终止心跳', false);
                            $socket->close();
                            MemoryMonitor::stop();
                            $this->exitCoroutineRuntime();
                            return;
                        }
                        $reply = $socket->recv(1.0);
                        if ($reply === false) {
                            continue;
                        }
                        if ($reply && !empty($reply->data) && $reply->data !== "::pong") {
                            if (isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                                $socket->push('', WEBSOCKET_OPCODE_PONG);
                            }
                            if (JsonHelper::is($reply->data)) {
                                $data = JsonHelper::recover($reply->data);
                                $event = $data['event'] ?? 'unknow';
                                if ($event == 'command') {
                                    $command = $data['data']['command'];
                                    $params = $data['data']['params'];
                                    if (!$this->handleRemoteCommand($command, $params, $socket)) {
                                        Console::warning("【Heatbeat】Command '$command' is not supported", false);
                                    }
                                } elseif ($event == 'slave_node_report_response') {
                                    $masterHost = Manager::instance()->getMasterHost();
                                    Console::success('【Heatbeat】已与master[' . $masterHost . ']建立连接,客户端ID:' . $data['data'], false);
                                } elseif ($event == 'console_subscription') {
                                    ConsoleRelay::setRemoteSubscribed((bool)($data['data']['enabled'] ?? false));
                                } else {
                                    Console::info("【Heatbeat】收到master消息:" . $reply->data, false);
                                }
                            } else {
                                Console::info("【Heatbeat】收到master消息:" . $reply->data, false);
                            }
                        }
                    }
                }
            });
        }, false, SOCK_DGRAM);
    }

    /**
     * 日志备份
     * @return Process
     */
    private function createLogBackupProcess(): Process {
        return new Process(function (Process $process) {
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            Runtime::instance()->set(Key::RUNTIME_LOG_BACKUP_PID, (int)$process->pid);
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【LogBackup】日志备份PID:" . $process->pid, false);
            }
            App::mount();
            MemoryMonitor::start('LogBackup');
            $serverConfig = Config::server();
            $logger = Log::instance();
            $logExpireDays = $serverConfig['log_expire_days'] ?? 15;
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $nextTickAt = 0.0;

            $clearCount = $logger->clear($logExpireDays);
            if ($clearCount) {
                Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
            }

            while (true) {
                $cmd = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($cmd === false) {
                    $cmd = '';
                }
                if ($cmd == 'shutdown') {
                    MemoryMonitor::stop();
                    Console::warning("【LogBackup】管理进程退出,结束备份", false);
                    break;
                }
                if (microtime(true) >= $nextTickAt) {
                    if ((int)Runtime::instance()->get('_LOG_CLEAR_DAY_') !== (int)Date::today()) {
                        $clearCount = $logger->clear($logExpireDays);
                        if ($clearCount) {
                            Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
                        }
                        $countKeyDay = Key::COUNTER_REQUEST . Date::leftday(2);
                        if (Counter::instance()->get($countKeyDay)) {
                            Counter::instance()->delete($countKeyDay);
                        }
                    }
                    $logger->backup();
                    MemoryMonitor::updateUsage('LogBackup');
                    $nextTickAt = microtime(true) + 5;
                }
                usleep(200000);
            }
            is_resource($commandPipe) and fclose($commandPipe);
        }, false, SOCK_DGRAM);
    }

    /**
     * 文件变更监听
     * @return Process
     */
    private function createFileWatchProcess(): Process {
        return new Process(function (Process $process) {
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            run(function () use ($process) {
                Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_PID, (int)$process->pid);
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【FileWatcher】文件改动监听服务PID:" . $process->pid, false);
                }
                sleep(1);
                App::mount();
                MemoryMonitor::start('FileWatcher');
                $scanDirectories = function () {
                    if (APP_SRC_TYPE == 'dir') {
                        $appFiles = Dir::scan(APP_PATH . '/src');
                    } else {
                        $appFiles = [];
                    }
                    return [...$appFiles, ...Dir::scan(Root::dir())];
                };
                $files = $scanDirectories();
                $fileList = [];
                foreach ($files as $path) {
                    $fileList[] = [
                        'path' => $path,
                        'mtime' => filemtime($path),
                        'size' => filesize($path),
                    ];
                }
                while (true) {
                    $socket = $process->exportSocket();
                    $msg = $socket->recv(timeout: 0.1);
                    if ($msg == 'shutdown') {
                        Timer::clearAll();
                        MemoryMonitor::stop();
                        Console::warning("【FileWatcher】管理进程退出,结束监听", false);
                        $this->exitCoroutineRuntime();
                        return;
                    }
                    $changed = false;
                    $changedFiles = [];
                    $currentFiles = $scanDirectories();
                    $currentFilePaths = array_map(fn($file) => $file, $currentFiles);
                    foreach ($currentFilePaths as $path) {
                        if (!in_array($path, array_column($fileList, 'path'))) {
                            $fileList[] = [
                                'path' => $path,
                                'mtime' => filemtime($path),
                                'size' => filesize($path),
                            ];
                            $changed = true;
                            $changedFiles[] = $path;
                        }
                    }
                    foreach ($fileList as $key => &$file) {
                        if (!file_exists($file['path'])) {
                            $changed = true;
                            $changedFiles[] = $file['path'];
                            unset($fileList[$key]);
                            continue;
                        }
                        $mtime = filemtime($file['path']);
                        $size = filesize($file['path']);
                        if ($file['mtime'] !== $mtime || $file['size'] !== $size) {
                            $file['mtime'] = $mtime;
                            $file['size'] = $size;
                            $changed = true;
                            $changedFiles[] = $file['path'];
                        }
                    }
                    if ($changed) {
                        $shouldRestart = $this->shouldRestartForChangedFiles($changedFiles);
                        $reloadDescription = '---------以下文件发生变动,即将重载业务平面---------';
                        if ($this->isProxyGatewayMode()) {
                            $reloadDescription = '---------以下文件发生变动,即将重载Gateway与业务平面---------';
                        }
                        Console::warning($shouldRestart
                            ? '---------以下文件发生变动,即将重启Gateway---------'
                            : $reloadDescription);
                        foreach ($changedFiles as $f) {
                            Console::write($f);
                        }
                        Console::warning('-------------------------------------------');
                        if ($shouldRestart) {
                            $this->triggerRestart();
                            MemoryMonitor::stop();
                            Console::warning("【FileWatcher】管理进程退出,结束监听", false);
                            $this->exitCoroutineRuntime();
                            return;
                        }
                        if ($this->isProxyGatewayMode()) {
                            $this->triggerGatewayCodeReload();
                        } else {
                            $this->triggerReload();
                        }
                    }
                    MemoryMonitor::updateUsage('FileWatcher');
                    Coroutine::sleep(2);
                }
            });
        }, false, SOCK_DGRAM);
    }

    protected function shouldRestartForChangedFiles(array $files): bool {
        foreach ($files as $file) {
            $path = str_replace('\\', '/', (string)$file);
            if ($path === '') {
                continue;
            }
            if (str_ends_with($path, '/scf/src/Server/Proxy/GatewayServer.php')) {
                return true;
            }
            if (str_ends_with($path, '/scf/src/Server/Proxy/GatewayNginxProxyHandler.php') && defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true) {
                return true;
            }
            if (str_ends_with($path, '/scf/src/Server/Proxy/GatewayTcpRelayHandler.php') && defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true) {
                return true;
            }
            if (str_ends_with($path, '/scf/src/Server/SubProcessManager.php')) {
                return true;
            }
            if (str_ends_with($path, '/scf/src/Core/Console.php')) {
                return true;
            }
            if (str_ends_with($path, '/scf/src/Server/Manager.php')) {
                return true;
            }
        }
        return false;
    }

}
