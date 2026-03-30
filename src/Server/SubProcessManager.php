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
    protected const MANAGER_COMMAND_RETRY_TIMES = 6;
    protected const MANAGER_COMMAND_RETRY_INTERVAL_US = 50000;
    protected const MANAGER_HEARTBEAT_FRESH_SECONDS = 3;
    protected const MANAGER_HEARTBEAT_SHUTDOWN_STALE_GRACE_SECONDS = 20;

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
        // 心跳进程在 gateway 模式下与 GatewayClusterCoordinator 职责重叠。
        // 当 cluster 协调进程可用时，Heartbeat 仅保留为轻量保活进程，
        // 不再承担到 master 的长连接上报职责，避免重复链路和无效开销。
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
        $shutdownDispatched = false;
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_PID, getmypid());
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT, time());
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
        try {
            if ($this->consolePushProcess) {
                $this->consolePushProcess->start();
            }
            foreach ($this->processList as $name => $process) {
                /** @var Process $process */
                $process->start();
                $this->pidList[$process->pid] = $name;
            }
            while (true) {
                // worker 侧可能先把“关停意图”写入共享表，再把命令投递到 manager pipe。
                // 这里优先吸收该标记，避免在 pipe 命令尚未读到前进入 wait() 分支误重拉子进程。
                if (!$shutdownRequested && (bool)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN) ?? false)) {
                    $shutdownRequested = true;
                }
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
                        $shutdownDispatched = true;
                        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, true);
                    }
                }

                if ($shutdownRequested && !$this->aliveManagedProcesses()) {
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                    break;
                }

                if (!Runtime::instance()->serverIsAlive()) {
                    if (!$shutdownDispatched) {
                        // Gateway restart 场景里 serverIsAlive 可能先被置为 false，再触发
                        // manager pipe 的 shutdown_processes。若这里不主动关停一次子进程，
                        // 会直接落入“被动等待超时 -> SIGTERM”路径，形成关停卡顿。
                        $this->shutdownManagedProcessesDirect($this->isProxyGatewayMode());
                        $shutdownDispatched = true;
                    }
                    $this->drainManagedProcesses($this->managedProcessDrainGraceSeconds());
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, count($this->aliveManagedProcesses()));
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                    break;
                }
                while ($ret = Process::wait(false)) {
                    if (!Runtime::instance()->serverIsAlive()) {
                        if (!$shutdownDispatched) {
                            $this->shutdownManagedProcessesDirect($this->isProxyGatewayMode());
                            $shutdownDispatched = true;
                        }
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
                        $managerShuttingDown = $shutdownRequested
                            || (bool)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN) ?? false);
                        if ($managerShuttingDown) {
                            continue;
                        }
                        Console::warning("【{$oldProcessName}】子进程#{$pid}退出，准备重启");
                        if (!$this->recreateManagedProcess($oldProcessName)) {
                            Console::warning("子进程 {$pid} 退出，未知进程");
                        }
                    }
                }
                usleep(200000);
            }
        } finally {
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT, 0);
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_PID, 0);
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
                $dispatched = $this->sendCommandToProcesses($forwardCommand, $forwardParams, $targets);
                if (!$dispatched && $targets) {
                    // manager 侧可能在“子进程刚退出，wait() 尚未来得及重拉”的窗口收到命令。
                    // 这里主动确保目标子进程存活后重试一次，避免 reload/restart 命令被静默吞掉。
                    foreach ($targets as $target) {
                        $this->ensureProcessAliveForDispatch($target);
                    }
                    $dispatched = $this->sendCommandToProcesses($forwardCommand, $forwardParams, $targets);
                }
                if (!$dispatched) {
                    Console::warning("【SubProcessManager】命令转发失败: command={$forwardCommand}, targets=" . implode(',', $targets ?? []));
                }
                return false;
            case 'shutdown_processes':
                $this->shutdownManagedProcessesDirect((bool)($params['graceful_business'] ?? false));
                return true;
            default:
                return false;
        }
    }

    /**
     * 确保指定托管子进程处于可接收命令的存活状态。
     *
     * manager 转发命令时会遇到“目标刚退出、尚未进入 wait() 自动重拉”窗口。
     * 这里用于在该窗口里主动补拉目标进程，避免控制命令被静默丢失。
     *
     * @param string $name 子进程名
     * @return bool
     */
    protected function ensureProcessAliveForDispatch(string $name): bool {
        /** @var Process|null $process */
        $process = $this->processList[$name] ?? null;
        if ($process instanceof Process) {
            $pid = (int)($process->pid ?? 0);
            if ($pid > 0 && @Process::kill($pid, 0)) {
                return true;
            }
            unset($this->pidList[$pid]);
        }
        return $this->recreateManagedProcess($name);
    }

    /**
     * 按进程名重建并拉起一个受管子进程。
     *
     * @param string $name 子进程名
     * @return bool 是否重建成功
     */
    protected function recreateManagedProcess(string $name): bool {
        $newProcess = match ($name) {
            'GatewayClusterCoordinator' => $this->createGatewayClusterCoordinatorProcess(),
            'MemoryUsageCount' => $this->createMemoryUsageCountProcess(),
            'GatewayBusinessCoordinator' => $this->createGatewayBusinessCoordinatorProcess(),
            'GatewayHealthMonitor' => $this->createGatewayHealthMonitorProcess(),
            'Heartbeat' => $this->createHeartbeatProcess(),
            'LogBackup' => $this->createLogBackupProcess(),
            'CrontabManager' => $this->createCrontabManagerProcess(),
            'FileWatch' => $this->createFileWatchProcess(),
            'RedisQueue' => $this->createRedisQueueProcess(),
            default => null,
        };
        if (!$newProcess instanceof Process) {
            return false;
        }
        $this->processList[$name] = $newProcess;
        $newProcess->start();
        $pid = (int)($newProcess->pid ?? 0);
        if ($pid > 0) {
            $this->pidList[$pid] = $name;
            Console::warning("【{$name}】子进程已重拉，PID:{$pid}");
        }
        return true;
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
        if ($command !== '') {
            $channel = ($this->isProxyGatewayMode() || $this->hasProcess('GatewayClusterCoordinator')) ? 'GatewayCluster' : 'Heatbeat';
            Console::info("【{$channel}】收到master指令: command={$command}", false);
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
                $socket->push("【" . SERVER_HOST . "】start restart");
                $this->triggerRestart();
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
                // 远端集群命令属于强约束控制面协议：只允许白名单。
                // 明确业务流程下不能再走“未知命令兜底分发”，否则命令会在错误进程上下文
                // 被执行，造成重启链路状态漂移（例如 registry/实例状态不一致）。
                if ($command !== '') {
                    $channel = ($this->isProxyGatewayMode() || $this->hasProcess('GatewayClusterCoordinator')) ? 'GatewayCluster' : 'Heatbeat';
                    Console::warning("【{$channel}】未支持的远端命令，已拒绝: command={$command}", false);
                }
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
            Console::info("【GatewayCluster】转交业务编排执行升级: task={$taskId}, type={$type}, version={$version}", false);
            $response = $this->requestGatewayInternalCommand('appoint_update_remote', [
                'task_id' => $taskId,
                'type' => $type,
                'version' => $version,
            ], self::REMOTE_APPOINT_UPDATE_TIMEOUT_SECONDS + 5);
            if (empty($response['ok'])) {
                return [
                    'ok' => false,
                    'message' => (string)($response['message'] ?? 'Gateway 内部升级命令执行失败'),
                    'data' => [],
                    'updated_at' => time(),
                ];
            }
            return [
                'ok' => true,
                'message' => (string)($response['message'] ?? 'success'),
                'data' => (array)($response['data'] ?? []),
                'updated_at' => time(),
            ];
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
     * 从 Runtime 队列取出一条待执行的业务编排命令。
     *
     * worker 侧会把高价值控制命令（例如 appoint_update）先写入共享内存队列，
     * 业务编排子进程在这里按 FIFO 出队执行，避免瞬时 pipe 写入不稳定导致命令丢失。
     *
     * @return array<string, mixed>|null
     */
    protected function dequeueGatewayBusinessRuntimeCommand(): ?array {
        $queue = Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE);
        if (!is_array($queue) || !is_array($queue['items'] ?? null)) {
            return null;
        }

        $items = array_values(array_filter((array)$queue['items'], 'is_array'));
        if (!$items) {
            // 队列空时保留 key，避免 Runtime 表接近满载时下次入队因为“新建 key”
            // 失败而出现“命令已 accepted 但实际未入队”。
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE, [
                'items' => [],
                'updated_at' => time(),
            ]);
            return null;
        }

        $item = array_shift($items);
        if ($items) {
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE, [
                'items' => array_values($items),
                'updated_at' => time(),
            ]);
        } else {
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE, [
                'items' => [],
                'updated_at' => time(),
            ]);
        }

        $command = trim((string)($item['command'] ?? ''));
        if ($command === '') {
            return null;
        }
        return [
            'token' => (string)($item['token'] ?? ''),
            'command' => $command,
            'params' => (array)($item['params'] ?? []),
            'queued_at' => (int)($item['queued_at'] ?? time()),
        ];
    }

    /**
     * 执行并回写一条业务编排命令结果。
     *
     * 统一收口 pipe 命令和 Runtime 队列命令，保证结果写回与日志语义一致。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @param string $source 命令来源，pipe/runtime_queue
     * @param string $token 队列投递 token（runtime_queue 来源可用）
     * @return void
     */
    protected function runGatewayBusinessCommand(string $command, array $params = [], string $source = 'pipe', string $token = ''): void {
        $requestId = (string)($params['request_id'] ?? '');
        unset($params['request_id']);
        Console::info(
            "【GatewayBusiness】收到业务编排命令: command={$command}"
            . ($requestId !== '' ? ", request_id={$requestId}" : '')
            . ", source={$source}"
            . ($token !== '' ? ", token={$token}" : ''),
            false
        );

        $result = $this->executeGatewayBusinessCommand($command, $params);
        Console::info(
            "【GatewayBusiness】业务编排命令执行完成: command={$command}"
            . ($requestId !== '' ? ", request_id={$requestId}" : '')
            . ", ok=" . (!empty($result['ok']) ? 'yes' : 'no')
            . ", message=" . (string)($result['message'] ?? ''),
            false
        );

        if ($requestId === '') {
            return;
        }
        $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
        Runtime::instance()->delete($resultKey);
        Runtime::instance()->set($resultKey, $result);
        Console::info("【GatewayBusiness】业务编排结果已写回: command={$command}, request_id={$requestId}, result_key={$resultKey}", false);
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
                try {
                    // 控制面状态表由 cluster 子进程直接按秒刷新，避免只依赖 pipe 到 worker
                    // 时出现“push 没发出去但状态表也没更新”的双重丢失。
                    ServerNodeStatusTable::instance()->set('localhost', $this->buildGatewayClusterStatusPayload());
                } catch (Throwable $throwable) {
                    Console::warning('【GatewayCluster】刷新本地节点状态失败:' . $throwable->getMessage(), false);
                }
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
                $heartbeatBroken = false;
                $socketLivenessSuspectCount = 0;
                $heartbeatTimerId = null;
                $clearHeartbeatTimer = function () use (&$heartbeatTimerId): void {
                    if ($heartbeatTimerId !== null) {
                        Timer::clear((int)$heartbeatTimerId);
                        $heartbeatTimerId = null;
                    }
                };
                // 心跳发送必须与 recv 链路解耦。某些 Saber/WebSocket 组合下，
                // recv(timeout) 可能长期阻塞而不是按秒返回 false，若把心跳绑定在
                // recv 分支里，slave 会只上报启动那一跳，dashboard 很快误判离线。
                $sendHeartbeat = function () use (&$socket, &$heartbeatBroken): void {
                    if ($heartbeatBroken || !Runtime::instance()->serverIsAlive()) {
                        return;
                    }
                    try {
                        $this->pushGatewayClusterSocketMessage($socket, [
                            'event' => 'node_heart_beat',
                            'data' => [
                                'host' => APP_NODE_ID,
                                'status' => $this->buildGatewayClusterStatusPayload(),
                            ]
                        ], 'node_heart_beat');
                    } catch (Throwable) {
                        $heartbeatBroken = true;
                    }
                };
                $this->pushGatewayClusterSocketMessage($socket, [
                    'event' => 'slave_node_report',
                    'data' => [
                        'host' => APP_NODE_ID,
                        'ip' => SERVER_HOST,
                        'role' => SERVER_ROLE,
                    ]
                ], 'slave_node_report');
                $sendHeartbeat();
                $heartbeatTimerId = Timer::tick(5000, function () use (&$heartbeatBroken, $sendHeartbeat): void {
                    if ($heartbeatBroken) {
                        return;
                    }
                    $sendHeartbeat();
                });
                while (true) {
                    if (!Runtime::instance()->serverIsAlive()) {
                        $clearHeartbeatTimer();
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
                        $clearHeartbeatTimer();
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
                            $this->pushGatewayClusterSocketMessage($socket, [
                                'event' => 'console_log',
                                'data' => [
                                    'host' => APP_NODE_ID,
                                    ...$params,
                                ]
                            ], 'console_log');
                        }
                    }
                    if ($heartbeatBroken) {
                        $clearHeartbeatTimer();
                        try {
                            $socket->close();
                        } catch (Throwable) {
                        }
                        Console::warning('【GatewayCluster】与master gateway连接已断开,准备重连', false);
                        break;
                    }
                    $reply = $socket->recv(1.0);
                    // Saber websocket 在“这一秒内没有收到任何帧”时也可能返回 false，
                    // 这不等于连接已断开。之前这里把 timeout 直接当成断线，slave
                    // 会每隔几秒就主动 close 并重新握手，所以日志里不断出现新的
                    // “已与master gateway建立连接,客户端ID:xx”。
                    //
                    // 这里需要同时补上“底层连接已断开”的判断：如果 master 已经下线，
                    // recv(false) 不能再被当成普通 timeout，否则 slave 会一直卡在
                    // 已失效的 socket 上，永远回不到外层重连循环。
                    if ($reply === false || !$reply || $reply->data === '' || $reply->data === '::pong') {
                        if (!Manager::instance()->isSocketConnected($socket)) {
                            // 部分 Saber 客户端在 recv(timeout) 后会短暂把 connected 标记成 false，
                            // 但连接实际上仍可写。这里先做一次轻量写探活，再用连续失败阈值兜底，
                            // 避免 slave 刚连上就被误判断链而进入“频繁重连”循环。
                            $probeAlive = false;
                            try {
                                $probeAlive = $socket->push('::ping') !== false;
                            } catch (Throwable) {
                                $probeAlive = false;
                            }
                            if ($probeAlive) {
                                $socketLivenessSuspectCount = 0;
                                MemoryMonitor::updateUsage('GatewayCluster');
                                continue;
                            }
                            $socketLivenessSuspectCount++;
                            if ($socketLivenessSuspectCount < 3) {
                                MemoryMonitor::updateUsage('GatewayCluster');
                                continue;
                            }
                            $clearHeartbeatTimer();
                            try {
                                $socket->close();
                            } catch (Throwable) {
                            }
                            Console::warning('【GatewayCluster】与master gateway连接已断开,准备重连', false);
                            break;
                        }
                        $socketLivenessSuspectCount = 0;
                        MemoryMonitor::updateUsage('GatewayCluster');
                        continue;
                    }
                    $socketLivenessSuspectCount = 0;
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
                $clearHeartbeatTimer();
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
     * 向 master cluster socket 推送一条消息，并在失败时抛出异常触发重连。
     *
     * slave 桥接循环不能把 push 失败当成“普通无消息”，否则会卡在半断链连接里，
     * 外层重连分支永远进不去，表现为节点不再自动回连。
     *
     * @param object $socket slave->master websocket 客户端对象
     * @param array<string, mixed> $payload 待发送消息
     * @param string $label 日志语义标签
     * @return void
     */
    protected function pushGatewayClusterSocketMessage(object $socket, array $payload, string $label): void {
        $encoded = JsonHelper::toJson($payload);
        try {
            $pushed = $socket->push($encoded);
        } catch (Throwable $throwable) {
            throw new \RuntimeException("push({$label}) failed: " . $throwable->getMessage(), 0, $throwable);
        }
        // 这里以 push 结果作为发送成败的一手信号。部分客户端会在 timeout 后短暂
        // 暴露不稳定 connected 状态，若再叠加二次连接判定，会把成功发送误判为断链。
        if ($pushed === false) {
            throw new \RuntimeException("push({$label}) failed: socket disconnected");
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
                    // 业务编排只允许单活。若 manager 重拉导致短时并存双 coordinator，
                    // 非 owner 实例必须立即退出，避免同一批回收状态被重复推进。
                    $ownerPid = (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID) ?? 0);
                    if ($ownerPid > 0 && $ownerPid !== (int)$process->pid) {
                        Console::warning("【GatewayBusiness】检测到新实例接管(owner_pid={$ownerPid})，当前实例退出");
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
                    if ($message === 'shutdown') {
                        Console::warning('【GatewayBusiness】收到shutdown,安全退出');
                        $this->exitCoroutineRuntime();
                        break;
                    }
                    if (is_string($message) && $message !== '') {
                        if (!StringHelper::isJson($message)) {
                            continue;
                        }
                        $payload = JsonHelper::recover($message);
                        $command = trim((string)($payload['command'] ?? ''));
                        if ($command === '') {
                            continue;
                        }
                        $this->runGatewayBusinessCommand($command, (array)($payload['params'] ?? []), 'pipe');
                        continue;
                    }

                    // pipe 在部分时序下可能瞬时返回不可写，worker 会把命令写入 Runtime 队列；
                    // 业务编排子进程这里兜底消费，确保升级/reload 这类控制命令不会丢失。
                    $queued = $this->dequeueGatewayBusinessRuntimeCommand();
                    if (is_array($queued)) {
                        $this->runGatewayBusinessCommand(
                            (string)$queued['command'],
                            (array)$queued['params'],
                            'runtime_queue',
                            (string)($queued['token'] ?? '')
                        );
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
    protected function dispatchGatewayInternalCommand(string $command, array $params = [], float $timeoutSeconds = 1.0): bool {
        $response = $this->requestGatewayInternalCommand($command, $params, $timeoutSeconds);
        return !empty($response['ok']);
    }

    /**
     * 向 gateway 内部控制面发起一条带返回值的本机命令请求。
     *
     * cluster/heartbeat/filewatcher 等子进程不直接操作 worker 私有对象，
     * 需要通过 `/_gateway/internal/command` 这条稳定入口把命令交还给 worker。
     * 返回值统一规整成 `ok/message/data`，便于子进程继续做后续收口。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return array<string, mixed>
     */
    protected function requestGatewayInternalCommand(string $command, array $params = [], float $timeoutSeconds = 1.0): array {
        $port = $this->gatewayInternalControlPort();
        if ($port <= 0) {
            return [
                'ok' => false,
                'message' => 'Gateway 内部控制端口不可用',
                'data' => [],
            ];
        }
        $payload = json_encode([
            'command' => $command,
            'params' => $params,
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($payload === false) {
            return [
                'ok' => false,
                'message' => 'Gateway 内部命令编码失败',
                'data' => [],
            ];
        }
        $timeoutSeconds = max(0.1, $timeoutSeconds);
        if (Coroutine::getCid() > 0) {
            return $this->requestGatewayInternalCommandByCoroutine($port, $payload, $timeoutSeconds);
        }
        return $this->requestGatewayInternalCommandByStream($port, $payload, $timeoutSeconds);
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
    protected function dispatchGatewayInternalCommandByCoroutine(int $port, string $payload, float $timeoutSeconds = 1.0): bool {
        $result = $this->requestGatewayInternalCommandByCoroutine($port, $payload, $timeoutSeconds);
        return !empty($result['ok']);
    }

    /**
     * 协程场景下通过 HTTP client 调用 gateway 内部控制面并读取返回值。
     *
     * @param int $port 内部控制端口
     * @param string $payload JSON 请求体
     * @return array<string, mixed>
     */
    protected function requestGatewayInternalCommandByCoroutine(int $port, string $payload, float $timeoutSeconds = 1.0): array {
        $client = new Client('127.0.0.1', $port);
        $client->setHeaders([
            'Host' => '127.0.0.1:' . $port,
            'Content-Type' => 'application/json',
        ]);
        $client->set(['timeout' => max(0.1, $timeoutSeconds)]);
        $ok = $client->post('/_gateway/internal/command', $payload);
        $statusCode = (int)$client->statusCode;
        $body = (string)($client->body ?? '');
        // appoint_update_remote 这类命令会同步等待业务编排结果，若传输层失败且 body 为空，
        // 这里补齐错误信息，避免上游只能拿到“未知原因”。
        if ($body === '' && !$ok) {
            $errno = (int)($client->errCode ?? 0);
            $errMsg = trim((string)($client->errMsg ?? ''));
            $message = $errMsg !== ''
                ? "Gateway 内部控制请求失败({$errno}):{$errMsg}"
                : "Gateway 内部控制请求失败({$errno})";
            $body = JsonHelper::toJson(['message' => $message]);
        }
        $client->close();
        return $this->normalizeGatewayInternalCommandResponse($ok, $statusCode, $body);
    }

    /**
     * 非协程场景下通过 stream socket 回投内部命令。
     *
     * @param int $port
     * @param string $payload
     * @return bool
     */
    protected function dispatchGatewayInternalCommandByStream(int $port, string $payload, float $timeoutSeconds = 1.0): bool {
        $result = $this->requestGatewayInternalCommandByStream($port, $payload, $timeoutSeconds);
        return !empty($result['ok']);
    }

    /**
     * 非协程场景下通过 stream socket 调用 gateway 内部控制面并读取返回值。
     *
     * @param int $port 内部控制端口
     * @param string $payload JSON 请求体
     * @return array<string, mixed>
     */
    protected function requestGatewayInternalCommandByStream(int $port, string $payload, float $timeoutSeconds = 1.0): array {
        $timeoutSeconds = max(0.1, $timeoutSeconds);
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            $timeoutSeconds,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return [
                'ok' => false,
                'message' => $errstr !== '' ? $errstr : 'Gateway 内部控制连接失败',
                'data' => [],
            ];
        }
        stream_set_timeout($socket, (int)max(1, ceil($timeoutSeconds)));
        $request = "POST /_gateway/internal/command HTTP/1.1\r\n"
            . "Host: 127.0.0.1:{$port}\r\n"
            . "Content-Type: application/json\r\n"
            . "Connection: close\r\n"
            . "Content-Length: " . strlen($payload) . "\r\n\r\n"
            . $payload;
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);
        if (!is_string($response) || $response === '') {
            return [
                'ok' => false,
                'message' => 'Gateway 内部控制响应为空',
                'data' => [],
            ];
        }

        $headerBody = explode("\r\n\r\n", $response, 2);
        $header = (string)($headerBody[0] ?? '');
        $body = (string)($headerBody[1] ?? '');
        preg_match('/HTTP\\/\\d\\.\\d\\s+(\\d+)/', $header, $matches);
        $statusCode = (int)($matches[1] ?? 0);
        return $this->normalizeGatewayInternalCommandResponse($statusCode > 0, $statusCode, $body);
    }

    /**
     * 统一解析 gateway 内部控制命令的 HTTP 返回体。
     *
     * 内部控制面返回的是简单 JSON，不同命令的消息字段结构并不完全一致。
     * 这里统一折叠成 `ok/message/data`，让子进程调用方不需要关心具体 HTTP 细节。
     *
     * @param bool $transportOk HTTP 请求是否成功发出
     * @param int $statusCode HTTP 状态码
     * @param string $body 响应体
     * @return array<string, mixed>
     */
    protected function normalizeGatewayInternalCommandResponse(bool $transportOk, int $statusCode, string $body): array {
        $decoded = JsonHelper::recover($body);
        $data = is_array($decoded) ? $decoded : [];
        $message = (string)($data['message'] ?? '');
        if ($message === '' && isset($data['error']) && is_string($data['error'])) {
            $message = $data['error'];
        }
        if ($message === '' && isset($data['data']) && is_string($data['data'])) {
            $message = $data['data'];
        }
        if ($message === '' && !$transportOk) {
            $message = 'Gateway 内部控制请求失败';
        }
        return [
            'ok' => $transportOk && $statusCode === 200,
            'status' => $statusCode,
            'message' => $message,
            'data' => is_array($data['result'] ?? null) ? (array)$data['result'] : [],
            'raw' => $data,
        ];
    }

    public function shutdown(bool $gracefulBusiness = false): void {
        if ($this->managerProcessAvailableForDispatch()) {
            if ($this->dispatchManagerProcessCommand('shutdown_processes', [
                'graceful_business' => $gracefulBusiness,
            ])) {
                return;
            }
            Console::warning('【SubProcessManager】manager 命令通道写入失败，回退到本地句柄执行 shutdown');
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
        if ($this->managerProcessAvailableForDispatch()) {
            return $this->dispatchManagerProcessCommand('forward_process_command', [
                'command' => (string)$cmd,
                'payload' => $params,
                'targets' => $targets ?? [],
            ]);
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
        $payload = JsonHelper::toJson([
            'command' => $cmd,
            'params' => $params,
        ]);
        foreach ($this->processList as $name => $process) {
            if ($targetLookup !== null && !isset($targetLookup[$name])) {
                continue;
            }
            /** @var Process $process */
            $pid = (int)($process->pid ?? 0);
            if ($pid <= 0 || !@Process::kill($pid, 0)) {
                continue;
            }
            // 先走 Process::write 的主通道。部分子进程（例如 GatewayBusinessCoordinator）
            // 在某些运行态下通过 exportSocket()->send 会出现静默失败，write 更稳定。
            if ((bool)$process->write($payload)) {
                $sent = true;
                continue;
            }
            try {
                $socket = $process->exportSocket();
                $sent = ((bool)$socket->send($payload)) || $sent;
            } catch (Throwable) {
            }
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
        if ($this->managerProcessAvailableForDispatch()) {
            if ($this->dispatchManagerProcessCommand('iterate_processes', ['targets' => $targets])) {
                return;
            }
            Console::warning('【SubProcessManager】manager 命令通道写入失败，取消本轮 iterate 以避免误投旧句柄');
            return;
        }
        $this->bumpProcessGenerations($targets);
        $this->sendCommandToProcesses('upgrade', [], $targets);
    }

    /**
     * 判断 manager 总控进程是否可作为稳定命令入口。
     *
     * @return bool
     */
    protected function managerProcessAvailableForDispatch(): bool {
        if (!$this->managerProcess instanceof Process) {
            return false;
        }
        return $this->isManagerProcessAlive();
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
        $markShuttingDown = $command === 'shutdown_processes';
        if ($markShuttingDown) {
            // 先写共享关停标记，再写 pipe 命令，消除“命令尚未被 manager 读到时子进程先退出”
            // 导致的误重拉窗口。
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, true);
        }
        $payload = JsonHelper::toJson([
            'command' => $command,
            'params' => $params,
        ]);
        for ($attempt = 1; $attempt <= self::MANAGER_COMMAND_RETRY_TIMES; $attempt++) {
            if ((bool)$this->managerProcess->write($payload)) {
                return true;
            }
            try {
                $socket = $this->managerProcess->exportSocket();
                if ((bool)$socket->send($payload)) {
                    return true;
                }
            } catch (Throwable) {
            }
            if (!$this->isManagerProcessAlive()) {
                if ($markShuttingDown) {
                    Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                }
                return false;
            }
            if ($attempt < self::MANAGER_COMMAND_RETRY_TIMES) {
                usleep(self::MANAGER_COMMAND_RETRY_INTERVAL_US);
            }
        }
        if ($markShuttingDown) {
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
        }
        return false;
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
        $runtimePid = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_PID) ?? 0);
        $localPid = (int)($this->managerProcess?->pid ?? 0);
        $pid = $runtimePid > 0 ? $runtimePid : $localPid;
        if ($pid <= 0 || !@Process::kill($pid, 0)) {
            return false;
        }
        $heartbeatAt = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT) ?? 0);
        if ($heartbeatAt <= 0) {
            return true;
        }
        if ($this->isSubprocessManagerHeartbeatFresh($heartbeatAt)) {
            return true;
        }
        $runtimeShuttingDown = (bool)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN) ?? false);
        if ($runtimeShuttingDown) {
            return (time() - $heartbeatAt) <= self::MANAGER_HEARTBEAT_SHUTDOWN_STALE_GRACE_SECONDS;
        }
        return false;
    }

    /**
     * 返回当前受管子进程释放状态，供 gateway 关停阶段判断是否还有 FD 持有者。
     *
     * @return array{
     *     manager_pid:int,
     *     manager_alive:bool,
     *     manager_heartbeat_at:int,
     *     manager_heartbeat_age:int,
     *     manager_heartbeat_fresh:bool,
     *     runtime_alive_count:int,
     *     runtime_shutting_down:bool,
     *     tracked_alive_count:int,
     *     tracked_alive:array<string,int>
     * }
     */
    public function managedProcessReleaseStatus(): array {
        $trackedAlive = [];
        $runtimeAliveCount = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT) ?? 0);
        // manager 仍活着且 runtime 显示有在管子进程时，额外附带本地句柄视图做诊断。
        // 这里不再把该视图作为释放判定主依据，避免 worker 侧旧句柄带来的误判。
        if ($runtimeAliveCount > 0 && $this->isManagerProcessAlive()) {
            $trackedAlive = $this->aliveManagedProcesses();
        }
        $managerHeartbeatAt = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT) ?? 0);
        $managerPid = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_PID) ?? 0);
        $managerHeartbeatAge = $managerHeartbeatAt > 0 ? max(0, time() - $managerHeartbeatAt) : -1;
        return [
            'manager_pid' => $managerPid,
            'manager_alive' => $this->isManagerProcessAlive(),
            'manager_heartbeat_at' => $managerHeartbeatAt,
            'manager_heartbeat_age' => $managerHeartbeatAge,
            'manager_heartbeat_fresh' => $this->isSubprocessManagerHeartbeatFresh($managerHeartbeatAt),
            'runtime_alive_count' => $runtimeAliveCount,
            'runtime_shutting_down' => (bool)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN) ?? false),
            'tracked_alive_count' => count($trackedAlive),
            'tracked_alive' => $trackedAlive,
        ];
    }

    /**
     * 判断 subprocess manager 心跳是否仍在“活跃窗口”内。
     *
     * manager 主循环每轮都会刷新心跳，因此超过短 TTL 仍未刷新通常表示：
     * 1) manager 已经退出；或
     * 2) manager 被阻塞，已不再可靠。
     * 在关停等待阶段可借此区分“真实存活”与“僵尸句柄误判”。
     *
     * @param int $heartbeatAt 心跳时间戳
     * @param int $ttlSeconds 心跳存活阈值（秒）
     * @return bool
     */
    protected function isSubprocessManagerHeartbeatFresh(int $heartbeatAt, int $ttlSeconds = self::MANAGER_HEARTBEAT_FRESH_SECONDS): bool {
        if ($heartbeatAt <= 0) {
            return false;
        }
        return (time() - $heartbeatAt) <= max(1, $ttlSeconds);
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
                        if ($reply && isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                            $masterSocket->push('', WEBSOCKET_OPCODE_PONG);
                        }
                        if (($reply === false || !$reply || $reply->data === '' || $reply->data === '::pong')
                            && !Manager::instance()->isSocketConnected($masterSocket)) {
                            try {
                                $masterSocket->close();
                            } catch (Throwable) {
                            }
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
                $clusterBridgeEnabled = $this->isProxyGatewayMode() && $this->hasProcess('GatewayClusterCoordinator');
                // gateway 模式下，当 GatewayClusterCoordinator 可用时，节点上报与远程命令桥接
                // 统一由它负责。Heartbeat 在这里仅做生命周期保活，不再建立到 master 的 socket，
                // 避免重复链路导致“Heatbeat 重连刷屏”和状态干扰。
                if ($clusterBridgeEnabled) {
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
                        if (!Runtime::instance()->serverIsAlive()) {
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
                // gateway 模式下节点身份统一使用 APP_NODE_ID，避免 master/slave 同机时
                // 因共享 IP 产生 host 键冲突，导致命令目标与心跳覆盖错位。
                $nodeHost = $this->isProxyGatewayMode() ? APP_NODE_ID : SERVER_HOST;
                while (true) {
                    $socket = Manager::instance()->getMasterSocketConnection();
                    $socket->push(JsonHelper::toJson(['event' => 'slave_node_report', 'data' => [
                        'host' => $nodeHost,
                        'ip' => SERVER_HOST,
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
                                'host' => $nodeHost,
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
                            if (!Manager::instance()->isSocketConnected($socket)) {
                                Timer::clear($pingTimerId);
                                try {
                                    $socket->close();
                                } catch (Throwable) {
                                }
                                Console::warning('【Heatbeat】与master节点连接已断开,准备重连', false);
                                break;
                            }
                            continue;
                        }
                        if ($reply) {
                            // 有些 websocket 服务端会下发“空 data 的 ping 帧”，这里必须先回 pong，
                            // 否则会被服务端判定为心跳失联并主动断开连接。
                            if (isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                                $socket->push('', WEBSOCKET_OPCODE_PONG);
                                continue;
                            }
                            if (!empty($reply->data) && $reply->data !== "::pong") {
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
                    $meta = $this->readFileWatcherMeta($path);
                    $meta && $fileList[$path] = $meta;
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
                    $changedFiles = [];
                    $currentFiles = $scanDirectories();

                    // 使用 path=>meta 快照做增量比对，避免循环内 array_column/in_array 的 O(n²) 开销。
                    $currentSet = array_fill_keys($currentFiles, true);
                    foreach ($currentFiles as $path) {
                        $meta = $this->readFileWatcherMeta($path);
                        if ($meta === null) {
                            continue;
                        }
                        if (!isset($fileList[$path])) {
                            $fileList[$path] = $meta;
                            $changedFiles[$path] = true;
                            continue;
                        }
                        if ($fileList[$path]['mtime'] !== $meta['mtime'] || $fileList[$path]['size'] !== $meta['size']) {
                            $fileList[$path] = $meta;
                            $changedFiles[$path] = true;
                        }
                    }

                    foreach (array_keys($fileList) as $path) {
                        if (!isset($currentSet[$path])) {
                            unset($fileList[$path]);
                            $changedFiles[$path] = true;
                        }
                    }

                    if ($changedFiles) {
                        $changedPaths = array_keys($changedFiles);
                        $shouldRestart = $this->shouldRestartForChangedFiles($changedPaths);
                        Console::warning($shouldRestart
                            ? '---------以下文件发生变动,检测到Gateway核心目录变动,即将重启Gateway---------'
                            : '---------以下文件发生变动,即将重载业务平面---------');
                        foreach ($changedPaths as $f) {
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
                        // 业务代码变动只触发业务平面 reload；Gateway 重启仅由核心目录变动触发。
                        $this->triggerReload();
                    }
                    MemoryMonitor::updateUsage('FileWatcher');
                    Coroutine::sleep(2);
                }
            });
        }, false, SOCK_DGRAM);
    }

    /**
     * 读取 file watcher 需要的文件元信息。
     *
     * @param string $path 文件路径
     * @return array{mtime:int,size:int}|null
     */
    protected function readFileWatcherMeta(string $path): ?array {
        $mtime = @filemtime($path);
        $size = @filesize($path);
        if ($mtime === false || $size === false) {
            return null;
        }
        return [
            'mtime' => (int)$mtime,
            'size' => (int)$size,
        ];
    }

    protected function shouldRestartForChangedFiles(array $files): bool {
        foreach ($files as $file) {
            $path = str_replace('\\', '/', (string)$file);
            if ($path === '') {
                continue;
            }
            // 仅当 Gateway 代理核心目录变动时才触发 Gateway 重启，
            // 业务目录和其它框架目录变动统一走业务平面 reload。
            if (str_contains($path, '/scf/src/Server/Proxy/')) {
                return true;
            }
        }
        return false;
    }

}
