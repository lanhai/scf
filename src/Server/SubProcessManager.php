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
use Scf\Server\Struct\Node;
use Scf\Server\Task\CrontabManager;
use Scf\Server\Task\RQueue;
use Scf\Server\Proxy\ConsoleRelay;
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Throwable;
use function Co\run;

class SubProcessManager {
    protected const PROCESS_EXIT_GRACE_SECONDS = 8;
    protected const PROCESS_EXIT_WARN_AFTER_SECONDS = 60;
    protected const PROCESS_EXIT_WARN_INTERVAL_SECONDS = 60;

    protected array $processList = [];
    protected array $pidList = [];
    protected ?array $includedProcesses = null;
    protected array $excludedProcesses = [];
    protected array $serverConfig;
    protected Server $server;
    protected ?Process $consolePushProcess = null;
    protected $shutdownHandler = null;
    protected $reloadHandler = null;
    protected $commandHandler = null;
    protected $nodeStatusBuilder = null;

    public function __construct(Server $server, $serverConfig, array $options = []) {
        $this->server = $server;
        $this->serverConfig = $serverConfig;
        $this->shutdownHandler = $options['shutdown_handler'] ?? null;
        $this->reloadHandler = $options['reload_handler'] ?? null;
        $this->commandHandler = $options['command_handler'] ?? null;
        $this->nodeStatusBuilder = $options['node_status_builder'] ?? null;
        $this->includedProcesses = isset($options['include_processes']) ? array_fill_keys((array)$options['include_processes'], true) : null;
        $this->excludedProcesses = isset($options['exclude_processes']) ? array_fill_keys((array)$options['exclude_processes'], true) : [];
        $runQueueInMaster = $serverConfig['redis_queue_in_master'] ?? true;
        $runQueueInSlave = $serverConfig['redis_queue_in_slave'] ?? false;
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

    public function start(): void {
        $subProcess = new Process(function () {
            while (true) {
                if (Runtime::instance()->serverIsReady() && (!(defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true) || App::isReady())) {
                    break;
                }
                usleep(100000);
            }
            $this->run();
        });
        $subProcess->start();
    }

    private function run(): void {
        if ($this->consolePushProcess) {
            $this->consolePushProcess->start();
        }
        foreach ($this->processList as $name => $process) {
            /** @var Process $process */
            $process->start();
            $this->pidList[$process->pid] = $name;
        }
        while (true) {
            if (!Runtime::instance()->serverIsAlive()) {
                $this->drainManagedProcesses($this->managedProcessDrainGraceSeconds());
                break;
            }
            while ($ret = Process::wait(false)) {
                if (!Runtime::instance()->serverIsAlive()) {
                    $this->drainManagedProcesses($this->managedProcessDrainGraceSeconds());
                    break;
                }
                $pid = $ret['pid'];
                if (isset($this->pidList[$pid])) {
                    $oldProcessName = $this->pidList[$pid];
                    unset($this->pidList[$pid]);
                    Console::warning("【{$oldProcessName}】子进程#{$pid}退出，准备重启");
                    switch ($oldProcessName) {
                        case 'MemoryUsageCount':
                            $newProcess = $this->createMemoryUsageCountProcess();
                            $this->processList['MemoryUsageCount'] = $newProcess;
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
        if (is_callable($this->shutdownHandler)) {
            ($this->shutdownHandler)();
            return;
        }
        Http::instance()->shutdown();
    }

    protected function triggerReload(): void {
        if (is_callable($this->reloadHandler)) {
            ($this->reloadHandler)();
            return;
        }
        Http::instance()->reload();
    }

    protected function handleRemoteCommand(string $command, array $params, object $socket): bool {
        if (is_callable($this->commandHandler)) {
            return (bool)($this->commandHandler)($command, $params, $socket);
        }

        switch ($command) {
            case 'shutdown':
                $socket->push("【" . SERVER_HOST . "】start shutdown");
                $this->triggerShutdown();
                return true;
            case 'restart':
                $socket->push("【" . SERVER_HOST . "】start reload");
                $this->triggerReload();
                return true;
            case 'appoint_update':
                $taskId = $params['task_id'] ?? '';
                $statePayload = [
                    'event' => 'node_update_state',
                    'data' => [
                        'task_id' => $taskId,
                        'host' => SERVER_HOST,
                        'type' => $params['type'],
                        'version' => $params['version'],
                        'updated_at' => time(),
                    ]
                ];
                $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                    'data' => [
                        'state' => 'running',
                        'message' => "【" . SERVER_HOST . "】开始更新 {$params['type']} => {$params['version']}",
                    ]
                ])));
                if (App::appointUpdateTo($params['type'], $params['version'])) {
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => 'success',
                            'message' => "【" . SERVER_HOST . "】版本更新成功:{$params['type']} => {$params['version']}",
                            'updated_at' => time(),
                        ]
                    ])));
                    $socket->push("【" . SERVER_HOST . "】版本更新成功:{$params['type']} => {$params['version']}");
                } else {
                    $error = App::getLastUpdateError() ?: '未知原因';
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => 'failed',
                            'error' => $error,
                            'message' => "【" . SERVER_HOST . "】版本更新失败:{$params['type']} => {$params['version']},原因:{$error}",
                            'updated_at' => time(),
                        ]
                    ])));
                    $socket->push("【" . SERVER_HOST . "】版本更新失败:{$params['type']} => {$params['version']},原因:{$error}");
                }
                return true;
            default:
                return false;
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

    public function shutdown(): void {
        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
        //Process::kill($this->consolePushProcess->pid, SIGTERM);
        $this->consolePushProcess?->write('shutdown');
        foreach ($this->processList as $process) {
            /** @var Process $process */
            $process->write('shutdown');
            //$process->exit();
            //Process::kill($process->pid, SIGTERM);
        }
    }

    public function quiesceBusinessProcesses(): void {
        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
        $this->sendCommand('upgrade', [], ['CrontabManager', 'RedisQueue']);
    }

    public function sendCommand($cmd, array $params = [], ?array $targets = null): void {
        $targetLookup = $targets ? array_fill_keys($targets, true) : null;
        foreach ($this->processList as $name => $process) {
            if ($targetLookup !== null && !isset($targetLookup[$name])) {
                continue;
            }
            /** @var Process $process */
            $socket = $process->exportSocket();
            $socket->send(JsonHelper::toJson([
                'command' => $cmd,
                'params' => $params,
            ]));
        }
    }

    public function iterateBusinessProcesses(): void {
        $this->sendCommand('upgrade', [], ['CrontabManager', 'RedisQueue']);
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
     * redis队列
     * @return Process
     */
    private function createRedisQueueProcess(): Process {
        return new Process(function (Process $process) {
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            Console::info("【RedisQueue】Redis队列管理PID:" . $process->pid, false);
            define('IS_REDIS_QUEUE_PROCESS', true);
            $quiescing = false;
            while (true) {
                if (!Runtime::instance()->serverIsAlive()) {
                    Console::warning("【RedisQueue】服务器已关闭,结束运行");
                    break;
                }
                if (!Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }
                $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
                if (!$quiescing && !Runtime::instance()->redisQueueProcessStatus() && Runtime::instance()->serverIsAlive() && !Runtime::instance()->serverIsDraining()) {
                    $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
                    Runtime::instance()->redisQueueProcessStatus(true);
                    RQueue::startProcess();
                }
                Runtime::instance()->redisQueueProcessStatus(false);
                $cmd = $process->read();
                if ($cmd == 'shutdown') {
                    Console::warning("【RedisQueue】#{$managerId} 服务器已关闭,结束运行");
                    break;
                }
                if ($cmd !== '') {
                    $quiescing = true;
                    Console::warning("【RedisQueue】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                }
                sleep(1);
            }
        });
    }

    /**
     * 排程任务
     * @return Process
     */
    private function createCrontabManagerProcess(): Process {
        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
        return new Process(function (Process $process) {
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            Console::info("【Crontab】排程任务管理PID:" . $process->pid, false);
            define('IS_CRONTAB_PROCESS', true);
            $quiescing = false;
            while (true) {
                if (!Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }
                $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
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
                        if ($taskInstance['process_is_alive'] == STATUS_OFF) {
                            sleep($processTask['retry_timeout'] ?? 60);
                            if ($managerId == $processTask['manager_id']) {//重新创建发生致命错误的任务进程
                                CrontabManager::createTaskProcess($processTask, $processTask['restart_num'] + 1);
                            } else {
                                CrontabManager::removeTaskTable($processTask['id']);
                            }
                        } elseif ($taskInstance['manager_id'] !== $managerId) {
                            CrontabManager::removeTaskTable($processTask['id']);
                        }
                    }
                }
                $shouldExit = false;
                run(function () use ($process, $managerId, &$shouldExit, &$quiescing) {
                    $socket = $process->exportSocket();
                    $msg = $socket->recv(timeout: 5);
                    if ($msg) {
                        if (StringHelper::isJson($msg)) {
                            $payload = JsonHelper::recover($msg);
                            $command = $payload['command'] ?? 'unknow';
                            Console::log("【Crontab】#{$managerId} 收到命令:" . Color::cyan($command));
                            switch ($command) {
                                case 'upgrade':
                                    $quiescing = true;
                                    Console::warning("【Crontab】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                                case 'shutdown':
                                    Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
                                    Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                                    break;
                                default:
                                    Console::info($command);
                            }
                        } elseif ($msg == 'shutdown') {
                            $shouldExit = true;
                            Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
                            Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                        }
                    }
                    unset($socket);
                });
                if ($shouldExit) {
                    Console::warning("【Crontab】服务器已关闭,结束运行", $this->shouldPushManagedLifecycleLog());
                    break;
                }
            }
        });
    }


    /**
     * 内存占用统计
     * @return Process
     */
    private function createMemoryUsageCountProcess(): Process {
        return new Process(function (Process $process) {
            $expectedShutdown = false;
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            register_shutdown_function(function () use ($process, &$expectedShutdown) {
                $err = error_get_last();
                if ($expectedShutdown) {
                    return;
                }
                if ($err && str_contains((string)($err['message'] ?? ''), 'swoole exit')) {
                    return;
                }
                if ($err && in_array($err['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
                    Console::error(
                        "【MemoryMonitor】致命错误退出 [{$err['type']}]: {$err['message']} @ {$err['file']}:{$err['line']}"
                    );
                    // 必须显式退出，让 master 的 wait() 感知
                    Process::kill($process->pid, SIGTERM);
                }
            });
            run(function () use ($process, &$expectedShutdown) {
                Console::info("【MemoryMonitor】内存监控PID:" . $process->pid, false);
                MemoryMonitor::start('MemoryMonitor');
                $schedule = function () use (&$schedule, $process, &$expectedShutdown) {
                    try {
                            // 1) 退出条件：仅当上一轮完全完成后才安排下一轮
                            $socket = $process->exportSocket();
                            $msg = $socket->recv(timeout: 0.1);
                            if ($msg === 'shutdown') {
                                $expectedShutdown = true;
                                Timer::clearAll();
                                MemoryMonitor::stop();
                                Console::warning("【MemoryMonitor】收到shutdown,安全退出", $this->shouldPushManagedLifecycleLog());
                                return;
                            }
                            // 2) 执行一次完整统计
                            $processList = MemoryMonitorTable::instance()->rows();
                            if ($processList) {
                                $isDarwin = (PHP_OS_FAMILY === 'Darwin');
                                $barrier = Coroutine\Barrier::make();
                                foreach ($processList as $processInfo) {
                                    Coroutine::create(function () use ($barrier, $processInfo, $process, $isDarwin) {
                                        try {
                                            $processName = $processInfo['process'];
                                            $limitMb = $processInfo['limit_memory_mb'] ?? 300;
                                            $pid = (int)$processInfo['pid'];

                                            if (PHP_OS_FAMILY !== 'Darwin' && !Process::kill($pid, 0)) {
                                                return;
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
                                            ) {
                                                if (preg_match('/^worker:(\d+)/', $processName, $m)) {
                                                    if (time() - ($processInfo['restart_ts'] ?? 0) >= 120) {
                                                        Log::instance()->setModule('system')
                                                            ->error("{$processName}[PID:$pid] 内存 {$osActualMb}MB ≥ {$limitMb}MB，强制重启");
                                                        Process::kill($pid, SIGTERM);
                                                        $processInfo['restart_ts'] = time();
                                                        $processInfo['restart_count'] = ($processInfo['restart_count'] ?? 0) + 1;
                                                    }
                                                }
                                            }
                                            $curr = MemoryMonitorTable::instance()->get($processName);
                                            if ($curr && $curr['pid'] !== $processInfo['pid']) {
                                                $processInfo['pid'] = $curr['pid'];
                                            }
                                            MemoryMonitorTable::instance()->set($processName, $processInfo);
                                        } catch (Throwable $e) {
                                            Log::instance()->error("【MemoryMonitor】统计发生错误：" . $e->getMessage());
                                            Process::kill($process->pid, SIGTERM);
                                        }
                                    });
                                }
                                try {
                                    Coroutine\Barrier::wait($barrier, 60);
                                } catch (Throwable $throwable) {
                                    Log::instance()->error("内存统计错误:" . $throwable->getMessage());
                                }
                            } else {
                                Console::warning("【MemoryMonitor】暂无待统计进程", false);
                            }
                            MemoryMonitor::updateUsage('MemoryMonitor');
                            // 3) 统计完成后再安排下一轮，避免 tick 重叠
                            Timer::after(5000, $schedule);
                    } catch (Throwable $e) {
                        if (str_contains($e->getMessage(), 'swoole exit')) {
                            return;
                        }
                        Log::instance()->error("【MemoryMonitor】调度异常:" . $e->getMessage());
                        Process::kill($process->pid, SIGTERM);
                    }
                };
                $schedule();
            });
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
                Console::info("【Heatbeat】心跳进程PID:" . $process->pid, false);
                if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !App::isMaster()) {
                    MemoryMonitor::start('Heatbeat');
                    $processSocket = $process->exportSocket();
                    while (true) {
                        $cmd = $processSocket->recv(timeout: 0.1);
                        if ($cmd == 'shutdown') {
                            Timer::clearAll();
                            Console::error('【Heatbeat】服务器已关闭,终止心跳', false);
                            MemoryMonitor::stop();
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
                $node->started = time();
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
                        $node->role = SERVER_ROLE;
                        $node->app_version = App::version() ?: (App::info()?->toArray()['version'] ?? App::profile()->version);
                        $node->public_version = App::publicVersion() ?: (App::info()?->toArray()['public_version'] ?? (App::profile()->public_version ?: '--'));
                        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
                        $node->heart_beat = time();
                        $node->framework_update_ready = file_exists(SCF_ROOT . '/build/update.pack');
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
                        $node->tasks = CrontabManager::allStatus();
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
                        $reply = $socket->recv();
                        if ($reply === false) {
                            Timer::clear($pingTimerId);
                            unset($pingTimerId);
                            Console::warning('【Heatbeat】已断开master节点连接', false);
                            $socket->close();
                            $processSocket = $process->exportSocket();
                            $cmd = $processSocket->recv(timeout: 0.1);
                            if ($cmd == 'shutdown') {
                                Timer::clearAll();
                                Console::error('【Heatbeat】服务器已关闭,终止心跳', false);
                                $socket->close();
                                MemoryMonitor::stop();
                                return;
                            }
                            break;
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
            run(function () use ($process) {
                if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                    define('IS_GATEWAY_SUB_PROCESS', true);
                }
                Console::info("【LogBackup】日志备份PID:" . $process->pid, false);
                App::mount();
                MemoryMonitor::start('LogBackup');
                $serverConfig = Config::server();
                $logger = Log::instance();

                $logExpireDays = $serverConfig['log_expire_days'] ?? 15;
                //清理过期日志
                $clearCount = $logger->clear($logExpireDays);
                if ($clearCount) {
                    Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
                }
                
                $sock = $process->exportSocket();
                while (true) {
                    $cmd = $sock->recv(timeout: 0.1);
                    if ($cmd == 'shutdown') {
                        Timer::clearAll();
                        MemoryMonitor::stop();
                        Console::warning("【LogBackup】管理进程退出,结束备份", false);
                        return;
                    }
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
                    Coroutine::sleep(5);
                }
            });
        }, false, SOCK_DGRAM);
    }

    /**
     * 文件变更监听
     * @return Process
     */
    private function createFileWatchProcess(): Process {
        return new Process(function (Process $process) {
            $expectedShutdown = false;
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true && !defined('IS_GATEWAY_SUB_PROCESS')) {
                define('IS_GATEWAY_SUB_PROCESS', true);
            }
            register_shutdown_function(function () use ($process, &$expectedShutdown) {
                $err = error_get_last();
                if ($expectedShutdown) {
                    return;
                }
                if ($err && str_contains((string)($err['message'] ?? ''), 'swoole exit')) {
                    return;
                }
                if ($err && in_array($err['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
                    Console::error("【FileWatcher】子进程致命错误，退出: " . $err['message']);
                    Process::kill($process->pid, SIGTERM);
                }
            });
            run(function () use ($process, &$expectedShutdown) {
                Console::info("【FileWatcher】文件改动监听服务PID:" . $process->pid, false);
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
                        $expectedShutdown = true;
                        Timer::clearAll();
                        MemoryMonitor::stop();
                        Console::warning("【FileWatcher】管理进程退出,结束监听", false);
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
                        Console::warning('---------以下文件发生变动,即将重启---------');
                        foreach ($changedFiles as $f) {
                            Console::write($f);
                        }
                        Console::warning('-------------------------------------------');
                        $this->triggerReload();
                    }
                    MemoryMonitor::updateUsage('FileWatcher');
                    Coroutine::sleep(2);
                }
            });
        }, false, SOCK_DGRAM);
    }

}
