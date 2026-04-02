<?php

namespace Scf\Server;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Server\SubProcess\ConsolePushProcess;
use Scf\Server\SubProcess\CrontabManagerProcess;
use Scf\Server\SubProcess\FileWatchProcess;
use Scf\Server\SubProcess\GatewayBusinessCoordinatorProcess;
use Scf\Server\SubProcess\GatewayClusterCoordinatorProcess;
use Scf\Server\SubProcess\GatewayHealthMonitorProcess;
use Scf\Server\SubProcess\HeartbeatProcess;
use Scf\Server\SubProcess\LogBackupProcess;
use Scf\Server\SubProcess\MemoryUsageCountProcess;
use Scf\Server\SubProcess\RedisQueueProcess;
use Scf\Server\Struct\Node;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Process;
use Swoole\WebSocket\Server;
use Throwable;

class SubProcessManager {
    protected const PROCESS_EXIT_GRACE_SECONDS = 8;
    protected const PROCESS_EXIT_WARN_AFTER_SECONDS = 60;
    protected const PROCESS_EXIT_WARN_INTERVAL_SECONDS = 60;
    protected const MANAGER_COMMAND_RETRY_TIMES = 6;
    protected const MANAGER_COMMAND_RETRY_INTERVAL_US = 50000;
    protected const MANAGER_HEARTBEAT_FRESH_SECONDS = 3;
    protected const MANAGER_HEARTBEAT_SHUTDOWN_STALE_GRACE_SECONDS = 20;
    protected const PROCESS_RESPAWN_RETRY_SECONDS = 2;
    protected const PROCESS_HEARTBEAT_STALE_SECONDS = 120;
    protected const PROCESS_HEARTBEAT_FORCE_KILL_SECONDS = 300;
    protected const PROCESS_HEARTBEAT_HANDLE_COOLDOWN_SECONDS = 30;
    protected const HEARTBEAT_COUNTER_PREFIX = '__hb__:';
    protected const HEARTBEAT_WRITE_FAIL_WARNING_INTERVAL_SECONDS = 30;
    protected const TEMP_HEARTBEAT_TRACE_ENABLED = false;

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
    protected array $processLastRespawnAttemptAt = [];
    protected array $processLastStaleHandleAt = [];
    protected array $manualStoppedProcesses = [];
    protected array $heartbeatWriteFailWarnAt = [];

    public function __construct(Server $server, $serverConfig, array $options = []) {
        $this->server = $server;
        $this->serverConfig = $serverConfig;
        $this->assertGatewayControlPlaneRuntime();
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
        if ($this->processEnabled('GatewayClusterCoordinator')) {
            $this->processList['GatewayClusterCoordinator'] = $this->createGatewayClusterCoordinatorProcess();
        }
        if ($this->processEnabled('GatewayBusinessCoordinator') && is_callable($this->gatewayBusinessTickHandler) && is_callable($this->gatewayBusinessCommandHandler)) {
            $this->processList['GatewayBusinessCoordinator'] = $this->createGatewayBusinessCoordinatorProcess();
        }
        if ($this->processEnabled('GatewayHealthMonitor') && is_callable($this->gatewayHealthTickHandler)) {
            $this->processList['GatewayHealthMonitor'] = $this->createGatewayHealthMonitorProcess();
        }
        // 控制面内存补采子进程（主要补齐 gateway 子进程 rss/pss/os_actual）。
        // upstream worker 的内存轮换主链路已走 gateway->upstream IPC 平滑 stop，
        // 不依赖 MemoryUsageCount 直接发信号。
        if ($this->processEnabled('MemoryUsageCount')) {
            $this->processList['MemoryUsageCount'] = $this->createMemoryUsageCountProcess();
        }
        // Heartbeat 负责节点心跳/命令链路，GatewayClusterCoordinator 只做轻量桥接。
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
        // gateway 控制面统一通过 GatewayCluster -> master 链路回传控制台日志，
        // 这里不再启动旧的 ConsolePush 长连接子进程，避免重复上报链路。
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
     * 当前子进程管理器只允许在 gateway 控制面运行。
     *
     * HTTP 直连模式已下线，继续在非 gateway 场景构造会让控制命令与
     * Runtime 队列回执链路失配，导致 reload/restart/upgrade 行为漂移。
     *
     * @return void
     */
    protected function assertGatewayControlPlaneRuntime(): void {
        if (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true) {
            throw new \RuntimeException('SubProcessManager 仅支持 gateway 控制面模式');
        }
    }

    /**
     * 标记当前子进程属于 gateway 控制面派生子进程。
     *
     * 相关逻辑在多个子进程入口都会读取该标记，统一收口成一个方法，
     * 避免每个入口都重复保留模式判断分支。
     *
     * @return void
     */
    protected function markGatewaySubProcessContext(): void {
        if (!defined('IS_GATEWAY_SUB_PROCESS')) {
            define('IS_GATEWAY_SUB_PROCESS', true);
        }
    }

    /**
     * 构建子进程运行类所需的回调依赖映射。
     *
     * SubProcessManager 只保留“进程编排/管理”职责，具体子进程运行逻辑拆到
     * `Server/SubProcess/*` 后，通过这张回调表注入必要的管理能力与公共工具。
     *
     * @return array<string, callable>
     */
    protected function subProcessRuntimeCallbacks(): array {
        return [
            'mark_gateway_sub_process_context' => fn() => $this->markGatewaySubProcessContext(),
            'touch_managed_heartbeat' => fn(string $runtimeKey, int $heartbeatAt, string $processName = '') => $this->touchManagedHeartbeat($runtimeKey, $heartbeatAt, $processName),
            'trace_heartbeat_step' => fn(string $step, float $startedAt, array $context = []) => $this->traceHeartbeatStep($step, $startedAt, $context),
            'update_gateway_cluster_trace_snapshot' => fn(string $step, array $context = []) => $this->updateGatewayClusterTraceSnapshot($step, $context),
            'send_gateway_pipe_message' => fn(string $event, array $data = []) => $this->sendGatewayPipeMessage($event, $data),
            'run_gateway_business_tick' => fn() => $this->runGatewayBusinessTick(),
            'run_gateway_health_tick' => fn() => $this->runGatewayHealthTick(),
            'run_gateway_business_command' => fn(string $command, array $params = [], string $source = 'pipe', string $token = '') => $this->runGatewayBusinessCommand($command, $params, $source, $token),
            'dequeue_gateway_business_runtime_command' => fn(): ?array => $this->dequeueGatewayBusinessRuntimeCommand(),
            'build_node_status_payload' => fn(Node $node): array => $this->buildNodeStatusPayload($node),
            'server_started_at' => fn(): int => $this->serverStartedAt(),
            'should_skip_heartbeat_status_build' => fn(): bool => $this->shouldSkipHeartbeatStatusBuild(),
            'read_file_watcher_meta' => fn(string $path): ?array => $this->readFileWatcherMeta($path),
            'should_restart_for_changed_files' => fn(array $files): bool => $this->shouldRestartForChangedFiles($files),
            'trigger_shutdown' => fn() => $this->triggerShutdown(),
            'trigger_restart' => fn() => $this->triggerRestart(),
            'trigger_reload' => fn() => $this->triggerReload(),
            'request_gateway_business_command' => fn(string $command, array $params = [], int $timeoutSeconds = 30): array => $this->requestGatewayBusinessCommand($command, $params, $timeoutSeconds),
            'restart_managed_processes' => fn(array $names = []): array => $this->restartManagedProcesses($names),
            'stop_managed_processes' => fn(array $names = []): array => $this->stopManagedProcesses($names),
            'restart_all_managed_processes' => fn(): array => $this->restartAllManagedProcesses(),
            'has_process' => fn(string $name): bool => $this->hasProcess($name),
            'should_push_managed_lifecycle_log' => fn(): bool => $this->shouldPushManagedLifecycleLog(),
            'exit_coroutine_runtime' => fn() => $this->exitCoroutineRuntime(),
        ];
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
                if (Runtime::instance()->serverIsReady() && App::isReady()) {
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
        $this->manualStoppedProcesses = [];
        $this->flushSubprocessControlState();
        try {
            if ($this->consolePushProcess) {
                try {
                    $consolePushPid = (int)($this->consolePushProcess->start() ?: 0);
                    if ($consolePushPid <= 0 || !@Process::kill($consolePushPid, 0)) {
                        Console::warning('【ConsolePush】子进程启动失败，后续仅保留本地日志输出');
                    }
                } catch (Throwable $throwable) {
                    Console::warning('【ConsolePush】子进程启动异常: ' . $throwable->getMessage());
                }
            }
            foreach ($this->processList as $name => $process) {
                /** @var Process $process */
                if (!$this->startManagedProcess($name, $process, false)) {
                    Console::warning("【{$name}】子进程首次拉起失败，进入保活重试");
                }
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

                if (!$shutdownRequested) {
                    // wait(false) 只能回收“已经退出并被内核上报”的子进程。这里额外做一层保活巡检：
                    // 1) 首次 start 失败或 pid 丢失时，主动补拉；
                    // 2) 进程存活但心跳长期不更新时，视为卡死并回收重拉。
                    $this->reconcileManagedProcessesHealth();
                }

                if (!Runtime::instance()->serverIsAlive()) {
                    if (!$shutdownDispatched) {
                        // Gateway restart 场景里 serverIsAlive 可能先被置为 false，再触发
                        // manager pipe 的 shutdown_processes。若这里不主动关停一次子进程，
                        // 会直接落入“被动等待超时 -> SIGTERM”路径，形成关停卡顿。
                        $this->shutdownManagedProcessesDirect(true);
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
                            $this->shutdownManagedProcessesDirect(true);
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
                        if ($this->isManagedProcessManuallyStopped($oldProcessName)) {
                            $this->clearManagedProcessRuntimeState($oldProcessName);
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
            $this->manualStoppedProcesses = [];
            $this->flushSubprocessControlState();
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
            case 'restart_named_processes':
                $restartTargets = $this->sanitizeManagedProcessTargets((array)($params['targets'] ?? []));
                if (!$restartTargets) {
                    return false;
                }
                $this->restartManagedProcessesDirect($restartTargets);
                return false;
            case 'stop_named_processes':
                $stopTargets = $this->sanitizeManagedProcessTargets((array)($params['targets'] ?? []));
                if (!$stopTargets) {
                    return false;
                }
                $this->stopManagedProcessesDirect($stopTargets);
                return false;
            case 'shutdown_processes':
                $this->shutdownManagedProcessesDirect((bool)($params['graceful_business'] ?? false));
                return true;
            default:
                return false;
        }
    }

    /**
     * 归一化并过滤子进程控制目标。
     *
     * 子进程控制命令来自 dashboard/cluster socket，必须做白名单过滤，
     * 避免误传不存在的进程名导致 manager 状态漂移。
     *
     * @param array<int, mixed> $targets 原始目标列表
     * @param bool $controllableOnly 是否限制为可控制进程（排除根 manager）
     * @return array<int, string>
     */
    protected function sanitizeManagedProcessTargets(array $targets, bool $controllableOnly = false): array {
        $lookup = [];
        foreach ($targets as $target) {
            if (!is_string($target)) {
                continue;
            }
            $name = trim($target);
            if ($name === '') {
                continue;
            }
            if (!$this->hasProcess($name)) {
                continue;
            }
            if ($controllableOnly && !in_array($name, $this->controllableManagedProcesses(), true)) {
                continue;
            }
            $lookup[$name] = true;
        }
        return array_keys($lookup);
    }

    /**
     * 返回允许 dashboard 执行 stop/restart 的受管子进程名。
     *
     * 控制面根进程 SubProcessManager 自身不在可控名单里，避免误操作导致
     * 命令通道中断；其余 addProcess 子进程均可被显式停启用于排障。
     *
     * @return array<int, string>
     */
    protected function controllableManagedProcesses(): array {
        $names = [];
        foreach (array_keys($this->processList) as $name) {
            $names[] = $name;
        }
        return $names;
    }

    /**
     * 在 manager 进程内执行“重启指定子进程”。
     *
     * 重启语义分两步：
     * 1. 清除手动停用标记，允许后续自动拉起；
     * 2. 对存活进程发送 SIGTERM，wait(false) 分支会自动重拉。
     *
     * @param array<int, string> $targets 目标子进程名
     * @return void
     */
    protected function restartManagedProcessesDirect(array $targets): void {
        foreach ($targets as $name) {
            $this->setManagedProcessManualStopped($name, false);
            $process = $this->processList[$name] ?? null;
            if (!$process instanceof Process) {
                continue;
            }
            $pid = (int)($process->pid ?? 0);
            if ($pid > 0 && @Process::kill($pid, 0)) {
                @Process::kill($pid, SIGTERM);
                continue;
            }
            $this->recreateManagedProcess($name);
        }
    }

    /**
     * 在 manager 进程内执行“停止指定子进程”。
     *
     * 停止语义会先设置手动停用标记，再触发进程退出。这样 wait(false) 与
     * 健康巡检分支都不会把该进程自动拉回。
     *
     * @param array<int, string> $targets 目标子进程名
     * @return void
     */
    protected function stopManagedProcessesDirect(array $targets): void {
        foreach ($targets as $name) {
            $this->setManagedProcessManualStopped($name, true);
            $process = $this->processList[$name] ?? null;
            if (!$process instanceof Process) {
                continue;
            }
            $pid = (int)($process->pid ?? 0);
            if ($pid > 0 && @Process::kill($pid, 0)) {
                @Process::kill($pid, SIGTERM);
            }
        }
    }

    /**
     * 设置单个子进程的手动停用状态，并同步到 Runtime。
     *
     * @param string $name 子进程名
     * @param bool $stopped 是否手动停用
     * @return void
     */
    protected function setManagedProcessManualStopped(string $name, bool $stopped): void {
        if ($stopped) {
            $this->manualStoppedProcesses[$name] = true;
        } else {
            unset($this->manualStoppedProcesses[$name]);
        }
        $this->flushSubprocessControlState();
    }

    /**
     * 判断指定子进程是否处于“手动停用”状态。
     *
     * @param string $name 子进程名
     * @return bool
     */
    protected function isManagedProcessManuallyStopped(string $name): bool {
        return isset($this->manualStoppedProcesses[$name]);
    }

    /**
     * 将当前手动停用状态刷新到 Runtime，供 dashboard 节点状态展示使用。
     *
     * @return void
     */
    protected function flushSubprocessControlState(): void {
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_CONTROL_STATE, [
            'manual_stopped' => array_values(array_keys($this->manualStoppedProcesses)),
            'updated_at' => time(),
        ]);
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
        if (!$this->startManagedProcess($name, $newProcess, true)) {
            $this->clearManagedProcessRuntimeState($name);
            return false;
        }
        return true;
    }

    /**
     * 启动一个受管子进程并注册 pid 映射。
     *
     * 这里必须显式校验 `Process::start()` 的返回值。若启动失败却继续把对象留在
     * processList，wait(false) 不会再收到该 pid 的退出事件，子进程就会永久缺席。
     *
     * @param string $name 子进程名
     * @param Process $process 子进程对象
     * @param bool $recreated 是否属于重拉场景
     * @return bool
     */
    protected function startManagedProcess(string $name, Process $process, bool $recreated): bool {
        try {
            $startedPid = (int)($process->start() ?: 0);
        } catch (Throwable $throwable) {
            Console::warning("【{$name}】子进程启动异常: " . $throwable->getMessage());
            return false;
        }
        $pid = $startedPid > 0 ? $startedPid : (int)($process->pid ?? 0);
        if ($pid <= 0 || !@Process::kill($pid, 0)) {
            Console::warning("【{$name}】子进程启动失败: pid=0");
            return false;
        }
        $this->pidList[$pid] = $name;
        unset($this->processLastRespawnAttemptAt[$name], $this->processLastStaleHandleAt[$name]);
        if ($recreated) {
            Console::warning("【{$name}】子进程已重拉，PID:{$pid}");
        }
        return true;
    }

    /**
     * 周期巡检受管子进程健康状态。
     *
     * manager 不仅要依赖 wait(false) 的退出事件，还要处理两类现实故障：
     * 1) start 失败导致 pid=0；
     * 2) 进程仍存活但业务循环卡死（心跳不再推进）。
     *
     * @return void
     */
    protected function reconcileManagedProcessesHealth(): void {
        $now = time();
        foreach (array_keys($this->processList) as $name) {
            if ($this->isManagedProcessManuallyStopped($name)) {
                continue;
            }
            /** @var Process|null $process */
            $process = $this->processList[$name] ?? null;
            if (!$process instanceof Process) {
                continue;
            }
            $pid = (int)($process->pid ?? 0);
            if ($pid <= 0 || !@Process::kill($pid, 0)) {
                if ($pid > 0) {
                    unset($this->pidList[$pid]);
                }
                $lastAttemptAt = (int)($this->processLastRespawnAttemptAt[$name] ?? 0);
                if (($now - $lastAttemptAt) < self::PROCESS_RESPAWN_RETRY_SECONDS) {
                    continue;
                }
                $this->processLastRespawnAttemptAt[$name] = $now;
                Console::warning("【{$name}】检测到子进程不在线，准备重拉");
                if (!$this->recreateManagedProcess($name)) {
                    Console::warning("【{$name}】子进程重拉失败，等待下轮重试");
                }
                continue;
            }

            $heartbeatKey = $this->managedProcessHeartbeatRuntimeKey($name);
            if ($heartbeatKey === '') {
                continue;
            }
            $runtimeHeartbeatAt = (int)(Runtime::instance()->get($heartbeatKey) ?? 0);
            $fallbackHeartbeatAt = (int)(Counter::instance()->get($this->heartbeatCounterKey($heartbeatKey)) ?? 0);
            $heartbeatAt = max($runtimeHeartbeatAt, $fallbackHeartbeatAt);
            if ($heartbeatAt <= 0) {
                continue;
            }
            $staleSeconds = $now - $heartbeatAt;
            if ($staleSeconds <= self::PROCESS_HEARTBEAT_STALE_SECONDS) {
                continue;
            }
            if (!Runtime::instance()->serverIsReady() || Runtime::instance()->serverIsDraining()) {
                continue;
            }
            $lastHandledAt = (int)($this->processLastStaleHandleAt[$name] ?? 0);
            if (($now - $lastHandledAt) < self::PROCESS_HEARTBEAT_HANDLE_COOLDOWN_SECONDS) {
                continue;
            }
            $this->processLastStaleHandleAt[$name] = $now;
            $this->dumpManagedProcessTimeoutTrace($name, $staleSeconds);
            Console::warning("【{$name}】子进程心跳超时({$staleSeconds}s), runtime={$runtimeHeartbeatAt}, counter={$fallbackHeartbeatAt}, pid={$pid}，发送 SIGTERM 回收");
            @Process::kill($pid, SIGTERM);
            if ($staleSeconds >= self::PROCESS_HEARTBEAT_FORCE_KILL_SECONDS) {
                usleep(200000);
                if (@Process::kill($pid, 0)) {
                    Console::warning("【{$name}】子进程心跳持续超时({$staleSeconds}s)，升级 SIGKILL");
                    @Process::kill($pid, SIGKILL);
                }
            }
        }
    }

    /**
     * 返回受管子进程对应的 runtime 心跳 key。
     *
     * @param string $name
     * @return string
     */
    protected function managedProcessHeartbeatRuntimeKey(string $name): string {
        return match ($name) {
            'GatewayClusterCoordinator' => Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT,
            'GatewayBusinessCoordinator' => Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT,
            'GatewayHealthMonitor' => Key::RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT,
            'MemoryUsageCount' => Key::RUNTIME_MEMORY_MONITOR_HEARTBEAT_AT,
            'Heartbeat' => Key::RUNTIME_HEARTBEAT_PROCESS_HEARTBEAT_AT,
            'LogBackup' => Key::RUNTIME_LOG_BACKUP_HEARTBEAT_AT,
            'CrontabManager' => Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT,
            'RedisQueue' => Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT,
            'FileWatch' => Key::RUNTIME_FILE_WATCHER_HEARTBEAT_AT,
            default => '',
        };
    }

    /**
     * 将子进程心跳写入 Runtime，并同步写入 Counter 兜底槽位。
     *
     * Runtime 行数较小且承载大量动态 key，极端情况下可能出现写入失败。
     * 心跳链路额外写一份 Counter，可以避免“进程还活着但 Runtime 心跳断更”被误判超时。
     *
     * @param string $runtimeKey Runtime 心跳 key
     * @param int $heartbeatAt 心跳时间戳
     * @param string $processName 进程名（仅用于日志）
     * @return void
     */
    protected function touchManagedHeartbeat(string $runtimeKey, int $heartbeatAt, string $processName = ''): void {
        $written = Runtime::instance()->set($runtimeKey, $heartbeatAt);
        Counter::instance()->set($this->heartbeatCounterKey($runtimeKey), $heartbeatAt);
        if ($written) {
            unset($this->heartbeatWriteFailWarnAt[$runtimeKey]);
            return;
        }
        $now = time();
        $lastWarnAt = (int)($this->heartbeatWriteFailWarnAt[$runtimeKey] ?? 0);
        if (($now - $lastWarnAt) < self::HEARTBEAT_WRITE_FAIL_WARNING_INTERVAL_SECONDS) {
            return;
        }
        $this->heartbeatWriteFailWarnAt[$runtimeKey] = $now;
        $label = $processName !== '' ? $processName : $runtimeKey;
        Console::warning("【{$label}】Runtime 心跳写入失败，已回退 Counter 兜底");
    }

    /**
     * 读取子进程心跳时间戳（Runtime 主读 + Counter 兜底）。
     *
     * @param string $runtimeKey Runtime 心跳 key
     * @return int
     */
    protected function managedHeartbeatAt(string $runtimeKey): int {
        $runtimeHeartbeat = (int)(Runtime::instance()->get($runtimeKey) ?? 0);
        $fallbackHeartbeat = (int)(Counter::instance()->get($this->heartbeatCounterKey($runtimeKey)) ?? 0);
        return max($runtimeHeartbeat, $fallbackHeartbeat);
    }

    /**
     * 生成心跳在 Counter 表中的兜底 key。
     *
     * @param string $runtimeKey Runtime 心跳 key
     * @return string
     */
    protected function heartbeatCounterKey(string $runtimeKey): string {
        return self::HEARTBEAT_COUNTER_PREFIX . md5($runtimeKey);
    }

    /**
     * 临时输出心跳链路步骤耗时。
     *
     * 该日志用于排查“子进程心跳超时”时，定位单轮循环到底阻塞在哪个步骤。
     * 默认仅在临时排障期开启，问题确认后应关闭或移除。
     *
     * @param string $step 步骤名
     * @param float $startedAt 步骤开始时间（microtime(true)）
     * @param array<string, scalar|null> $context 追加上下文字段
     * @return void
     */
    protected function traceHeartbeatStep(string $step, float $startedAt, array $context = []): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $elapsedMs = round((microtime(true) - $startedAt) * 1000, 2);
        $parts = [];
        foreach ($context as $key => $value) {
            if (is_scalar($value) || $value === null) {
                $parts[] = $key . '=' . (is_null($value) ? 'null' : (string)$value);
            }
        }
        $suffix = $parts ? (', ' . implode(', ', $parts)) : '';
        Console::info("【HeartbeatTrace】{$step}, cost={$elapsedMs}ms{$suffix}", false);
    }

    /**
     * 在子进程心跳超时时输出对应进程的缓冲诊断信息。
     *
     * @param string $name 子进程名
     * @param int $staleSeconds 心跳过期秒数
     * @return void
     */
    protected function dumpManagedProcessTimeoutTrace(string $name, int $staleSeconds): void {
        if ($name === 'GatewayHealthMonitor') {
            $this->dumpGatewayHealthTimeoutTrace($staleSeconds);
            return;
        }
        if ($name === 'GatewayClusterCoordinator') {
            $this->dumpGatewayClusterTimeoutTrace($staleSeconds);
        }
    }

    /**
     * 输出 GatewayHealthMonitor 最近单轮探测缓冲明细。
     *
     * 缓冲快照由 GatewayServer 在 health 子进程内持续覆盖写入 Runtime。
     * 默认运行期不输出该明细，仅在心跳超时回收前一次性打印，避免常态日志噪声。
     *
     * @param int $staleSeconds 心跳过期秒数
     * @return void
     */
    protected function dumpGatewayHealthTimeoutTrace(int $staleSeconds): void {
        $snapshot = Runtime::instance()->get(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_TRACE_SNAPSHOT);
        if (!is_array($snapshot)) {
            Console::warning("【GatewayHealthTrace】心跳超时({$staleSeconds}s)，无可用轮次缓冲快照");
            return;
        }

        $entries = array_values(array_filter((array)($snapshot['entries'] ?? []), static function ($item): bool {
            return is_string($item) && trim($item) !== '';
        }));
        $updatedAt = (float)($snapshot['updated_at'] ?? 0);
        $roundStartedAt = (float)($snapshot['round_started_at'] ?? 0);
        $roundElapsedMs = (float)($snapshot['round_elapsed_ms'] ?? 0);
        $roundSequence = (int)($snapshot['round_sequence'] ?? 0);
        $activeRound = !empty($snapshot['active']) ? 1 : 0;
        $tracePid = (int)($snapshot['pid'] ?? 0);
        $now = microtime(true);
        $lastUpdateAgoMs = $updatedAt > 0 ? round(max(0, ($now - $updatedAt) * 1000), 2) : -1;
        $startedAgoMs = $roundStartedAt > 0 ? round(max(0, ($now - $roundStartedAt) * 1000), 2) : -1;

        Console::warning(
            "【GatewayHealthTrace】心跳超时({$staleSeconds}s)触发单轮缓冲回放: "
            . "pid={$tracePid}, active={$activeRound}, round_sequence={$roundSequence}, "
            . "entries=" . count($entries) . ", round_elapsed_ms={$roundElapsedMs}, "
            . "started_ago_ms={$startedAgoMs}, last_update_ago_ms={$lastUpdateAgoMs}"
        );

        if (!$entries) {
            Console::warning('【GatewayHealthTrace】单轮缓冲为空');
            return;
        }
        foreach ($entries as $index => $line) {
            $no = $index + 1;
            Console::warning("【GatewayHealthTrace】[{$no}] {$line}");
        }
    }

    /**
     * 输出 GatewayClusterCoordinator 最近一次循环步骤快照。
     *
     * @param int $staleSeconds 心跳过期秒数
     * @return void
     */
    protected function dumpGatewayClusterTimeoutTrace(int $staleSeconds): void {
        $snapshot = Runtime::instance()->get(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_TRACE_SNAPSHOT);
        if (!is_array($snapshot)) {
            Console::warning("【GatewayClusterTrace】心跳超时({$staleSeconds}s)，无可用步骤快照");
            return;
        }

        $step = trim((string)($snapshot['step'] ?? ''));
        $tracePid = (int)($snapshot['pid'] ?? 0);
        $updatedAt = (float)($snapshot['updated_at'] ?? 0);
        $now = microtime(true);
        $lastUpdateAgoMs = $updatedAt > 0 ? round(max(0, ($now - $updatedAt) * 1000), 2) : -1;
        $context = (array)($snapshot['context'] ?? []);
        $parts = [];
        foreach ($context as $key => $value) {
            if (is_scalar($value) || $value === null) {
                $parts[] = $key . '=' . (is_null($value) ? 'null' : (string)$value);
            }
        }
        $contextLine = $parts ? implode(', ', $parts) : '--';
        Console::warning(
            "【GatewayClusterTrace】心跳超时({$staleSeconds}s)最近步骤: "
            . "pid={$tracePid}, step=" . ($step !== '' ? $step : '--')
            . ", last_update_ago_ms={$lastUpdateAgoMs}, context={$contextLine}"
        );
    }

    /**
     * 更新 GatewayClusterCoordinator 的最近步骤快照。
     *
     * 该快照默认不主动打印，仅在心跳超时回收前由 manager 读取并输出，
     * 用于定位 cluster 循环卡在了哪一步。
     *
     * @param string $step 当前步骤
     * @param array<string, scalar|null> $context 追加上下文
     * @return void
     */
    protected function updateGatewayClusterTraceSnapshot(string $step, array $context = []): void {
        $normalizedContext = [];
        foreach ($context as $key => $value) {
            if (is_scalar($value) || $value === null) {
                $normalizedContext[$key] = $value;
            }
        }
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_TRACE_SNAPSHOT, [
            'pid' => (int)(getmypid() ?: 0),
            'step' => $step,
            'updated_at' => round(microtime(true), 6),
            'context' => $normalizedContext,
        ]);
    }

    /**
     * 清理指定子进程的 runtime pid/heartbeat 状态，避免残留值误导后续健康判定。
     *
     * @param string $name 子进程名
     * @return void
     */
    protected function clearManagedProcessRuntimeState(string $name): void {
        $pidKey = match ($name) {
            'GatewayClusterCoordinator' => Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID,
            'GatewayBusinessCoordinator' => Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID,
            'GatewayHealthMonitor' => Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID,
            'MemoryUsageCount' => Key::RUNTIME_MEMORY_MONITOR_PID,
            'Heartbeat' => Key::RUNTIME_HEARTBEAT_PID,
            'LogBackup' => Key::RUNTIME_LOG_BACKUP_PID,
            'CrontabManager' => Key::RUNTIME_CRONTAB_MANAGER_PID,
            'RedisQueue' => Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
            'FileWatch' => Key::RUNTIME_FILE_WATCHER_PID,
            default => '',
        };
        $heartbeatKey = $this->managedProcessHeartbeatRuntimeKey($name);
        $pidKey !== '' && Runtime::instance()->set($pidKey, 0);
        if ($heartbeatKey !== '') {
            Runtime::instance()->set($heartbeatKey, 0);
            Counter::instance()->set($this->heartbeatCounterKey($heartbeatKey), 0);
        }
        if ($name === 'GatewayHealthMonitor') {
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_TRACE_SNAPSHOT, '');
        } elseif ($name === 'GatewayClusterCoordinator') {
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_TRACE_SNAPSHOT, '');
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
        return self::PROCESS_EXIT_GRACE_SECONDS;
    }

    protected function triggerShutdown(): void {
        if ($this->sendGatewayPipeMessage('gateway_control_shutdown', [
            'preserve_managed_upstreams' => false,
        ])) {
            return;
        }
        if (is_callable($this->shutdownHandler)) {
            ($this->shutdownHandler)();
            return;
        }
        Http::instance()->shutdown();
    }

    protected function triggerReload(): void {
        if ($this->sendGatewayPipeMessage('gateway_control_reload')) {
            return;
        }
        if (is_callable($this->reloadHandler)) {
            ($this->reloadHandler)();
            return;
        }
        Http::instance()->reload();
    }

    protected function triggerRestart(): void {
        if ($this->sendGatewayPipeMessage('gateway_control_restart', [
            'preserve_managed_upstreams' => false,
        ])) {
            return;
        }
        if (is_callable($this->restartHandler)) {
            ($this->restartHandler)();
            return;
        }
        $this->triggerShutdown();
    }

    protected function buildNodeStatusPayload(Node $node): array {
        $status = $node->asArray();
        $subprocesses = $this->managedProcessDashboardSnapshot();
        $status['subprocesses'] = $subprocesses;
        $status['subprocess_signature'] = (string)($subprocesses['signature'] ?? '');
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
     * 通过 Runtime 队列向 GatewayBusinessCoordinator 投递命令并等待结果。
     *
     * 该入口供 heartbeat/cluster 等子进程使用，避免再绕经本机 HTTP 控制面。
     * 命令写入 `RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE`，结果从
     * `gateway_business_result:*` 回收，和 dashboard worker 侧保持同一语义。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @param int $timeoutSeconds 最长等待秒数
     * @return array<string, mixed>
     */
    protected function requestGatewayBusinessCommand(string $command, array $params = [], int $timeoutSeconds = 30): array {
        $command = trim($command);
        if ($command === '') {
            return [
                'ok' => false,
                'message' => 'Gateway 业务编排命令不能为空',
                'data' => [],
                'updated_at' => time(),
            ];
        }
        if (!$this->hasProcess('GatewayBusinessCoordinator')) {
            return [
                'ok' => false,
                'message' => 'Gateway 业务编排子进程未启用',
                'data' => [],
                'updated_at' => time(),
            ];
        }

        $requestId = uniqid('gateway_business_', true);
        $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
        Runtime::instance()->delete($resultKey);
        $params['request_id'] = $requestId;
        $token = $this->enqueueGatewayBusinessRuntimeCommand($command, $params);
        if ($token === false) {
            Runtime::instance()->delete($resultKey);
            return [
                'ok' => false,
                'message' => 'Gateway 业务编排命令入队失败',
                'data' => [],
                'updated_at' => time(),
            ];
        }

        return $this->waitGatewayBusinessRuntimeResult($requestId, $timeoutSeconds);
    }

    /**
     * 将业务编排命令写入 Runtime 队列，等待 GatewayBusinessCoordinator 拉取消费。
     *
     * 队列是低频控制面通道，保留最近 32 条足够覆盖升级/重载场景，同时限制
     * Runtime 表值体积，避免命令暴涨时影响其它运行态键写入。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return string|false 写入成功返回 token
     */
    protected function enqueueGatewayBusinessRuntimeCommand(string $command, array $params = []): string|false {
        $token = uniqid('gateway_business_runtime_', true);
        $queue = Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE);
        $items = (is_array($queue) && is_array($queue['items'] ?? null)) ? (array)$queue['items'] : [];
        $items[] = [
            'token' => $token,
            'command' => $command,
            'params' => $params,
            'queued_at' => time(),
        ];
        if (count($items) > 32) {
            $items = array_slice($items, -32);
        }
        $written = Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE, [
            'items' => array_values($items),
            'updated_at' => time(),
        ]);
        if (!$written) {
            Console::error(
                "【GatewayBusiness】命令入队失败: command={$command}, token={$token}, runtime_count="
                . Runtime::instance()->count() . ", runtime_size=" . Runtime::instance()->size() . ", queued_items=" . count($items)
            );
            return false;
        }
        return $token;
    }

    /**
     * 等待指定业务编排命令的 Runtime 结果回执。
     *
     * @param string $requestId 命令请求 id
     * @param int $timeoutSeconds 最长等待秒数
     * @return array<string, mixed>
     */
    protected function waitGatewayBusinessRuntimeResult(string $requestId, int $timeoutSeconds = 30): array {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
        while (microtime(true) < $deadline) {
            $payload = Runtime::instance()->get($resultKey);
            if (is_array($payload) && array_key_exists('ok', $payload)) {
                Runtime::instance()->delete($resultKey);
                return [
                    'ok' => (bool)($payload['ok'] ?? false),
                    'message' => (string)($payload['message'] ?? (!empty($payload['ok']) ? 'success' : 'failed')),
                    'data' => (array)($payload['data'] ?? []),
                    'updated_at' => (int)($payload['updated_at'] ?? time()),
                ];
            }
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(0.1);
            } else {
                usleep(100000);
            }
        }

        Runtime::instance()->delete($resultKey);
        return [
            'ok' => false,
            'message' => 'Gateway 业务编排子进程执行超时',
            'data' => [],
            'updated_at' => time(),
        ];
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
        return (new GatewayClusterCoordinatorProcess($this->subProcessRuntimeCallbacks()))->create();
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
     * @return bool 是否成功投递到 worker pipe
     */
    protected function sendGatewayPipeMessage(string $event, array $data = []): bool {
        try {
            return (bool)$this->server->sendMessage(JsonHelper::toJson([
                'event' => $event,
                'data' => $data,
            ]), 0);
        } catch (Throwable) {
            return false;
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
        return (new GatewayBusinessCoordinatorProcess($this->subProcessRuntimeCallbacks()))->create();
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
        return (new GatewayHealthMonitorProcess($this->subProcessRuntimeCallbacks()))->create();
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
            $pid = (int)($process->pid ?? 0);
            if ($pid > 0) {
                @Process::kill($pid, SIGKILL);
                continue;
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

    /**
     * 重启指定子进程。
     *
     * dashboard/cluster 命令都走这条入口，统一通过 manager pipe 下发，避免
     * worker 持有旧子进程句柄时把命令写到失效 pipe。
     *
     * @param array<int, string> $targets 目标子进程名
     * @return array{ok:bool,message:string,targets:array<int,string>}
     */
    public function restartManagedProcesses(array $targets): array {
        $targets = $this->sanitizeManagedProcessTargets($targets, true);
        if (!$targets) {
            return [
                'ok' => false,
                'message' => '未匹配到可重启的子进程',
                'targets' => [],
            ];
        }
        if (!$this->managerProcessAvailableForDispatch()) {
            return [
                'ok' => false,
                'message' => '子进程管理通道不可用，请稍后重试',
                'targets' => $targets,
            ];
        }
        $ok = $this->dispatchManagerProcessCommand('restart_named_processes', ['targets' => $targets]);
        return [
            'ok' => $ok,
            'message' => $ok
                ? ('子进程重启指令已投递: ' . implode(',', $targets))
                : ('子进程重启指令投递失败: ' . implode(',', $targets)),
            'targets' => $targets,
        ];
    }

    /**
     * 停止指定子进程。
     *
     * stop 与 restart 不同：stop 会写入“手动停用”标记，后续健康巡检不会自动重拉。
     *
     * @param array<int, string> $targets 目标子进程名
     * @return array{ok:bool,message:string,targets:array<int,string>}
     */
    public function stopManagedProcesses(array $targets): array {
        $targets = $this->sanitizeManagedProcessTargets($targets, true);
        if (!$targets) {
            return [
                'ok' => false,
                'message' => '未匹配到可停止的子进程',
                'targets' => [],
            ];
        }
        if (!$this->managerProcessAvailableForDispatch()) {
            return [
                'ok' => false,
                'message' => '子进程管理通道不可用，请稍后重试',
                'targets' => $targets,
            ];
        }
        $ok = $this->dispatchManagerProcessCommand('stop_named_processes', ['targets' => $targets]);
        return [
            'ok' => $ok,
            'message' => $ok
                ? ('子进程停止指令已投递: ' . implode(',', $targets))
                : ('子进程停止指令投递失败: ' . implode(',', $targets)),
            'targets' => $targets,
        ];
    }

    /**
     * 一键重启当前节点全部可控子进程。
     *
     * @return array{ok:bool,message:string,targets:array<int,string>}
     */
    public function restartAllManagedProcesses(): array {
        return $this->restartManagedProcesses($this->controllableManagedProcesses());
    }

    /**
     * 生成 dashboard 节点页所需的子进程运行态快照。
     *
     * @return array{
     *     signature:string,
     *     updated_at:int,
     *     summary:array<string,int>,
     *     items:array<int,array<string,mixed>>
     * }
     */
    public function managedProcessDashboardSnapshot(): array {
        $now = time();
        $state = Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_CONTROL_STATE);
        $manualStoppedLookup = [];
        if (is_array($state)) {
            foreach ((array)($state['manual_stopped'] ?? []) as $name) {
                if (is_string($name) && $name !== '') {
                    $manualStoppedLookup[$name] = true;
                }
            }
        }

        $items = [];
        $summary = [
            'total' => 0,
            'running' => 0,
            'stale' => 0,
            'offline' => 0,
            'stopped' => 0,
        ];
        foreach ($this->managedProcessDashboardDefinitions() as $definition) {
            $name = (string)$definition['name'];
            $enabled = (bool)$definition['enabled'];
            if (!$enabled) {
                continue;
            }
            $pid = (int)(Runtime::instance()->get((string)$definition['pid_key']) ?? 0);
            $lastActiveAt = $this->managedHeartbeatAt((string)$definition['heartbeat_key']);
            $alive = $pid > 0 && @Process::kill($pid, 0);
            $activeAge = $lastActiveAt > 0 ? max(0, $now - $lastActiveAt) : -1;
            $staleAfter = (int)$definition['stale_after'];
            $manualStopped = isset($manualStoppedLookup[$name]);
            $stale = !$manualStopped && $alive && $lastActiveAt > 0 && $activeAge > $staleAfter;

            $status = 'running';
            if ($manualStopped) {
                $status = $alive ? 'stopping' : 'stopped';
            } elseif (!$alive) {
                $status = 'offline';
            } elseif ($stale) {
                $status = 'stale';
            }

            $summary['total']++;
            if ($status === 'running') {
                $summary['running']++;
            } elseif ($status === 'stale') {
                $summary['stale']++;
            } elseif ($status === 'offline') {
                $summary['offline']++;
            } elseif ($status === 'stopped' || $status === 'stopping') {
                $summary['stopped']++;
            }

            $items[] = [
                'name' => $name,
                'label' => (string)$definition['label'],
                'pid' => $pid,
                'status' => $status,
                'enabled' => true,
                'alive' => $alive,
                'manual_stopped' => $manualStopped,
                'last_active_at' => $lastActiveAt,
                'last_active_age' => $activeAge,
                'stale_after' => $staleAfter,
                'restart_supported' => (bool)$definition['restart_supported'],
                'stop_supported' => (bool)$definition['stop_supported'],
                'generation' => $this->managedProcessGeneration($name),
            ];
        }

        $signaturePayload = array_map(static function (array $item): array {
            return [
                'name' => $item['name'],
                'pid' => $item['pid'],
                'status' => $item['status'],
                'last_active_at' => $item['last_active_at'],
                'generation' => $item['generation'],
            ];
        }, $items);

        return [
            'signature' => substr(md5(JsonHelper::toJson($signaturePayload)), 0, 12),
            'updated_at' => $now,
            'summary' => $summary,
            'items' => $items,
        ];
    }

    /**
     * 返回 dashboard 子进程展示的静态定义。
     *
     * @return array<int, array<string, mixed>>
     */
    protected function managedProcessDashboardDefinitions(): array {
        $definitions = [
            [
                'name' => 'SubProcessManager',
                'label' => 'SubProcessManager',
                'pid_key' => Key::RUNTIME_SUBPROCESS_MANAGER_PID,
                'heartbeat_key' => Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT,
                'stale_after' => self::MANAGER_HEARTBEAT_SHUTDOWN_STALE_GRACE_SECONDS,
                'enabled' => true,
                'restart_supported' => false,
                'stop_supported' => false,
            ],
            [
                'name' => 'GatewayClusterCoordinator',
                'label' => 'GatewayCluster',
                'pid_key' => Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID,
                'heartbeat_key' => Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('GatewayClusterCoordinator'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'GatewayBusinessCoordinator',
                'label' => 'GatewayBusiness',
                'pid_key' => Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID,
                'heartbeat_key' => Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('GatewayBusinessCoordinator'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'GatewayHealthMonitor',
                'label' => 'GatewayHealth',
                'pid_key' => Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID,
                'heartbeat_key' => Key::RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('GatewayHealthMonitor'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'MemoryUsageCount',
                'label' => 'MemoryMonitor',
                'pid_key' => Key::RUNTIME_MEMORY_MONITOR_PID,
                'heartbeat_key' => Key::RUNTIME_MEMORY_MONITOR_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('MemoryUsageCount'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'Heartbeat',
                'label' => 'Heartbeat',
                'pid_key' => Key::RUNTIME_HEARTBEAT_PID,
                'heartbeat_key' => Key::RUNTIME_HEARTBEAT_PROCESS_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('Heartbeat'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'LogBackup',
                'label' => 'LogBackup',
                'pid_key' => Key::RUNTIME_LOG_BACKUP_PID,
                'heartbeat_key' => Key::RUNTIME_LOG_BACKUP_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('LogBackup'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'CrontabManager',
                'label' => 'Crontab',
                'pid_key' => Key::RUNTIME_CRONTAB_MANAGER_PID,
                'heartbeat_key' => Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('CrontabManager'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'RedisQueue',
                'label' => 'RedisQueue',
                'pid_key' => Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
                'heartbeat_key' => Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('RedisQueue'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
            [
                'name' => 'FileWatch',
                'label' => 'FileWatcher',
                'pid_key' => Key::RUNTIME_FILE_WATCHER_PID,
                'heartbeat_key' => Key::RUNTIME_FILE_WATCHER_HEARTBEAT_AT,
                'stale_after' => self::PROCESS_HEARTBEAT_STALE_SECONDS,
                'enabled' => $this->hasProcess('FileWatch'),
                'restart_supported' => true,
                'stop_supported' => true,
            ],
        ];

        return array_values(array_filter($definitions, static fn(array $item): bool => (bool)($item['enabled'] ?? false)));
    }

    /**
     * 返回子进程代际编号，便于 dashboard 识别 manager 轮换。
     *
     * @param string $name 子进程名
     * @return int
     */
    protected function managedProcessGeneration(string $name): int {
        return match ($name) {
            'CrontabManager' => (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0),
            'RedisQueue' => (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0),
            default => 0,
        };
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
        return false;
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
        return (new ConsolePushProcess())->create();
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
        return (new RedisQueueProcess($this->subProcessRuntimeCallbacks()))->create();
    }

    /**
     * 排程任务
     * @return Process
     */
    private function createCrontabManagerProcess(): Process {
        return (new CrontabManagerProcess($this->subProcessRuntimeCallbacks()))->create();
    }


    /**
     * 内存占用统计
     * @return Process
     */
    private function createMemoryUsageCountProcess(): Process {
        return (new MemoryUsageCountProcess($this->subProcessRuntimeCallbacks()))->create();
    }

    /**
     * 心跳和状态推送
     * @return Process
     */
    private function createHeartbeatProcess(): Process {
        return (new HeartbeatProcess($this->subProcessRuntimeCallbacks()))
            ->create((int)($this->server->master_pid ?? 0), (int)($this->server->manager_pid ?? 0));
    }

    /**
     * 日志备份
     * @return Process
     */
    private function createLogBackupProcess(): Process {
        return (new LogBackupProcess($this->subProcessRuntimeCallbacks()))->create();
    }

    /**
     * 文件变更监听
     * @return Process
     */
    private function createFileWatchProcess(): Process {
        return (new FileWatchProcess($this->subProcessRuntimeCallbacks()))->create();
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
            if (str_contains($path, '/scf/src/Server/Gateway/')) {
                return true;
            }
        }
        return false;
    }

}
