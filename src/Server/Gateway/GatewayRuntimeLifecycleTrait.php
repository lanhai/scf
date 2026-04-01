<?php

namespace Scf\Server\Gateway;

use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Server as CoreServer;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Runtime;
use Scf\Root;
use Scf\Server\SubProcessManager;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Exception as SwooleException;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Throwable;

/**
 * Gateway 控制面运行时生命周期编排职责。
 *
 * 职责边界：
 * - 仅负责 Gateway 进程生命周期、lease 生命周期与基础运行时初始化；
 * - 不参与 dashboard 协议处理与业务指标聚合逻辑。
 *
 * 架构位置：
 * - 位于 GatewayServer 的“运行时总控”层，承接 start/stop/reload 相关底座能力；
 * - 为命令编排、cluster heartbeat 与 telemetry 提供稳定运行上下文。
 *
 * 设计意图：
 * - 把易产生副作用的生命周期控制从 GatewayServer 主类抽离，
 *   降低总控类体量并避免控制流与协议逻辑交织。
 */
trait GatewayRuntimeLifecycleTrait {
    /**
     * 初始化 Gateway 运行时共享状态与内存表。
     *
     * 该方法在启动早期执行，确保 worker/子进程 fork 之前完成共享表注册，
     * 避免不同进程各自懒加载导致运行态分叉。
     *
     * @return void
     */
    protected function bootstrapRuntimeState(): void {
        // Keep the same shared-table bootstrap order as Http::start(),
        // otherwise Gateway workers and subprocesses will each lazily
        // create private tables after fork and runtime state will drift.
        ATable::register([
            'Scf\Core\Table\PdoPoolTable',
            'Scf\Core\Table\LogTable',
            'Scf\Core\Table\Counter',
            'Scf\Core\Table\Runtime',
            'Scf\Core\Table\RouteTable',
            'Scf\Core\Table\RouteCache',
            'Scf\Core\Table\SocketRouteTable',
            'Scf\Core\Table\SocketConnectionTable',
            'Scf\Core\Table\CrontabTable',
            'Scf\Core\Table\MemoryMonitorTable',
            'Scf\Core\Table\ServerNodeTable',
            'Scf\Core\Table\ServerNodeStatusTable',
        ]);
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(false);
        Runtime::instance()->serverIsAlive(true);
        Runtime::instance()->httpPort($this->port);
        Runtime::instance()->rpcPort($this->rpcPort);
        Runtime::instance()->set(Key::RUNTIME_SERVER_STARTED_AT, $this->startedAt);
        Runtime::instance()->dashboardPort($this->resolvedDashboardPort());
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING, true);
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY, false);
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES);
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES);
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS);
        Runtime::instance()->delete('gateway_lease_override_state');
        ConsoleRelay::setGatewayPort($this->port);
        ConsoleRelay::setLocalSubscribed(false);
        ConsoleRelay::setRemoteSubscribed(false);
    }

    /**
     * 初始化并注册当前 Gateway owner lease。
     *
     * lease 是 managed upstream 生命周期协调的基线，epoch 无效时必须阻断启动。
     *
     * @return void
     */
    protected function bootstrapGatewayLease(): void {
        $serverConfig = Config::server();
        $this->gatewayLeaseTtlSeconds = max(3, (int)($serverConfig['gateway_lease_ttl'] ?? 15));
        $this->gatewayLeaseRestartTtlSeconds = max(3, (int)($serverConfig['gateway_lease_restart_ttl'] ?? 30));
        $this->gatewayLeaseGraceSeconds = max(3, (int)($serverConfig['gateway_lease_grace'] ?? 20));
        $this->gatewayLeaseRestartGraceSeconds = max(
            $this->gatewayLeaseGraceSeconds,
            (int)($serverConfig['gateway_lease_restart_grace'] ?? max(120, $this->gatewayLeaseGraceSeconds * 3))
        );
        $this->gatewayLeaseRestartReuseGraceSeconds = max(
            $this->gatewayLeaseGraceSeconds,
            (int)($serverConfig['gateway_lease_restart_reuse_grace'] ?? $this->gatewayLeaseRestartGraceSeconds)
        );
        $payload = GatewayLease::issueStartupLease($this->businessPort(), SERVER_ROLE, $this->gatewayLeaseTtlSeconds, [
            'gateway_pid' => getmypid(),
            'control_port' => $this->controlPort(),
            'rpc_port' => $this->rpcPort,
            'started_at' => $this->startedAt,
            'grace_seconds' => $this->gatewayLeaseGraceSeconds,
            'restart_grace_seconds' => $this->gatewayLeaseRestartGraceSeconds,
            'restart_reuse_grace' => $this->gatewayLeaseRestartReuseGraceSeconds,
        ]);
        $this->gatewayLeaseEpoch = (int)($payload['epoch'] ?? 0);
        $this->gatewayLeaseReusedEpoch = (bool)(($payload['meta']['reused_epoch'] ?? false) ?: false);
        $this->lastGatewayLeaseRenewAt = time();
        if ($this->gatewayLeaseEpoch <= 0) {
            throw new RuntimeException('gateway lease 初始化失败，owner epoch 无效');
        }
        if ($this->gatewayLeaseReusedEpoch) {
            Console::info("【Gateway】租约续接成功: epoch={$this->gatewayLeaseEpoch}, state=running(reused)");
            $this->reclaimStaleUpstreamProcessesByEpoch();
            return;
        }
        Console::info("【Gateway】租约初始化成功: epoch={$this->gatewayLeaseEpoch}, state=running");
        $this->reclaimStaleUpstreamProcessesByEpoch();
    }

    /**
     * 续租 Gateway owner lease。
     *
     * @param string $state 租约状态: running/restarting/stopped
     * @param bool $force 是否强制立即续租
     * @return void
     */
    protected function renewGatewayLease(string $state = 'running', bool $force = false): void {
        if ($this->gatewayLeaseEpoch <= 0 || $this->businessPort() <= 0) {
            return;
        }
        $state = in_array($state, ['running', 'restarting', 'stopped'], true) ? $state : 'running';
        $now = time();
        $minInterval = self::GATEWAY_LEASE_RENEW_INTERVAL_SECONDS;
        if (!$force && $state === 'running' && ($now - $this->lastGatewayLeaseRenewAt) < $minInterval) {
            return;
        }
        $ttlSeconds = match ($state) {
            'restarting' => $this->gatewayLeaseRestartTtlSeconds,
            'stopped' => 0,
            default => $this->gatewayLeaseTtlSeconds,
        };
        $leaseMeta = [
            'gateway_pid' => $this->serverMasterPid > 0 ? $this->serverMasterPid : getmypid(),
            'control_port' => $this->controlPort(),
            'rpc_port' => $this->rpcPort,
            'started_at' => $this->startedAt,
            'grace_seconds' => $this->gatewayLeaseGraceSeconds,
            'restart_grace_seconds' => $this->gatewayLeaseRestartGraceSeconds,
        ];
        if ($state === 'restarting') {
            // 计划重启时显式写入 restart_started_at，供 upstream watcher 区分
            // “正常重启恢复窗口”与“owner 真正失联”。
            $leaseMeta['restart_started_at'] = $now;
            $leaseMeta['restart_reuse_grace'] = $this->gatewayLeaseRestartReuseGraceSeconds;
        }
        $ok = GatewayLease::renewLease(
            $this->businessPort(),
            SERVER_ROLE,
            $this->gatewayLeaseEpoch,
            $state,
            $ttlSeconds,
            $leaseMeta
        );
        if ($ok) {
            $this->lastGatewayLeaseRenewAt = $now;
            return;
        }
        Console::warning(
            "【Gateway】租约续租失败(可能被新epoch接管): epoch={$this->gatewayLeaseEpoch}, state={$state}"
        );
    }

    /**
     * 启动租约续租定时器。
     *
     * @return void
     */
    protected function startGatewayLeaseRenewTimer(): void {
        if ($this->gatewayLeaseRenewTimerId !== null) {
            return;
        }
        $this->gatewayLeaseRenewTimerId = Timer::tick(self::GATEWAY_LEASE_RENEW_INTERVAL_SECONDS * 1000, function (int $timerId): void {
            $overrideState = (string)(Runtime::instance()->get('gateway_lease_override_state') ?? '');
            if ($overrideState !== '') {
                Timer::clear($timerId);
                $this->gatewayLeaseRenewTimerId = null;
                return;
            }
            if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                Timer::clear($timerId);
                $this->gatewayLeaseRenewTimerId = null;
                return;
            }
            $this->renewGatewayLease('running');
        });
    }

    protected function stopGatewayLeaseRenewTimer(): void {
        if ($this->gatewayLeaseRenewTimerId === null) {
            return;
        }
        Timer::clear($this->gatewayLeaseRenewTimerId);
        $this->gatewayLeaseRenewTimerId = null;
    }

    /**
     * 按当前关停策略持久化 lease 终态。
     *
     * @return void
     */
    protected function persistGatewayLeaseForShutdown(): void {
        $state = $this->preserveManagedUpstreamsOnShutdown ? 'restarting' : 'stopped';
        Runtime::instance()->set('gateway_lease_override_state', $state);
        $this->renewGatewayLease($state, true);
    }

    protected function gatewayLeaseEpoch(): int {
        return $this->gatewayLeaseEpoch;
    }

    /**
     * 启动阶段按 lease epoch 回收旧代 upstream 进程。
     *
     * @return void
     */
    protected function reclaimStaleUpstreamProcessesByEpoch(): void {
        $stale = $this->collectStaleUpstreamProcessesByEpoch();
        if (!$stale) {
            return;
        }

        $targets = array_map(static fn(array $item): int => (int)$item['pid'], $stale);
        $details = array_map(
            static fn(array $item): string => ((string)$item['pid']) . '(epoch=' . (string)$item['epoch'] . ')',
            $stale
        );
        Console::warning(
            '【Gateway】启动阶段回收旧代upstream进程: current_epoch=' . $this->gatewayLeaseEpoch
            . ', targets=' . implode(', ', $details)
        );

        foreach ($targets as $pid) {
            if ($pid > 0 && @Process::kill($pid, 0)) {
                @Process::kill($pid, SIGTERM);
            }
        }
        $remaining = $this->waitPidsExit($targets, 6) ? [] : $this->filterAlivePids($targets);
        if ($remaining) {
            Console::warning('【Gateway】旧代upstream进程未在预期时间内退出，升级为强制回收: pids=' . implode(', ', $remaining));
            foreach ($remaining as $pid) {
                if ($pid > 0 && @Process::kill($pid, 0)) {
                    @Process::kill($pid, SIGKILL);
                }
            }
            $this->waitPidsExit($remaining, 2);
        }
    }

    /**
     * 收集所有 owner epoch 落后于当前 Gateway 的 upstream 进程。
     *
     * @return array<int, array<string, int>>
     */
    protected function collectStaleUpstreamProcessesByEpoch(): array {
        if ($this->port <= 0 || $this->gatewayLeaseEpoch <= 0) {
            return [];
        }
        $output = @shell_exec('ps -axo pid=,command=');
        if (!is_string($output) || trim($output) === '') {
            return [];
        }

        $appFlag = '-app=' . APP_DIR_NAME;
        $gatewayPortFlag = '-gateway_port=' . $this->port;
        $stale = [];
        foreach (preg_split('/\r?\n/', $output) ?: [] as $line) {
            $line = trim((string)$line);
            if ($line === '' || !preg_match('/^(\d+)\s+(.+)$/', $line, $matches)) {
                continue;
            }
            $pid = (int)$matches[1];
            $command = (string)$matches[2];
            if ($pid <= 0 || $pid === getmypid()) {
                continue;
            }
            if (!str_contains($command, 'boot gateway_upstream start')) {
                continue;
            }
            if (!str_contains($command, $appFlag) || !str_contains($command, $gatewayPortFlag)) {
                continue;
            }
            $epoch = 0;
            if (preg_match('/(?:^|\s)-gateway_epoch=(\d+)(?:\s|$)/', $command, $epochMatch)) {
                $epoch = (int)($epochMatch[1] ?? 0);
            }
            if ($epoch <= 0 || $epoch !== $this->gatewayLeaseEpoch) {
                $stale[] = [
                    'pid' => $pid,
                    'epoch' => $epoch,
                ];
            }
        }

        usort($stale, static fn(array $a, array $b): int => ((int)$a['pid']) <=> ((int)$b['pid']));
        return $stale;
    }

    protected function filterAlivePids(array $pids): array {
        $alive = [];
        foreach ($pids as $pid) {
            $pid = (int)$pid;
            if ($pid > 0 && @Process::kill($pid, 0)) {
                $alive[$pid] = $pid;
            }
        }
        ksort($alive);
        return array_values($alive);
    }

    protected function waitPidsExit(array $pids, int $timeoutSeconds = 6, int $intervalMs = 200): bool {
        $targets = array_values(array_filter(array_map('intval', $pids), static fn(int $pid): bool => $pid > 0));
        if (!$targets) {
            return true;
        }
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            if (!$this->filterAlivePids($targets)) {
                return true;
            }
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(max(0.05, $intervalMs / 1000));
            } else {
                usleep(max(50, $intervalMs) * 1000);
            }
        }
        return !$this->filterAlivePids($targets);
    }

    protected function startupSummaryPending(): bool {
        return (bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false);
    }

    protected function recordStartupUpstreamSupervisorSync(int $pid, int $instanceCount): void {
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES, [
            'pid' => $pid,
            'instances' => $instanceCount,
        ]);
    }

    protected function recordStartupRemovedGenerations(array $generations): void {
        $normalized = array_values(array_unique(array_filter(array_map(
            static fn(mixed $generation): string => trim((string)$generation),
            $generations
        ))));
        if (!$normalized) {
            return;
        }
        $existing = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS) ?: []);
        $merged = array_values(array_unique(array_merge(
            array_values(array_filter(array_map(
                static fn(mixed $generation): string => trim((string)$generation),
                (array)($existing['generations'] ?? [])
            ))),
            $normalized
        )));
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS, [
            'count' => count($merged),
            'generations' => $merged,
        ]);
    }

    protected function recordStartupReadyInstance(array $plan): void {
        $version = trim((string)($plan['version'] ?? ''));
        $host = trim((string)($plan['host'] ?? '127.0.0.1'));
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            return;
        }
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $descriptor = $version . ' ' . $host . ':' . $port . ($rpcPort > 0 ? ', RPC:' . $rpcPort : '');
        $existing = array_values(array_filter(array_map(
            static fn(mixed $item): string => trim((string)$item),
            (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES) ?: [])
        )));
        if (in_array($descriptor, $existing, true)) {
            return;
        }
        $existing[] = $descriptor;
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES, array_values($existing));
    }

    protected function scheduleStartupInfoRender(): void {
        if ((bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY) ?? false)) {
            return;
        }
        $scheduledAt = microtime(true);
        Timer::tick(200, function (int $timerId) use ($scheduledAt): void {
            if (!Runtime::instance()->serverIsAlive()) {
                Timer::clear($timerId);
                return;
            }
            if ((bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY) ?? false)) {
                Timer::clear($timerId);
                return;
            }

            $resultRows = $this->startupResultRows();
            if (!$this->shouldRenderStartupInfo($resultRows, microtime(true) - $scheduledAt)) {
                return;
            }

            $this->renderStartupInfo($resultRows);
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING, false);
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY, true);
            Timer::clear($timerId);
        });
    }

    protected function shouldRenderStartupInfo(array $resultRows, float $elapsedSeconds): bool {
        $requiredKeys = [
            Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID,
            Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID,
            Key::RUNTIME_MEMORY_MONITOR_PID,
            Key::RUNTIME_HEARTBEAT_PID,
            Key::RUNTIME_LOG_BACKUP_PID,
        ];
        foreach ($requiredKeys as $key) {
            if ((int)(Runtime::instance()->get($key) ?? 0) <= 0) {
                return $elapsedSeconds >= 5.0;
            }
        }

        $supervisorSync = Runtime::instance()->get(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES);
        if ($supervisorSync === false || $supervisorSync === null) {
            return $elapsedSeconds >= 5.0;
        }

        if ($this->managedUpstreamPlans) {
            $readyInstances = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES) ?: []);
            if (!$readyInstances) {
                return $elapsedSeconds >= 5.0;
            }
        }

        if ((int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0) > 0
            && (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_WORKER_PID) ?? 0) <= 0
        ) {
            return $elapsedSeconds >= 5.0;
        }

        return !empty($resultRows);
    }

    protected function startupResultRows(): array {
        $pidRows = [
            '业务编排' => (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID) ?? 0),
            '健康检查' => (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID) ?? 0),
            '内存监控' => (int)(Runtime::instance()->get(Key::RUNTIME_MEMORY_MONITOR_PID) ?? 0),
            '心跳进程' => (int)(Runtime::instance()->get(Key::RUNTIME_HEARTBEAT_PID) ?? 0),
            '日志备份' => (int)(Runtime::instance()->get(Key::RUNTIME_LOG_BACKUP_PID) ?? 0),
            '队列管理' => (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0),
            '队列执行' => (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_WORKER_PID) ?? 0),
            '文件监听' => (int)(Runtime::instance()->get(Key::RUNTIME_FILE_WATCHER_PID) ?? 0),
            '集群协调' => (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID) ?? 0),
        ];
        $rows = [];
        foreach ($pidRows as $label => $pid) {
            if ($pid > 0) {
                $rows[] = ['label' => $label, 'value' => 'PID:' . $pid];
            }
        }

        $readyInstances = array_values(array_filter(array_map(
            static fn(mixed $item): string => trim((string)$item),
            (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES) ?: [])
        )));
        foreach ($readyInstances as $index => $descriptor) {
            $rows[] = [
                'label' => '业务实例' . ($index + 1),
                'value' => $descriptor,
            ];
        }

        $supervisorSync = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES) ?: []);
        if ($supervisorSync) {
            $rows[] = [
                'label' => 'Supervisor',
                'value' => 'Supervisor PID:' . (int)($supervisorSync['pid'] ?? 0) . ', 实例:' . (int)($supervisorSync['instances'] ?? 0),
            ];
        }

        $removedGenerations = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS) ?: []);
        $generationList = array_values(array_filter(array_map(
            static fn(mixed $generation): string => trim((string)$generation),
            (array)($removedGenerations['generations'] ?? [])
        )));
        if ($generationList) {
            $rows[] = [
                'label' => '代际清理',
                'value' => 'count=' . count($generationList) . ', versions=' . implode(' | ', $generationList),
            ];
        }

        return $rows;
    }

    protected function formatStartupResultRows(array $rows): array {
        return $this->formatAlignedPairs($rows);
    }

    protected function formatAlignedPairs(array $rows, ?int $labelWidth = null): array {
        unset($labelWidth);
        $formatted = [];
        foreach ($rows as $row) {
            $label = trim((string)($row['label'] ?? ''));
            $value = trim((string)($row['value'] ?? ''));
            if ($label === '' || $value === '') {
                continue;
            }
            $formatted[] = $label . '：' . $value;
        }
        return $formatted;
    }

    protected function displayTextWidth(string $text): int {
        if ($text === '') {
            return 0;
        }
        $width = 0;
        foreach (preg_split('//u', $text, -1, PREG_SPLIT_NO_EMPTY) ?: [] as $char) {
            $width += strlen($char) === 1 ? 1 : 2;
        }
        return $width;
    }

    protected function ensureLocalIpcServerStarted(): void {
        // Gateway 本地控制统一走现有内部 HTTP 接口，避免独立 LocalIpcServer 的 accept 协程
        // 在空闲时触发 "all coroutines are asleep - deadlock"。
    }

    protected function stopLocalIpcServer(): void {
        $this->localIpcServer = null;
    }

    protected function handleLocalIpcRequest(array $request): array {
        $action = (string)($request['action'] ?? '');
        $payload = is_array($request['payload'] ?? null) ? $request['payload'] : [];
        return match ($action) {
            'gateway.health' => [
                'ok' => true,
                'status' => 200,
                'data' => [
                    'message' => 'ok',
                    'active_version' => $this->currentGatewayStateSnapshot()['active_version'] ?? null,
                ],
            ],
            'gateway.console.subscription' => [
                'ok' => true,
                'status' => 200,
                'data' => ['enabled' => $this->dashboardEnabled() ? $this->hasConsoleSubscribers() : ConsoleRelay::remoteSubscribed()],
            ],
            'gateway.console.log' => [
                'ok' => true,
                'status' => 200,
                'data' => [
                    'accepted' => $this->acceptConsolePayload([
                        'time' => (string)($payload['time'] ?? ''),
                        'message' => (string)($payload['message'] ?? ''),
                        'source_type' => (string)($payload['source_type'] ?? 'gateway'),
                        'node' => (string)($payload['node'] ?? SERVER_HOST),
                    ]),
                ],
            ],
            'gateway.command' => $this->dispatchInternalGatewayCommand(
                (string)($payload['command'] ?? ''),
                (array)($payload['params'] ?? [])
            ),
            default => ['ok' => false, 'status' => 404, 'message' => 'unknown action'],
        };
    }

    protected function currentGatewayStateSnapshot(): array {
        if (App::isReady()) {
            $this->instanceManager->reload();
        }
        return $this->instanceManager->snapshot();
    }

    protected function disconnectAllClients(Server $server): int {
        $startFd = 0;
        $disconnected = 0;
        $summary = [
            'loopback' => 0,
            'dashboard' => 0,
            'node' => 0,
            'business' => 0,
            'control' => 0,
            'rpc' => 0,
            'other_port' => 0,
        ];
        while (true) {
            $clients = $server->getClientList($startFd, 100);
            if (!$clients) {
                break;
            }
            foreach ($clients as $fd) {
                $fd = (int)$fd;
                $startFd = max($startFd, $fd);
                $isDashboardClient = isset($this->dashboardClients[$fd]);
                $isNodeClient = isset($this->nodeClients[$fd]);
                $clientInfo = $server->getClientInfo($fd) ?: [];
                $remoteIp = (string)($clientInfo['remote_ip'] ?? '');
                $serverPort = (int)($clientInfo['server_port'] ?? 0);
                if (in_array($remoteIp, ['127.0.0.1', '::1'], true)) {
                    $summary['loopback']++;
                }
                if ($isDashboardClient) {
                    $summary['dashboard']++;
                }
                if ($isNodeClient) {
                    $summary['node']++;
                }
                if ($serverPort === $this->businessPort()) {
                    $summary['business']++;
                } elseif ($serverPort === $this->controlPort()) {
                    $summary['control']++;
                } elseif ($this->rpcPort > 0 && $serverPort === $this->rpcPort) {
                    $summary['rpc']++;
                } else {
                    $summary['other_port']++;
                }
                unset($this->dashboardClients[$fd], $this->nodeClients[$fd]);
                $this->tcpRelayHandler()->handleClose($fd);
                if (!$server->exist($fd)) {
                    continue;
                }
                if ($server->isEstablished($fd)) {
                    $server->disconnect($fd);
                } else {
                    $server->close($fd);
                }
                $disconnected++;
            }
            if (count($clients) < 100) {
                break;
            }
        }
        $this->lastDisconnectedClientSummary = $summary;
        return $disconnected;
    }

    protected function logDisconnectedClientSummary(int $total): void {
        $summary = (array)$this->lastDisconnectedClientSummary;
        if ($total <= 0 || !$summary) {
            return;
        }
        Console::info(
            "【Gateway】连接释放明细: total={$total}, loopback=" . (int)($summary['loopback'] ?? 0)
            . ", dashboard=" . (int)($summary['dashboard'] ?? 0)
            . ", node=" . (int)($summary['node'] ?? 0)
            . ", business=" . (int)($summary['business'] ?? 0)
            . ", control=" . (int)($summary['control'] ?? 0)
            . ", rpc=" . (int)($summary['rpc'] ?? 0)
            . ", other=" . (int)($summary['other_port'] ?? 0)
        );
    }

    protected function runGatewayBusinessCoordinatorTick(): void {
        if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }

        // 业务编排子进程统一推进 gateway 的后台状态机，worker 只保留 socket 入口。
        if (!App::isReady()) {
            $this->refreshGatewayInstallTakeover();
            return;
        }
        if (App::isReady()) {
            // coordinator 在子进程内持续推进 registry / recycle 状态机，
            // 每轮先 reload，避免长生命周期子进程持有旧的 generation 视图。
            $this->instanceManager->reload();
            if ($this->consumeFallbackBusinessReloadSignal()) {
                Console::info('【Gateway】检测到reload运行信号，开始执行业务平面重载');
                $this->restartGatewayBusinessPlane(false);
            }
        }
        $this->observeUpstreamSupervisorProcess();
        $this->syncUpstreamSupervisorState();
        $this->refreshManagedUpstreamRuntimeStates();
        $this->instanceManager->tick();
        $this->pollPendingManagedRecycles();
        $this->drainQuarantinedManagedRecycles();
        $this->warnStuckDrainingManagedUpstreams();
        $this->cleanupOfflineManagedUpstreams();
        if (App::isReady()) {
            $this->bootstrapManagedUpstreams();
            $this->refreshGatewayInstallTakeover();
        }
    }

    protected function runGatewayHealthMonitorTick(): void {
        $this->gatewayHealthTraceActive = true;
        $this->beginGatewayHealthTraceRound();
        try {
            $this->maintainManagedUpstreamHealth();
        } finally {
            $this->finishGatewayHealthTraceRound();
            $this->gatewayHealthTraceActive = false;
        }
    }

    protected function traceGatewayHealthStep(string $step, float $startedAt, array $context = []): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED || !$this->gatewayHealthTraceActive) {
            return;
        }
        $elapsedMs = round((microtime(true) - $startedAt) * 1000, 2);
        $stepLabel = $this->gatewayHealthStepLabel($step);
        $parts = [];
        foreach ($context as $key => $value) {
            if (is_scalar($value) || $value === null) {
                $parts[] = $key . '=' . (is_null($value) ? 'null' : (string)$value);
            }
        }
        $suffix = $parts ? (', ' . implode(', ', $parts)) : '';
        $this->appendGatewayHealthTraceEntry("{$stepLabel}, step={$step}, cost={$elapsedMs}ms{$suffix}");
    }

    protected function beginGatewayHealthTraceRound(): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $now = microtime(true);
        $this->gatewayHealthTraceRoundStartedAt = $now;
        $this->gatewayHealthTraceRoundUpdatedAt = $now;
        $this->gatewayHealthTraceRoundSequence++;
        $this->gatewayHealthTraceRoundBuffer = [];
        $this->persistGatewayHealthTraceSnapshot(true);
    }

    protected function finishGatewayHealthTraceRound(): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $this->gatewayHealthTraceRoundUpdatedAt = microtime(true);
        $this->persistGatewayHealthTraceSnapshot(false);
    }

    protected function appendGatewayHealthTraceEntry(string $entry): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $this->gatewayHealthTraceRoundUpdatedAt = microtime(true);
        $this->gatewayHealthTraceRoundBuffer[] = $entry;
        if (count($this->gatewayHealthTraceRoundBuffer) > self::GATEWAY_HEALTH_TRACE_BUFFER_MAX_LINES) {
            $this->gatewayHealthTraceRoundBuffer = array_slice($this->gatewayHealthTraceRoundBuffer, -self::GATEWAY_HEALTH_TRACE_BUFFER_MAX_LINES);
        }
        $this->persistGatewayHealthTraceSnapshot(true);
    }

    protected function persistGatewayHealthTraceSnapshot(bool $activeRound): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $roundStartedAt = $this->gatewayHealthTraceRoundStartedAt > 0 ? $this->gatewayHealthTraceRoundStartedAt : microtime(true);
        $updatedAt = $this->gatewayHealthTraceRoundUpdatedAt > 0 ? $this->gatewayHealthTraceRoundUpdatedAt : microtime(true);
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_TRACE_SNAPSHOT, [
            'pid' => (int)(getmypid() ?: 0),
            'active' => $activeRound ? 1 : 0,
            'round_sequence' => $this->gatewayHealthTraceRoundSequence,
            'round_started_at' => round($roundStartedAt, 6),
            'updated_at' => round($updatedAt, 6),
            'round_elapsed_ms' => round(max(0, ($updatedAt - $roundStartedAt) * 1000), 2),
            'entries' => $this->gatewayHealthTraceRoundBuffer,
        ]);
    }

    protected function gatewayHealthStepLabel(string $step): string {
        $map = [
            'maintain.health.guard_flags' => '维护链路-前置标志检查',
            'maintain.health.guard_runtime' => '维护链路-运行模式检查',
            'maintain.health.guard_rolling_cooldown' => '维护链路-滚动发布冷却检查',
            'maintain.health.resolve_interval' => '维护链路-解析探测间隔',
            'maintain.health.guard_interval' => '维护链路-探测间隔未到直接返回',
            'maintain.health.guard_pending_recycles' => '维护链路-待回收进程检查',
            'maintain.health.instance_reload' => '维护链路-重载实例配置',
            'maintain.health.instance_snapshot' => '维护链路-读取实例快照',
            'maintain.health.active_version_empty' => '维护链路-无活跃版本',
            'maintain.health.generation_iterated' => '维护链路-版本已进入迭代阶段',
            'maintain.health.collect_active_plans' => '维护链路-收集活跃计划',
            'maintain.health.active_plans_empty' => '维护链路-无可用计划',
            'maintain.health.plan_skip' => '维护链路-跳过计划探测',
            'maintain.health.plan_probe' => '维护链路-执行计划探测',
            'maintain.health.finish_without_unhealthy' => '维护链路-本轮无异常结束',
            'maintain.health.guard_selfheal_cooldown' => '维护链路-自愈冷却检查',
            'maintain.health.trigger_selfheal' => '维护链路-触发自愈',
            'probe.health.invalid_port' => '探测链路-端口参数非法',
            'probe.health.http_listening' => '探测链路-HTTP 监听检查',
            'probe.health.rpc_listening' => '探测链路-RPC 监听检查',
            'probe.health.fetch_internal_status' => '探测链路-拉取内部状态',
            'probe.health.http_probe' => '探测链路-HTTP 探活',
            'probe.health.barrier_wait_exception' => '探测链路-并发栅栏等待异常',
            'probe.health.total' => '探测链路-单计划总耗时',
            'fetch.internal.invalid_target' => '内部拉取-目标地址非法',
            'fetch.internal.listen_check' => '内部拉取-监听检查',
            'fetch.internal.ipc_request' => '内部拉取-IPC 请求',
            'fetch.internal.success' => '内部拉取-请求成功',
            'fetch.internal.non_200' => '内部拉取-非 200 响应',
            'fetch.internal.no_ipc_action' => '内部拉取-无可用 IPC 动作',
            'fetch.internal.exception' => '内部拉取-请求异常',
        ];
        return $map[$step] ?? ('未知步骤(' . $step . ')');
    }

    protected function shutdownGateway(bool $preserveManagedUpstreams = false): void {
        if ($this->gatewayShutdownScheduled) {
            return;
        }
        $this->preserveManagedUpstreamsOnShutdown = $preserveManagedUpstreams;
        $this->gatewayShutdownScheduled = true;
        $this->prepareGatewayShutdown();
        $this->waitForGatewayShutdownDrain();
        Runtime::instance()->serverIsAlive(false);
        $masterPid = (int)($this->server->master_pid ?? 0);
        if ($masterPid > 0 && $masterPid !== getmypid() && @Process::kill($masterPid, SIGTERM)) {
            return;
        }
        $this->server->shutdown();
    }

    protected function scheduleGatewayShutdown(bool $preserveManagedUpstreams = false): void {
        $this->shutdownGateway($preserveManagedUpstreams);
    }

    protected function waitForGatewayPortsReleased(int $timeoutSeconds = 15, int $intervalMs = 200): void {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $ports = array_values(array_unique(array_filter([
            $this->nginxProxyModeEnabled() ? 0 : $this->port,
            $this->rpcPort,
            ($this->tcpRelayModeEnabled() || $this->nginxProxyModeEnabled()) ? $this->controlPort() : 0,
        ], static fn(int $port) => $port > 0)));
        if (!$ports) {
            return;
        }

        $logged = false;
        while (microtime(true) < $deadline) {
            $occupied = false;
            foreach ($ports as $port) {
                if (CoreServer::isListeningPortInUse($port)) {
                    $occupied = true;
                    break;
                }
            }
            if (!$occupied) {
                if ($logged) {
                    Console::success("【Gateway】启动前端口已释放: " . implode(', ', $ports));
                }
                return;
            }
            if (!$logged) {
                Console::warning("【Gateway】启动前等待端口释放: " . implode(', ', $ports));
                $logged = true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        if ($logged) {
            Console::warning("【Gateway】等待端口释放超时，继续尝试启动: " . implode(', ', $ports));
        }
    }

    protected function createGatewaySocketServer(int $timeoutSeconds = 10, int $intervalMs = 200): Server {
        $listenPort = $this->controlPort();
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $logged = false;

        while (microtime(true) < $deadline) {
            if (!CoreServer::isListeningPortInUse($listenPort)) {
                break;
            }
            if (!$logged) {
                Console::warning("【Gateway】监听端口占用，等待重试: {$this->host}:{$listenPort}");
                $logged = true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }

        if ($logged && !CoreServer::isListeningPortInUse($listenPort)) {
            Console::success("【Gateway】监听端口已释放，准备启动: {$this->host}:{$listenPort}");
        }

        try {
            return new Server($this->host, $listenPort, SWOOLE_PROCESS, SWOOLE_SOCK_TCP);
        } catch (SwooleException $exception) {
            if (str_contains($exception->getMessage(), 'Address already in use')) {
                throw new RuntimeException("Gateway 监听失败，端口仍被占用: {$this->host}:{$listenPort}", previous: $exception);
            }
            throw $exception;
        }
    }

    protected function managedUpstreamPortsReleased(): bool {
        if ($this->preserveManagedUpstreamsOnShutdown) {
            return true;
        }
        $plans = $this->managedUpstreamPlans;
        if (!$plans) {
            return true;
        }

        foreach ($plans as $plan) {
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            $rpcPort = (int)($plan['rpc_port'] ?? 0);
            if ($port > 0 && $this->launcher && $this->launcher->isListening($host, $port, 0.2)) {
                return false;
            }
            if ($rpcPort > 0 && $this->launcher && $this->launcher->isListening($host, $rpcPort, 0.2)) {
                return false;
            }
        }

        return true;
    }

    protected function gatewayAuxiliaryProcessesReleased(): bool {
        if ($this->subProcessManager) {
            $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
            $managerAlive = (bool)($releaseStatus['manager_alive'] ?? false);
            $runtimeShuttingDown = (bool)($releaseStatus['runtime_shutting_down'] ?? false);
            $runtimeAliveCount = (int)($releaseStatus['runtime_alive_count'] ?? 0);
            $trackedAliveCount = (int)($releaseStatus['tracked_alive_count'] ?? 0);
            $subprocessShuttingDown = (bool)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN) ?: false);
            $subprocessAliveCount = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT) ?: 0);
            $effectiveShuttingDown = $runtimeShuttingDown || $subprocessShuttingDown;
            $effectiveAliveCount = max($runtimeAliveCount, $subprocessAliveCount);

            // manager 已退出且本地句柄也确认没有托管子进程时，Runtime 里的
            // shutting_down/alive_count 可能是上一次关停残留值，这里主动清理，
            // 避免 gateway 一直卡在 aux=waiting。
            if (!$managerAlive && $trackedAliveCount <= 0 && ($effectiveShuttingDown || $effectiveAliveCount > 0)) {
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
                $effectiveShuttingDown = false;
                $effectiveAliveCount = 0;
            }

            // Runtime 里的 alive_count 只覆盖 manager 下面托管的直系子进程，
            // addProcess 根进程自身若仍存活，同样会继续持有旧 gateway 继承下来的监听 FD。
            if (
                $managerAlive
                || $effectiveAliveCount > 0
                || $effectiveShuttingDown
            ) {
                return false;
            }
        }
        if ($this->upstreamSupervisor && $this->upstreamSupervisor->isAlive()) {
            return false;
        }
        return true;
    }

    protected function forceReleaseGatewayAuxiliaryProcesses(string $reason): void {
        $pidSet = [];
        $addPid = static function (array &$target, int $pid): void {
            if ($pid > 0) {
                $target[$pid] = true;
            }
        };

        $runtimePidKeys = [
            Key::RUNTIME_SUBPROCESS_MANAGER_PID,
            Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID,
            Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID,
            Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID,
            Key::RUNTIME_MEMORY_MONITOR_PID,
            Key::RUNTIME_HEARTBEAT_PID,
            Key::RUNTIME_LOG_BACKUP_PID,
            Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
            Key::RUNTIME_REDIS_QUEUE_WORKER_PID,
            Key::RUNTIME_FILE_WATCHER_PID,
        ];
        foreach ($runtimePidKeys as $key) {
            $addPid($pidSet, (int)(Runtime::instance()->get($key) ?? 0));
        }

        if ($this->subProcessManager) {
            $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
            $addPid($pidSet, (int)($releaseStatus['manager_pid'] ?? 0));
            foreach ((array)($releaseStatus['tracked_alive'] ?? []) as $pid) {
                $addPid($pidSet, (int)$pid);
            }
        }

        $selfPid = getmypid();
        $masterPid = (int)($this->server->master_pid ?? 0);
        $managerPid = (int)($this->server->manager_pid ?? 0);
        unset($pidSet[$selfPid], $pidSet[$masterPid], $pidSet[$managerPid]);
        if (!$pidSet) {
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
            return;
        }

        $termTargets = [];
        foreach (array_keys($pidSet) as $pid) {
            if (@Process::kill($pid, 0)) {
                $termTargets[] = (int)$pid;
            }
        }
        if (!$termTargets) {
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
            return;
        }

        foreach ($termTargets as $pid) {
            @Process::kill($pid, SIGTERM);
        }
        usleep(300 * 1000);

        $killTargets = [];
        foreach ($termTargets as $pid) {
            if (@Process::kill($pid, 0)) {
                $killTargets[] = $pid;
            }
        }
        foreach ($killTargets as $pid) {
            @Process::kill($pid, SIGKILL);
        }

        // 强制收敛后主动清理 runtime 关停态，避免等待环继续被旧计数卡住。
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT, 0);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_PID, 0);

        Console::warning(
            '【Gateway】附属进程强制收敛完成: reason=' . $reason
            . ', term=' . count($termTargets)
            . ', kill=' . count($killTargets)
        );
    }

    protected function waitForGatewayShutdownDrain(int $timeoutSeconds = 20, int $intervalMs = 200): void {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $logged = false;
        $auxReleased = false;
        $upstreamReleased = false;
        $shutdownRetried = false;
        $forceReleased = false;
        $statusLogged = false;

        while (microtime(true) < $deadline) {
            $auxReleased = $this->gatewayAuxiliaryProcessesReleased();
            $upstreamReleased = $this->preserveManagedUpstreamsOnShutdown || $this->managedUpstreamPortsReleased();
            if ($auxReleased && $upstreamReleased) {
                if ($logged) {
                    Console::success('【Gateway】关停前附属进程与旧监听 FD 已释放');
                }
                return;
            }

            if (!$logged) {
                Console::warning(
                    '【Gateway】关停前等待附属进程释放旧监听 FD: aux=' . ($auxReleased ? 'ready' : 'waiting')
                    . ', upstream=' . ($upstreamReleased ? 'ready' : 'waiting')
                );
                $logged = true;
            }

            if (!$statusLogged && !$auxReleased && $this->subProcessManager) {
                $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
                Console::warning(
                    '【Gateway】附属进程释放状态: manager_pid=' . (int)($releaseStatus['manager_pid'] ?? 0)
                    . ', manager_alive=' . ((bool)($releaseStatus['manager_alive'] ?? false) ? 'yes' : 'no')
                    . ', manager_hb_age=' . (int)($releaseStatus['manager_heartbeat_age'] ?? -1) . 's'
                    . ', runtime_alive=' . (int)($releaseStatus['runtime_alive_count'] ?? 0)
                    . ', runtime_shutting_down=' . ((bool)($releaseStatus['runtime_shutting_down'] ?? false) ? 'yes' : 'no')
                );
                $statusLogged = true;
            }

            if (!$shutdownRetried && !$auxReleased && (microtime(true) + 8) >= $deadline && $this->subProcessManager) {
                // 关停等待接近尾声时再补发一次 subprocess shutdown，收敛“第一次投递丢失”
                // 导致的 aux=waiting 卡住场景；该操作只发生在 shutdown 流程内部。
                $this->subProcessManager->shutdown(true);
                $shutdownRetried = true;
                Console::warning('【Gateway】关停等待附属进程释放耗时较长，已重试投递子进程关停命令');
            }
            if (
                !$forceReleased
                && !$auxReleased
                && (microtime(true) + 3) >= $deadline
            ) {
                // 临近超时仍未释放时直接收敛附属进程，避免持续占用旧监听 FD。
                $this->forceReleaseGatewayAuxiliaryProcesses('shutdown_wait_timeout');
                $forceReleased = true;
            }

            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(max(0.05, $intervalMs / 1000));
            } else {
                usleep(max(50, $intervalMs) * 1000);
            }
        }

        if ($logged) {
            $detail = '';
            if ($this->subProcessManager) {
                $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
                $detail = ', manager_pid=' . (int)($releaseStatus['manager_pid'] ?? 0)
                    . ', manager_alive=' . ((bool)($releaseStatus['manager_alive'] ?? false) ? 'yes' : 'no')
                    . ', manager_hb_age=' . (int)($releaseStatus['manager_heartbeat_age'] ?? -1) . 's'
                    . ', runtime_alive=' . (int)($releaseStatus['runtime_alive_count'] ?? 0)
                    . ', runtime_shutting_down=' . ((bool)($releaseStatus['runtime_shutting_down'] ?? false) ? 'yes' : 'no');
            }
            Console::warning(
                '【Gateway】关停等待附属进程释放超时，继续执行 shutdown: aux=' . ($auxReleased ? 'ready' : 'waiting')
                    . ', upstream=' . ($upstreamReleased ? 'ready' : 'waiting')
                    . $detail
            );
        }
    }

    protected function prepareGatewayShutdown(): void {
        if ($this->gatewayShutdownPrepared) {
            return;
        }
        $this->gatewayShutdownPrepared = true;
        $this->stopGatewayLeaseRenewTimer();
        $this->persistGatewayLeaseForShutdown();
        // 关停早期就把 serverIsAlive 拉低，避免 SubProcessManager 在 wait(false) 回收窗口里
        // 误判为“父服务仍存活”而把刚退出的子进程立即重拉。
        Runtime::instance()->serverIsAlive(false);
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        $this->stopLocalIpcServer();
        $this->clearPendingManagedRecycleWatchers();
        isset($this->subProcessManager) && $this->subProcessManager->shutdown(true);
        if ($this->preserveManagedUpstreamsOnShutdown) {
            $this->detachUpstreamSupervisor();
        } else {
            $this->shutdownManagedUpstreams();
        }
    }

    protected function detachUpstreamSupervisor(): void {
        if (!$this->upstreamSupervisor) {
            return;
        }
        $result = $this->dispatchUpstreamSupervisorCommand('detach', [], true, 15);
        if ($result->hasError()) {
            Console::warning('【Gateway】等待 UpstreamSupervisor 脱离控制面板失败，将继续按关停流程推进: ' . $result->getMessage());
        }
    }

    protected function reloadGateway(bool $restartManagedUpstreams = true): void {
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        Console::info('【Gateway】正在重启服务');
        $this->iterateGatewayBusinessProcesses();
        if ($restartManagedUpstreams) {
            $summary = $this->restartManagedUpstreams();
            if ($summary['failed_nodes']) {
                Console::warning(
                    "【Gateway】业务实例重启存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                    . ', ' . $this->managedRestartFailureDetails($summary)
                );
            }
        }
        $this->server->reload();
    }

    protected function reserveGatewayReload(bool $restartManagedUpstreams = true): array {
        if ($this->gatewayShutdownScheduled) {
            return [
                'accepted' => false,
                'message' => 'Gateway 已进入关闭流程，无法再发起重载',
                'scheduled' => false,
                'restart_managed_upstreams' => $restartManagedUpstreams,
            ];
        }

        if ($this->gatewayReloadScheduled || $this->managedUpstreamRolling) {
            return [
                'accepted' => true,
                'message' => 'Gateway 重载已在进行中，已跳过重复触发',
                'scheduled' => false,
                'restart_managed_upstreams' => $restartManagedUpstreams,
            ];
        }

        $this->gatewayReloadScheduled = true;
        return [
            'accepted' => true,
            'message' => $restartManagedUpstreams
                ? 'Gateway 与业务实例已开始重载'
                : 'Gateway 已开始重载',
            'scheduled' => true,
            'restart_managed_upstreams' => $restartManagedUpstreams,
        ];
    }

    protected function scheduleReservedGatewayReload(bool $restartManagedUpstreams = true): void {
        Timer::after(1, function () use ($restartManagedUpstreams): void {
            try {
                $this->reloadGateway($restartManagedUpstreams);
            } catch (Throwable $throwable) {
                // 异步 reload 失败时需要恢复运行态标记，避免控制面长时间停在 draining。
                Runtime::instance()->serverIsDraining(false);
                Runtime::instance()->serverIsReady(true);
                Console::error('【Gateway】异步重载失败: ' . $throwable->getMessage());
            } finally {
                $this->gatewayReloadScheduled = false;
            }
        });
    }

    protected function shutdownManagedUpstreams(): void {
        if (!$this->upstreamSupervisor) {
            $this->instanceManager->removeManagedInstances();
            return;
        }

        $result = $this->dispatchUpstreamSupervisorCommand('shutdown', [], true, 120);
        if ($result->hasError()) {
            Console::warning('【Gateway】等待 UpstreamSupervisor 回收业务实例超时或失败，保留 registry 等待下次启动继续接管: ' . $result->getMessage());
            return;
        }

        $this->instanceManager->removeManagedInstances();
    }

    protected function refreshGatewayInstallTakeover(): void {
        $isReady = App::isReady();
        $takeoverActive = (bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER) ?? false);
        if (!$isReady) {
            if (!$takeoverActive) {
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER, true);
                Console::info('【Gateway】应用尚未安装，业务入口切换到 gateway 控制面板承接安装流程');
                $this->syncNginxProxyTargets('install_takeover');
            }
            return;
        }

        if ($takeoverActive) {
            $snapshot = $this->instanceManager->snapshot();
            if (((string)($snapshot['active_version'] ?? '')) === '') {
                return;
            }
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER, false);
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_UPDATING, false);
            Console::success('【Gateway】应用安装完成，业务入口准备切回托管 upstream');
            $this->syncNginxProxyTargets('install_takeover_release');
        }
    }

    protected function ensureGatewaySubProcessManagerRegistered(): void {
        if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        if ($this->subProcessManager) {
            return;
        }
        $this->subProcessManager = new SubProcessManager($this->server, Config::server(), [
            'exclude_processes' => ['CrontabManager'],
            'shutdown_handler' => function (): void {
                $this->scheduleGatewayShutdown();
            },
            'reload_handler' => function (): void {
                $this->restartGatewayBusinessPlane();
            },
            'restart_handler' => function (): void {
                Console::warning('【Gateway】检测到控制面板文件变动，准备重启 Gateway');
                $this->scheduleGatewayShutdown(false);
            },
            'command_handler' => function (string $command, array $params, object $socket): bool {
                return $this->handleGatewayProcessCommand($command, $params, $socket);
            },
            'node_status_builder' => function (array $status): array {
                return $this->buildGatewayHeartbeatStatus($status);
            },
            'gateway_business_tick_handler' => function (): void {
                $this->runGatewayBusinessCoordinatorTick();
            },
            'gateway_health_tick_handler' => function (): void {
                $this->runGatewayHealthMonitorTick();
            },
            'gateway_business_command_handler' => function (string $command, array $params): array {
                return $this->handleGatewayBusinessCommand($command, $params);
            },
        ]);
        $this->subProcessManager->start();
    }

    protected function attachUpstreamSupervisor(): void {
        if (!$this->launcher || !$this->managedUpstreamPlans) {
            return;
        }

        $this->upstreamSupervisor = new UpstreamSupervisor(
            $this->launcher,
            [],
            25,
            $this->gatewayLeaseEpoch(),
            $this->businessPort(),
            $this->gatewayLeaseGraceSeconds
        );
        $this->server->addProcess($this->upstreamSupervisor->getProcess());
        $this->lastObservedUpstreamSupervisorPid = 0;
        $this->lastObservedUpstreamSupervisorStartedAt = 0;
        $this->pendingUpstreamSupervisorSyncReason = 'initial';
    }

    protected function bootstrapManagedUpstreams(): void {
        if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        if (!$this->launcher || !$this->managedUpstreamPlans) {
            return;
        }

        foreach ($this->managedUpstreamPlans as $index => $plan) {
            $originalVersion = (string)($plan['version'] ?? '');
            $originalHost = (string)($plan['host'] ?? '127.0.0.1');
            $originalPort = (int)($plan['port'] ?? 0);
            $originalKey = ($originalVersion . '@' . $originalHost . ':' . $originalPort);
            // 已经成功拉起过的 plan 先按原始 key 去重，避免先做端口改写后把“自家已启动实例”
            // 误当成新 plan 再启动一次。
            if ($originalPort > 0 && isset($this->bootstrappedManagedInstances[$originalKey])) {
                continue;
            }

            $plan = $this->resolveManagedBootstrapPlan($plan);
            $plan = $this->withManagedPlanLeaseBinding($plan);
            $this->managedUpstreamPlans[$index] = $plan;
            $version = (string)($plan['version'] ?? '');
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            $key = ($version . '@' . $host . ':' . $port);
            if (isset($this->bootstrappedManagedInstances[$key])) {
                continue;
            }

            $rpcPort = (int)($plan['rpc_port'] ?? 0);
            if ($port <= 0) {
                continue;
            }
            if (!$this->launcher->isListening($host, $port, 0.2)) {
                if (!$this->upstreamSupervisor) {
                    continue;
                }
                if (!$this->startupSummaryPending()) {
                    Console::info("【Gateway】启动业务实例: " . $this->describePlan($plan));
                }
                $this->upstreamSupervisor->sendCommand([
                    'action' => 'spawn',
                    'owner_epoch' => $this->gatewayLeaseEpoch(),
                    'plan' => $plan,
                ]);
                if (!$this->launcher->waitUntilServicesReady(
                    $host,
                    $port,
                    $rpcPort,
                    (int)($plan['start_timeout'] ?? 25),
                    200,
                    !$this->startupSummaryPending()
                )) {
                    continue;
                }
            }

            if ($rpcPort > 0 && !$this->launcher->isListening($host, $rpcPort, 0.2)) {
                continue;
            }

            $weight = (int)($plan['weight'] ?? 100);
            $metadata = (array)($plan['metadata'] ?? []);
            $metadata['managed'] = true;
            $metadata['role'] = (string)($plan['role'] ?? ($metadata['role'] ?? SERVER_ROLE));
            $metadata['rpc_port'] = (int)($plan['rpc_port'] ?? ($metadata['rpc_port'] ?? 0));
            $metadata['started_at'] = (int)($metadata['started_at'] ?? time());
            $metadata['managed_mode'] = 'gateway_supervisor';

            if ($this->startupSummaryPending()) {
                $this->recordStartupReadyInstance($plan);
            }
            $this->registerManagedPlan($plan, true);
            $this->instanceManager->removeOtherInstances($version, $host, $port);
            $rpcInfo = (int)($plan['rpc_port'] ?? 0) > 0 ? ', RPC:' . (int)$plan['rpc_port'] : '';
            $cutoverReady = $this->syncNginxProxyTargets('register_managed_plan_activate');
            if (!$cutoverReady) {
                Console::warning("【Gateway】业务实例已启动，但入口切换未完成: {$version} {$host}:{$port}{$rpcInfo}");
            }
        }
    }

    protected function renderStartupInfo(array $resultRows = []): void {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $stateFile = $this->instanceManager->stateFile();
        $frameworkRoot = Root::dir();
        $frameworkBuildTime = FRAMEWORK_BUILD_TIME;
        $packageVersion = $frameworkBuildTime === 'development' ? 'development' : FRAMEWORK_BUILD_VERSION;
        $packagistVersion = SCF_COMPOSER_VERSION === 'development' ? '非composer引用' : SCF_COMPOSER_VERSION;
        $loadedFiles = count(get_included_files());
        $taskWorkers = (int)(Config::server()['task_worker_num'] ?? 0);
        $listenLabel = $this->nginxProxyModeEnabled() ? '业务入口' : '业务TCP入口';
        $listenValue = "{$this->host}:{$this->port}";
        $controlValue = $this->internalControlHost() . ':' . $this->controlPort();
        $rpcValue = $this->rpcPort > 0 ? ($this->host . ':' . $this->rpcPort) : '--';
        $masterPid = $this->serverMasterPid > 0 ? $this->serverMasterPid : (int)($this->server->master_pid ?? 0);
        $managerPid = $this->serverManagerPid > 0 ? $this->serverManagerPid : (int)($this->server->manager_pid ?? 0);
        $infoLines = [
            '------------------Gateway启动完成------------------',
            '应用指纹：' . APP_FINGERPRINT,
            '运行系统：' . OS_ENV,
            '运行环境：' . SERVER_RUN_ENV,
            '应用目录：' . APP_DIR_NAME,
            '源码类型：' . APP_SRC_TYPE,
            '节点角色：' . SERVER_ROLE,
            '文件加载：' . $loadedFiles,
            '环境版本：' . swoole_version(),
            '框架源码：' . $frameworkRoot,
            'Packagist：' . $packagistVersion,
            '打包版本：' . $packageVersion,
            '打包时间：' . $frameworkBuildTime,
            '工作进程：' . $this->workerNum,
            '任务进程：' . $taskWorkers,
            '主机地址：' . SERVER_HOST,
            '应用版本：' . $appVersion,
            '资源版本：' . $publicVersion,
            '流量模式：' . $this->trafficMode(),
            $listenLabel . '：' . $listenValue,
            '控制面板：' . $controlValue,
            'RPC监听：' . $rpcValue,
            '进程信息：Master:' . $masterPid . ',Manager:' . $managerPid,
            '状态文件：' . $stateFile,
        ];
        $formattedResultRows = $this->formatStartupResultRows($resultRows);
        if ($formattedResultRows) {
            $infoLines = array_merge($infoLines, $formattedResultRows);
        }
        $infoLines[] = '--------------------------------------------------';
        $info = implode("\n", $infoLines);
        Console::write(Color::cyan($info));
    }

    protected function createBusinessTrafficListener(): void {
        if (!$this->tcpRelayModeEnabled()) {
            return;
        }
        if ($this->controlPort() === $this->port) {
            throw new RuntimeException('tcp流量模式下控制面板端口不能与业务端口相同');
        }
        $listener = $this->server->listen($this->host, $this->port, SWOOLE_SOCK_TCP);
        if ($listener === false) {
            throw new RuntimeException("Gateway业务转发监听失败: {$this->host}:{$this->port}");
        }
        $listener->set([
            'package_max_length' => 20 * 1024 * 1024,
            'open_http_protocol' => false,
            'open_http2_protocol' => false,
            'open_websocket_protocol' => false,
        ]);
    }

    protected function resolveAppVersion(array $appInfo = []): string {
        return App::version()
            ?: (string)($appInfo['version'] ?? (App::profile()->version ?? '--'));
    }

    protected function resolvePublicVersion(array $appInfo = []): string {
        return App::publicVersion()
            ?: (string)($appInfo['public_version'] ?? (App::profile()->public_version ?? '--'));
    }

    protected function describePlan(array $plan): string {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $version = (string)($plan['version'] ?? 'unknown');
        return $rpcPort > 0
            ? "{$version} {$host}:{$port}, RPC:{$rpcPort}"
            : "{$version} {$host}:{$port}";
    }

    protected function pidFile(): string {
        return dirname(SCF_ROOT) . '/var/' . APP_DIR_NAME . '_gateway_' . SERVER_ROLE . '.pid';
    }
}
