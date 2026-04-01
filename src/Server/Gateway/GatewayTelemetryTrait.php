<?php

namespace Scf\Server\Gateway;

use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Core\Table\ServerNodeTable;
use Scf\Helper\JsonHelper;
use Scf\Util\File;
use Swoole\Coroutine;
use Swoole\Coroutine\Barrier;
use Swoole\Process;
use Throwable;

/**
 * Gateway 观测指标聚合与 dashboard 遥测输出职责。
 *
 * 职责边界：
 * - 负责节点/upstream 运行态聚合、内存视图整合与 dashboard 状态包输出；
 * - 不处理 dashboard 登录鉴权、路由分发和 socket 协议握手。
 *
 * 架构位置：
 * - 位于 GatewayServer 控制面的可观测性层；
 * - 上游消费 heartbeat/instance runtime，下游服务 dashboard 轮询与推送。
 *
 * 设计意图：
 * - 统一实时指标聚合入口，避免在多个链路重复计算，
 *   并确保 active upstream 策略下的数据时效性和一致性。
 */
trait GatewayTelemetryTrait {
    /**
     * 聚合 dashboard 节点列表（localhost + 远端节点）。
     *
     * @param array<int, array<string, mixed>>|null $upstreams 可选 upstream 运行态快照
     * @return array<int, array<string, mixed>>
     */
    public function dashboardNodes(?array $upstreams = null): array {
        $nodes = [];
        // localhost 节点统一走 buildGatewayNode，让 1s dashboard 推送可以叠加
        // 实时 upstream 指标；否则会被 5s heartbeat 快照节奏锁住。
        $nodes[] = $this->buildGatewayNode($upstreams);

        foreach (ServerNodeStatusTable::instance()->rows() as $host => $status) {
            if ($host === 'localhost' || !is_array($status)) {
                continue;
            }
            $status['host'] = $host;
            $status['online'] = (time() - (int)($status['heart_beat'] ?? 0)) <= 20;
            $nodes[] = $status;
        }

        return $nodes;
    }

    /**
     * 构建 dashboard server 首页状态包。
     *
     * @param string $token 当前 dashboard token
     * @param string $host 请求 Host 头
     * @param string $referer 请求 Referer
     * @param string $forwardedProto 反向代理透传协议
     * @param bool $forceRefreshVersions 是否强制刷新版本缓存
     * @return array<string, mixed>
     */
    public function dashboardServerStatus(string $token, string $host = '', string $referer = '', string $forwardedProto = '', bool $forceRefreshVersions = false): array {
        $upstreams = $this->dashboardUpstreams();
        $status = $this->buildDashboardRealtimeStatus($upstreams);
        $status['socket_host'] = $this->buildDashboardSocketHost($host, $referer, $forwardedProto) . '?token=' . rawurlencode($token);
        $status['latest_version'] = App::latestVersion($forceRefreshVersions);
        $status['dashboard'] = $this->buildDashboardVersionStatus($forceRefreshVersions);
        $status['framework'] = $this->buildFrameworkVersionStatus($upstreams, $forceRefreshVersions);
        return $status;
    }

    /**
     * 组装 dashboard 实时状态主体。
     *
     * @param array<int, array<string, mixed>>|null $upstreams upstream 运行态快照
     * @return array<string, mixed>
     */
    protected function buildDashboardRealtimeStatus(?array $upstreams = null): array {
        $upstreams ??= $this->dashboardUpstreams();
        $nodes = $this->dashboardNodes($upstreams);
        $master = 0;
        $slave = 0;
        foreach ($nodes as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                $master++;
            } else {
                $slave++;
            }
        }

        $appInfo = App::info()?->toArray() ?: [];
        $appInfo['version'] = $this->resolveAppVersion($appInfo);
        $appInfo['public_version'] = $this->resolvePublicVersion($appInfo);
        $logger = Log::instance();

        return [
            'event' => 'server_status',
            'info' => [...$appInfo, 'master' => $master, 'slave' => $slave],
            'nodes' => $nodes,
            'logs' => [
                'error' => [
                    'total' => $logger->count('error', date('Y-m-d')),
                    'list' => []
                ],
                'info' => [
                    'total' => $logger->count('info', date('Y-m-d')),
                    'list' => []
                ],
                'slow' => [
                    'total' => $logger->count('slow', date('Y-m-d')),
                    'list' => []
                ]
            ]
        ];
    }

    /**
     * 拉取 gateway 视角的 upstream 列表与运行态。
     *
     * @param bool $fetchLiveRuntime 是否实时通过 IPC 抓取运行态
     * @return array<int, array<string, mixed>>
     */
    protected function dashboardUpstreams(bool $fetchLiveRuntime = true): array {
        $snapshot = $this->currentGatewayStateSnapshot();
        $plans = [];
        foreach ($snapshot['generations'] ?? [] as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                $plans[] = [
                    'generation' => (array)$generation,
                    'instance' => (array)$instance,
                ];
            }
        }
        if (!$plans) {
            return [];
        }

        if (!$fetchLiveRuntime) {
            $instances = [];
            foreach ($plans as $plan) {
                $generation = (array)($plan['generation'] ?? []);
                $instance = (array)($plan['instance'] ?? []);
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                $runtimeStatus = $this->instanceManager->instanceRuntimeStatus($host, $port, 30);
                $instances[] = $this->buildUpstreamNode($generation, $instance, $runtimeStatus, true);
            }
            return $instances;
        }

        $instances = array_fill(0, count($plans), null);
        $barrier = Barrier::make();
        foreach ($plans as $index => $plan) {
            Coroutine::create(function () use ($barrier, &$instances, $index, $plan): void {
                $generation = (array)($plan['generation'] ?? []);
                $instance = (array)($plan['instance'] ?? []);
                $runtimeStatus = $this->fetchUpstreamRuntimeStatus($instance);
                $instances[$index] = $this->buildUpstreamNode($generation, $instance, $runtimeStatus, false);
            });
        }
        try {
            Barrier::wait($barrier);
        } catch (Throwable) {
            // dashboard 状态链路里并发探测异常时，降级为已完成分支结果，避免整包状态失败。
        }

        return array_values(array_filter($instances, static fn($item): bool => is_array($item)));
    }

    protected function refreshManagedUpstreamRuntimeStates(): void {
        $snapshot = $this->instanceManager->snapshot();
        foreach (($snapshot['generations'] ?? []) as $generation) {
            if (!in_array((string)($generation['status'] ?? ''), ['active', 'draining', 'prepared'], true)) {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((($instance['metadata']['managed'] ?? false) !== true)) {
                    continue;
                }
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($port <= 0) {
                    continue;
                }
                $runtimeStatus = $this->fetchUpstreamRuntimeStatus($instance);
                $this->instanceManager->updateInstanceRuntimeStatus($host, $port, $runtimeStatus);
                $masterPid = (int)($runtimeStatus['master_pid'] ?? 0);
                $managerPid = (int)($runtimeStatus['manager_pid'] ?? 0);
                $metadataPatch = [];
                // lifecycle 回收链必须始终用 upstream master pid 作为实例主 PID。
                // 若写成 manager pid，SIGTERM/SIGKILL 会被 manager 进程吞掉并被 master 立刻拉回，
                // 最终表现为“回收一直重试、端口迟迟不释放”。
                if ($masterPid > 0) {
                    $metadataPatch['pid'] = $masterPid;
                    $metadataPatch['master_pid'] = $masterPid;
                }
                if ($managerPid > 0) {
                    $metadataPatch['manager_pid'] = $managerPid;
                }
                if ($metadataPatch) {
                    $this->instanceManager->mergeInstanceMetadata($host, $port, $metadataPatch);
                    $this->mergeManagedPlanMetadata($host, $port, $metadataPatch);
                }
            }
        }
    }

    protected function syncUpstreamSupervisorState(): void {
        if (!$this->upstreamSupervisor || !$this->launcher || $this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        $now = time();
        $syncReason = $this->pendingUpstreamSupervisorSyncReason;
        if ($syncReason === null && $this->lastUpstreamSupervisorSyncAt > 0 && ($now - $this->lastUpstreamSupervisorSyncAt) < self::UPSTREAM_SUPERVISOR_SYNC_INTERVAL_SECONDS) {
            return;
        }
        $instances = $this->buildUpstreamSupervisorSyncInstances();
        if (!$this->upstreamSupervisor->sendCommand([
            'action' => 'sync_instances',
            'owner_epoch' => $this->gatewayLeaseEpoch(),
            'instances' => $instances,
        ])) {
            if ($syncReason === 'restart') {
                Console::warning('【Gateway】UpstreamSupervisor 重建后状态同步失败');
            } elseif ($syncReason === 'initial') {
                Console::warning('【Gateway】UpstreamSupervisor 初始状态同步失败');
            } else {
                Console::warning('【Gateway】业务实例管理器状态同步失败');
            }
            return;
        }
        $this->lastUpstreamSupervisorSyncAt = $now;
        $this->recordStartupUpstreamSupervisorSync($this->lastObservedUpstreamSupervisorPid, count($instances));
        if ($syncReason === 'restart') {
            Console::success('【Gateway】UpstreamSupervisor 已异常重建，状态已重新同步: pid=' . $this->lastObservedUpstreamSupervisorPid . ', instances=' . count($instances));
        } elseif ($syncReason === 'initial' && !$this->startupSummaryPending()) {
            Console::info('【Gateway】UpstreamSupervisor 状态已同步: pid=' . $this->lastObservedUpstreamSupervisorPid . ', instances=' . count($instances));
        }
        $this->pendingUpstreamSupervisorSyncReason = null;
    }

    protected function observeUpstreamSupervisorProcess(): void {
        if (!$this->upstreamSupervisor) {
            return;
        }
        $pid = (int)(Runtime::instance()->get(Key::RUNTIME_UPSTREAM_SUPERVISOR_PID) ?? 0);
        $startedAt = (int)(Runtime::instance()->get(Key::RUNTIME_UPSTREAM_SUPERVISOR_STARTED_AT) ?? 0);
        if ($pid <= 0 || $startedAt <= 0) {
            return;
        }
        if ($this->lastObservedUpstreamSupervisorPid <= 0 || $this->lastObservedUpstreamSupervisorStartedAt <= 0) {
            $this->lastObservedUpstreamSupervisorPid = $pid;
            $this->lastObservedUpstreamSupervisorStartedAt = $startedAt;
            $this->pendingUpstreamSupervisorSyncReason ??= 'initial';
            return;
        }
        if ($pid === $this->lastObservedUpstreamSupervisorPid && $startedAt === $this->lastObservedUpstreamSupervisorStartedAt) {
            return;
        }
        Console::warning(
            '【Gateway】UpstreamSupervisor 进程PID发生变化，疑似异常重建: old='
            . $this->lastObservedUpstreamSupervisorPid . ', new=' . $pid
        );
        $this->lastObservedUpstreamSupervisorPid = $pid;
        $this->lastObservedUpstreamSupervisorStartedAt = $startedAt;
        $this->pendingUpstreamSupervisorSyncReason = 'restart';
    }

    protected function buildUpstreamSupervisorSyncInstances(): array {
        $snapshot = $this->instanceManager->snapshot();
        $instances = [];
        foreach (($snapshot['generations'] ?? []) as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((($instance['metadata']['managed'] ?? false) !== true)) {
                    continue;
                }
                $version = (string)($instance['version'] ?? $generation['version'] ?? '');
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($version === '' || $port <= 0) {
                    continue;
                }
                $rpcPort = (int)($instance['metadata']['rpc_port'] ?? 0);
                $pendingRecycle = $this->isManagedPlanRecyclePending([
                    'version' => $version,
                    'host' => $host,
                    'port' => $port,
                ]);
                if ($pendingRecycle) {
                    continue;
                }
                $httpAlive = $this->launcher->isListening($host, $port, 0.1) || $this->launcher->isListening('0.0.0.0', $port, 0.1);
                $rpcAlive = $rpcPort <= 0 || $this->launcher->isListening($host, $rpcPort, 0.1) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.1);
                if (!$httpAlive || !$rpcAlive) {
                    continue;
                }
                $instances[] = [
                    'version' => $version,
                    'host' => $host,
                    'port' => $port,
                    'weight' => (int)($instance['weight'] ?? 100),
                    'metadata' => (array)($instance['metadata'] ?? []),
                ];
            }
        }
        return $instances;
    }

    protected function maintainManagedUpstreamHealth(): void {
        $maintainStartedAt = microtime(true);
        if ($this->managedUpstreamSelfHealing || $this->managedUpstreamRolling || !$this->launcher || !$this->upstreamSupervisor) {
            $this->traceGatewayHealthStep('maintain.health.guard_flags', $maintainStartedAt, [
                'return' => 'self_healing_or_rolling_or_dependency_missing',
            ]);
            return;
        }
        if (!App::isReady() || $this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            $this->traceGatewayHealthStep('maintain.health.guard_runtime', $maintainStartedAt, [
                'return' => 'app_not_ready_or_shutdown_or_server_down',
            ]);
            return;
        }
        $now = time();
        if ($this->lastManagedUpstreamRollingAt > 0 && ($now - $this->lastManagedUpstreamRollingAt) < self::ACTIVE_UPSTREAM_HEALTH_ROLLING_COOLDOWN_SECONDS) {
            $this->traceGatewayHealthStep('maintain.health.guard_rolling_cooldown', $maintainStartedAt, [
                'return' => 'rolling_cooldown',
            ]);
            return;
        }
        $intervalResolveStartedAt = microtime(true);
        $healthCheckInterval = $this->resolveManagedUpstreamHealthCheckIntervalSeconds($now);
        $elapsedSinceLastRound = $this->lastManagedUpstreamHealthCheckAt > 0
            ? ($now - $this->lastManagedUpstreamHealthCheckAt)
            : -1;
        if ($elapsedSinceLastRound >= 0 && $elapsedSinceLastRound < $healthCheckInterval) {
            return;
        }
        $this->traceGatewayHealthStep('maintain.health.resolve_interval', $intervalResolveStartedAt, [
            'interval' => $healthCheckInterval,
            'elapsed' => $elapsedSinceLastRound >= 0 ? $elapsedSinceLastRound : 'first_round',
        ]);
        $this->lastManagedUpstreamHealthCheckAt = $now;
        if ($this->pendingManagedRecycles) {
            $this->traceGatewayHealthStep('maintain.health.guard_pending_recycles', $maintainStartedAt, [
                'return' => 'pending_recycles',
                'pending_count' => count($this->pendingManagedRecycles),
            ]);
            return;
        }

        // 健康检测统一以 registry 持久化状态为准。
        // rolling / recycle / self-heal 期间有多条协程在推进 instanceManager 内存态，
        // 这里先 reload 一次，避免使用到尚未收敛的旧代 in-memory 视图。
        $reloadStartedAt = microtime(true);
        $this->instanceManager->reload();
        $this->traceGatewayHealthStep('maintain.health.instance_reload', $reloadStartedAt);
        $snapshotStartedAt = microtime(true);
        $snapshot = $this->instanceManager->snapshot();
        $this->traceGatewayHealthStep('maintain.health.instance_snapshot', $snapshotStartedAt);
        $activeVersion = (string)($snapshot['active_version'] ?? '');
        if ($activeVersion === '') {
            $this->lastManagedHealthActiveVersion = '';
            $this->managedUpstreamHealthState = [];
            $this->managedUpstreamHealthStableRounds = 0;
            $this->managedUpstreamHealthLastFailureAt = 0;
            $this->traceGatewayHealthStep('maintain.health.active_version_empty', $maintainStartedAt, [
                'return' => 'active_version_empty',
            ]);
            return;
        }
        if ($activeVersion !== $this->lastManagedHealthActiveVersion) {
            // active generation 一变，历史失败计数就没有参考意义了，需要对新代重新观察。
            $this->notifyManagedUpstreamGenerationIterated($activeVersion);
            $this->traceGatewayHealthStep('maintain.health.generation_iterated', $maintainStartedAt, [
                'return' => 'generation_iterated',
                'active_version' => $activeVersion,
            ]);
            return;
        }
        $plansStartedAt = microtime(true);
        $activePlans = $this->managedPlansFromSnapshot();
        $this->traceGatewayHealthStep('maintain.health.collect_active_plans', $plansStartedAt, [
            'plan_count' => count($activePlans),
        ]);
        if (!$activePlans) {
            $this->managedUpstreamHealthState = [];
            $this->managedUpstreamHealthStableRounds = 0;
            $this->managedUpstreamHealthLastFailureAt = 0;
            $this->traceGatewayHealthStep('maintain.health.active_plans_empty', $maintainStartedAt, [
                'return' => 'active_plans_empty',
            ]);
            return;
        }

        $unhealthyPlans = [];
        $activeKeys = [];
        $hasFailure = false;
        foreach ($activePlans as $plan) {
            $key = $this->managedPlanKey($plan);
            $activeKeys[$key] = true;

            if ($this->shouldSkipManagedUpstreamHealthCheck($plan, $now)) {
                unset($this->managedUpstreamHealthState[$key]);
                $this->traceGatewayHealthStep('maintain.health.plan_skip', $maintainStartedAt, [
                    'plan' => $this->managedPlanDescriptor($plan),
                    'reason' => 'startup_grace_or_pending_recycle',
                ]);
                continue;
            }

            // 健康检查采用“端口 + internal health”双重标准，避免只看 listen 就过早判定 ready。
            $probeStartedAt = microtime(true);
            $probe = $this->probeManagedUpstreamHealth($plan);
            $this->traceGatewayHealthStep('maintain.health.plan_probe', $probeStartedAt, [
                'plan' => $this->managedPlanDescriptor($plan),
                'healthy' => !empty($probe['healthy']) ? 1 : 0,
                'reason' => (string)($probe['reason'] ?? 'unknown'),
            ]);
            if ($probe['healthy']) {
                $previous = $this->managedUpstreamHealthState[$key] ?? null;
                if (is_array($previous) && (int)($previous['failures'] ?? 0) > 0) {
                    Console::success("【Gateway】active业务实例健康恢复: " . $this->managedPlanDescriptor($plan));
                }
                unset($this->managedUpstreamHealthState[$key]);
                continue;
            }

            $state = $this->managedUpstreamHealthState[$key] ?? [
                'failures' => 0,
                'reason' => '',
                'last_failed_at' => 0,
            ];
            $state['failures'] = (int)$state['failures'] + 1;
            $state['reason'] = (string)($probe['reason'] ?? 'unknown');
            $state['last_failed_at'] = $now;
            $this->managedUpstreamHealthState[$key] = $state;
            $hasFailure = true;
            if ((int)($state['failures'] ?? 0) === 1) {
                Console::warning(
                    "【Gateway】active业务实例健康异常(1/" . self::ACTIVE_UPSTREAM_HEALTH_FAILURE_THRESHOLD . '): '
                    . $this->managedPlanDescriptor($plan)
                    . ', reason=' . $state['reason']
                );
            }

            if ($state['failures'] >= self::ACTIVE_UPSTREAM_HEALTH_FAILURE_THRESHOLD) {
                $unhealthyPlans[] = $plan;
            }
        }

        foreach (array_keys($this->managedUpstreamHealthState) as $key) {
            if (!isset($activeKeys[$key])) {
                unset($this->managedUpstreamHealthState[$key]);
            }
        }
        if ($hasFailure) {
            $this->managedUpstreamHealthStableRounds = 0;
            $this->managedUpstreamHealthLastFailureAt = $now;
        } else {
            $this->managedUpstreamHealthStableRounds = min(3600, $this->managedUpstreamHealthStableRounds + 1);
        }

        if (!$unhealthyPlans) {
            $this->traceGatewayHealthStep('maintain.health.finish_without_unhealthy', $maintainStartedAt, [
                'active_plan_count' => count($activePlans),
                'stable_rounds' => $this->managedUpstreamHealthStableRounds,
            ]);
            return;
        }
        if ($this->lastManagedUpstreamSelfHealAt > 0 && ($now - $this->lastManagedUpstreamSelfHealAt) < self::ACTIVE_UPSTREAM_HEALTH_COOLDOWN_SECONDS) {
            $this->traceGatewayHealthStep('maintain.health.guard_selfheal_cooldown', $maintainStartedAt, [
                'return' => 'selfheal_cooldown',
                'unhealthy_count' => count($unhealthyPlans),
            ]);
            return;
        }

        $this->managedUpstreamSelfHealing = true;
        $descriptors = array_map(fn(array $plan) => $this->managedPlanDescriptor($plan), $unhealthyPlans);
        $recoveryPlans = $activePlans;
        Console::warning("【Gateway】检测到active业务实例连续异常，开始自动自愈: " . implode(' | ', $descriptors));
        $this->traceGatewayHealthStep('maintain.health.trigger_selfheal', $maintainStartedAt, [
            'unhealthy_count' => count($unhealthyPlans),
            'recovery_plan_count' => count($recoveryPlans),
        ]);
        Coroutine::create(function () use ($unhealthyPlans, $recoveryPlans) {
            try {
                // 自愈直接复用 rolling restart，确保切流、回滚、回收语义完全一致。
                $summary = $this->restartManagedUpstreams($recoveryPlans);
                if ($summary['failed_nodes']) {
                    Console::warning(
                        "【Gateway】自动自愈存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                        . ', ' . $this->managedRestartFailureDetails($summary, $recoveryPlans)
                    );
                } else {
                    Console::success("【Gateway】自动自愈完成: success={$summary['success_count']}");
                }
                foreach ($unhealthyPlans as $plan) {
                    unset($this->managedUpstreamHealthState[$this->managedPlanKey($plan)]);
                }
            } finally {
                $this->lastManagedUpstreamSelfHealAt = time();
                $this->managedUpstreamSelfHealing = false;
            }
        });
    }

    protected function shouldSkipManagedUpstreamHealthCheck(array $plan, int $now): bool {
        $startedAt = (int)($plan['metadata']['started_at'] ?? 0);
        if ($startedAt > 0 && ($now - $startedAt) < self::ACTIVE_UPSTREAM_HEALTH_STARTUP_GRACE_SECONDS) {
            return true;
        }
        return $this->isManagedPlanRecyclePending($plan);
    }

    protected function probeManagedUpstreamHealth(array $plan): array {
        $probeStartedAt = microtime(true);
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $planLabel = $host . ':' . $port;
        if ($port <= 0) {
            $this->traceGatewayHealthStep('probe.health.invalid_port', $probeStartedAt, [
                'plan' => $planLabel,
            ]);
            return ['healthy' => false, 'reason' => 'invalid_port'];
        }

        // 不同类型探测并发执行并在 Barrier 收口；健康判定以 HTTP 探活为准。
        // 用 Barrier 栅栏收口，避免串行探测累积拉长心跳周期。
        $rpcListening = $rpcPort <= 0;
        $httpProbeOk = false;
        $barrier = Barrier::make();

        if ($rpcPort > 0) {
            Coroutine::create(function () use ($barrier, &$rpcListening, $host, $rpcPort, $planLabel): void {
                $rpcListeningStartedAt = microtime(true);
                $rpcListening = $this->launcher
                    && ($this->launcher->isListening($host, $rpcPort, 0.2) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.2));
                $this->traceGatewayHealthStep('probe.health.rpc_listening', $rpcListeningStartedAt, [
                    'plan' => $planLabel,
                    'rpc_port' => $rpcPort,
                    'listening' => $rpcListening ? 1 : 0,
                ]);
            });
        }

        Coroutine::create(function () use ($barrier, &$httpProbeOk, $host, $port, $planLabel): void {
            $httpProbeStartedAt = microtime(true);
            $httpProbeOk = $this->launcher
                && $this->launcher->probeHttpConnectivity($host, $port, self::INTERNAL_UPSTREAM_HTTP_PROBE_PATH, 0.5);
            $this->traceGatewayHealthStep('probe.health.http_probe', $httpProbeStartedAt, [
                'plan' => $planLabel,
                'ok' => $httpProbeOk ? 1 : 0,
            ]);
        });

        try {
            Barrier::wait($barrier);
        } catch (Throwable $throwable) {
            $this->traceGatewayHealthStep('probe.health.barrier_wait_exception', $probeStartedAt, [
                'plan' => $planLabel,
                'error' => $throwable->getMessage(),
            ]);
            return ['healthy' => false, 'reason' => 'probe_barrier_exception'];
        }

        if ($rpcPort > 0 && !$rpcListening) {
            $this->traceGatewayHealthStep('probe.health.rpc_listening', $probeStartedAt, [
                'plan' => $planLabel,
                'rpc_port' => $rpcPort,
                'soft_fail' => 1,
            ]);
        }
        if (!$httpProbeOk) {
            return ['healthy' => false, 'reason' => 'http_probe_failed'];
        }

        $this->traceGatewayHealthStep('probe.health.total', $probeStartedAt, [
            'plan' => $planLabel,
            'healthy' => 1,
            'by' => 'http_probe',
            'rpc_listening' => ($rpcPort <= 0 || $rpcListening) ? 1 : 0,
        ]);
        return ['healthy' => true, 'reason' => 'ok'];
    }

    protected function notifyManagedUpstreamGenerationIterated(string $version): void {
        $version = trim($version);
        if ($version === '') {
            return;
        }
        $this->lastManagedHealthActiveVersion = $version;
        $this->managedUpstreamHealthState = [];
        $this->lastManagedUpstreamRollingAt = time();
        $this->lastManagedUpstreamHealthCheckAt = 0;
        $this->managedUpstreamHealthStableRounds = 0;
        $this->managedUpstreamHealthLastFailureAt = 0;
    }

    protected function resolveManagedUpstreamHealthCheckIntervalSeconds(int $now): int {
        unset($now);
        $serverConfig = Config::server();
        return max(
            5,
            (int)($serverConfig['gateway_upstream_health_check_interval'] ?? self::ACTIVE_UPSTREAM_HEALTH_CHECK_INTERVAL_SECONDS)
        );
    }

    protected function managedPlanKey(array $plan): string {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        return (string)($plan['version'] ?? '') . '@' . $host . ':' . $port;
    }

    protected function buildGatewayNode(?array $upstreams = null): array {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $stats = isset($this->server) ? $this->server->stats() : [];
        $subprocesses = $this->subProcessManager?->managedProcessDashboardSnapshot() ?: [
            'signature' => '--',
            'updated_at' => time(),
            'summary' => ['total' => 0, 'running' => 0, 'stale' => 0, 'offline' => 0, 'stopped' => 0],
            'items' => [],
        ];
        $node = [
            'host' => 'localhost',
            'name' => 'Gateway',
            'script' => 'gateway',
            'ip' => SERVER_HOST,
            'env' => SERVER_RUN_ENV ?: 'production',
            'port' => $this->port,
            'socketPort' => $this->resolvedDashboardPort(),
            'online' => true,
            'role' => SERVER_ROLE,
            'started' => $this->startedAt,
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_proxy',
            'app_version' => $appVersion,
            'public_version' => $publicVersion,
            'framework_build_version' => FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => function_exists('scf_framework_update_ready') && scf_framework_update_ready(),
            'swoole_version' => swoole_version(),
            'scf_version' => SCF_COMPOSER_VERSION,
            'manager_pid' => $this->serverManagerPid ?: '--',
            'restart_times' => Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0,
            'server_stats' => [
                'connection_num' => (int)($stats['connection_num'] ?? 0),
                'long_connection_num' => (int)($stats['connection_num'] ?? 0),
                'tasking_num' => (int)($stats['tasking_num'] ?? 0),
                'idle_worker_num' => (int)($stats['idle_worker_num'] ?? 0),
            ],
            'http_request_count_today' => 0,
            'http_request_count_current' => 0,
            'http_request_processing' => 0,
            'mysql_inflight' => 0,
            'redis_inflight' => 0,
            'outbound_http_inflight' => 0,
            'mysql_execute_count' => 0,
            'http_request_reject' => 0,
            'cpu_num' => function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0,
            'memory_usage' => $this->buildEmptyMemoryUsage(),
            'threads' => (int)($stats['worker_num'] ?? 0),
            'tables' => [],
            'tasks' => [],
            'subprocesses' => $subprocesses,
            'subprocess_signature' => (string)($subprocesses['signature'] ?? '--'),
            'fingerprint' => APP_FINGERPRINT . ':gateway',
        ];
        $heartbeatStatus = ServerNodeStatusTable::instance()->get('localhost');
        $useHeartbeatStatus = is_array($heartbeatStatus)
            && $heartbeatStatus
            && $this->isLocalGatewayHeartbeatStatusFresh($heartbeatStatus);
        if ($useHeartbeatStatus) {
            $node = array_replace_recursive($node, $heartbeatStatus);
        }
        $upstreams ??= $this->dashboardUpstreams();
        if ($useHeartbeatStatus) {
            // 心跳快照存在时仍叠加一次“轻量实时业务指标”，让 dashboard 1s 推送
            // 的连接数/并发类数据不再停留在 heartbeat 的 5s 粒度。
            $node = $this->composeGatewayNodeRealtimeBusinessMetrics($node, $upstreams);
        } else {
            $node = $this->composeGatewayNodeRuntimeStatus($node, $upstreams);
        }
        $node = array_replace_recursive($node, [
            'host' => 'localhost',
            'name' => 'Gateway',
            'script' => 'gateway',
            'ip' => SERVER_HOST,
            'env' => SERVER_RUN_ENV ?: 'production',
            'port' => $this->port,
            'socketPort' => $this->resolvedDashboardPort(),
            'role' => SERVER_ROLE,
            'online' => true,
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_proxy',
            'manager_pid' => (int)($this->server->manager_pid ?? $this->serverManagerPid) ?: '--',
            'fingerprint' => APP_FINGERPRINT . ':gateway',
        ]);
        return $node;
    }

    protected function composeGatewayNodeRealtimeBusinessMetrics(array $status, array $upstreams): array {
        if ($this->shouldSkipGatewayBusinessOverlayDuringShutdown()) {
            return $status;
        }
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        $businessOverlay = $this->buildGatewayBusinessOverlay($upstreams);
        if ($businessOverlay) {
            if (array_key_exists('tasks', $businessOverlay)) {
                $businessOverlay['tasks'] = $this->mergeGatewayTasks(
                    (array)($status['tasks'] ?? []),
                    (array)$businessOverlay['tasks']
                );
            }
            $status = array_replace_recursive($status, $businessOverlay);
        }
        if ($selected) {
            $primary = $selected[0];
            $status['started'] = (int)($primary['started'] ?? ($status['started'] ?? 0));
            $status['threads'] = (int)($primary['threads'] ?? ($status['threads'] ?? 0));
            $status['tables'] = (array)($primary['tables'] ?? ($status['tables'] ?? []));
        }
        return $status;
    }

    protected function isLocalGatewayHeartbeatStatusFresh(array $heartbeatStatus): bool {
        $now = time();
        $nodeHeartbeatAt = (int)($heartbeatStatus['heart_beat'] ?? 0);
        if ($nodeHeartbeatAt <= 0 || ($now - $nodeHeartbeatAt) > 8) {
            return false;
        }
        $clusterHeartbeatAt = (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT) ?? 0);
        if ($clusterHeartbeatAt <= 0 || ($now - $clusterHeartbeatAt) > 8) {
            return false;
        }
        return true;
    }

    protected function composeGatewayNodeRuntimeStatus(array $status, array $upstreams): array {
        // gateway 已进入 shutdown/restart 时，节点状态只保留本地控制面信息。
        // 这时继续向 upstream 做 memory/status 补采只会把退出路径重新拖进 IPC，
        // 既没有运维价值，也会增加旧控制面迟迟不释放监听 FD 的风险。
        if ($this->shouldSkipGatewayBusinessOverlayDuringShutdown()) {
            return $status;
        }

        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        $businessOverlay = $this->buildGatewayBusinessOverlay($upstreams);
        if ($businessOverlay) {
            if (array_key_exists('tasks', $businessOverlay)) {
                $businessOverlay['tasks'] = $this->mergeGatewayTasks(
                    (array)($status['tasks'] ?? []),
                    (array)$businessOverlay['tasks']
                );
            }
            $status = array_replace_recursive($status, $businessOverlay);
        }
        if ($selected) {
            $primary = $selected[0];
            // Business runtime metrics should reflect the active upstream,
            // while scheduling stays on Gateway subprocesses.
            $status['started'] = (int)($primary['started'] ?? ($status['started'] ?? 0));
            $status['threads'] = (int)($primary['threads'] ?? ($status['threads'] ?? 0));
            $status['tables'] = (array)($primary['tables'] ?? ($status['tables'] ?? []));
        }
        $status['memory_usage'] = $this->buildGatewayMemoryUsage(
            (array)($status['memory_usage'] ?? []),
            $upstreams
        );
        return $status;
    }

    protected function shouldSkipGatewayBusinessOverlayDuringShutdown(): bool {
        return $this->gatewayShutdownPrepared
            || !Runtime::instance()->serverIsAlive()
            || Runtime::instance()->serverIsDraining()
            || !Runtime::instance()->serverIsReady();
    }

    protected function mergeGatewayTasks(array $localTasks, array $businessTasks): array {
        $tasks = [];
        foreach ([$localTasks, $businessTasks] as $taskGroup) {
            foreach ($taskGroup as $task) {
                if (!is_array($task)) {
                    continue;
                }
                $taskId = (string)($task['id'] ?? '');
                if ($taskId !== '') {
                    $tasks[$taskId] = $task;
                    continue;
                }
                $tasks[] = $task;
            }
        }
        return array_values($tasks);
    }

    protected function buildUpstreamNode(array $generation, array $instance, array $runtimeStatus = [], bool $skipLivePortProbe = false): array {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $metadata = (array)($instance['metadata'] ?? []);
        $status = (string)($generation['status'] ?? ($instance['status'] ?? 'prepared'));
        $displayVersion = (string)($metadata['display_version'] ?? $generation['version'] ?? $instance['version'] ?? 'unknown');
        $runtimeOnline = array_key_exists('server_is_alive', $runtimeStatus)
            ? ((bool)$runtimeStatus['server_is_alive'])
            : null;
        $online = $runtimeOnline;
        if (is_null($online)) {
            if (!$skipLivePortProbe && $this->launcher) {
                $online = $this->launcher->isListening($host, $port, 0.2);
            } else {
                $online = true;
            }
        }

        $node = [
            'name' => '业务实例',
            'script' => $displayVersion,
            'ip' => $host,
            'port' => $port,
            'online' => $online,
            'role' => (string)($metadata['role'] ?? SERVER_ROLE),
            'started' => (int)($metadata['started_at'] ?? time()),
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_upstream/' . $status,
            'app_version' => $appVersion,
            'public_version' => $publicVersion,
            'framework_build_version' => FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => function_exists('scf_framework_update_ready') && scf_framework_update_ready(),
            'swoole_version' => swoole_version(),
            'scf_version' => SCF_COMPOSER_VERSION,
            'manager_pid' => (int)($metadata['manager_pid'] ?? 0) ?: '--',
            'restart_times' => 0,
            'server_stats' => [
                'connection_num' => (int)($instance['runtime_connections'] ?? 0),
                'long_connection_num' => (int)($instance['runtime_connections'] ?? 0),
                'tasking_num' => 0,
                'idle_worker_num' => 0,
            ],
            'http_request_count_today' => 0,
            'http_request_count_current' => 0,
            'http_request_processing' => 0,
            'mysql_execute_count' => 0,
            'http_request_reject' => 0,
            'cpu_num' => function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0,
            'memory_usage' => $this->buildEmptyMemoryUsage(),
            'threads' => 0,
            'tables' => [],
            'tasks' => [],
            'fingerprint' => APP_FINGERPRINT . ':' . md5(($instance['id'] ?? '') . '@' . $port),
        ];
        if ($runtimeStatus) {
            $node = array_replace_recursive($node, $runtimeStatus);
        }
        $node = array_replace_recursive($node, [
            'name' => '业务实例',
            'script' => $displayVersion,
            'ip' => $host,
            'port' => $port,
            'online' => $online,
            'role' => (string)($metadata['role'] ?? ($runtimeStatus['role'] ?? SERVER_ROLE)),
            'server_run_mode' => (string)($runtimeStatus['server_run_mode'] ?? APP_SRC_TYPE),
            'proxy_mode_label' => 'gateway_upstream/' . $status,
            'manager_pid' => ($runtimeStatus['manager_pid'] ?? ((int)($metadata['manager_pid'] ?? 0) ?: '--')),
            'fingerprint' => APP_FINGERPRINT . ':' . md5(($instance['id'] ?? '') . '@' . $port),
        ]);
        $node['memory_usage'] = $this->normalizeMemoryUsage((array)($node['memory_usage'] ?? []));
        return $node;
    }

    protected function fetchUpstreamRuntimeStatus(array $instance): array {
        return $this->fetchUpstreamInternalStatus($instance, self::INTERNAL_UPSTREAM_STATUS_PATH);
    }

    protected function fetchUpstreamHealthStatus(array $instance): array {
        return $this->fetchUpstreamInternalStatus($instance, self::INTERNAL_UPSTREAM_HEALTH_PATH);
    }

    protected function fetchUpstreamInternalStatus(array $instance, string $path): array {
        $fetchStartedAt = microtime(true);
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $timeoutSeconds = $path === self::INTERNAL_UPSTREAM_STATUS_PATH ? 5.0 : 1.0;
        $planLabel = $host . ':' . $port;
        if ($host === '' || $port <= 0) {
            $this->traceGatewayHealthStep('fetch.internal.invalid_target', $fetchStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
            ]);
            return [];
        }
        $listenCheckStartedAt = microtime(true);
        if ($this->launcher && !$this->launcher->isListening($host, $port, 0.2)) {
            $this->traceGatewayHealthStep('fetch.internal.listen_check', $listenCheckStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
                'listening' => 0,
            ]);
            return [];
        }
        $this->traceGatewayHealthStep('fetch.internal.listen_check', $listenCheckStartedAt, [
            'path' => $path,
            'plan' => $planLabel,
            'listening' => 1,
        ]);

        try {
            $ipcAction = $this->mapUpstreamStatusPathToIpcAction($path);
            if ($ipcAction !== '') {
                $ipcRequestStartedAt = microtime(true);
                $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $timeoutSeconds);
                $this->traceGatewayHealthStep('fetch.internal.ipc_request', $ipcRequestStartedAt, [
                    'path' => $path,
                    'plan' => $planLabel,
                    'action' => $ipcAction,
                    'timeout' => $timeoutSeconds,
                    'status' => (int)($ipcResponse['status'] ?? 0),
                ]);
                if (is_array($ipcResponse) && (int)($ipcResponse['status'] ?? 0) === 200) {
                    $data = $ipcResponse['data'] ?? null;
                    $this->traceGatewayHealthStep('fetch.internal.success', $fetchStartedAt, [
                        'path' => $path,
                        'plan' => $planLabel,
                    ]);
                    return is_array($data) ? $data : [];
                }
                $this->traceGatewayHealthStep('fetch.internal.non_200', $fetchStartedAt, [
                    'path' => $path,
                    'plan' => $planLabel,
                ]);
                return [];
            }
            $this->traceGatewayHealthStep('fetch.internal.no_ipc_action', $fetchStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
            ]);
            return [];
        } catch (Throwable) {
            $this->traceGatewayHealthStep('fetch.internal.exception', $fetchStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
            ]);
            return [];
        }
    }

    protected function mapUpstreamStatusPathToIpcAction(string $path): string {
        return match ($path) {
            self::INTERNAL_UPSTREAM_STATUS_PATH => 'upstream.status',
            self::INTERNAL_UPSTREAM_HEALTH_PATH => 'upstream.health',
            default => '',
        };
    }

    protected function fetchUpstreamRuntimeStatusSync(string $host, int $port): array {
        return $this->fetchUpstreamInternalStatusSync($host, $port, self::INTERNAL_UPSTREAM_STATUS_PATH, 5.0);
    }

    protected function fetchUpstreamHealthStatusSync(string $host, int $port): array {
        return $this->fetchUpstreamInternalStatusSync($host, $port, self::INTERNAL_UPSTREAM_HEALTH_PATH, 1.5);
    }

    protected function fetchUpstreamInternalStatusSync(string $host, int $port, string $path, float $timeoutSeconds = 0.0): array {
        if ($host === '' || $port <= 0) {
            return [];
        }

        $ipcAction = $this->mapUpstreamStatusPathToIpcAction($path);
        if ($ipcAction === '') {
            return [];
        }

        if ($timeoutSeconds <= 0) {
            $timeoutSeconds = $path === self::INTERNAL_UPSTREAM_STATUS_PATH ? 5.0 : 1.5;
        }
        try {
            $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $timeoutSeconds);
            if (!is_array($ipcResponse) || (int)($ipcResponse['status'] ?? 0) !== 200) {
                return [];
            }
            $data = $ipcResponse['data'] ?? null;
            return is_array($data) ? $data : [];
        } catch (Throwable) {
            return [];
        }
    }

    protected function buildGatewayBusinessOverlay(array $upstreams): array {
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        if (!$selected) {
            return [];
        }

        $base = $selected[0];
        $serverStats = [
            'connection_num' => 0,
            'long_connection_num' => 0,
            'tasking_num' => 0,
            'idle_worker_num' => 0,
        ];

        foreach ($selected as $item) {
            $stats = (array)($item['server_stats'] ?? []);
            $serverStats['connection_num'] += (int)($stats['connection_num'] ?? 0);
            $serverStats['long_connection_num'] += (int)($stats['long_connection_num'] ?? 0);
            $serverStats['tasking_num'] += (int)($stats['tasking_num'] ?? 0);
            $serverStats['idle_worker_num'] += (int)($stats['idle_worker_num'] ?? 0);
        }

        $tasks = [];
        foreach ($selected as $item) {
            foreach ((array)($item['tasks'] ?? []) as $task) {
                if (!is_array($task)) {
                    continue;
                }
                $taskId = (string)($task['id'] ?? '');
                if ($taskId !== '') {
                    $tasks[$taskId] = $task;
                    continue;
                }
                $tasks[] = $task;
            }
        }

        return [
            'app_version' => $base['app_version'] ?? '--',
            'framework_build_version' => $base['framework_build_version'] ?? FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => (bool)($base['framework_update_ready'] ?? false),
            'swoole_version' => $base['swoole_version'] ?? swoole_version(),
            'scf_version' => $base['scf_version'] ?? SCF_COMPOSER_VERSION,
            'server_stats' => $serverStats,
            'http_request_count_today' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_count_today'] ?? 0), $selected)),
            'http_request_count_current' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_count_current'] ?? 0), $selected)),
            'http_request_processing' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_processing'] ?? 0), $selected)),
            'mysql_inflight' => array_sum(array_map(static fn(array $item) => (int)($item['mysql_inflight'] ?? 0), $selected)),
            'redis_inflight' => array_sum(array_map(static fn(array $item) => (int)($item['redis_inflight'] ?? 0), $selected)),
            'outbound_http_inflight' => array_sum(array_map(static fn(array $item) => (int)($item['outbound_http_inflight'] ?? 0), $selected)),
            'mysql_execute_count' => array_sum(array_map(static fn(array $item) => (int)($item['mysql_execute_count'] ?? 0), $selected)),
            'http_request_reject' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_reject'] ?? 0), $selected)),
            'cpu_num' => (int)($base['cpu_num'] ?? (function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0)),
            'tasks' => array_values($tasks),
        ];
    }

    protected function selectGatewayBusinessUpstreams(array $upstreams): array {
        if (!$upstreams) {
            return [];
        }

        $selected = array_values(array_filter($upstreams, static function (array $item) {
            return ($item['proxy_mode_label'] ?? '') === 'gateway_upstream/active';
        }));
        if (!$selected) {
            $selected = array_values(array_filter($upstreams, static function (array $item) {
                return (bool)($item['online'] ?? false);
            }));
        }
        return $selected;
    }

    protected function buildGatewayMemoryUsage(array $gatewayMemoryUsage, array $upstreams): array {
        $gatewayMemoryUsage = $this->normalizeMemoryUsage($gatewayMemoryUsage);
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        if (!$selected) {
            return $gatewayMemoryUsage;
        }

        $rows = [];
        $usageTotal = 0.0;
        $realTotal = 0.0;
        $peakTotal = 0.0;
        $osActualTotal = 0.0;
        $rssTotal = 0.0;
        $pssTotal = 0.0;
        $online = 0;
        $offline = 0;
        $systemTotalMemGb = (float)($gatewayMemoryUsage['system_total_mem_gb'] ?? 0);
        $systemFreeMemGb = (float)($gatewayMemoryUsage['system_free_mem_gb'] ?? 0);

        foreach ((array)($gatewayMemoryUsage['rows'] ?? []) as $row) {
            if (!$this->shouldKeepGatewayMemoryRow((array)$row)) {
                continue;
            }
            $rows[] = $row;
            $usageTotal += $this->extractMemoryValue($row['usage'] ?? null);
            $realTotal += $this->extractMemoryValue($row['real'] ?? null);
            $peakTotal += $this->extractMemoryValue($row['peak'] ?? null);
            $osActualTotal += $this->extractMemoryValue($row['os_actual'] ?? null);
            $rssTotal += $this->extractMemoryValue($row['rss'] ?? null);
            $pssTotal += $this->extractMemoryValue($row['pss'] ?? null);
            if ($this->isOnlineMemoryRow((array)$row)) {
                $online++;
            } else {
                $offline++;
            }
        }

        foreach ($selected as $item) {
            $memoryUsage = $this->normalizeMemoryUsage((array)($item['memory_usage'] ?? []));
            $systemTotalMemGb = max($systemTotalMemGb, (float)($memoryUsage['system_total_mem_gb'] ?? 0));
            $systemFreeMemGb = max($systemFreeMemGb, (float)($memoryUsage['system_free_mem_gb'] ?? 0));
            foreach ($this->buildGatewayUpstreamMemoryRows($item, $memoryUsage) as $row) {
                $rows[] = $row;
                $usageTotal += $this->extractMemoryValue($row['usage'] ?? null);
                $realTotal += $this->extractMemoryValue($row['real'] ?? null);
                $peakTotal += $this->extractMemoryValue($row['peak'] ?? null);
                $osActualTotal += $this->extractMemoryValue($row['os_actual'] ?? null);
                $rssTotal += $this->extractMemoryValue($row['rss'] ?? null);
                $pssTotal += $this->extractMemoryValue($row['pss'] ?? null);
                if ($this->isOnlineMemoryRow((array)$row)) {
                    $online++;
                } else {
                    $offline++;
                }
            }
        }

        usort($rows, function (array $left, array $right) {
            $leftValue = $this->extractMemoryValue($left['os_actual'] ?? null);
            if ($leftValue <= 0) {
                $leftValue = $this->extractMemoryValue($left['real'] ?? null);
            }
            $rightValue = $this->extractMemoryValue($right['os_actual'] ?? null);
            if ($rightValue <= 0) {
                $rightValue = $this->extractMemoryValue($right['real'] ?? null);
            }
            return $rightValue <=> $leftValue;
        });

        return $this->normalizeMemoryUsage([
            'rows' => $rows,
            'online' => $online,
            'offline' => $offline,
            'total' => count($rows),
            'usage_total_mb' => round($usageTotal, 2),
            'real_total_mb' => round($realTotal, 2),
            'peak_total_mb' => round($peakTotal, 2),
            'os_actual_total_mb' => round($osActualTotal, 2),
            'rss_total_mb' => round($rssTotal, 2),
            'pss_total_mb' => round($pssTotal, 2),
            'system_total_mem_gb' => $systemTotalMemGb > 0 ? round($systemTotalMemGb, 2) : '--',
            'system_free_mem_gb' => $systemFreeMemGb > 0 ? round($systemFreeMemGb, 2) : '--',
        ]);
    }

    protected function buildGatewayUpstreamMemoryRows(array $instance, array $fallbackMemoryUsage): array {
        $fallbackRows = (array)($fallbackMemoryUsage['rows'] ?? []);
        $fallbackByName = [];
        foreach ($fallbackRows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $fallbackByName[$name] = $row;
            }
        }

        $snapshotRows = $this->fetchUpstreamMemoryRows($instance);
        if (!$snapshotRows) {
            return $fallbackRows;
        }

        $rows = [];
        $seen = [];
        foreach ($snapshotRows as $snapshotRow) {
            if (!is_array($snapshotRow)) {
                continue;
            }
            $name = (string)($snapshotRow['process'] ?? '');
            if ($name === '') {
                continue;
            }
            $seen[$name] = true;
            $rows[] = $this->composeGatewayUpstreamMemoryRow(
                $instance,
                $snapshotRow,
                $fallbackByName[$name] ?? []
            );
        }

        foreach ($fallbackRows as $fallbackRow) {
            if (!is_array($fallbackRow)) {
                continue;
            }
            $name = (string)($fallbackRow['name'] ?? '');
            if ($name === '' || isset($seen[$name])) {
                continue;
            }
            $rows[] = $fallbackRow;
        }

        return $rows;
    }

    protected function fetchUpstreamMemoryRows(array $instance): array {
        $port = (int)($instance['port'] ?? 0);
        if ($port <= 0) {
            return [];
        }
        $ipcResponse = LocalIpc::request(
            LocalIpc::upstreamSocketPath($port),
            'upstream.memory_rows',
            [],
            2.0
        );
        if (!is_array($ipcResponse) || (int)($ipcResponse['status'] ?? 0) !== 200) {
            return [];
        }
        $data = $ipcResponse['data'] ?? null;
        return is_array($data) ? $data : [];
    }

    protected function composeGatewayUpstreamMemoryRow(array $instance, array $snapshotRow, array $fallbackRow): array {
        $name = (string)($snapshotRow['process'] ?? ($fallbackRow['name'] ?? ''));
        $pid = (int)($snapshotRow['pid'] ?? ($fallbackRow['pid'] ?? 0));
        $usage = (float)($snapshotRow['usage_mb'] ?? $this->extractMemoryValue($fallbackRow['usage'] ?? null));
        $real = (float)($snapshotRow['real_mb'] ?? $this->extractMemoryValue($fallbackRow['real'] ?? null));
        $peak = (float)($snapshotRow['peak_mb'] ?? $this->extractMemoryValue($fallbackRow['peak'] ?? null));
        $usageUpdated = (int)($snapshotRow['usage_updated'] ?? ($fallbackRow['usage_updated'] ?? 0));
        $restartTs = (int)($snapshotRow['restart_ts'] ?? ($fallbackRow['restart_ts'] ?? 0));
        $restartCount = (int)($snapshotRow['restart_count'] ?? ($fallbackRow['restart_count'] ?? 0));
        $limitMb = (int)($snapshotRow['limit_memory_mb'] ?? ($fallbackRow['limit_memory_mb'] ?? 0));
        $autoRestart = (int)($snapshotRow['auto_restart'] ?? 0);

        $alive = $pid > 0 && @Process::kill($pid, 0);
        $rssMb = null;
        $pssMb = null;
        $osActualMb = null;
        if ($alive) {
            $mem = \Scf\Util\MemoryMonitor::getPssRssByPid($pid);
            $rssMb = isset($mem['rss_kb']) && is_numeric($mem['rss_kb']) ? round(((float)$mem['rss_kb']) / 1024, 1) : null;
            $pssMb = isset($mem['pss_kb']) && is_numeric($mem['pss_kb']) ? round(((float)$mem['pss_kb']) / 1024, 1) : null;
            $osActualMb = $pssMb ?? $rssMb;
        }

        $serverConfig = Config::server();
        $autoRestartEnabled = (bool)($serverConfig['worker_memory_auto_restart'] ?? false);

        if (
            $alive
            && $autoRestartEnabled
            && $autoRestart === STATUS_ON
            && str_starts_with($name, 'worker:')
            && $limitMb > 0
            && $osActualMb !== null
            && $osActualMb > $limitMb
        ) {
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            $restartKey = "gateway.upstream.memory.restart:{$host}:{$port}:{$name}";
            $lastRestartAt = (int)(Runtime::instance()->get($restartKey) ?? 0);
            if (time() - $lastRestartAt >= 120) {
                if ($this->requestUpstreamWorkerMemoryRestart($instance, $name, $pid, $osActualMb, $limitMb)) {
                    Runtime::instance()->set($restartKey, time());
                    $restartTs = time();
                    $restartCount++;
                }
            }
        }

        return [
            'name' => $name,
            'pid' => $pid > 0 ? $pid : ($fallbackRow['pid'] ?? '--'),
            'usage' => number_format($usage, 2) . ' MB',
            'real' => number_format($real, 2) . ' MB',
            'peak' => number_format($peak, 2) . ' MB',
            'os_actual' => $osActualMb === null ? '-' : (number_format($osActualMb, 2) . ' MB'),
            'rss' => $rssMb === null ? '-' : (number_format($rssMb, 2) . ' MB'),
            'pss' => $pssMb === null ? '-' : (number_format($pssMb, 2) . ' MB'),
            'updated' => time(),
            'usage_updated' => $usageUpdated,
            'status' => $alive ? Color::green('正常') : Color::red('离线'),
            'connection' => (int)($fallbackRow['connection'] ?? 0),
            'restart_ts' => $restartTs,
            'restart_count' => $restartCount,
            'limit_memory_mb' => $limitMb,
        ];
    }

    protected function requestUpstreamWorkerMemoryRestart(array $instance, string $processName, int $pid, float $osActualMb, int $limitMb): bool {
        if (!(bool)(Config::server()['worker_memory_auto_restart'] ?? false)) {
            return false;
        }
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        if ($port <= 0 || $processName === '' || $pid <= 0) {
            return false;
        }
        $ipcResponse = LocalIpc::request(
            LocalIpc::upstreamSocketPath($port),
            'upstream.restart_worker',
            [
                'process' => $processName,
                'pid' => $pid,
            ],
            1.5
        );
        $accepted = is_array($ipcResponse) && (int)($ipcResponse['status'] ?? 0) === 200;
        if ($accepted) {
            Log::instance()->setModule('system')
                ->error("{$processName}[PID:{$pid}] 内存 {$osActualMb}MB ≥ {$limitMb}MB，Gateway 已通知 upstream 平滑轮换");
            return true;
        }
        $message = is_array($ipcResponse) ? (string)($ipcResponse['message'] ?? 'request failed') : 'ipc unavailable';
        Log::instance()->setModule('system')
            ->warning("{$processName}[PID:{$pid}] 内存 {$osActualMb}MB ≥ {$limitMb}MB，但 upstream 平滑轮换请求失败: {$message}");
        return false;
    }

    protected function shouldKeepGatewayMemoryRow(array $row): bool {
        $name = (string)($row['name'] ?? '');
        if ($name === '') {
            return false;
        }
        if (str_starts_with($name, 'worker:')) {
            return false;
        }
        return $name !== 'Server:Master';
    }

    protected function isOnlineMemoryRow(array $row): bool {
        $status = preg_replace('/\e\[[\d;]*m/', '', (string)($row['status'] ?? ''));
        return str_contains($status, '正常');
    }

    protected function extractMemoryValue(mixed $value): float {
        if (is_int($value) || is_float($value)) {
            return (float)$value;
        }
        if (!is_string($value) || $value === '' || $value === '-') {
            return 0.0;
        }
        if (preg_match('/-?\d+(?:\.\d+)?/', $value, $matches)) {
            return (float)$matches[0];
        }
        return 0.0;
    }

    protected function normalizeMemoryUsage(array $memoryUsage): array {
        if ((float)($memoryUsage['os_actual_total_mb'] ?? 0) <= 0) {
            $fallback = (float)($memoryUsage['real_total_mb'] ?? ($memoryUsage['usage_total_mb'] ?? 0));
            if ($fallback > 0) {
                $memoryUsage['os_actual_total_mb'] = round($fallback, 2);
            }
        }
        return $memoryUsage + $this->buildEmptyMemoryUsage();
    }

    protected function buildEmptyMemoryUsage(): array {
        return [
            'rows' => [],
            'system_free_mem_gb' => 0,
            'system_total_mem_gb' => 0,
            'os_actual_total_mb' => 0,
        ];
    }

    protected function buildDashboardSocketHost(string $host, string $referer, string $forwardedProto = ''): string {
        $normalizedHost = trim($host);
        $normalizedReferer = trim($referer);
        $normalizedProto = strtolower(trim($forwardedProto));
        $pathPrefix = $this->dashboardRefererPathPrefix($normalizedReferer);

        if ($this->isLoopbackDashboardHost($normalizedHost)) {
            $refererAuthority = $this->dashboardRefererAuthority($normalizedReferer);
            if ($refererAuthority !== '' && !$this->isLoopbackDashboardHost($refererAuthority)) {
                $normalizedHost = $refererAuthority;
            }
        }

        // 反向代理有时会把 `Host` 里的非默认端口剥掉，但浏览器 Referer 仍保留了
        // 真正对外访问的 `ip:port`。遇到“同主机但 Host 无端口”的情况，要优先把
        // Referer 里的端口补回来，否则 dashboard socket 会错误回退到 80/443。
        $normalizedHost = $this->preferDashboardRefererAuthorityPort($normalizedHost, $normalizedReferer);

        if ($normalizedHost === '') {
            $normalizedHost = $this->dashboardRefererAuthority($normalizedReferer);
        }

        if ($normalizedHost === '') {
            $normalizedHost = '127.0.0.1:' . $this->resolvedDashboardPort();
        }
        $normalizedHost = $this->normalizeDashboardSocketAuthority($normalizedHost);

        $protocol = in_array($normalizedProto, ['https', 'wss'], true)
            || (!empty($normalizedReferer) && str_starts_with($normalizedReferer, 'https'))
            ? 'wss://'
            : 'ws://';

        return $protocol . $normalizedHost . $pathPrefix . '/dashboard.socket';
    }

    protected function preferDashboardRefererAuthorityPort(string $authority, string $referer): string {
        $authority = trim($authority);
        if ($authority === '' || $referer === '') {
            return $authority;
        }

        $authorityParts = parse_url(str_contains($authority, '://') ? $authority : ('tcp://' . $authority));
        $refererParts = parse_url($referer);
        if (!is_array($authorityParts) || !is_array($refererParts)) {
            return $authority;
        }

        $authorityHost = strtolower(trim((string)($authorityParts['host'] ?? $authorityParts['path'] ?? '')));
        $refererHost = strtolower(trim((string)($refererParts['host'] ?? '')));
        if ($authorityHost === '' || $refererHost === '' || $authorityHost !== $refererHost) {
            return $authority;
        }

        $authorityPort = (int)($authorityParts['port'] ?? 0);
        $refererPort = (int)($refererParts['port'] ?? 0);
        if ($authorityPort > 0 || $refererPort <= 0) {
            return $authority;
        }

        return $authorityHost . ':' . $refererPort;
    }

    protected function normalizeDashboardSocketAuthority(string $authority): string {
        $authority = trim($authority);
        if ($authority === '') {
            return '127.0.0.1:' . $this->resolvedDashboardPort();
        }
        $parts = parse_url(str_contains($authority, '://') ? $authority : ('tcp://' . $authority));
        if (!is_array($parts)) {
            return $authority;
        }
        $host = trim((string)($parts['host'] ?? ''));
        if ($host === '') {
            $host = trim((string)($parts['path'] ?? ''));
        }
        if ($host === '') {
            return $authority;
        }
        $port = (int)($parts['port'] ?? 0);
        if ($port <= 0 && $this->isLoopbackDashboardHost($host)) {
            $port = $this->resolvedDashboardPort();
        }
        return $port > 0 ? ($host . ':' . $port) : $host;
    }

    protected function isLoopbackDashboardHost(string $host): bool {
        if ($host === '') {
            return true;
        }
        $lowerHost = strtolower($host);
        return $lowerHost === 'localhost'
            || str_starts_with($lowerHost, 'localhost:')
            || $lowerHost === '127.0.0.1'
            || str_starts_with($lowerHost, '127.0.0.1:')
            || $lowerHost === '::1'
            || str_starts_with($lowerHost, '[::1]:');
    }

    protected function dashboardRefererAuthority(string $referer): string {
        if ($referer === '') {
            return '';
        }
        $parts = parse_url($referer);
        if (!is_array($parts)) {
            return '';
        }
        $host = trim((string)($parts['host'] ?? ''));
        if ($host === '') {
            return '';
        }
        $port = (int)($parts['port'] ?? 0);
        return $port > 0 ? ($host . ':' . $port) : $host;
    }

    protected function buildDashboardVersionStatus(bool $forceRefresh = false): array {
        $cached = $this->dashboardVersionStatusCache['value'] ?? null;
        if (!$forceRefresh && is_array($cached) && (int)($this->dashboardVersionStatusCache['expires_at'] ?? 0) > time()) {
            return $cached;
        }
        $dashboardDir = SCF_ROOT . '/build/public/dashboard';
        $versionJson = $dashboardDir . '/version.json';
        $currentDashboardVersion = ['version' => '0.0.0'];
        if (file_exists($versionJson)) {
            $currentDashboardVersion = JsonHelper::recover(File::read($versionJson)) ?: $currentDashboardVersion;
        }

        $dashboardVersion = ['version' => '--'];
        $client = \Scf\Client\Http::create($this->appendRemoteVersionCacheBust(str_replace('version.json', 'dashboard-version.json', (string)ENV_VARIABLES['scf_update_server'])));
        try {
            // dashboard 首页的版本状态只是展示信息，不能因为外部版本源抖动
            // 把 gateway 控制面 `/server` 卡成“像挂了一样”。
            $response = $client->get(self::REMOTE_VERSION_REQUEST_TIMEOUT_SECONDS);
            if (!$response->hasError()) {
                $dashboardVersion = $response->getData();
            }
        } finally {
            $client->close();
        }

        $result = [
            'version' => $currentDashboardVersion['version'] ?? '0.0.0',
            'latest_version' => $dashboardVersion['version'] ?? '--',
        ];
        $this->dashboardVersionStatusCache = [
            'expires_at' => time() + self::DASHBOARD_VERSION_CACHE_TTL_SECONDS,
            'value' => $result,
        ];
        return $result;
    }

    protected function buildFrameworkVersionStatus(?array $upstreams = null, bool $forceRefresh = false): array {
        $remoteVersion = $this->cachedFrameworkRemoteVersion($forceRefresh);
        $upstreams ??= $this->dashboardUpstreams();
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        $activeRecord = function_exists('scf_read_framework_active_record') ? scf_read_framework_active_record() : null;
        $activeFrameworkVersion = (string)($activeRecord['version'] ?? $selected[0]['framework_build_version'] ?? FRAMEWORK_BUILD_VERSION);
        $activeFrameworkBuild = (string)($activeRecord['build'] ?? FRAMEWORK_BUILD_TIME);
        $activeFrameworkReady = function_exists('scf_framework_update_ready') && scf_framework_update_ready();

        return [
            'is_phar' => FRAMEWORK_IS_PHAR,
            'version' => $activeFrameworkVersion,
            'latest_version' => $remoteVersion['version'] ?? FRAMEWORK_BUILD_VERSION,
            'latest_build' => $remoteVersion['build'] ?? $activeFrameworkBuild,
            'build' => FRAMEWORK_BUILD_TIME,
            'gateway_version' => FRAMEWORK_BUILD_VERSION,
            'gateway_build' => FRAMEWORK_BUILD_TIME,
            'gateway_pending_restart' => $activeFrameworkReady,
            'update_ready' => $activeFrameworkReady,
        ];
    }

    protected function cachedFrameworkRemoteVersion(bool $forceRefresh = false): array {
        $cached = $this->frameworkRemoteVersionCache['value'] ?? null;
        if (!$forceRefresh && is_array($cached) && (int)($this->frameworkRemoteVersionCache['expires_at'] ?? 0) > time()) {
            return $cached;
        }

        $remoteVersion = [
            'version' => FRAMEWORK_BUILD_VERSION,
            'build' => FRAMEWORK_BUILD_TIME,
        ];

        $client = \Scf\Client\Http::create($this->appendRemoteVersionCacheBust((string)ENV_VARIABLES['scf_update_server']));
        try {
            $response = $client->get(self::REMOTE_VERSION_REQUEST_TIMEOUT_SECONDS);
            if (!$response->hasError()) {
                $remoteVersion = $response->getData();
            }
        } finally {
            $client->close();
        }

        $this->frameworkRemoteVersionCache = [
            'expires_at' => time() + self::FRAMEWORK_VERSION_CACHE_TTL_SECONDS,
            'value' => $remoteVersion,
        ];

        return $remoteVersion;
    }

    protected function appendRemoteVersionCacheBust(string $url): string {
        if ($url === '') {
            return $url;
        }
        $separator = str_contains($url, '?') ? '&' : '?';
        return $url . $separator . 'time=' . time();
    }

    protected function pushDashboardStatus(?int $fd = null): void {
        $payload = $this->buildDashboardRealtimeStatus();
        $this->pushDashboardEvent($payload, $fd);
    }

    protected function pushDashboardEvent(array $payload, ?int $fd = null): void {
        $message = JsonHelper::toJson($payload);
        if ($fd !== null) {
            if ($this->server->exist($fd) && $this->server->isEstablished($fd)) {
                $this->server->push($fd, $message);
            }
            return;
        }

        foreach (array_keys($this->dashboardClients) as $clientFd) {
            if (!$this->server->exist($clientFd) || !$this->server->isEstablished($clientFd)) {
                unset($this->dashboardClients[$clientFd]);
                $this->syncConsoleSubscriptionState();
                continue;
            }
            $this->server->push($clientFd, $message);
        }
    }

    public function pushConsoleLog(string $time, string $message): bool|int {
        if (defined('IS_GATEWAY_SUB_PROCESS') && IS_GATEWAY_SUB_PROCESS === true) {
            return ConsoleRelay::reportToLocalGateway($time, $message, 'gateway', SERVER_HOST);
        }
        return $this->acceptConsolePayload([
            'time' => $time,
            'message' => $message,
            'source_type' => 'gateway',
            'node' => SERVER_HOST,
        ]);
    }

    protected function acceptConsolePayload(array $payload): bool {
        $message = (string)($payload['message'] ?? '');
        if ($message === '') {
            return false;
        }

        $time = (string)($payload['time'] ?? Console::timestamp());
        $node = (string)($payload['node'] ?? SERVER_HOST);
        $sourceType = (string)($payload['source_type'] ?? 'gateway');
        $oldInstance = (bool)($payload['old_instance'] ?? false);
        $instanceLabel = trim((string)($payload['instance_label'] ?? ''));

        if ($oldInstance) {
            $this->writeRelayedOldInstanceConsole($time, $message, $instanceLabel);
        }

        if ($this->dashboardEnabled()) {
            if (!$this->hasConsoleSubscribers()) {
                return $oldInstance;
            }
            $this->pushDashboardEvent([
                'event' => 'console',
                'message' => ['data' => $message, 'source_type' => $sourceType, 'old_instance' => $oldInstance],
                'time' => $time,
                'node' => $node,
            ]);
            return true;
        }

        if (!ConsoleRelay::remoteSubscribed()) {
            return false;
        }
        if (
            !$this->subProcessManager
            || !$this->subProcessManager->hasProcess('GatewayClusterCoordinator')
        ) {
            return false;
        }
        $this->subProcessManager->sendCommand('console_log', [
            'time' => $time,
            'message' => $message,
            'source_type' => $sourceType,
            'node' => $node,
            'old_instance' => $oldInstance,
        ], ['GatewayClusterCoordinator']);
        return true;
    }

    protected function hasConsoleSubscribers(): bool {
        return !empty($this->dashboardClients);
    }

    protected function writeRelayedOldInstanceConsole(string $time, string $message, string $instanceLabel = ''): void {
        $prefix = $instanceLabel !== '' ? ('#' . $instanceLabel . ' ') : '';
        $suffix = $time !== '' ? " (source_time={$time})" : '';
        echo Console::timestamp() . ' ' . "\033[90m" . $prefix . $message . $suffix . "\e[0m" . PHP_EOL;
        flush();
    }

    protected function syncConsoleSubscriptionState(bool $force = false): void {
        if (!$this->dashboardEnabled()) {
            return;
        }
        $enabled = $this->hasConsoleSubscribers();
        if (!$force && $this->lastConsoleSubscriptionState === $enabled) {
            return;
        }
        $this->lastConsoleSubscriptionState = $enabled;
        ConsoleRelay::setLocalSubscribed($enabled);
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                continue;
            }
            $this->pushConsoleSubscription((int)($node['socket_fd'] ?? 0), $force);
        }
    }

    protected function pushConsoleSubscription(int $fd, bool $force = false): void {
        if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
            return;
        }
        $enabled = $this->hasConsoleSubscribers();
        if (
            !$force
            && isset($this->nodeClients[$fd]['console_subscription'])
            && (bool)$this->nodeClients[$fd]['console_subscription'] === $enabled
        ) {
            return;
        }
        if (isset($this->nodeClients[$fd])) {
            $this->nodeClients[$fd]['console_subscription'] = $enabled;
        }
        $this->server->push($fd, JsonHelper::toJson([
            'event' => 'console_subscription',
            'data' => [
                'enabled' => $enabled,
            ],
        ]));
    }

    protected function proxyMaxInflightRequests(): int {
        $configured = Config::server()['gateway_proxy_max_inflight'] ?? 1024;
        $limit = (int)$configured;
        return $limit > 0 ? $limit : 1024;
    }

    protected function shouldRejectNewRelayConnection(): bool {
        if (!$this->tcpRelayModeEnabled()) {
            return false;
        }
        return $this->tcpRelayHandler()->activeRelayConnectionCount() >= $this->proxyMaxInflightRequests();
    }
}
