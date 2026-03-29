<?php

namespace Scf\Server\Proxy;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Table\Runtime;
use Swoole\Process;
use Swoole\Timer;
use Throwable;

/**
 * Gateway 托管 upstream 的生命周期协调层。
 *
 * 这个 trait 承接 managed upstream 的滚动重启、滚动升级、切流校验、
 * 旧代 quiesce / recycle、pending recycle watcher 与尾流告警等完整状态机。
 * 它的职责是把这条高耦合但内聚的生命周期链从 GatewayServer 主类中移出，
 * 让 GatewayServer 保持“总控入口”角色，而不是继续吞下所有 managed upstream
 * 的细节实现。
 */
trait GatewayManagedUpstreamLifecycleTrait {

    /**
     * 清理所有 pending recycle watcher。
     *
     * @return void
     */
    protected function clearPendingManagedRecycleWatchers(): void {
        foreach ($this->pendingManagedRecycleWatchers as $key => $timerId) {
            Timer::clear($timerId);
            unset($this->pendingManagedRecycleWatchers[$key]);
        }
        $this->pendingManagedRecycleCompletions = [];
    }

    /**
     * 统一入口，当前默认等价于滚动重启。
     *
     * @param array<int, array<string, mixed>>|null $plans 指定的业务实例计划集合
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function restartManagedUpstreams(?array $plans = null): array {
        return $this->rollingRestartManagedUpstreams($plans);
    }

    /**
     * 对 managed upstream 做一次滚动重启。
     *
     * 顺序不能乱：
     * 1. 先拉起新 generation；
     * 2. 等待新实例 serverIsReady；
     * 3. 切 active generation 并校验切流；
     * 4. 再通知旧代进入 quiesce / recycle。
     *
     * @param array<int, array<string, mixed>>|null $plans 指定需要滚动重启的 managed plan 集合
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function rollingRestartManagedUpstreams(?array $plans = null): array {
        $this->managedUpstreamRolling = true;
        try {
            $basePlans = $this->resolveRollingRestartBasePlans($plans);
            if (!$basePlans || !$this->upstreamSupervisor || !$this->launcher) {
                if (!$basePlans) {
                    Console::warning('【Gateway】未找到可重启的业务实例计划');
                } elseif (!$this->upstreamSupervisor) {
                    Console::warning('【Gateway】业务实例管理器未就绪，无法执行滚动重启');
                } elseif (!$this->launcher) {
                    Console::warning('【Gateway】业务实例启动器未就绪，无法执行滚动重启');
                }
                return [
                    'success_count' => 0,
                    'failed_nodes' => [],
                ];
            }

            $generationVersion = $this->buildReloadGenerationVersion($basePlans);
            Console::info("【Gateway】开始滚动重启业务实例: generation={$generationVersion}, count=" . count($basePlans));

            return $this->executeManagedRollingOperation(
                $basePlans,
                $generationVersion,
                'rolling_restart',
                '滚动重启',
                'rolling_restart_activate',
                function (array $plan, int $newRpcPort): array {
                    return [
                        'display_version' => (string)($plan['metadata']['display_version'] ?? $plan['version'] ?? ''),
                    ];
                }
            );
        } finally {
            $this->managedUpstreamRolling = false;
            $this->lastManagedUpstreamRollingAt = time();
            $this->managedUpstreamHealthState = [];
        }
    }

    /**
     * 对 managed upstream 做滚动升级。
     *
     * 它和 rolling restart 的差异主要在 generation 标识和升级上下文，
     * 切流、回滚、回收的控制流程保持一致。
     *
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function rollingUpdateManagedUpstreams(string $type, string $version): array {
        $this->managedUpstreamRolling = true;
        try {
            $basePlans = $this->activeManagedPlans();
            if (!$basePlans || !$this->upstreamSupervisor || !$this->launcher) {
                return [
                    'success_count' => 0,
                    'failed_nodes' => [],
                ];
            }

            $generationVersion = $this->buildRollingGenerationVersion($type, $version);
            Console::info("【Gateway】开始滚动升级业务实例: type={$type}, version={$version}, generation={$generationVersion}, base_count=" . count($basePlans));

            return $this->executeManagedRollingOperation(
                $basePlans,
                $generationVersion,
                'rolling_update',
                '滚动升级',
                'rolling_update_activate',
                static function (array $plan, int $newRpcPort): array {
                    return [];
                }
            );
        } finally {
            $this->managedUpstreamRolling = false;
            $this->lastManagedUpstreamRollingAt = time();
            $this->managedUpstreamHealthState = [];
        }
    }

    /**
     * 统一执行 managed upstream 的滚动切换流程。
     *
     * 滚动重启和滚动升级的差异，主要只在 generation 号与 metadata 补丁；
     * 启动新代、等待 ready、切流校验、旧代 quiesce、失败回滚这条状态机应该
     * 保持完全一致，否则同类问题会在两条链上分别出现。
     *
     * @param array<int, array<string, mixed>> $basePlans 当前活动的旧代 plan 集合
     * @param string $generationVersion 本轮新代 generation 标识
     * @param string $stage 切流阶段标识
     * @param string $operationLabel 日志里使用的中文动作名
     * @param string $activationReason nginx 切流同步原因
     * @param callable $metadataPatchBuilder 构建新 plan metadata 补丁
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function executeManagedRollingOperation(
        array $basePlans,
        string $generationVersion,
        string $stage,
        string $operationLabel,
        string $activationReason,
        callable $metadataPatchBuilder
    ): array {
        $failedNodes = [];
        $successCount = 0;
        $previousActiveVersion = (string)($this->instanceManager->state()['active_version'] ?? ($basePlans[0]['version'] ?? ''));
        $newPlans = [];
        $registeredPlans = [];
        $reserved = $this->collectReservedPorts();

        foreach ($basePlans as $plan) {
            $newPlan = $this->buildRollingManagedPlan($plan, $generationVersion, $reserved, $metadataPatchBuilder);
            $newPlans[] = $newPlan;
            Console::info("【Gateway】启动新业务实例: " . $this->describePlan($newPlan));

            $host = (string)($newPlan['host'] ?? '127.0.0.1');
            $newPort = (int)($newPlan['port'] ?? 0);
            $newRpcPort = (int)($newPlan['rpc_port'] ?? 0);
            $this->upstreamSupervisor->sendCommand(['action' => 'spawn', 'plan' => $newPlan]);
            if (!$this->launcher->waitUntilServicesReady($host, $newPort, $newRpcPort, 30)) {
                $failedNodes[] = [
                    'host' => $host . ':' . $newPort,
                    'error' => '新业务实例未在预期时间内完成启动',
                ];
                continue;
            }
            Console::success("【Gateway】新业务实例已通过就绪探测，等待切换流量: " . $this->describePlan($newPlan));

            $this->registerManagedPlan($newPlan, false);
            $registeredPlans[] = $newPlan;
            $successCount++;
        }

        if ($failedNodes) {
            Console::warning("【Gateway】{$operationLabel}失败，开始回收新实例: generation={$generationVersion}, failed=" . count($failedNodes));
            foreach ($registeredPlans as $plan) {
                $this->stopManagedPlan($plan, false);
            }
            foreach ($newPlans as $plan) {
                $this->removeManagedPlan($plan);
            }
            return [
                'success_count' => $successCount,
                'failed_nodes' => $failedNodes,
            ];
        }

        $this->instanceManager->activateVersion($generationVersion, self::ROLLING_DRAIN_GRACE_SECONDS);
        $this->notifyManagedUpstreamGenerationIterated($generationVersion);
        $this->syncNginxProxyTargets($activationReason);
        if (!$this->waitForManagedGenerationCutover($generationVersion, $registeredPlans, $stage)) {
            Console::warning("【Gateway】{$operationLabel}切换校验失败，回滚旧业务实例: generation={$generationVersion}");
            $this->rollbackManagedGenerationCutover($previousActiveVersion, $generationVersion, $registeredPlans, $newPlans, $stage);
            return [
                'success_count' => 0,
                'failed_nodes' => [[
                    'host' => $generationVersion,
                    'error' => '新业务实例切换校验失败，已回滚，旧实例保持运行',
                ]],
            ];
        }
        foreach ($newPlans as $plan) {
            $this->appendManagedPlan($plan);
        }
        foreach ($basePlans as $plan) {
            $this->quiesceManagedPlanBusinessPlane($plan);
        }
        Console::success("【Gateway】{$operationLabel}完成并切换流量: generation={$generationVersion}, success={$successCount}, drain_grace=" . self::ROLLING_DRAIN_GRACE_SECONDS . "s");
        $this->logPreviousGenerationTransition($previousActiveVersion);

        return [
            'success_count' => $successCount,
            'failed_nodes' => [],
        ];
    }

    /**
     * 为滚动切换构造新 generation 的 managed plan。
     *
     * 这里统一负责分配新端口、复制旧 plan 的运行元数据，并追加本轮切换需要的
     * metadata 补丁，避免滚动重启和滚动升级各自维护一份近似构造逻辑。
     *
     * @param array<string, mixed> $plan 旧 generation plan
     * @param string $generationVersion 新 generation 标识
     * @param array<int, int> $reserved 已保留端口列表，会在方法内追加新端口
     * @param callable $metadataPatchBuilder metadata 补丁构造器
     * @return array<string, mixed>
     */
    protected function buildRollingManagedPlan(
        array $plan,
        string $generationVersion,
        array &$reserved,
        callable $metadataPatchBuilder
    ): array {
        $newPort = $this->allocateManagedPort(max(1025, (int)($plan['port'] ?? 0) + 1), $reserved);
        $reserved[] = $newPort;
        $newRpcPort = 0;
        if ($this->rpcPort > 0) {
            $newRpcPort = $this->allocateManagedPort(max(1025, (int)($plan['rpc_port'] ?? $this->rpcPort) + 1), $reserved);
            $reserved[] = $newRpcPort;
        }

        $newPlan = $plan;
        $newPlan['version'] = $generationVersion;
        $newPlan['port'] = $newPort;
        $newPlan['rpc_port'] = $newRpcPort;
        $newPlan['metadata'] = array_merge(
            (array)($plan['metadata'] ?? []),
            [
                'managed' => true,
                'role' => (string)($plan['role'] ?? SERVER_ROLE),
                'rpc_port' => $newRpcPort,
                'started_at' => time(),
                'managed_mode' => 'gateway_supervisor',
                'rolling_from_version' => (string)($plan['version'] ?? ''),
            ],
            (array)$metadataPatchBuilder($plan, $newRpcPort)
        );

        return $newPlan;
    }

    /**
     * 为滚动重启解析基准计划。
     *
     * 重启链允许从 registry 快照恢复 managed plan，这是为了覆盖“gateway
     * 自身刚重拉，但业务实例仍存活”的场景；恢复出的计划会重新回填到内存表。
     *
     * @param array<int, array<string, mixed>>|null $plans 指定的计划集合
     * @return array<int, array<string, mixed>>
     */
    protected function resolveRollingRestartBasePlans(?array $plans = null): array {
        $basePlans = $plans ?? $this->activeManagedPlans();
        if (!$basePlans) {
            $basePlans = $this->managedPlansFromSnapshot();
            if ($basePlans) {
                foreach ($basePlans as $plan) {
                    $this->appendManagedPlan($plan);
                }
                Console::warning("【Gateway】业务实例计划内存为空，已从运行态快照恢复: count=" . count($basePlans));
            }
        }
        return array_values(array_filter($basePlans, static function ($plan) {
            return (int)($plan['port'] ?? 0) > 0;
        }));
    }

    /**
     * 校验新 generation 是否已经真正接管流量。
     *
     * @param string $generationVersion 新 generation 版本号
     * @param array<int, array<string, mixed>> $plans
     * @param string $stage 当前校验阶段名
     * @return bool
     */
    protected function waitForManagedGenerationCutover(string $generationVersion, array $plans, string $stage): bool {
        if ($generationVersion === '' || !$plans) {
            return false;
        }
        $deadline = microtime(true) + self::ROLLING_CUTOVER_VERIFY_TIMEOUT_SECONDS;
        $stableChecks = 0;
        $lastReason = 'unknown';
        while (microtime(true) < $deadline) {
            $snapshot = $this->instanceManager->snapshot();
            if ((string)($snapshot['active_version'] ?? '') !== $generationVersion) {
                $lastReason = 'active_version_not_switched';
                $stableChecks = 0;
                usleep(200000);
                continue;
            }
            $allHealthy = true;
            foreach ($plans as $plan) {
                $probe = $this->probeManagedUpstreamHealth($plan);
                if ($probe['healthy']) {
                    continue;
                }
                $allHealthy = false;
                $lastReason = (string)($probe['reason'] ?? 'unknown');
                $stableChecks = 0;
                break;
            }
            if (!$allHealthy) {
                usleep(200000);
                continue;
            }
            $stableChecks++;
            if ($stableChecks >= self::ROLLING_CUTOVER_STABLE_CHECKS) {
                Console::success("【Gateway】新业务实例切换校验通过: stage={$stage}, generation={$generationVersion}, checks={$stableChecks}");
                return true;
            }
            usleep(200000);
        }
        Console::warning("【Gateway】新业务实例切换校验超时: stage={$stage}, generation={$generationVersion}, reason={$lastReason}");
        return false;
    }

    /**
     * 在新 generation 切流失败时把流量恢复到旧 generation，并清理新代。
     *
     * @param string $previousActiveVersion 回滚目标 generation
     * @param string $generationVersion 本次失败的新 generation
     * @param array<int, array<string, mixed>> $registeredPlans 已注册进 registry 的新计划
     * @param array<int, array<string, mixed>> $newPlans 本轮启动过的新计划
     * @param string $stage 当前切换阶段
     * @return void
     */
    protected function rollbackManagedGenerationCutover(string $previousActiveVersion, string $generationVersion, array $registeredPlans, array $newPlans, string $stage): void {
        $snapshot = $this->instanceManager->state();
        if ($previousActiveVersion !== ''
            && $previousActiveVersion !== $generationVersion
            && isset(($snapshot['generations'] ?? [])[$previousActiveVersion])) {
            $this->instanceManager->activateVersion($previousActiveVersion, self::ROLLING_DRAIN_GRACE_SECONDS);
            $this->notifyManagedUpstreamGenerationIterated($previousActiveVersion);
            $this->syncNginxProxyTargets($stage . '_rollback');
            Console::warning("【Gateway】已回滚业务流量到旧实例: generation={$previousActiveVersion}");
        }
        foreach ($registeredPlans as $plan) {
            $this->stopManagedPlan($plan);
        }
        foreach ($newPlans as $plan) {
            $this->removeManagedPlan($plan);
        }
    }

    /**
     * 记录旧 generation 进入 draining 的起止时间窗口。
     *
     * @param string $version 旧 generation 版本号
     * @return void
     */
    protected function logDrainingGenerationSchedule(string $version): void {
        if ($version === '') {
            return;
        }
        $snapshot = $this->instanceManager->snapshot();
        $generation = (array)(($snapshot['generations'] ?? [])[$version] ?? []);
        if (($generation['status'] ?? '') !== 'draining') {
            return;
        }
        Console::info(
            "【Gateway】旧业务实例进入 draining: generation={$version}, "
            . "started_at=" . (int)($generation['drain_started_at'] ?? 0)
            . ", deadline_at=" . (int)($generation['drain_deadline_at'] ?? 0)
        );
    }

    /**
     * 记录切流后旧 generation 的状态。
     *
     * @param string $version 旧 generation 版本号
     * @return void
     */
    protected function logPreviousGenerationTransition(string $version): void {
        if ($version === '') {
            return;
        }
        $snapshot = $this->instanceManager->snapshot();
        $generation = (array)(($snapshot['generations'] ?? [])[$version] ?? []);
        $status = (string)($generation['status'] ?? '');
        if ($status === 'draining') {
            Console::info("【Gateway】旧业务实例保持服务尾流，待 nginx 切换稳定且无在途请求后再进入 shutdown: previous={$version}");
            $this->logDrainingGenerationSchedule($version);
            return;
        }
        if ($status === 'offline') {
            Console::info("【Gateway】旧业务实例已不可达或已无尾流，直接进入回收: previous={$version}");
        }
    }

    /**
     * 请求 supervisor 回收一个 managed plan。
     *
     * @param array<string, mixed> $plan 目标 managed plan
     * @param bool $removeState 回收完成后是否从 registry 中移除实例状态
     * @param bool $removePlan 回收完成后是否从内存计划表中移除
     * @return void
     */
    protected function stopManagedPlan(array $plan, bool $removeState = true, bool $removePlan = true): void {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $plan = $this->hydrateManagedPlanRuntimeMetadata($plan);
        $key = ((string)($plan['version'] ?? '') . '@' . $host . ':' . $port);
        $descriptor = $this->managedPlanDescriptor($plan);

        if ($this->upstreamSupervisor) {
            if (isset($this->pendingManagedRecycles[$key])) {
                return;
            }
            unset($this->managedUpstreamHealthState[$key]);
            if (!$this->upstreamSupervisor->sendCommand([
                'action' => 'stop_instance',
                'instance' => [
                    'version' => (string)($plan['version'] ?? ''),
                    'host' => $host,
                    'port' => $port,
                    'rpc_port' => (int)($plan['rpc_port'] ?? 0),
                    'metadata' => [
                        'managed' => true,
                        'rpc_port' => (int)($plan['rpc_port'] ?? 0),
                        'pid' => (int)(($plan['metadata']['pid'] ?? 0)),
                    ],
                ],
            ])) {
                Console::warning("【Gateway】旧业务实例回收命令发送失败 {$descriptor}");
                return;
            }
            $this->pendingManagedRecycles[$key] = [
                'plan' => $plan,
                'remove_state' => $removeState,
                'remove_plan' => $removePlan,
                'requested_at' => time(),
            ];
            unset($this->pendingManagedRecycleWarnState[$key]);
            $this->ensurePendingManagedRecycleWatcher($key);
            Console::info("【Gateway】开始回收旧业务实例 {$descriptor}");
            return;
        } elseif ($this->launcher) {
            $rpcPort = (int)($plan['rpc_port'] ?? 0);
            $this->launcher->stop([
                'host' => $host,
                'port' => $port,
                'rpc_port' => $rpcPort,
                'metadata' => [
                    'managed' => true,
                    'rpc_port' => $rpcPort,
                    'pid' => (int)(($plan['metadata']['pid'] ?? 0)),
                ],
            ], AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS);
        }

        if ($removeState) {
            $this->instanceManager->removeInstance($host, $port);
        }
        unset($this->bootstrappedManagedInstances[$key]);
        if ($removePlan) {
            $this->removeManagedPlan($plan);
        }
        if ($port > 0) {
            Console::success("【Gateway】旧业务实例回收完成 {$descriptor}");
        }
    }

    /**
     * 根据 host 筛选当前内存里的 managed plan。
     *
     * @param string $host 目标 host
     * @return array<int, array<string, mixed>>
     */
    protected function matchedPlansByHost(string $host): array {
        $host = trim($host);
        return array_values(array_filter($this->managedUpstreamPlans, function ($plan) use ($host) {
            $planHost = (string)($plan['host'] ?? '127.0.0.1');
            if ($host === $planHost) {
                return true;
            }
            if ($host === 'localhost' && $planHost === '127.0.0.1') {
                return true;
            }
            return false;
        }));
    }

    /**
     * 返回当前 active generation 对应的 managed plan 集合。
     *
     * @return array<int, array<string, mixed>>
     */
    protected function activeManagedPlans(): array {
        $activeVersion = (string)($this->instanceManager->state()['active_version'] ?? '');
        if ($activeVersion !== '') {
            $plans = array_values(array_filter($this->managedUpstreamPlans, static function ($plan) use ($activeVersion) {
                return (string)($plan['version'] ?? '') === $activeVersion;
            }));
            if ($plans) {
                return $plans;
            }
        }

        return array_values(array_filter($this->managedUpstreamPlans, static function ($plan) {
            return (int)($plan['port'] ?? 0) > 0;
        }));
    }

    /**
     * 从 instance snapshot 反推 managed plan，作为内存计划丢失时的兜底恢复。
     *
     * @return array<int, array<string, mixed>>
     */
    protected function managedPlansFromSnapshot(): array {
        $snapshot = $this->instanceManager->snapshot();
        $activeVersion = (string)($snapshot['active_version'] ?? '');
        $plans = [];
        foreach (($snapshot['generations'] ?? []) as $generation) {
            $version = (string)($generation['version'] ?? '');
            $status = (string)($generation['status'] ?? '');
            if ($activeVersion !== '') {
                if ($version !== $activeVersion) {
                    continue;
                }
            } elseif ($status !== 'active') {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                $metadata = (array)($instance['metadata'] ?? []);
                if (($metadata['managed'] ?? false) !== true) {
                    continue;
                }
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($port <= 0) {
                    continue;
                }
                $plans[] = [
                    'app' => APP_DIR_NAME,
                    'env' => SERVER_RUN_ENV,
                    'host' => $host,
                    'version' => (string)($instance['version'] ?? $version),
                    'weight' => (int)($instance['weight'] ?? 100),
                    'role' => (string)($metadata['role'] ?? SERVER_ROLE),
                    'port' => $port,
                    'rpc_port' => (int)($metadata['rpc_port'] ?? 0),
                    'src' => (string)($metadata['src'] ?? APP_SRC_TYPE),
                    'metadata' => array_merge($metadata, [
                        'managed' => true,
                        'managed_mode' => (string)($metadata['managed_mode'] ?? 'gateway_supervisor'),
                        'display_version' => (string)($metadata['display_version'] ?? ($instance['version'] ?? $version)),
                    ]),
                    'start_timeout' => (int)($metadata['start_timeout'] ?? 25),
                    'extra' => $this->normalizeManagedPlanExtraFlags([
                        'src' => (string)($metadata['src'] ?? APP_SRC_TYPE),
                        'metadata' => $metadata,
                    ]),
                ];
            }
        }
        return $plans;
    }

    /**
     * 归一化计划里的额外启动参数，确保 dev/pack/src/gateway_port 语义能继承到子进程。
     *
     * @param array<string, mixed> $plan managed plan
     * @return array<int, string>
     */
    protected function normalizeManagedPlanExtraFlags(array $plan): array {
        $flags = [];
        foreach ((array)($plan['extra'] ?? []) as $flag) {
            if (is_string($flag) && $flag !== '') {
                $flags[] = $flag;
            }
        }
        foreach ((array)(($plan['metadata']['extra_flags'] ?? [])) as $flag) {
            if (is_string($flag) && $flag !== '') {
                $flags[] = $flag;
            }
        }

        $hasDev = false;
        $hasDir = false;
        $hasPhar = false;
        $hasPackMode = false;
        $hasGatewayPort = false;
        foreach ($flags as $flag) {
            $hasDev = $hasDev || $flag === '-dev';
            $hasDir = $hasDir || $flag === '-dir';
            $hasPhar = $hasPhar || $flag === '-phar';
            $hasPackMode = $hasPackMode || in_array($flag, ['-pack', '-nopack'], true);
            $hasGatewayPort = $hasGatewayPort || str_starts_with($flag, '-gateway_port=');
        }

        if (!$hasDev && SERVER_RUN_ENV === 'dev') {
            $flags[] = '-dev';
        }
        if (!$hasPackMode && !(defined('FRAMEWORK_IS_PHAR') && FRAMEWORK_IS_PHAR === true)) {
            $flags[] = '-nopack';
        }
        if (!$hasDir && !$hasPhar) {
            $src = (string)($plan['src'] ?? ($plan['metadata']['src'] ?? APP_SRC_TYPE));
            if ($src === 'dir') {
                $flags[] = '-dir';
            } elseif ($src === 'phar') {
                $flags[] = '-phar';
            }
        }
        if (!$hasGatewayPort && $this->businessPort() > 0) {
            $flags[] = '-gateway_port=' . $this->businessPort();
        }

        $normalized = [];
        foreach ($flags as $flag) {
            if (!is_string($flag) || $flag === '') {
                continue;
            }
            $normalized[$flag] = true;
        }
        return array_keys($normalized);
    }

    /**
     * 构建滚动升级使用的 generation 版本号。
     *
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @return string
     */
    protected function buildRollingGenerationVersion(string $type, string $version): string {
        if ($type === 'app') {
            return $version;
        }
        return $type . '-' . $version . '-' . date('YmdHis');
    }

    /**
     * 构建普通 reload 使用的 generation 标识。
     *
     * @param array<int, array<string, mixed>> $plans 当前 active plan 集合
     * @return string
     */
    protected function buildReloadGenerationVersion(array $plans): string {
        $displayVersion = (string)($plans[0]['metadata']['display_version'] ?? $plans[0]['version'] ?? App::version() ?? 'reload');
        return $displayVersion . '-reload-' . date('YmdHis');
    }

    /**
     * 收集 gateway 与 managed upstream 当前占用或保留的端口。
     *
     * @return array<int, int>
     */
    protected function collectReservedPorts(): array {
        $ports = [$this->port];
        if ($this->rpcPort > 0) {
            $ports[] = $this->rpcPort;
        }
        foreach ($this->managedUpstreamPlans as $plan) {
            $planPort = (int)($plan['port'] ?? 0);
            $planRpcPort = (int)($plan['rpc_port'] ?? 0);
            $planPort > 0 and $ports[] = $planPort;
            $planRpcPort > 0 and $ports[] = $planRpcPort;
        }
        foreach (($this->instanceManager->snapshot()['generations'] ?? []) as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                $instancePort = (int)($instance['port'] ?? 0);
                $instanceRpcPort = (int)(($instance['metadata']['rpc_port'] ?? 0));
                $instancePort > 0 and $ports[] = $instancePort;
                $instanceRpcPort > 0 and $ports[] = $instanceRpcPort;
            }
        }
        return array_values(array_unique(array_filter($ports)));
    }

    /**
     * 为新的 managed upstream 分配未占用端口。
     *
     * @param int $startPort 起始扫描端口
     * @param array<int, int> $reserved 已保留端口列表
     * @return int
     */
    protected function allocateManagedPort(int $startPort, array $reserved): int {
        $port = max(1025, $startPort);
        while (true) {
            if (in_array($port, $reserved, true)) {
                $port++;
                continue;
            }
            if (!$this->launcher) {
                return $port;
            }
            if ($this->launcher->isListening('127.0.0.1', $port, 0.1) || $this->launcher->isListening('0.0.0.0', $port, 0.1)) {
                $port++;
                continue;
            }
            return $port;
        }
    }

    /**
     * 将一个 managed plan 注册到 instance registry，并按需激活该 generation。
     *
     * @param array<string, mixed> $plan managed plan
     * @param bool $activate 是否立即激活该 generation
     * @return void
     */
    protected function registerManagedPlan(array $plan, bool $activate = false): void {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            return;
        }
        $weight = (int)($plan['weight'] ?? 100);
        $metadata = (array)($plan['metadata'] ?? []);
        $metadata['managed'] = true;
        $metadata['role'] = (string)($plan['role'] ?? ($metadata['role'] ?? SERVER_ROLE));
        $metadata['rpc_port'] = (int)($plan['rpc_port'] ?? ($metadata['rpc_port'] ?? 0));
        $metadata['started_at'] = (int)($metadata['started_at'] ?? time());
        $metadata['managed_mode'] = 'gateway_supervisor';
        $metadata['src'] = (string)($plan['src'] ?? ($metadata['src'] ?? APP_SRC_TYPE));
        $metadata['start_timeout'] = (int)($plan['start_timeout'] ?? ($metadata['start_timeout'] ?? 25));
        $metadata['extra_flags'] = $this->normalizeManagedPlanExtraFlags($plan);
        $this->instanceManager->registerUpstream($version, $host, $port, $weight, $metadata);
        if ($activate) {
            $this->instanceManager->activateVersion($version, 0);
            $this->notifyManagedUpstreamGenerationIterated($version);
        }
        $this->bootstrappedManagedInstances[$version . '@' . $host . ':' . $port] = true;
    }

    /**
     * 把新启动的 managed plan 追加进内存计划表。
     *
     * @param array<string, mixed> $plan managed plan
     * @return void
     */
    protected function appendManagedPlan(array $plan): void {
        foreach ($this->managedUpstreamPlans as $existing) {
            if ((string)($existing['version'] ?? '') === (string)($plan['version'] ?? '')
                && (string)($existing['host'] ?? '') === (string)($plan['host'] ?? '')
                && (int)($existing['port'] ?? 0) === (int)($plan['port'] ?? 0)) {
                return;
            }
        }
        $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
        $this->managedUpstreamPlans[] = $plan;
    }

    /**
     * 将运行期探测到的 metadata 合并回内存计划表。
     *
     * @param string $host 实例 host
     * @param int $port 实例端口
     * @param array<string, mixed> $metadataPatch 需要合并的元数据补丁
     * @return void
     */
    protected function mergeManagedPlanMetadata(string $host, int $port, array $metadataPatch): void {
        if ($host === '' || $port <= 0 || !$metadataPatch) {
            return;
        }
        foreach ($this->managedUpstreamPlans as &$plan) {
            if ((string)($plan['host'] ?? '') !== $host || (int)($plan['port'] ?? 0) !== $port) {
                continue;
            }
            $plan['metadata'] = array_merge((array)($plan['metadata'] ?? []), $metadataPatch);
        }
        unset($plan);
    }

    /**
     * 用 registry/snapshot 中的最新运行时信息补全 managed plan。
     *
     * @param array<string, mixed> $plan 原始 managed plan
     * @return array<string, mixed>
     */
    protected function hydrateManagedPlanRuntimeMetadata(array $plan): array {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($host === '' || $port <= 0) {
            $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
            return $plan;
        }
        $snapshot = $this->instanceManager->snapshot();
        foreach (($snapshot['generations'] ?? []) as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((string)($instance['host'] ?? '') !== $host || (int)($instance['port'] ?? 0) !== $port) {
                    continue;
                }
                $plan['metadata'] = array_merge((array)($plan['metadata'] ?? []), (array)($instance['metadata'] ?? []));
                $plan['rpc_port'] = (int)($plan['rpc_port'] ?? (($instance['metadata']['rpc_port'] ?? 0)));
                $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
                return $plan;
            }
        }
        $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
        return $plan;
    }

    /**
     * 从内存计划表移除指定的 managed plan。
     *
     * @param array<string, mixed> $plan 目标 managed plan
     * @return void
     */
    protected function removeManagedPlan(array $plan): void {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $this->managedUpstreamPlans = array_values(array_filter($this->managedUpstreamPlans, static function ($item) use ($version, $host, $port) {
            return !(
                (string)($item['version'] ?? '') === $version
                && (string)($item['host'] ?? '127.0.0.1') === $host
                && (int)($item['port'] ?? 0) === $port
            );
        }));
    }

    /**
     * 生成人类可读的 plan 描述，用于日志串联整条生命周期。
     *
     * @param array<string, mixed> $plan managed plan
     * @return string
     */
    protected function managedPlanDescriptor(array $plan): string {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $parts = [];
        $version !== '' and $parts[] = "generation={$version}";
        $parts[] = "http={$host}:{$port}";
        $rpcPort > 0 and $parts[] = "rpc={$rpcPort}";
        return implode(', ', $parts);
    }

    /**
     * 在新代切流成功后，让旧业务实例停止接收新业务。
     *
     * @param array<string, mixed> $plan 旧 generation 对应的 managed plan
     * @return void
     */
    protected function quiesceManagedPlanBusinessPlane(array $plan): void {
        if (!$this->launcher) {
            return;
        }
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($port <= 0) {
            return;
        }
        if (!$this->launcher->requestBusinessQuiesce($host, $port, 1.0)) {
            $probe = $this->probeManagedUpstreamHealth($plan);
            $reachability = $this->managedPlanReachability($plan);
            $unrecoverableReasons = [
                'http_port_down',
                'rpc_port_down',
                'health_status_unreachable',
                'http_probe_failed',
                'server_not_alive',
                'server_not_ready',
            ];
            if (
                (!$reachability['http_listening'] && !$reachability['rpc_listening'])
                || (!$probe['healthy'] && in_array((string)($probe['reason'] ?? ''), $unrecoverableReasons, true))
            ) {
                Console::warning(
                    "【Gateway】旧业务实例已不可达，直接进入回收: " . $this->managedPlanDescriptor($plan)
                    . ", reason=" . (string)($probe['reason'] ?? 'unknown')
                    . ", http=" . ($reachability['http_listening'] ? 'listening' : 'down')
                    . ", rpc=" . ($reachability['rpc_port'] > 0 ? ($reachability['rpc_listening'] ? 'listening' : 'down') : 'n/a')
                    . ", process=" . ($reachability['pid_alive'] ? 'alive' : 'down')
                );
                $this->instanceManager->markInstanceOffline($host, $port, $version);
                $this->stopManagedPlan($plan, true);
                return;
            }
            Console::warning("【Gateway】旧业务实例业务平面静默失败: " . $this->managedPlanDescriptor($plan));
            return;
        }
        $runtimeStatus = $this->fetchUpstreamRuntimeStatus($plan);
        $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
        $httpProcessing = (int)($runtimeStatus['http_request_processing'] ?? 0);
        $rpcProcessing = (int)($runtimeStatus['rpc_request_processing'] ?? 0);
        Console::info(
            "【Gateway】旧业务实例开始平滑回收: " . $this->managedPlanDescriptor($plan)
            . ", ws=" . $gatewayWs
            . ", http=" . $httpProcessing
            . ", rpc=" . $rpcProcessing
            . ", queue=" . (int)($runtimeStatus['redis_queue_processing'] ?? 0)
            . ", crontab=" . (int)($runtimeStatus['crontab_busy'] ?? 0)
        );
        $disconnected = $this->disconnectManagedPlanClients($plan);
        if ($disconnected > 0) {
            $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
            Console::info(
                "【Gateway】旧业务实例客户端连接已主动断开: " . $this->managedPlanDescriptor($plan)
                . ", disconnected={$disconnected}, remaining_ws={$gatewayWs}"
            );
        }
        if ($gatewayWs === 0 && $httpProcessing === 0 && $rpcProcessing === 0) {
            Console::info("【Gateway】旧业务实例已无在途请求，立即进入 shutdown: " . $this->managedPlanDescriptor($plan));
            $this->stopManagedPlan($plan, true);
        }
    }

    /**
     * 主动断开仍绑定在旧 managed plan 上的全部客户端连接。
     *
     * @param array<string, mixed> $plan 旧 generation 对应的 managed plan。
     * @return int 实际断开的客户端数量。
     */
    protected function disconnectManagedPlanClients(array $plan): int {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            return 0;
        }

        $fds = $this->instanceManager->gatewayConnectionFdsFor($version, $host, $port);
        if (!$fds) {
            return 0;
        }

        $disconnected = 0;
        foreach ($fds as $fd) {
            try {
                $this->tcpRelayHandler()->disconnectClient($fd);
                $disconnected++;
            } catch (Throwable) {
            }
        }
        return $disconnected;
    }

    /**
     * 辅助判断旧实例当前是“还能服务但在排空”，还是“已经不可达”。
     *
     * @param array<string, mixed> $plan 旧实例 plan
     * @return array{http_listening: bool, rpc_listening: bool, pid_alive: bool, rpc_port: int}
     */
    protected function managedPlanReachability(array $plan): array {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? (($plan['metadata']['rpc_port'] ?? 0)));
        $pid = (int)($plan['metadata']['pid'] ?? 0);

        $httpListening = $port > 0 && (
            $this->launcher->isListening($host, $port, 0.2)
            || $this->launcher->isListening('0.0.0.0', $port, 0.2)
        );
        $rpcListening = $rpcPort > 0 && (
            $this->launcher->isListening($host, $rpcPort, 0.2)
            || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.2)
        );
        $pidAlive = $pid > 0 && Process::kill($pid, 0);

        return [
            'http_listening' => $httpListening,
            'rpc_listening' => $rpcListening,
            'pid_alive' => $pidAlive,
            'rpc_port' => $rpcPort,
        ];
    }

    /**
     * 清理已经 offline 的 managed upstream generation。
     *
     * @return void
     */
    protected function cleanupOfflineManagedUpstreams(): void {
        $snapshot = $this->instanceManager->snapshot();
        $removedVersions = [];
        foreach (($snapshot['generations'] ?? []) as $generation) {
            if (($generation['status'] ?? '') !== 'offline') {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((($instance['metadata']['managed'] ?? false) !== true)) {
                    continue;
                }
                $plan = [
                    'version' => (string)($instance['version'] ?? $generation['version'] ?? ''),
                    'host' => (string)($instance['host'] ?? '127.0.0.1'),
                    'port' => (int)($instance['port'] ?? 0),
                    'rpc_port' => (int)(($instance['metadata']['rpc_port'] ?? 0)),
                ];
                if ($this->isManagedPlanRecyclePending($plan)) {
                    continue;
                }
                $this->stopManagedPlan($plan);
            }
            $version = (string)($generation['version'] ?? '');
            if ($this->hasPendingRecycleForVersion($version)) {
                continue;
            }
            $this->instanceManager->removeVersion($version);
            if ($version !== '') {
                $removedVersions[] = $version;
            }
        }
        if ($removedVersions) {
            if ($this->startupSummaryPending()) {
                $this->recordStartupRemovedGenerations($removedVersions);
            } else {
                Console::success("【Gateway】旧业务实例代际已移除: count=" . count($removedVersions) . ", generations=" . implode(' | ', $removedVersions));
            }
        }
    }

    /**
     * 主动轮询全部 pending recycle 项。
     *
     * @return void
     */
    protected function pollPendingManagedRecycles(): void {
        if (!$this->launcher || !$this->pendingManagedRecycles) {
            return;
        }

        foreach (array_keys($this->pendingManagedRecycles) as $key) {
            $this->checkPendingManagedRecycle($key);
        }
    }

    /**
     * 为 pending recycle 项建立 watcher。
     *
     * @param string $key
     * @return void
     */
    protected function ensurePendingManagedRecycleWatcher(string $key): void {
        if (isset($this->pendingManagedRecycleWatchers[$key]) || !isset($this->pendingManagedRecycles[$key])) {
            return;
        }

        $timerId = Timer::tick(1000, function () use ($key) {
            if (!isset($this->pendingManagedRecycleWatchers[$key])) {
                return;
            }
            if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                $this->clearPendingManagedRecycleWatcher($key);
                return;
            }
            if ($this->checkPendingManagedRecycle($key)) {
                $this->clearPendingManagedRecycleWatcher($key);
            }
        });
        $this->pendingManagedRecycleWatchers[$key] = $timerId;
    }

    /**
     * 清理单个 pending recycle watcher。
     *
     * @param string $key
     * @return void
     */
    protected function clearPendingManagedRecycleWatcher(string $key): void {
        if (!isset($this->pendingManagedRecycleWatchers[$key])) {
            return;
        }
        Timer::clear($this->pendingManagedRecycleWatchers[$key]);
        unset($this->pendingManagedRecycleWatchers[$key]);
    }

    /**
     * 轮询某个 pending recycle 的最终完成态。
     *
     * @param string $key pending recycle 项唯一键
     * @return bool true 表示 watcher 可移除；false 表示仍需继续轮询
     */
    protected function checkPendingManagedRecycle(string $key): bool {
        if (!$this->launcher || !isset($this->pendingManagedRecycles[$key])) {
            return true;
        }
        if (($this->pendingManagedRecycleCompletions[$key] ?? false) === true) {
            return false;
        }

        $item = $this->pendingManagedRecycles[$key];
        $plan = (array)($item['plan'] ?? []);
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $httpAlive = $port > 0 && ($this->launcher->isListening($host, $port, 0.1) || $this->launcher->isListening('0.0.0.0', $port, 0.1));
        $rpcAlive = $rpcPort > 0 && ($this->launcher->isListening($host, $rpcPort, 0.1) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.1));
        if ($httpAlive || $rpcAlive) {
            $requestedAt = (int)($item['requested_at'] ?? time());
            $elapsed = max(0, time() - $requestedAt);
            if ($elapsed >= 60) {
                $lastWarnAt = (int)($this->pendingManagedRecycleWarnState[$key] ?? 0);
                if ($lastWarnAt <= 0 || (time() - $lastWarnAt) >= 60) {
                    $this->pendingManagedRecycleWarnState[$key] = time();
                    Console::warning(
                        "【Gateway】旧业务实例回收等待中: " . $this->managedPlanDescriptor($plan)
                        . ", waiting={$elapsed}s, http=" . ($httpAlive ? 'listening' : 'closed')
                        . ", rpc=" . ($rpcAlive ? 'listening' : 'closed')
                    );
                }
            }
            return false;
        }

        $removeState = (bool)($item['remove_state'] ?? true);
        $removePlan = (bool)($item['remove_plan'] ?? true);
        $this->pendingManagedRecycleCompletions[$key] = true;
        if ($removeState) {
            $this->instanceManager->removeInstance($host, $port);
        }
        unset($this->bootstrappedManagedInstances[$key]);
        if ($removePlan) {
            $this->removeManagedPlan($plan);
        }
        unset($this->managedUpstreamHealthState[$key]);
        $removedGeneration = '';
        if ($removeState) {
            $version = (string)($plan['version'] ?? '');
            if ($version !== '') {
                $snapshot = $this->instanceManager->snapshot();
                $generation = (array)(($snapshot['generations'] ?? [])[$version] ?? []);
                if ($generation && empty($generation['instances'] ?? [])) {
                    $this->instanceManager->removeVersion($version);
                    $removedGeneration = $version;
                }
            }
        }
        unset($this->pendingManagedRecycleWarnState[$key], $this->pendingManagedRecycles[$key], $this->pendingManagedRecycleCompletions[$key]);
        if (!$this->startupSummaryPending()) {
            Console::success("【Gateway】旧业务实例回收完成 " . $this->managedPlanDescriptor($plan));
        }
        if ($removedGeneration !== '') {
            if ($this->startupSummaryPending()) {
                $this->recordStartupRemovedGenerations([$removedGeneration]);
            } else {
                Console::success("【Gateway】旧业务实例代际已移除: count=1, generations={$removedGeneration}");
            }
        }
        $activeVersion = (string)($this->instanceManager->snapshot()['active_version'] ?? '');
        if ($activeVersion !== '') {
            $this->notifyManagedUpstreamGenerationIterated($activeVersion);
        } else {
            $this->lastManagedHealthActiveVersion = '';
            $this->managedUpstreamHealthState = [];
            $this->lastManagedUpstreamHealthCheckAt = 0;
        }
        return true;
    }

    /**
     * 判断某个 plan 是否已经处于 recycle pending 状态。
     *
     * @param array<string, mixed> $plan
     * @return bool
     */
    protected function isManagedPlanRecyclePending(array $plan): bool {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $key = ((string)($plan['version'] ?? '') . '@' . $host . ':' . $port);
        return isset($this->pendingManagedRecycles[$key]);
    }

    /**
     * 判断指定 generation 是否仍有 recycle pending 项。
     *
     * @param string $version
     * @return bool
     */
    protected function hasPendingRecycleForVersion(string $version): bool {
        if ($version === '') {
            return false;
        }
        foreach ($this->pendingManagedRecycles as $item) {
            $plan = (array)($item['plan'] ?? []);
            if ((string)($plan['version'] ?? '') === $version) {
                return true;
            }
        }
        return false;
    }

    /**
     * 对长时间 draining 的旧 generation 输出诊断告警。
     *
     * @return void
     */
    protected function warnStuckDrainingManagedUpstreams(): void {
        $diagnostics = $this->instanceManager->drainingDiagnostics();
        $activeVersions = [];
        $now = time();
        foreach ($diagnostics as $item) {
            $version = (string)($item['version'] ?? '');
            if ($version === '') {
                continue;
            }
            $activeVersions[$version] = true;
            $startedAt = (int)($item['drain_started_at'] ?? 0);
            if ($startedAt <= 0) {
                continue;
            }
            $elapsed = $now - $startedAt;
            $warnAfter = max(
                self::ROLLING_DRAIN_GRACE_SECONDS,
                max(1, (int)(($item['drain_deadline_at'] ?? 0) - $startedAt))
            );
            if ($elapsed < $warnAfter) {
                continue;
            }
            $lastWarnAt = (int)($this->drainingGenerationWarnState[$version] ?? 0);
            if ($lastWarnAt > 0 && ($now - $lastWarnAt) < 60) {
                continue;
            }
            $this->drainingGenerationWarnState[$version] = $now;
            Console::warning(
                "【Gateway】旧业务实例仍在 draining，尚未进入 shutdown: generation={$version}, "
                . "draining={$elapsed}s, ws=" . (int)($item['connections'] ?? 0)
                . ", http=" . (int)($item['http_processing'] ?? 0)
                . ", rpc=" . (int)($item['rpc_processing'] ?? 0)
                . ", queue=" . (int)($item['redis_queue_processing'] ?? 0)
                . ", crontab=" . (int)($item['crontab_busy'] ?? 0)
                . ", deadline_at=" . (int)($item['drain_deadline_at'] ?? 0)
            );
        }
        foreach (array_keys($this->drainingGenerationWarnState) as $version) {
            if (!isset($activeVersions[$version])) {
                unset($this->drainingGenerationWarnState[$version]);
            }
        }
    }
}
