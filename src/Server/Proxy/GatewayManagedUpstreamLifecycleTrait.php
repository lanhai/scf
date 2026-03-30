<?php

namespace Scf\Server\Proxy;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Server as CoreServer;
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
     * 在真正拉起 managed upstream 前，确认计划端口没有被外部进程抢占。
     *
     * gateway 控制面端口是固定入口，冲突时可以按“回收旧自己”的策略处理；
     * 但 upstream 端口属于内部动态端口，若被无关进程占用，绝不能直接 kill 对方。
     * 这里统一在 spawn 前识别“当前端口上的监听者是不是自家的 managed upstream”，
     * 若不是且计划允许动态端口，就改分配下一个可用端口。
     *
     * @param array<string, mixed> $plan 原始 managed plan
     * @return array<string, mixed> 调整后的 managed plan
     */
    protected function resolveManagedBootstrapPlan(array $plan): array {
        if (!$this->launcher) {
            return $plan;
        }

        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $metadata = (array)($plan['metadata'] ?? []);
        $dynamicPort = !array_key_exists('dynamic_port', $metadata) || (bool)$metadata['dynamic_port'];
        $dynamicRpcPort = !array_key_exists('dynamic_rpc_port', $metadata) || (bool)$metadata['dynamic_rpc_port'];
        $httpPortReassigned = false;
        $reservedPorts = $this->collectReservedPorts();
        $port > 0 && !in_array($port, $reservedPorts, true) and $reservedPorts[] = $port;
        $rpcPort > 0 && !in_array($rpcPort, $reservedPorts, true) and $reservedPorts[] = $rpcPort;

        if ($port > 0 && $this->managedBootstrapPortOccupiedByForeignListener($host, $port)) {
            if ($dynamicPort) {
                $newPort = $this->launcher->findAvailablePortAvoiding(
                    '127.0.0.1',
                    max(1025, $port + 1),
                    $reservedPorts,
                    2000
                );
                Console::warning("【Gateway】业务实例目标端口已被外部进程占用，改用新端口: {$host}:{$port} => {$host}:{$newPort}");
                $metadata['reassigned_from_port'] = $port;
                $plan['port'] = $newPort;
                $port = $newPort;
                !in_array($newPort, $reservedPorts, true) and $reservedPorts[] = $newPort;
                $httpPortReassigned = true;
            } else {
                Console::warning("【Gateway】业务实例目标端口已被外部进程占用，但当前计划不允许改端口: {$host}:{$port}");
            }
        }

        // HTTP 端口一旦因为冲突被换掉，RPC 端口也要重新做一次真实监听探测，
        // 避免出现“HTTP 已避让但 RPC 仍撞在旧实例上”的半迁移状态。
        if ($rpcPort > 0 && ($this->managedBootstrapRpcPortOccupied($rpcPort) || ($httpPortReassigned && $dynamicRpcPort))) {
            if ($dynamicRpcPort) {
                $newRpcPort = $this->launcher->findAvailablePortAvoiding(
                    '127.0.0.1',
                    max(1025, $rpcPort + 1),
                    array_merge($reservedPorts, [$port]),
                    2000
                );
                Console::warning("【Gateway】业务实例RPC端口已被占用，改用新端口: 127.0.0.1:{$rpcPort} => 127.0.0.1:{$newRpcPort}");
                $metadata['reassigned_from_rpc_port'] = $rpcPort;
                $plan['rpc_port'] = $newRpcPort;
            } else {
                Console::warning("【Gateway】业务实例RPC端口已被占用，但当前计划不允许改端口: 127.0.0.1:{$rpcPort}");
            }
        }

        $metadata['dynamic_port'] = $dynamicPort;
        $metadata['dynamic_rpc_port'] = $dynamicRpcPort;
        $plan['metadata'] = $metadata;
        $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
        return $plan;
    }

    /**
     * 判断目标 RPC 端口是否已处于监听态。
     *
     * RPC 端口不暴露 HTTP 状态接口，因此这里不去区分“是否属于自家实例”，
     * 只要端口当前已经有人监听，就让动态 upstream 直接换端口，避免误杀
     * 其他业务或与旧实例互相踩踏。
     *
     * @param int $rpcPort 目标 RPC 端口
     * @return bool true 表示当前 RPC 端口不可复用
     */
    protected function managedBootstrapRpcPortOccupied(int $rpcPort): bool {
        if ($rpcPort <= 0 || !$this->launcher) {
            return false;
        }

        return $this->launcher->isListening('127.0.0.1', $rpcPort, 0.2)
            || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.2)
            || \Scf\Core\Server::isListeningPortInUse($rpcPort);
    }

    /**
     * 判断当前目标 HTTP 端口上是否是“非本应用 managed upstream”的监听者。
     *
     * @param string $host 目标 host
     * @param int $port 目标端口
     * @return bool true 表示端口已被外部监听者占用
     */
    protected function managedBootstrapPortOccupiedByForeignListener(string $host, int $port): bool {
        if ($port <= 0 || !$this->launcher) {
            return false;
        }

        $listening = $this->launcher->isListening($host, $port, 0.2)
            || $this->launcher->isListening('127.0.0.1', $port, 0.2)
            || $this->launcher->isListening('0.0.0.0', $port, 0.2);
        if (!$listening) {
            return false;
        }

        // 端口已在监听时，只有“当前 gateway epoch+port 所属 upstream”才允许复用。
        // 过去仅按 app/fingerprint 识别会把旧代实例误判为可复用，导致把不可用端口
        // 继续下发给新 upstream，最终在 child listen 阶段失败。
        if ($this->managedBootstrapPortOwnedByCurrentGatewayLease($port)) {
            return false;
        }

        $status = $this->fetchUpstreamRuntimeStatusSync('127.0.0.1', $port);
        if (!$status) {
            return true;
        }

        if ((string)($status['name'] ?? '') !== APP_DIR_NAME
            || (string)($status['fingerprint'] ?? '') !== APP_FINGERPRINT) {
            return true;
        }

        // upstream.status 的 owner 字段用于兜底兼容无法读取命令行的场景。
        // 对“同应用但 owner 不明”的监听者默认按外部占用处理，宁可换端口，
        // 也不能把高概率不可用端口继续分配给新实例。
        $expectedEpoch = $this->gatewayLeaseEpoch();
        $expectedGatewayPort = $this->businessPort();
        $statusEpoch = (int)($status['upstream_owner_epoch'] ?? 0);
        $statusGatewayPort = (int)($status['upstream_gateway_port'] ?? 0);
        if ($expectedEpoch <= 0) {
            return true;
        }
        if ($statusEpoch <= 0 || $statusEpoch !== $expectedEpoch) {
            return true;
        }
        if ($expectedGatewayPort > 0 && $statusGatewayPort > 0 && $statusGatewayPort !== $expectedGatewayPort) {
            return true;
        }
        if ($expectedGatewayPort > 0 && $statusGatewayPort <= 0) {
            return true;
        }

        return false;
    }

    /**
     * 判断目标端口监听者是否属于“当前 gateway lease”。
     *
     * 复用端口必须满足以下条件：
     * 1. 监听进程是 `gateway_upstream start`；
     * 2. `-app` 与当前应用一致；
     * 3. `-gateway_port/-gateway_epoch` 与当前 gateway lease 完全一致。
     *
     * 只要任一监听者满足条件，就允许把该端口视作“当前代已接管实例”而复用；
     * 否则统一按外部占用处理，触发动态改端口。
     *
     * @param int $port 目标端口
     * @return bool
     */
    protected function managedBootstrapPortOwnedByCurrentGatewayLease(int $port): bool {
        if ($port <= 0) {
            return false;
        }
        $expectedEpoch = $this->gatewayLeaseEpoch();
        $expectedGatewayPort = $this->businessPort();
        if ($expectedEpoch <= 0 || $expectedGatewayPort <= 0) {
            return false;
        }

        $pids = CoreServer::findPidsByPort($port);
        if (!$pids) {
            return false;
        }

        $appFlag = '-app=' . APP_DIR_NAME;
        $gatewayPortFlag = '-gateway_port=' . $expectedGatewayPort;
        foreach ($pids as $pid) {
            $command = $this->readManagedBootstrapProcessCommand((int)$pid);
            if ($command === '' || !str_contains($command, 'boot gateway_upstream start')) {
                continue;
            }
            if (!str_contains($command, $appFlag) || !str_contains($command, $gatewayPortFlag)) {
                continue;
            }
            if (preg_match('/(?:^|\\s)-gateway_epoch=(\\d+)(?:\\s|$)/', $command, $matches)) {
                $epoch = (int)($matches[1] ?? 0);
                if ($epoch > 0 && $epoch === $expectedEpoch) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 读取指定 PID 的完整命令行。
     *
     * @param int $pid
     * @return string
     */
    protected function readManagedBootstrapProcessCommand(int $pid): string {
        if ($pid <= 0 || !@Process::kill($pid, 0)) {
            return '';
        }
        $command = @shell_exec('ps -o command= -p ' . $pid . ' 2>/dev/null');
        return is_string($command) ? trim($command) : '';
    }

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
        $this->pendingManagedRecycleWarnState = [];
        $this->pendingManagedRecycles = [];
        $this->pendingManagedRecycleCompletions = [];
        $this->pendingManagedRecycleCheckInFlight = [];
        $this->pendingManagedRecycleEndpointReservations = [];
        $this->quarantinedManagedRecycles = [];
        $this->quarantinedManagedRecycleWarnState = [];
        $this->managedRecyclePortCooldowns = [];
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
            try {
                $newPlan = $this->buildRollingManagedPlan($plan, $generationVersion, $reserved, $metadataPatchBuilder);
                $newPlan = $this->resolveManagedBootstrapPlan($newPlan);
                $newPlan = $this->withManagedPlanLeaseBinding($newPlan);
            } catch (Throwable $throwable) {
                $failedNodes[] = [
                    'host' => (string)($plan['host'] ?? '127.0.0.1') . ':' . (int)($plan['port'] ?? 0),
                    'error' => "新业务实例计划构建失败: {$throwable->getMessage()}",
                ];
                Console::error('【Gateway】业务实例计划构建失败: ' . $this->describePlan($plan) . ', error=' . $throwable->getMessage());
                continue;
            }
            $resolvedHttpPort = (int)($newPlan['port'] ?? 0);
            $resolvedRpcPort = (int)($newPlan['rpc_port'] ?? 0);
            $resolvedHttpPort > 0 && !in_array($resolvedHttpPort, $reserved, true) and $reserved[] = $resolvedHttpPort;
            $resolvedRpcPort > 0 && !in_array($resolvedRpcPort, $reserved, true) and $reserved[] = $resolvedRpcPort;
            $newPlans[] = $newPlan;
            Console::info("【Gateway】启动新业务实例: " . $this->describePlan($newPlan));

            $host = (string)($newPlan['host'] ?? '127.0.0.1');
            $newPort = (int)($newPlan['port'] ?? 0);
            $newRpcPort = (int)($newPlan['rpc_port'] ?? 0);
            $this->upstreamSupervisor->sendCommand([
                'action' => 'spawn',
                'owner_epoch' => $this->gatewayLeaseEpoch(),
                'plan' => $newPlan,
            ]);
            if (!$this->launcher->waitUntilServicesReady($host, $newPort, $newRpcPort, 30)) {
                $failedNodes[] = [
                    'host' => $host . ':' . $newPort,
                    'error' => '新业务实例未在预期时间内完成启动',
                ];
                continue;
            }

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
        // 先执行 nginx 同步但延后摘要输出，确保“挂载实例完成”只会在切流校验通过后打印。
        // 这里必须显式校验同步结果：如果 nginx 同步本身失败，不能继续进入切流校验流程。
        $nginxSyncOk = $this->syncNginxProxyTargets($activationReason, [], true);
        if (!$nginxSyncOk) {
            $this->clearPendingNginxSyncSummary();
            $oldPort = (int)(($basePlans[0]['port'] ?? 0));
            $this->logOldInstanceLifecycle(
                "【Gateway】{$operationLabel}切流前同步失败，回滚旧实例: new={$generationVersion}",
                $oldPort
            );
            $this->rollbackManagedGenerationCutover($previousActiveVersion, $generationVersion, $registeredPlans, $newPlans, $stage);
            return [
                'success_count' => 0,
                'failed_nodes' => [[
                    'host' => $generationVersion,
                    'error' => 'nginx转发同步失败，已回滚，旧实例保持运行',
                ]],
            ];
        }
        if (!$this->waitForManagedGenerationCutover($generationVersion, $registeredPlans, $stage)) {
            $this->clearPendingNginxSyncSummary();
            Console::warning("【Gateway】Nginx挂载实例完成日志未输出: stage={$stage}, generation={$generationVersion}, reason=切流校验未通过(已回滚)");
            $oldPort = (int)(($basePlans[0]['port'] ?? 0));
            $this->logOldInstanceLifecycle("【Gateway】{$operationLabel}切换校验失败，回滚旧实例: new={$generationVersion}", $oldPort);
            $this->rollbackManagedGenerationCutover($previousActiveVersion, $generationVersion, $registeredPlans, $newPlans, $stage);
            return [
                'success_count' => 0,
                'failed_nodes' => [[
                    'host' => $generationVersion,
                    'error' => '新业务实例切换校验失败，已回滚，旧实例保持运行',
                ]],
            ];
        }
        $this->flushPendingNginxSyncSummary();
        foreach ($newPlans as $plan) {
            $this->appendManagedPlan($plan);
        }
        // 先明确“切流校验已通过且入口已稳定命中新代”，再进入旧代回收，
        // 避免日志时序看起来像“还在切流就提前回收旧实例”。
        $oldPort = (int)(($basePlans[0]['port'] ?? 0));
        $this->logOldInstanceLifecycle(
            "【Gateway】{$operationLabel}切流完成，开始回收: success={$successCount}, drain_grace="
            . self::ROLLING_DRAIN_GRACE_SECONDS . "s",
            $oldPort
        );
        $this->logPreviousGenerationTransition($previousActiveVersion);
        foreach ($basePlans as $plan) {
            $this->quiesceManagedPlanBusinessPlane($plan);
        }

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
                    $cutoverProbe = $this->probeManagedIngressCutover($plan);
                    if ($cutoverProbe['healthy']) {
                        continue;
                    }
                    $allHealthy = false;
                    $cutoverReason = (string)($cutoverProbe['reason'] ?? 'unknown');
                    $lastReason = 'ingress_' . $cutoverReason;
                    if ($cutoverReason === 'gateway_ingress_target_mismatch') {
                        $expected = (int)($cutoverProbe['expected_port'] ?? ($plan['port'] ?? 0));
                        $actual = (int)($cutoverProbe['actual_port'] ?? 0);
                        $probeHost = trim((string)($cutoverProbe['probe_host'] ?? ''));
                        $lastReason .= "(expect={$expected},actual={$actual}";
                        if ($probeHost !== '') {
                            $lastReason .= ",host={$probeHost}";
                        }
                        $lastReason .= ')';
                    } elseif ($cutoverReason === 'gateway_ingress_non_200') {
                        $statusCode = (int)($cutoverProbe['status_code'] ?? 0);
                        $probeHost = trim((string)($cutoverProbe['probe_host'] ?? ''));
                        $lastReason .= "(status={$statusCode}";
                        if ($probeHost !== '') {
                            $lastReason .= ",host={$probeHost}";
                        }
                        $lastReason .= ')';
                    }
                    $stableChecks = 0;
                    break;
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
                return true;
            }
            usleep(200000);
        }
        Console::warning("【Gateway】新业务实例切换校验超时: stage={$stage}, generation={$generationVersion}, reason={$lastReason}");
        return false;
    }

    /**
     * 通过 gateway 对外入口探测，确认流量已经命中新实例。
     *
     * 切流不仅要看“新实例本身健康”，还要确认 nginx 入口已经把请求导向目标实例。
     * 这里走 `gateway business port -> nginx -> upstream` 的真实链路，并校验响应里
     * 的 `upstream_port` 与目标 plan 一致，防止出现“内部健康通过但入口仍打旧代”的窗口。
     *
     * @param array<string, mixed> $plan 新代实例 plan
     * @return array{healthy: bool, reason: string, expected_port?: int, actual_port?: int, status_code?: int, probe_host?: string}
     */
    protected function probeManagedIngressCutover(array $plan): array {
        if (!$this->nginxProxyModeEnabled()) {
            return ['healthy' => true, 'reason' => 'skipped_non_nginx_mode'];
        }
        $gatewayPort = $this->businessPort();
        $expectedPort = (int)($plan['port'] ?? 0);
        if ($gatewayPort <= 0 || $expectedPort <= 0) {
            return ['healthy' => false, 'reason' => 'invalid_probe_port'];
        }

        $probePath = self::INTERNAL_UPSTREAM_CUTOVER_PROBE_PATH . '?expect_port=' . $expectedPort
            . '&_probe_ts=' . rawurlencode((string)microtime(true));
        $probeHost = $this->resolveManagedIngressProbeHost($gatewayPort);
        $headers = [
            'Host: ' . $probeHost,
            'Connection: close',
            'X-Scf-Cutover-Probe: 1',
        ];
        $context = stream_context_create([
            'http' => [
                'method' => 'GET',
                'timeout' => 0.8,
                'ignore_errors' => true,
                'header' => implode("\r\n", $headers),
            ],
        ]);

        $body = @file_get_contents('http://127.0.0.1:' . $gatewayPort . $probePath, false, $context);
        if (!is_string($body) || $body === '') {
            return ['healthy' => false, 'reason' => 'gateway_ingress_unreachable'];
        }

        $statusCode = 0;
        foreach ((array)($http_response_header ?? []) as $headerLine) {
            if (preg_match('#^HTTP/\S+\s+(\d{3})#', (string)$headerLine, $matches)) {
                $statusCode = (int)($matches[1] ?? 0);
                break;
            }
        }
        if ($statusCode !== 200) {
            return [
                'healthy' => false,
                'reason' => 'gateway_ingress_non_200',
                'status_code' => $statusCode,
                'probe_host' => $probeHost,
            ];
        }

        $decoded = json_decode($body, true);
        if (!is_array($decoded) || json_last_error() !== JSON_ERROR_NONE) {
            return ['healthy' => false, 'reason' => 'gateway_ingress_invalid_json'];
        }
        if ((int)($decoded['errCode'] ?? -1) !== 0) {
            return ['healthy' => false, 'reason' => 'gateway_ingress_error_payload'];
        }
        $actualPort = (int)($decoded['data']['upstream_port'] ?? 0);
        if ($actualPort !== $expectedPort) {
            return [
                'healthy' => false,
                'reason' => 'gateway_ingress_target_mismatch',
                'expected_port' => $expectedPort,
                'actual_port' => $actualPort,
                'probe_host' => $probeHost,
            ];
        }
        return ['healthy' => true, 'reason' => 'ok'];
    }

    /**
     * 为入口切流探针生成 Host 头。
     *
     * 切流校验请求应尽量命中与真实外部流量一致的 nginx server block。若配置了
     * `gateway_nginx_server_name`，优先选取其中首个确定主机名；否则回退到
     * `127.0.0.1:{gatewayPort}`。
     *
     * @param int $gatewayPort gateway 业务入口端口
     * @return string Host 头值
     */
    protected function resolveManagedIngressProbeHost(int $gatewayPort): string {
        if (method_exists($this, 'resolvedGatewayIngressProbeHost')) {
            $resolved = (string)$this->resolvedGatewayIngressProbeHost($gatewayPort);
            if ($resolved !== '') {
                return $resolved;
            }
        }
        return '127.0.0.1:' . $gatewayPort;
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
        $this->logOldInstanceLifecycle(
            "【Gateway】进入draining: started_at=" . (int)($generation['drain_started_at'] ?? 0)
            . ", deadline_at=" . (int)($generation['drain_deadline_at'] ?? 0),
            $this->oldGenerationRepresentativePort($version)
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
            $this->logOldInstanceLifecycle(
                "【Gateway】保持尾流，待切流稳定后进入shutdown",
                $this->oldGenerationRepresentativePort($version)
            );
            $this->logDrainingGenerationSchedule($version);
            return;
        }
        if ($status === 'offline') {
            $this->logOldInstanceLifecycle(
                "【Gateway】实例不可达或无尾流，直接进入回收",
                $this->oldGenerationRepresentativePort($version)
            );
        }
    }

    /**
     * 启动一个 managed plan 的异步回收流程。
     *
     * 先投递 graceful shutdown，再由 pending/quarantine/reaper 状态机持续收口，
     * 避免单个顽固进程阻塞 gateway 主链路。
     *
     * @param array<string, mixed> $plan 目标 managed plan
     * @param bool $removeState 回收完成后是否从 registry 中移除实例状态
     * @param bool $removePlan 回收完成后是否从内存计划表中移除
     * @param array<string, mixed> $recycleOptions 回收时序覆盖参数：
     *                                              - force_after_seconds
     *                                              - quarantine_after_seconds
     * @return void
     */
    protected function stopManagedPlan(array $plan, bool $removeState = true, bool $removePlan = true, array $recycleOptions = []): void {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $endpointKey = $this->managedRecycleEndpointKey($host, $port);
        if (!$this->reserveManagedRecycleEndpoint($endpointKey)) {
            return;
        }
        try {
            $plan = $this->hydrateManagedPlanRuntimeMetadata($plan);
            $key = ((string)($plan['version'] ?? '') . '@' . $host . ':' . $port);
            $existingEndpointKey = $this->findManagedRecycleKeyByEndpoint($host, $port);

            // 回收去重必须以“端点(host:port)”为主键，而不是 version。
            // 旧实例版本字符串在 draining/offline 迁移期间可能发生变化，若仅按 version@host:port
            // 去重，会出现同一端口并存两条 pending 项，导致同一秒重复执行 SIGKILL。
            if ($existingEndpointKey !== null || isset($this->pendingManagedRecycles[$key]) || isset($this->quarantinedManagedRecycles[$key])) {
                return;
            }

            unset($this->managedUpstreamHealthState[$key]);
            if ($this->launcher) {
                $requestedAt = time();
                $forceAfterSeconds = array_key_exists('force_after_seconds', $recycleOptions)
                    ? max(3, (int)$recycleOptions['force_after_seconds'])
                    : $this->managedRecycleForceAfterSeconds();
                $gracefulAccepted = $this->launcher->requestManagedGracefulShutdown($plan, 1.0);
                $quarantineAfterSeconds = array_key_exists('quarantine_after_seconds', $recycleOptions)
                    ? max($forceAfterSeconds + 30, (int)$recycleOptions['quarantine_after_seconds'])
                    : $this->managedRecycleQuarantineAfterSeconds($forceAfterSeconds);
                $nextForceElapsed = min($forceAfterSeconds, $this->managedRecycleInitialForceWindowSeconds());
                if (!$gracefulAccepted) {
                    $this->logOldInstanceLifecycle("【Gateway】优雅回收请求未响应，进入异步兜底", $port);
                }
                $this->pendingManagedRecycles[$key] = [
                    'plan' => $plan,
                    'remove_state' => $removeState,
                    'remove_plan' => $removePlan,
                    'requested_at' => $requestedAt,
                    'force_after_seconds' => $forceAfterSeconds,
                    'quarantine_after_seconds' => $quarantineAfterSeconds,
                    'force_attempts' => 0,
                    'last_force_at' => 0,
                    'next_force_elapsed' => $nextForceElapsed,
                    'graceful_accepted' => $gracefulAccepted,
                ];
                unset($this->pendingManagedRecycleWarnState[$key], $this->pendingManagedRecycleCompletions[$key]);
                $this->ensurePendingManagedRecycleWatcher($key);
                $this->logOldInstanceLifecycle(
                    "【Gateway】开始异步回收: force_after={$forceAfterSeconds}s, quarantine_after={$quarantineAfterSeconds}s, "
                    . "force_windows={$nextForceElapsed}s->2x...->deadline",
                    $port
                );
                return;
            } elseif ($this->upstreamSupervisor) {
                if (!$this->upstreamSupervisor->sendCommand([
                    'action' => 'stop_instance',
                    'owner_epoch' => $this->gatewayLeaseEpoch(),
                    'grace_seconds' => self::ROLLING_DRAIN_GRACE_SECONDS,
                    'instance' => [
                        'version' => (string)($plan['version'] ?? ''),
                        'host' => $host,
                        'port' => $port,
                        'rpc_port' => (int)($plan['rpc_port'] ?? 0),
                        'metadata' => [
                            'managed' => true,
                            'rpc_port' => (int)($plan['rpc_port'] ?? 0),
                            'pid' => (int)(($plan['metadata']['pid'] ?? 0)),
                            'owner_epoch' => (int)(($plan['metadata']['owner_epoch'] ?? 0)),
                        ],
                    ],
                ])) {
                    $this->logOldInstanceLifecycle("【Gateway】回收命令发送失败", $port);
                    return;
                }
                $this->logOldInstanceLifecycle("【Gateway】已交由supervisor回收", $port);
                return;
            } else {
                $this->logOldInstanceLifecycle("【Gateway】缺少回收执行器，无法继续回收", $port);
            }

            if ($removeState) {
                $this->instanceManager->removeInstance($host, $port);
            }
            unset($this->bootstrappedManagedInstances[$key]);
            if ($removePlan) {
                $this->removeManagedPlan($plan);
            }
            if ($port > 0) {
                $this->logOldInstanceLifecycle("【Gateway】回收完成", $port);
            }
        } finally {
            $this->releaseManagedRecycleEndpoint($endpointKey);
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
     * 给 managed plan 注入当前 gateway 租约绑定信息。
     *
     * 每个 upstream 在启动参数里都要携带 `gateway_epoch/gateway_port`，运行期才能
     * 在独立进程里校验 owner lease，避免 gateway 异常退出后遗留孤儿实例。
     *
     * @param array<string, mixed> $plan managed plan
     * @return array<string, mixed>
     */
    protected function withManagedPlanLeaseBinding(array $plan): array {
        $metadata = (array)($plan['metadata'] ?? []);
        if ($this->gatewayLeaseEpoch() > 0) {
            $metadata['owner_epoch'] = $this->gatewayLeaseEpoch();
        }
        $metadata['gateway_port'] = $this->businessPort();
        $plan['metadata'] = $metadata;
        $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
        return $plan;
    }

    /**
     * 归一化计划里的额外启动参数，确保 dev/pack/src/gateway lease 语义能继承到子进程。
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
        $hasGatewayEpoch = false;
        $hasGatewayLeaseGrace = false;
        $hasGatewayRestartGrace = false;
        foreach ($flags as $flag) {
            $hasDev = $hasDev || $flag === '-dev';
            $hasDir = $hasDir || $flag === '-dir';
            $hasPhar = $hasPhar || $flag === '-phar';
            $hasPackMode = $hasPackMode || in_array($flag, ['-pack', '-nopack'], true);
            $hasGatewayPort = $hasGatewayPort || str_starts_with($flag, '-gateway_port=');
            $hasGatewayEpoch = $hasGatewayEpoch || str_starts_with($flag, '-gateway_epoch=');
            $hasGatewayLeaseGrace = $hasGatewayLeaseGrace || str_starts_with($flag, '-gateway_lease_grace=');
            $hasGatewayRestartGrace = $hasGatewayRestartGrace || str_starts_with($flag, '-gateway_restart_grace=');
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
        if (!$hasGatewayEpoch && $this->gatewayLeaseEpoch() > 0) {
            $flags[] = '-gateway_epoch=' . $this->gatewayLeaseEpoch();
        }
        if (!$hasGatewayLeaseGrace && $this->gatewayLeaseGraceSeconds > 0) {
            $flags[] = '-gateway_lease_grace=' . $this->gatewayLeaseGraceSeconds;
        }
        if (!$hasGatewayRestartGrace && $this->gatewayLeaseRestartGraceSeconds > 0) {
            $flags[] = '-gateway_restart_grace=' . $this->gatewayLeaseRestartGraceSeconds;
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
        $this->sweepManagedRecyclePortCooldowns();
        $ports = [$this->port];
        if ($this->rpcPort > 0) {
            $ports[] = $this->rpcPort;
        }
        foreach ($this->managedRecyclePortCooldowns as $port => $expiresAt) {
            if ((int)$expiresAt > time()) {
                $ports[] = (int)$port;
            }
        }
        foreach ($this->quarantinedManagedRecycles as $item) {
            $plan = (array)($item['plan'] ?? []);
            $quarantinedHttpPort = (int)($plan['port'] ?? 0);
            $quarantinedRpcPort = (int)($plan['rpc_port'] ?? (($plan['metadata']['rpc_port'] ?? 0)));
            $quarantinedHttpPort > 0 and $ports[] = $quarantinedHttpPort;
            $quarantinedRpcPort > 0 and $ports[] = $quarantinedRpcPort;
        }
        foreach ($this->managedUpstreamPlans as $plan) {
            $planPort = (int)($plan['port'] ?? 0);
            $planRpcPort = (int)($plan['rpc_port'] ?? 0);
            $planPort > 0 and $ports[] = $planPort;
            $planRpcPort > 0 and $ports[] = $planRpcPort;
        }
        foreach (($this->instanceManager->snapshot()['generations'] ?? []) as $generation) {
            $status = (string)($generation['status'] ?? '');
            $isLiveGeneration = in_array($status, ['active', 'prepared', 'draining'], true);
            foreach (($generation['instances'] ?? []) as $instance) {
                $instancePort = (int)($instance['port'] ?? 0);
                $instanceRpcPort = (int)(($instance['metadata']['rpc_port'] ?? 0));
                if (!$isLiveGeneration) {
                    // offline 代际里的历史端口不应长期占用端口池，否则会导致“端口
                    // 明明可用却一直扫不到”的假失败。仅当端口现在仍被监听时才保留。
                    $httpInUse = $instancePort > 0 && \Scf\Core\Server::isPortInUse($instancePort, '127.0.0.1');
                    $rpcInUse = $instanceRpcPort > 0 && \Scf\Core\Server::isPortInUse($instanceRpcPort, '127.0.0.1');
                    if (!$httpInUse && !$rpcInUse) {
                        continue;
                    }
                }
                $instancePort > 0 and $ports[] = $instancePort;
                $instanceRpcPort > 0 and $ports[] = $instanceRpcPort;
            }
        }
        return array_values(array_unique(array_filter($ports)));
    }

    /**
     * 清理过期端口冷却项，避免历史隔离端口永久占用分配池。
     *
     * @return void
     */
    protected function sweepManagedRecyclePortCooldowns(): void {
        if (!$this->managedRecyclePortCooldowns) {
            return;
        }
        $now = time();
        foreach ($this->managedRecyclePortCooldowns as $port => $expiresAt) {
            if ((int)$expiresAt <= $now) {
                unset($this->managedRecyclePortCooldowns[$port]);
            }
        }
    }

    /**
     * 把端口放入冷却窗口，避免刚回收失败/恢复的端口被立即复用。
     *
     * @param int $port 端口号。
     * @param int|null $ttlSeconds 冷却时长；为空时走配置默认值。
     * @return void
     */
    protected function markManagedRecyclePortCooldown(int $port, ?int $ttlSeconds = null): void {
        if ($port <= 0) {
            return;
        }
        $ttlSeconds = is_int($ttlSeconds)
            ? max(10, $ttlSeconds)
            : $this->managedRecyclePortCooldownSeconds();
        $expiresAt = time() + $ttlSeconds;
        $existing = (int)($this->managedRecyclePortCooldowns[$port] ?? 0);
        if ($existing < $expiresAt) {
            $this->managedRecyclePortCooldowns[$port] = $expiresAt;
        }
    }

    /**
     * 读取“端口冷却窗口”配置。
     *
     * @return int
     */
    protected function managedRecyclePortCooldownSeconds(): int {
        return max(30, (int)(\Scf\Core\Config::server()['gateway_managed_recycle_port_cooldown'] ?? 300));
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
        if (!$this->launcher) {
            while (in_array($port, $reserved, true)) {
                $port++;
            }
            return $port;
        }
        try {
            return $this->launcher->findAvailablePortAvoiding('127.0.0.1', $port, $reserved, 4000);
        } catch (Throwable) {
            // 第一段扫描失败后，跳到 gateway 业务端口附近再给一次更大窗口，
            // 避免历史连续端口区把滚动重启卡死在单一递增区间。
            $fallbackStart = max(1025, $this->businessPort() + 16);
            if ($fallbackStart <= $port) {
                $fallbackStart = $port + 1;
            }
            Console::warning("【Gateway】业务实例端口主扫描失败，尝试扩展区间: start={$port}, fallback_start={$fallbackStart}");
            return $this->launcher->findAvailablePortAvoiding('127.0.0.1', $fallbackStart, $reserved, 20000);
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
     * 统一输出旧实例生命周期日志。
     *
     * 旧实例日志统一带 `#U{port}` 前缀并使用灰色，便于与当前 active 实例日志区分，
     * 同时保证 dashboard/终端看到的文案一致。
     *
     * @param string $message 日志正文（含模块前缀）
     * @param int $port 旧实例端口
     * @return void
     */
    protected function logOldInstanceLifecycle(string $message, int $port = 0): void {
        $prefix = $port > 0 ? "#U{$port} " : '#U ';
        Console::log($prefix . $message, true, 'gray');
    }

    /**
     * 从 generation 快照中解析一个可用于日志前缀的旧实例端口。
     *
     * @param string $version generation 版本号
     * @return int
     */
    protected function oldGenerationRepresentativePort(string $version): int {
        if ($version === '') {
            return 0;
        }
        $snapshot = $this->instanceManager->snapshot();
        $generation = (array)(($snapshot['generations'] ?? [])[$version] ?? []);
        foreach ((array)($generation['instances'] ?? []) as $instance) {
            $port = (int)($instance['port'] ?? 0);
            if ($port > 0) {
                return $port;
            }
        }
        return 0;
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
                $this->logOldInstanceLifecycle(
                    "【Gateway】实例不可达，直接进入回收: reason=" . (string)($probe['reason'] ?? 'unknown')
                    . ", http=" . ($reachability['http_listening'] ? 'listening' : 'down')
                    . ", rpc=" . ($reachability['rpc_port'] > 0 ? ($reachability['rpc_listening'] ? 'listening' : 'down') : 'n/a')
                    . ", process=" . ($reachability['pid_alive'] ? 'alive' : 'down'),
                    $port
                );
                $this->instanceManager->markInstanceOffline($host, $port, $version);
                $this->stopManagedPlan($plan, true);
                return;
            }
            $this->logOldInstanceLifecycle("【Gateway】业务平面静默失败", $port);
            return;
        }
        $runtimeStatus = $this->fetchUpstreamRuntimeStatus($plan);
        $runtimeMasterPid = (int)($runtimeStatus['master_pid'] ?? 0);
        $runtimeManagerPid = (int)($runtimeStatus['manager_pid'] ?? 0);
        if ($runtimeMasterPid > 0 || $runtimeManagerPid > 0) {
            $pidPatch = [];
            if ($runtimeMasterPid > 0) {
                // 回收链统一以 upstream master pid 作为主 PID，避免 TERM/KILL 只打到 manager
                // 触发 master 补拉 worker，出现“旧实例关闭后又启动”的连锁。
                $pidPatch['pid'] = $runtimeMasterPid;
                $pidPatch['master_pid'] = $runtimeMasterPid;
            }
            if ($runtimeManagerPid > 0) {
                $pidPatch['manager_pid'] = $runtimeManagerPid;
            }
            $this->instanceManager->mergeInstanceMetadata($host, $port, $pidPatch);
            $this->mergeManagedPlanMetadata($host, $port, $pidPatch);
            $plan['metadata'] = array_merge((array)($plan['metadata'] ?? []), $pidPatch);
        }
        $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
        $httpProcessing = (int)($runtimeStatus['http_request_processing'] ?? 0);
        $rpcProcessing = (int)($runtimeStatus['rpc_request_processing'] ?? 0);
        $queueProcessing = (int)($runtimeStatus['redis_queue_processing'] ?? 0);
        $crontabBusy = (int)($runtimeStatus['crontab_busy'] ?? 0);
        $mysqlInflight = (int)($runtimeStatus['mysql_inflight'] ?? 0);
        $redisInflight = (int)($runtimeStatus['redis_inflight'] ?? 0);
        $outboundHttpInflight = (int)($runtimeStatus['outbound_http_inflight'] ?? 0);
        $inflightTotal = $httpProcessing + $rpcProcessing + $mysqlInflight + $redisInflight + $outboundHttpInflight;
        $this->logOldInstanceLifecycle(
            "【Gateway】开始平滑回收: ws=" . $gatewayWs
            . ", inflight={$inflightTotal}"
            . ", queue=" . $queueProcessing
            . ", crontab=" . $crontabBusy
            . ", http={$httpProcessing}, rpc={$rpcProcessing}",
            $port
        );
        $disconnected = $this->disconnectManagedPlanClients($plan);
        if ($disconnected > 0) {
            $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
            $this->logOldInstanceLifecycle(
                "【Gateway】已主动断开客户端: disconnected={$disconnected}, remaining_ws={$gatewayWs}",
                $port
            );
        }
        // 旧代已经不再承接流量时，立即进入异步回收窗口序列（5/10/20/40...）。
        // 这样可以保证“尽快收口”与“30 分钟 deadline 无条件清理”同时成立。
        if (
            $gatewayWs === 0
            && $httpProcessing === 0
            && $rpcProcessing === 0
            && $queueProcessing === 0
            && $crontabBusy === 0
            && $mysqlInflight === 0
            && $redisInflight === 0
            && $outboundHttpInflight === 0
        ) {
            $this->logOldInstanceLifecycle("【Gateway】无在途请求，立即进入shutdown", $port);
            $this->stopManagedPlan($plan, true, true, [
                'force_after_seconds' => $this->managedRecycleForceAfterWhenIdleSeconds(),
            ]);
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
        // 回收链必须按“真实存活”判定，僵尸进程不应继续占用 pending/quarantine 状态。
        $pidAlive = $pid > 0 && $this->launcher->isProcessAlive($pid);

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
        $removedVersionPorts = [];
        $offlineForceAfterSeconds = $this->managedOfflineRecycleForceAfterSeconds();
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
                // offline 代际已经不承接入口流量，只做后台收口。
                // 这里统一收紧强制窗口，避免“无业务请求但旧代长期驻留”。
                $this->stopManagedPlan($plan, true, true, [
                    'force_after_seconds' => $offlineForceAfterSeconds,
                ]);
            }
            $version = (string)($generation['version'] ?? '');
            if ($this->hasPendingRecycleForVersion($version)) {
                continue;
            }
            foreach ((array)($generation['instances'] ?? []) as $instance) {
                $instancePort = (int)($instance['port'] ?? 0);
                if ($instancePort > 0) {
                    $removedVersionPorts[$version] = $instancePort;
                    break;
                }
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
                foreach ($removedVersions as $removedVersion) {
                    $this->logOldInstanceLifecycle(
                        "【Gateway】旧实例代际已移除: generation={$removedVersion}",
                        (int)($removedVersionPorts[$removedVersion] ?? 0)
                    );
                }
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
            // 正常路径由 per-key watcher 驱动；主循环只兜底“意外缺 watcher”的项，
            // 避免同一回收项在同一轮被双重推进，导致重复 SIGTERM/SIGKILL 日志与重试抖动。
            if (isset($this->pendingManagedRecycleWatchers[$key])) {
                continue;
            }
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
        if (($this->pendingManagedRecycleCheckInFlight[$key] ?? false) === true) {
            return false;
        }
        $this->pendingManagedRecycleCheckInFlight[$key] = true;
        try {
            if (($this->pendingManagedRecycleCompletions[$key] ?? false) === true) {
                return false;
            }

            $item = $this->pendingManagedRecycles[$key];
            $plan = (array)($item['plan'] ?? []);
        // pending recycle 可能在创建时拿到的是旧 runtime 快照（例如 pid 仍是 manager）。
        // 每轮都用最新 registry/runtime 元数据回填，确保强制回收始终命中当前 master pid，
        // 避免“杀了 manager 又被 master 拉起 worker”的反复重启现象。
        $latestPlan = $this->hydrateManagedPlanRuntimeMetadata($plan);
        if ($latestPlan !== $plan) {
            $plan = $latestPlan;
            $item['plan'] = $plan;
            $this->pendingManagedRecycles[$key] = $item;
        }
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $reachability = $this->managedPlanReachability($plan);
        $httpAlive = $port > 0 && ($reachability['http_listening'] ?? false);
        $rpcAlive = $rpcPort > 0 && ($reachability['rpc_listening'] ?? false);
        $pidAlive = (bool)($reachability['pid_alive'] ?? false);
        $requestedAt = (int)($item['requested_at'] ?? time());
        $elapsed = max(0, time() - $requestedAt);
        $forceAfter = max(10, (int)($item['force_after_seconds'] ?? $this->managedRecycleForceAfterSeconds()));
        $quarantineAfter = max(
            $forceAfter + 30,
            (int)($item['quarantine_after_seconds'] ?? $this->managedRecycleQuarantineAfterSeconds($forceAfter))
        );
        if ($httpAlive || $rpcAlive || $pidAlive) {
            $runtimeStatus = $this->fetchUpstreamRuntimeStatus($plan);
            $runtimeSource = 'live';
            if ($runtimeStatus === []) {
                // 旧实例在 shutdown 阶段可能先于回收状态机停止响应状态接口。
                // 这里允许短时回退到最近一次 runtime 快照，避免窗口判断因“采样空洞”
                // 被迫拖到更晚轮次甚至 deadline。
                $cachedRuntime = $this->instanceManager->instanceRuntimeStatus($host, $port, 15);
                if ($cachedRuntime) {
                    $runtimeStatus = $cachedRuntime;
                    $runtimeSource = 'cache';
                } else {
                    $runtimeSource = 'unavailable';
                }
            }
            $runtimeStatusAvailable = $runtimeStatus !== [];
            $gatewayWs = $this->instanceManager->gatewayConnectionCountFor((string)($plan['version'] ?? ''), $host, $port);
            $httpProcessing = (int)($runtimeStatus['http_request_processing'] ?? 0);
            $rpcProcessing = (int)($runtimeStatus['rpc_request_processing'] ?? 0);
            $queueProcessing = (int)($runtimeStatus['redis_queue_processing'] ?? 0);
            $crontabBusy = (int)($runtimeStatus['crontab_busy'] ?? 0);
            $mysqlInflight = (int)($runtimeStatus['mysql_inflight'] ?? 0);
            $redisInflight = (int)($runtimeStatus['redis_inflight'] ?? 0);
            $outboundHttpInflight = (int)($runtimeStatus['outbound_http_inflight'] ?? 0);
            $serverConnectionNum = (int)(($runtimeStatus['server_stats']['connection_num'] ?? 0));
            $inflightTotal = $httpProcessing + $rpcProcessing + $mysqlInflight + $redisInflight + $outboundHttpInflight;
            $noInflight = $runtimeStatusAvailable
                && $httpProcessing === 0
                && $rpcProcessing === 0
                && $queueProcessing === 0
                && $crontabBusy === 0
                && $mysqlInflight === 0
                && $redisInflight === 0
                && $outboundHttpInflight === 0;
            // 回收窗口命中时，只要“无连接 + 无 inflight”就可以强制收口，
            // 即使端口仍在 LISTEN，也不再继续等待“自退出”。
            $windowKillEligible = $noInflight
                && $gatewayWs === 0
                && $serverConnectionNum === 0;
            $deadlineReached = $elapsed >= $forceAfter;
            $currentWindowElapsed = max(1, (int)($item['next_force_elapsed'] ?? $this->managedRecycleInitialForceWindowSeconds()));
            $currentWindowElapsed = min($forceAfter, $currentWindowElapsed);
            $windowDue = $elapsed >= $currentWindowElapsed;
            $nextWindowElapsed = $currentWindowElapsed;
            if ($windowDue || $deadlineReached) {
                $shouldForceKill = $deadlineReached || $windowKillEligible;
                $now = time();
                $nextWindowElapsed = $this->managedRecycleNextForceWindowSeconds($currentWindowElapsed, $forceAfter);
                $item['next_force_elapsed'] = $nextWindowElapsed;
                if ($shouldForceKill) {
                    $attempts = (int)($item['force_attempts'] ?? 0) + 1;
                    $hard = true;
                    $this->launcher->forceStopManagedInstance($plan, $hard);
                    $item['force_attempts'] = $attempts;
                    $item['last_force_at'] = $now;
                    $this->markManagedRecyclePortCooldown($port);
                    $rpcPort > 0 and $this->markManagedRecyclePortCooldown($rpcPort);
                    $this->logOldInstanceLifecycle(
                        "【Gateway】回收升级执行: phase=SIGKILL"
                        . ", waiting={$elapsed}s, attempts={$attempts}, reason=" . ($deadlineReached ? 'deadline' : 'window_empty')
                        . ", window={$currentWindowElapsed}s",
                        $port
                    );
                } elseif ($windowDue) {
                    $this->logOldInstanceLifecycle(
                        "【Gateway】回收窗口命中但仍有在途，继续等待: waiting={$elapsed}s, window={$currentWindowElapsed}s"
                        . ", runtime={$runtimeSource}, ws={$gatewayWs}, conn={$serverConnectionNum}"
                        . ", inflight=" . ($runtimeStatusAvailable ? (string)$inflightTotal : 'n/a')
                        . ", queue={$queueProcessing}, crontab={$crontabBusy}",
                        $port
                    );
                }
                $this->pendingManagedRecycles[$key] = $item;
            }
            if ($elapsed >= $quarantineAfter) {
                $this->quarantinePendingManagedRecycle($key, $item, "recycle_timeout_{$elapsed}s");
                return true;
            }
            if ($elapsed >= 60) {
                $lastWarnAt = (int)($this->pendingManagedRecycleWarnState[$key] ?? 0);
                if ($lastWarnAt <= 0 || (time() - $lastWarnAt) >= 60) {
                    $this->pendingManagedRecycleWarnState[$key] = time();
                    $this->logOldInstanceLifecycle(
                        "【Gateway】回收等待中: waiting={$elapsed}s, deadline={$forceAfter}s, next_window={$nextWindowElapsed}s, quarantine_after={$quarantineAfter}s"
                        . ", runtime={$runtimeSource}, ws={$gatewayWs}, conn={$serverConnectionNum}"
                        . ", inflight=" . ($runtimeStatusAvailable ? (string)$inflightTotal : 'n/a')
                        . ", queue={$queueProcessing}, crontab={$crontabBusy}"
                        . ", http=" . ($httpAlive ? 'listening' : 'closed')
                        . ", rpc=" . ($rpcAlive ? 'listening' : 'closed')
                        . ", process=" . ($pidAlive ? 'alive' : 'down'),
                        $port
                    );
                }
            }
            return false;
        }

            $this->completeManagedRecycle($key, $item, 'pending_drained');
            return true;
        } finally {
            unset($this->pendingManagedRecycleCheckInFlight[$key]);
        }
    }

    /**
     * 处理已进入 quarantine 的回收项，持续执行后台收口。
     *
     * @return void
     */
    protected function drainQuarantinedManagedRecycles(): void {
        if (!$this->launcher || !$this->quarantinedManagedRecycles) {
            return;
        }
        $this->sweepManagedRecyclePortCooldowns();
        $now = time();
        $retryInterval = $this->managedRecycleQuarantineRetryIntervalSeconds();
        foreach (array_keys($this->quarantinedManagedRecycles) as $key) {
            $item = (array)($this->quarantinedManagedRecycles[$key] ?? []);
            $plan = (array)($item['plan'] ?? []);
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            $rpcPort = (int)($plan['rpc_port'] ?? (($plan['metadata']['rpc_port'] ?? 0)));
            if ($host === '' || $port <= 0) {
                unset($this->quarantinedManagedRecycles[$key], $this->quarantinedManagedRecycleWarnState[$key]);
                continue;
            }

            $reachability = $this->managedPlanReachability($plan);
            $httpAlive = (bool)($reachability['http_listening'] ?? false);
            $rpcAlive = $rpcPort > 0 && (bool)($reachability['rpc_listening'] ?? false);
            $pidAlive = (bool)($reachability['pid_alive'] ?? false);
            if (!$httpAlive && !$rpcAlive && !$pidAlive) {
                $this->completeManagedRecycle($key, $item, 'quarantine_reaped');
                continue;
            }

            $this->markManagedRecyclePortCooldown($port);
            $rpcPort > 0 and $this->markManagedRecyclePortCooldown($rpcPort);
            $lastRetryAt = (int)($item['last_retry_at'] ?? 0);
            if ($lastRetryAt > 0 && ($now - $lastRetryAt) < $retryInterval) {
                continue;
            }
            $retryCount = (int)($item['retry_count'] ?? 0) + 1;
            $hard = $retryCount >= 2;
            $this->launcher->forceStopManagedInstance($plan, $hard);
            $item['last_retry_at'] = $now;
            $item['retry_count'] = $retryCount;
            $this->quarantinedManagedRecycles[$key] = $item;

            $lastWarnAt = (int)($this->quarantinedManagedRecycleWarnState[$key] ?? 0);
            if ($lastWarnAt <= 0 || ($now - $lastWarnAt) >= 60) {
                $this->quarantinedManagedRecycleWarnState[$key] = $now;
                $quarantinedAt = (int)($item['quarantined_at'] ?? $now);
                $quarantinedElapsed = max(0, $now - $quarantinedAt);
                $this->logOldInstanceLifecycle(
                    "【Gateway】隔离态后台回收: phase=" . ($hard ? 'SIGKILL' : 'SIGTERM')
                    . ", quarantined={$quarantinedElapsed}s, retries={$retryCount}"
                    . ", http=" . ($httpAlive ? 'listening' : 'closed')
                    . ", rpc=" . ($rpcAlive ? 'listening' : 'closed')
                    . ", process=" . ($pidAlive ? 'alive' : 'down'),
                    $port
                );
            }
        }
    }

    /**
     * 将长时间无法回收的实例转入 quarantine 隔离态。
     *
     * @param string $key 回收项键。
     * @param array<string, mixed> $item 回收上下文。
     * @param string $reason 转入隔离的原因。
     * @return void
     */
    protected function quarantinePendingManagedRecycle(string $key, array $item, string $reason): void {
        $plan = (array)($item['plan'] ?? []);
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? (($plan['metadata']['rpc_port'] ?? 0)));
        $now = time();
        $item['quarantined_at'] = $now;
        $item['quarantine_reason'] = $reason;
        $item['last_retry_at'] = 0;
        $item['retry_count'] = 0;
        $this->quarantinedManagedRecycles[$key] = $item;

        // 隔离态下先把实例从路由/计划中摘掉，避免单个顽固进程拖住主链路。
        $removeState = (bool)($item['remove_state'] ?? true);
        $removePlan = (bool)($item['remove_plan'] ?? true);
        if ($removeState) {
            $this->instanceManager->removeInstance($host, $port);
        } else {
            $this->instanceManager->markInstanceOffline($host, $port, (string)($plan['version'] ?? ''));
            $this->instanceManager->mergeInstanceMetadata($host, $port, [
                'quarantined' => true,
                'quarantine_reason' => $reason,
                'quarantined_at' => $now,
            ]);
        }
        unset($this->bootstrappedManagedInstances[$key]);
        if ($removePlan) {
            $this->removeManagedPlan($plan);
        } else {
            $this->mergeManagedPlanMetadata($host, $port, [
                'quarantined' => true,
                'quarantine_reason' => $reason,
                'quarantined_at' => $now,
            ]);
        }
        $this->markManagedRecyclePortCooldown($port);
        $rpcPort > 0 and $this->markManagedRecyclePortCooldown($rpcPort);
        unset(
            $this->pendingManagedRecycleWarnState[$key],
            $this->pendingManagedRecycles[$key],
            $this->pendingManagedRecycleCompletions[$key]
        );
        $this->logOldInstanceLifecycle(
            "【Gateway】回收超时，转入隔离态: reason={$reason}",
            $port
        );
    }

    /**
     * 统一收口回收完成态，清理计划/实例与辅助状态。
     *
     * @param string $key 回收项键。
     * @param array<string, mixed> $item 回收上下文。
     * @param string $stage 完成阶段标识。
     * @return void
     */
    protected function completeManagedRecycle(string $key, array $item, string $stage): void {
        $plan = (array)($item['plan'] ?? []);
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $removeState = (bool)($item['remove_state'] ?? true);
        $removePlan = (bool)($item['remove_plan'] ?? true);

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
        unset(
            $this->pendingManagedRecycleWarnState[$key],
            $this->pendingManagedRecycles[$key],
            $this->pendingManagedRecycleCompletions[$key],
            $this->quarantinedManagedRecycles[$key],
            $this->quarantinedManagedRecycleWarnState[$key]
        );
        if ($port > 0) {
            $this->markManagedRecyclePortCooldown($port, 60);
            $rpcPort = (int)($plan['rpc_port'] ?? ($plan['metadata']['rpc_port'] ?? 0));
            $rpcPort > 0 and $this->markManagedRecyclePortCooldown($rpcPort, 60);
        }
        if (!$this->startupSummaryPending()) {
            $this->logOldInstanceLifecycle("【Gateway】回收完成({$stage})", $port);
        }
        if ($removedGeneration !== '') {
            if ($this->startupSummaryPending()) {
                $this->recordStartupRemovedGenerations([$removedGeneration]);
            } else {
                $this->logOldInstanceLifecycle(
                    "【Gateway】旧实例代际已移除: generation={$removedGeneration}",
                    $port
                );
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
    }

    /**
     * 读取“升级到强制回收”之前的等待窗口（秒）。
     *
     * @return int
     */
    protected function managedRecycleForceAfterSeconds(): int {
        return max(
            self::ROLLING_DRAIN_GRACE_SECONDS,
            (int)(\Scf\Core\Config::server()['gateway_managed_recycle_force_after'] ?? AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS)
        );
    }

    /**
     * 无在途业务时的强制回收等待窗口。
     *
     * 仅用于“ws/http/rpc/queue/crontab 全空”的旧代收口。
     * 默认沿用标准 30 分钟平滑窗口，只有显式配置时才缩短。
     *
     * @return int
     */
    protected function managedRecycleForceAfterWhenIdleSeconds(): int {
        return max(
            5,
            (int)(\Scf\Core\Config::server()['gateway_managed_recycle_force_after_idle'] ?? AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS)
        );
    }

    /**
     * offline 代际后台回收的强制窗口。
     *
     * offline 代际已经从流量平面摘除，默认沿用空载快速窗口，避免旧代长期滞留。
     *
     * @return int
     */
    protected function managedOfflineRecycleForceAfterSeconds(): int {
        return max(
            5,
            (int)(\Scf\Core\Config::server()['gateway_managed_offline_recycle_force_after'] ?? $this->managedRecycleForceAfterWhenIdleSeconds())
        );
    }

    /**
     * 读取“转入隔离态”阈值（秒）。
     *
     * @param int $forceAfterSeconds 强制回收起始阈值。
     * @return int
     */
    protected function managedRecycleQuarantineAfterSeconds(int $forceAfterSeconds): int {
        $configured = (int)(\Scf\Core\Config::server()['gateway_managed_recycle_quarantine_after'] ?? ($forceAfterSeconds + 120));
        return max($forceAfterSeconds + 30, $configured);
    }

    /**
     * 读取 pending 回收的首个强制窗口（秒）。
     *
     * 窗口序列按指数推进：5 -> 10 -> 20 -> 40 ... 直到 deadline。
     *
     * @return int
     */
    protected function managedRecycleInitialForceWindowSeconds(): int {
        return max(
            1,
            (int)(\Scf\Core\Config::server()['gateway_managed_recycle_force_window_initial'] ?? 5)
        );
    }

    /**
     * 计算下一次强制窗口（秒）。
     *
     * @param int $currentWindow 当前窗口阈值（相对 requested_at 的秒数）。
     * @param int $deadline 强制回收 deadline（秒）。
     * @return int
     */
    protected function managedRecycleNextForceWindowSeconds(int $currentWindow, int $deadline): int {
        $currentWindow = max(1, $currentWindow);
        $deadline = max($currentWindow, $deadline);
        if ($currentWindow >= $deadline) {
            return $deadline;
        }
        $next = max($currentWindow + 1, $currentWindow * 2);
        return min($deadline, $next);
    }

    /**
     * quarantine 阶段后台 reaper 的重试间隔（秒）。
     *
     * @return int
     */
    protected function managedRecycleQuarantineRetryIntervalSeconds(): int {
        return max(10, (int)(\Scf\Core\Config::server()['gateway_managed_recycle_quarantine_retry_interval'] ?? 30));
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
        if (isset($this->pendingManagedRecycles[$key]) || isset($this->quarantinedManagedRecycles[$key])) {
            return true;
        }
        return $this->findManagedRecycleKeyByEndpoint($host, $port) !== null;
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
        foreach ($this->quarantinedManagedRecycles as $item) {
            $plan = (array)($item['plan'] ?? []);
            if ((string)($plan['version'] ?? '') === $version) {
                return true;
            }
        }
        return false;
    }

    /**
     * 按端点查找已存在的 recycle 键。
     *
     * pending/quarantine 的 map key 历史上包含 version 维度，容易出现
     * “同端点不同 version 的重复回收项”。这里统一按 host:port 做一次归一化
     * 查找，确保同一实例端点在任意时刻只有一条回收状态机。
     *
     * @param string $host 实例 host
     * @param int $port 实例端口
     * @return string|null 命中的 recycle key
     */
    protected function findManagedRecycleKeyByEndpoint(string $host, int $port): ?string {
        if ($port <= 0) {
            return null;
        }
        $endpoint = $this->managedRecycleEndpointKey($host, $port);
        foreach ($this->pendingManagedRecycles as $existingKey => $item) {
            $plan = (array)($item['plan'] ?? []);
            if ($this->managedRecycleEndpointKey((string)($plan['host'] ?? '127.0.0.1'), (int)($plan['port'] ?? 0)) === $endpoint) {
                return (string)$existingKey;
            }
        }
        foreach ($this->quarantinedManagedRecycles as $existingKey => $item) {
            $plan = (array)($item['plan'] ?? []);
            if ($this->managedRecycleEndpointKey((string)($plan['host'] ?? '127.0.0.1'), (int)($plan['port'] ?? 0)) === $endpoint) {
                return (string)$existingKey;
            }
        }
        return null;
    }

    /**
     * 生成回收端点的归一化键。
     *
     * @param string $host 实例 host
     * @param int $port 实例端口
     * @return string
     */
    protected function managedRecycleEndpointKey(string $host, int $port): string {
        $host = strtolower(trim($host));
        if ($host === '' || $host === 'localhost') {
            $host = '127.0.0.1';
        }
        return $host . ':' . max(0, $port);
    }

    /**
     * 预占回收端点，防止并发 stopManagedPlan 对同端点重复入队。
     *
     * stopManagedPlan 在写入 pending map 前会先发优雅回收请求，该步骤可能发生协程切换。
     * 若不做端点预占，同一端口会在“map 尚未写入”的窗口被再次入队，造成重复 SIGKILL。
     *
     * @param string $endpointKey 端点键（host:port）
     * @return bool true=预占成功；false=已有流程占用
     */
    protected function reserveManagedRecycleEndpoint(string $endpointKey): bool {
        if ($endpointKey === '') {
            return true;
        }
        if (isset($this->pendingManagedRecycleEndpointReservations[$endpointKey])) {
            return false;
        }
        $this->pendingManagedRecycleEndpointReservations[$endpointKey] = time();
        return true;
    }

    /**
     * 释放回收端点预占。
     *
     * @param string $endpointKey 端点键（host:port）
     * @return void
     */
    protected function releaseManagedRecycleEndpoint(string $endpointKey): void {
        if ($endpointKey === '') {
            return;
        }
        unset($this->pendingManagedRecycleEndpointReservations[$endpointKey]);
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
            $httpProcessing = (int)($item['http_processing'] ?? 0);
            $rpcProcessing = (int)($item['rpc_processing'] ?? 0);
            $mysqlInflight = (int)($item['mysql_inflight'] ?? 0);
            $redisInflight = (int)($item['redis_inflight'] ?? 0);
            $outboundHttpInflight = (int)($item['outbound_http_inflight'] ?? 0);
            $inflightTotal = $httpProcessing + $rpcProcessing + $mysqlInflight + $redisInflight + $outboundHttpInflight;
            $this->logOldInstanceLifecycle(
                "【Gateway】draining超时仍未shutdown: draining={$elapsed}s, ws=" . (int)($item['connections'] ?? 0)
                . ", inflight={$inflightTotal}"
                . ", queue=" . (int)($item['redis_queue_processing'] ?? 0)
                . ", crontab=" . (int)($item['crontab_busy'] ?? 0)
                . ", http={$httpProcessing}, rpc={$rpcProcessing}"
                . ", deadline_at=" . (int)($item['drain_deadline_at'] ?? 0),
                $this->oldGenerationRepresentativePort($version)
            );
        }
        foreach (array_keys($this->drainingGenerationWarnState) as $version) {
            if (!isset($activeVersions[$version])) {
                unset($this->drainingGenerationWarnState[$version]);
            }
        }
    }
}
