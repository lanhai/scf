<?php

namespace Scf\Server\SubProcess;

use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Server\ProcessRespawnBackoff;
use Scf\Server\Task\RQueue;
use Swoole\Process;
use Throwable;

/**
 * RedisQueue manager 子进程运行逻辑。
 *
 * 责任边界：
 * - 管理 Redis 队列消费子进程的生命周期与排空。
 */
class RedisQueueProcess extends AbstractRuntimeProcess {
    /**
     * 旧版 worker 没有 token/文件锁，只允许在近期采样仍有效时跨版本接管。
     */
    protected const LEGACY_WORKER_FRESH_SECONDS = 30;

    /**
     * @return Process
     */
    public function create(): Process {
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);

        return new Process(function (Process $process) {
            $this->call('mark_gateway_sub_process_context');
            $managerGeneration = $this->captureManagerGeneration();
            $managerPid = getmypid() ?: 0;
            if (!$this->claimRuntimeOwnership(
                $managerGeneration,
                Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
                Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT,
                $managerPid
            )) {
                return;
            }
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【RedisQueue】Redis队列管理PID:" . $process->pid, false);
            }
            define('IS_REDIS_QUEUE_PROCESS', true);
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $managerId = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
            $quiescing = false;
            $queueWorkerName = 'RedisQueueWorker';
            $respawnBackoff = new ProcessRespawnBackoff();
            $discovery = $this->discoverQueueWorker($managerPid, $managerId, $managerGeneration);
            $queueWorkerPid = (int)($discovery['pid'] ?? 0);
            $queueWorkerToken = (string)($discovery['token'] ?? '');
            if ((bool)($discovery['verified'] ?? false)) {
                // manager 意外重拉时先接管仍存活的消费进程，避免重复创建 worker。
                Runtime::instance()->redisQueueProcessStatus(true);
                $respawnBackoff->recordStarted($queueWorkerName);
            } else {
                $queueWorkerPid = 0;
                $queueWorkerToken = '';
                $this->clearObservedQueueWorkerRuntimeIfCurrent($managerPid, $managerGeneration, $discovery);
                if ((bool)($discovery['lock_pending'] ?? false)) {
                    // 锁已被持有但 owner 元数据尚未完成回填时保持 busy，下一轮再接管。
                    if ($this->isCurrentQueueManager($managerPid, $managerGeneration)) {
                        Runtime::instance()->redisQueueProcessStatus(true);
                    }
                } else {
                    $this->markQueueWorkerIdleIfNoOwner($managerPid, $managerGeneration);
                }
            }
            $this->writeQueueRespawnStateIfCurrent(
                $managerPid,
                $managerGeneration,
                $respawnBackoff->state($queueWorkerName),
                $queueWorkerPid
            );
            while (true) {
                $now = time();
                if ($this->managedRuntimeShouldStop(
                    $managerGeneration,
                    Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
                    $managerPid,
                    $commandPipe,
                    true
                )) {
                    break;
                }
                $this->touchRuntimeOwnershipIfCurrent(
                    $managerGeneration,
                    Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
                    Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT,
                    $managerPid,
                    $now
                );
                while ($ret = Process::wait(false)) {
                    $pid = (int)($ret['pid'] ?? 0);
                    if ($pid > 0 && $pid === $queueWorkerPid) {
                        $retryDelay = $respawnBackoff->recordExit($queueWorkerName, $now);
                        $this->clearQueueWorkerRuntimeIfCurrent(
                            $managerPid,
                            $managerGeneration,
                            $queueWorkerPid,
                            $queueWorkerToken,
                            $managerPid,
                            $managerGeneration,
                            $queueWorkerPid
                        );
                        $queueWorkerPid = 0;
                        $queueWorkerToken = '';
                        $this->writeQueueRespawnStateIfCurrent(
                            $managerPid,
                            $managerGeneration,
                            $respawnBackoff->state($queueWorkerName),
                            0
                        );
                        if ($retryDelay > 0) {
                            Console::warning("【RedisQueue】消费子进程短时退出，{$retryDelay}s 后重试");
                        }
                    }
                }
                // manager 重拉后接管的旧 worker 不是当前进程的 child，wait(false)
                // 无法回收它，因此用 kill(pid, 0) 补齐退出检测。
                if ($queueWorkerPid > 0 && !@Process::kill($queueWorkerPid, 0)) {
                    $retryDelay = $respawnBackoff->recordExit($queueWorkerName, $now);
                    $this->clearQueueWorkerRuntimeIfCurrent(
                        $managerPid,
                        $managerGeneration,
                        $queueWorkerPid,
                        $queueWorkerToken,
                        $managerPid,
                        $managerGeneration,
                        $queueWorkerPid
                    );
                    $queueWorkerPid = 0;
                    $queueWorkerToken = '';
                    $this->writeQueueRespawnStateIfCurrent(
                        $managerPid,
                        $managerGeneration,
                        $respawnBackoff->state($queueWorkerName),
                        0
                    );
                    if ($retryDelay > 0) {
                        Console::warning("【RedisQueue】接管的消费子进程已退出，{$retryDelay}s 后重试");
                    }
                }
                if ($queueWorkerPid <= 0) {
                    // startProcess 因锁冲突返回 null 时不盲清 Runtime，而是在每轮重新读取
                    // 锁 owner。owner 回填完成后直接接管；锁释放后才允许进入重试拉起。
                    $discovery = $this->discoverQueueWorker($managerPid, $managerId, $managerGeneration);
                    if ((bool)($discovery['verified'] ?? false)) {
                        $queueWorkerPid = (int)($discovery['pid'] ?? 0);
                        $queueWorkerToken = (string)($discovery['token'] ?? '');
                        Runtime::instance()->redisQueueProcessStatus(true);
                        $respawnBackoff->recordStarted($queueWorkerName, $now);
                        $this->writeQueueRespawnStateIfCurrent(
                            $managerPid,
                            $managerGeneration,
                            $respawnBackoff->state($queueWorkerName),
                            $queueWorkerPid
                        );
                    } else {
                        $this->clearObservedQueueWorkerRuntimeIfCurrent($managerPid, $managerGeneration, $discovery);
                        if ((bool)($discovery['lock_pending'] ?? false)) {
                            if ($this->isCurrentQueueManager($managerPid, $managerGeneration)) {
                                Runtime::instance()->redisQueueProcessStatus(true);
                            }
                        } else {
                            $this->markQueueWorkerIdleIfNoOwner($managerPid, $managerGeneration);
                        }
                    }
                }
                if ($queueWorkerPid > 0) {
                    if ($respawnBackoff->markStable($queueWorkerName, $now)) {
                        $this->writeQueueRespawnStateIfCurrent(
                            $managerPid,
                            $managerGeneration,
                            $respawnBackoff->state($queueWorkerName),
                            $queueWorkerPid
                        );
                    }
                }

                if (!Runtime::instance()->serverIsAlive()) {
                    Console::warning("【RedisQueue】服务器已关闭,结束运行");
                    break;
                }
                $latestManagerId = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
                if (!$quiescing && $latestManagerId !== $managerId) {
                    $quiescing = true;
                    Console::warning("【RedisQueue】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                }

                if (!$quiescing && !Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }

                if (!$quiescing
                    && $queueWorkerPid <= 0
                    && !Runtime::instance()->redisQueueProcessStatus()
                    && Runtime::instance()->serverIsAlive()
                    && !Runtime::instance()->serverIsDraining()
                    && $respawnBackoff->canStart($queueWorkerName, $now)
                ) {
                    Runtime::instance()->redisQueueProcessStatus(true);
                    try {
                        try {
                            $queueWorkerToken = bin2hex(random_bytes(16));
                        } catch (Throwable) {
                            $queueWorkerToken = str_replace('.', '', uniqid('rq', true));
                        }
                        $queueProcess = RQueue::startProcess($queueWorkerToken, $managerGeneration);
                        $queueWorkerPid = (int)($queueProcess?->pid ?? 0);
                        if (
                            $queueWorkerPid > 0
                            && !RQueue::workerLockIsHeld($queueWorkerPid, $queueWorkerToken)
                        ) {
                            $queueWorkerPid = 0;
                        }
                    } catch (Throwable $throwable) {
                        $queueWorkerPid = 0;
                        Console::warning('【RedisQueue】消费子进程启动异常: ' . $throwable->getMessage());
                    }
                    if ($queueWorkerPid <= 0) {
                        // null 既可能是真启动失败，也可能是另一个 manager 已持锁。
                        // 重新读取 owner，能接管就接管，正在回填则等待，只有锁确实
                        // 不存在时才计入失败退避。
                        $discovery = $this->discoverQueueWorker($managerPid, $managerId, $managerGeneration);
                        if ((bool)($discovery['verified'] ?? false)) {
                            $queueWorkerPid = (int)($discovery['pid'] ?? 0);
                            $queueWorkerToken = (string)($discovery['token'] ?? '');
                            $respawnBackoff->recordStarted($queueWorkerName, $now);
                        } elseif ((bool)($discovery['lock_pending'] ?? false)) {
                            $queueWorkerToken = '';
                            if ($this->isCurrentQueueManager($managerPid, $managerGeneration)) {
                                Runtime::instance()->redisQueueProcessStatus(true);
                            }
                        } else {
                            $queueWorkerToken = '';
                            $this->clearObservedQueueWorkerRuntimeIfCurrent($managerPid, $managerGeneration, $discovery);
                            $this->markQueueWorkerIdleIfNoOwner($managerPid, $managerGeneration);
                            $retryDelay = $respawnBackoff->recordStartFailure($queueWorkerName, $now);
                            Console::warning("【RedisQueue】消费子进程启动失败，{$retryDelay}s 后重试");
                        }
                    } else {
                        $respawnBackoff->recordStarted($queueWorkerName, $now);
                    }
                    $this->writeQueueRespawnStateIfCurrent(
                        $managerPid,
                        $managerGeneration,
                        $respawnBackoff->state($queueWorkerName),
                        $queueWorkerPid
                    );
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
            if ($this->isCurrentQueueManager($managerPid, $managerGeneration)) {
                Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_RESPAWN_STATE, []);
            }
            $this->clearRuntimeOwnershipIfCurrent(
                $managerGeneration,
                Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
                Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT,
                $managerPid
            );
            is_resource($commandPipe) and fclose($commandPipe);
        });
    }

    protected function isCurrentQueueManager(int $managerPid, string $managerGeneration): bool {
        return $managerPid > 0
            && $this->ownsRuntimeProcess(
                $managerGeneration,
                Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
                $managerPid
            );
    }

    /**
     * 从文件锁、Runtime 与旧版内存采样中发现一个可安全接管的 worker。
     *
     * 新版 worker 以锁文件里的 pid+token 为唯一可信身份；Runtime 丢失或属于
     * 上一代 manager 时会从锁 owner 重建。旧版 worker 没有 token/锁，只在
     * 采样仍新鲜或明确有任务处理中时做一次兼容接管。
     *
     * @return array<string,mixed>
     */
    protected function discoverQueueWorker(int $managerPid, int $managerId, string $managerGeneration): array {
        $runtime = Runtime::instance();
        $state = (array)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE) ?? []);
        $statePid = max(0, (int)($state['pid'] ?? 0));
        $runtimePid = max(0, (int)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_PID) ?? 0));
        $stateToken = (string)($state['token'] ?? '');
        $stateManagerPid = max(0, (int)($state['manager_pid'] ?? 0));
        $stateManagerGeneration = (string)($state['manager_generation'] ?? '');
        $result = [
            'verified' => false,
            'lock_pending' => false,
            'pid' => 0,
            'token' => '',
            'observed_state_pid' => $statePid,
            'observed_runtime_pid' => $runtimePid,
            'observed_token' => $stateToken,
            'observed_manager_pid' => $stateManagerPid,
            'observed_manager_generation' => $stateManagerGeneration,
        ];

        $lockOwner = RQueue::workerLockOwner();
        if ((bool)($lockOwner['held'] ?? false)) {
            $ownerPid = max(0, (int)($lockOwner['pid'] ?? 0));
            $ownerToken = (string)($lockOwner['token'] ?? '');
            if (
                $this->isCurrentQueueManager($managerPid, $managerGeneration)
                && $ownerPid > 0
                && $ownerToken !== ''
                && @Process::kill($ownerPid, 0)
                && RQueue::workerLockIsHeld($ownerPid, $ownerToken)
            ) {
                $sameState = $statePid === $ownerPid
                    && $stateToken !== ''
                    && hash_equals($stateToken, $ownerToken);
                $adoptedState = [
                    'pid' => $ownerPid,
                    'token' => $ownerToken,
                    'legacy' => false,
                    'manager_pid' => $managerPid,
                    'manager_id' => $managerId,
                    'manager_generation' => $managerGeneration,
                    'started_at' => max(
                        1,
                        (int)($sameState ? ($state['started_at'] ?? 0) : 0),
                        (int)($lockOwner['started_at'] ?? 0)
                    ),
                    'heartbeat_at' => $sameState
                        ? max(0, (int)($state['heartbeat_at'] ?? 0))
                        : time(),
                ];
                // 文件锁仍被 owner 持有，因此清理路径无法在这些写入之间穿插。
                $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE, $adoptedState);
                $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_PID, $ownerPid);
                $runtime->redisQueueProcessStatus(true);
                return array_merge($result, [
                    'verified' => true,
                    'pid' => $ownerPid,
                    'token' => $ownerToken,
                    'observed_state_pid' => $ownerPid,
                    'observed_runtime_pid' => $ownerPid,
                    'observed_token' => $ownerToken,
                    'observed_manager_pid' => $managerPid,
                    'observed_manager_generation' => $managerGeneration,
                ]);
            }
            $ownerManagerPid = max(0, (int)($lockOwner['manager_pid'] ?? 0));
            $startedAt = max(0, (int)($lockOwner['started_at'] ?? 0));
            $ownerAlive = $ownerPid > 0 && @Process::kill($ownerPid, 0);
            $pendingOwner = $ownerAlive
                || (
                    $ownerPid <= 0
                    && ($startedAt <= 0 || (time() - $startedAt) <= 5)
                    && (
                        $ownerManagerPid <= 0
                        || $ownerManagerPid === $managerPid
                        || @Process::kill($ownerManagerPid, 0)
                    )
                );
            // 元数据还在 fork/PID 回填窗口或锁文件不可读时继续等待；若 owner PID
            // 已明确死亡，则必须允许 startProcess 进入旧 inode 轮换路径，避免被
            // handler 后代继承的 lease fd 永久阻塞队列恢复。
            $result['lock_pending'] = $pendingOwner;
            $result['stale_lock'] = !$pendingOwner;
            return $result;
        }

        if (!$this->isCurrentQueueManager($managerPid, $managerGeneration)) {
            return $result;
        }
        $queueWorkerRow = (array)(MemoryMonitorTable::instance()->get('redis:queue') ?: []);
        $sampleAt = max(
            (int)($queueWorkerRow['usage_updated'] ?? 0),
            (int)($queueWorkerRow['updated'] ?? 0)
        );
        $processing = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0);
        $legacyState = (bool)($state['legacy'] ?? false)
            && $statePid > 0
            && hash_equals('legacy:' . $statePid, $stateToken);
        $legacyCandidatePid = $statePid > 0 ? $statePid : $runtimePid;
        $legacyWorker = $legacyCandidatePid > 0
            && ($stateToken === '' || $legacyState)
            && (int)($queueWorkerRow['pid'] ?? 0) === $legacyCandidatePid
            && @Process::kill($legacyCandidatePid, 0)
            && (
                ($sampleAt > 0 && time() - $sampleAt <= self::LEGACY_WORKER_FRESH_SECONDS)
                || $processing > 0
            );
        if (!$legacyWorker) {
            return $result;
        }

        // 在同一生命周期锁下发布 legacy 接管状态，阻止新版 worker 与它并发拉起。
        $guard = RQueue::tryAcquireWorkerLifecycleGuard();
        if (!is_resource($guard)) {
            $result['lock_pending'] = true;
            return $result;
        }
        try {
            if (
                !$this->isCurrentQueueManager($managerPid, $managerGeneration)
                || !@Process::kill($legacyCandidatePid, 0)
            ) {
                return $result;
            }
            $legacyToken = 'legacy:' . $legacyCandidatePid;
            $legacyRuntimeState = [
                'pid' => $legacyCandidatePid,
                'token' => $legacyToken,
                'legacy' => true,
                'manager_pid' => $managerPid,
                'manager_id' => $managerId,
                'manager_generation' => $managerGeneration,
                'started_at' => max(1, (int)($queueWorkerRow['updated'] ?? time())),
                'heartbeat_at' => max(0, $sampleAt),
            ];
            $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE, $legacyRuntimeState);
            $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_PID, $legacyCandidatePid);
            $runtime->redisQueueProcessStatus(true);
            return array_merge($result, [
                'verified' => true,
                'pid' => $legacyCandidatePid,
                'token' => $legacyToken,
                'observed_state_pid' => $legacyCandidatePid,
                'observed_runtime_pid' => $legacyCandidatePid,
                'observed_token' => $legacyToken,
                'observed_manager_pid' => $managerPid,
                'observed_manager_generation' => $managerGeneration,
            ]);
        } finally {
            RQueue::releaseWorkerLifecycleGuard($guard);
        }
    }

    /**
     * 使用发现阶段读到的完整身份做 compare-and-clear。
     */
    protected function clearObservedQueueWorkerRuntimeIfCurrent(
        int $managerPid,
        string $managerGeneration,
        array $discovery
    ): bool {
        return $this->clearQueueWorkerRuntimeIfCurrent(
            $managerPid,
            $managerGeneration,
            max(0, (int)($discovery['observed_state_pid'] ?? 0)),
            (string)($discovery['observed_token'] ?? ''),
            max(0, (int)($discovery['observed_manager_pid'] ?? 0)),
            (string)($discovery['observed_manager_generation'] ?? ''),
            max(0, (int)($discovery['observed_runtime_pid'] ?? 0))
        );
    }

    protected function clearQueueWorkerRuntimeIfCurrent(
        int $managerPid,
        string $managerGeneration,
        int $expectedStateWorkerPid,
        string $expectedWorkerToken,
        int $expectedStateManagerPid,
        string $expectedStateManagerGeneration,
        int $expectedRuntimeWorkerPid
    ): bool {
        // 禁止无条件清理。至少要携带 discovery 读到的 PID 或 token，避免旧
        // manager 的空 expected 把新 manager 刚发布的 live state 擦除。
        if (
            $expectedStateWorkerPid <= 0
            && $expectedRuntimeWorkerPid <= 0
            && $expectedWorkerToken === ''
        ) {
            return false;
        }
        $guard = RQueue::tryAcquireWorkerLifecycleGuard();
        if (!is_resource($guard)) {
            return false;
        }
        try {
            if (!$this->isCurrentQueueManager($managerPid, $managerGeneration)) {
                return false;
            }
            $runtime = Runtime::instance();
            $state = (array)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE) ?? []);
            if (
                (int)($state['pid'] ?? 0) !== $expectedStateWorkerPid
                || (int)($state['manager_pid'] ?? 0) !== $expectedStateManagerPid
                || !hash_equals((string)($state['token'] ?? ''), $expectedWorkerToken)
                || !hash_equals(
                    (string)($state['manager_generation'] ?? ''),
                    $expectedStateManagerGeneration
                )
                || (int)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_PID) ?? 0) !== $expectedRuntimeWorkerPid
                || !$this->isCurrentQueueManager($managerPid, $managerGeneration)
            ) {
                return false;
            }
            $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE, []);
            $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_PID, 0);
            $runtime->redisQueueProcessStatus(false);
            return true;
        } finally {
            RQueue::releaseWorkerLifecycleGuard($guard);
        }
    }

    /**
     * 只有在 worker 身份与文件锁都为空时才能把队列状态标为空闲。
     */
    protected function markQueueWorkerIdleIfNoOwner(int $managerPid, string $managerGeneration): bool {
        $guard = RQueue::tryAcquireWorkerLifecycleGuard();
        if (!is_resource($guard)) {
            return false;
        }
        try {
            if (!$this->isCurrentQueueManager($managerPid, $managerGeneration)) {
                return false;
            }
            $runtime = Runtime::instance();
            $state = (array)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE) ?? []);
            $runtimePid = (int)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_PID) ?? 0);
            if (
                $state !== []
                || $runtimePid > 0
                || !$this->isCurrentQueueManager($managerPid, $managerGeneration)
            ) {
                return false;
            }
            $runtime->redisQueueProcessStatus(false);
            return true;
        } finally {
            RQueue::releaseWorkerLifecycleGuard($guard);
        }
    }

    protected function writeQueueRespawnStateIfCurrent(
        int $managerPid,
        string $managerGeneration,
        array $state,
        int $workerPid
    ): void {
        if (!$this->isCurrentQueueManager($managerPid, $managerGeneration)) {
            return;
        }
        $state['manager_pid'] = $managerPid;
        $state['manager_generation'] = $managerGeneration;
        $state['worker_pid'] = max(0, $workerPid);
        Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_RESPAWN_STATE, $state);
    }
}
