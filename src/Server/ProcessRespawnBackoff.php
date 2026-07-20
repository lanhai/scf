<?php

namespace Scf\Server;

/**
 * 为常驻子进程提供有上限的 crash-loop 退避。
 *
 * 进程正常稳定运行后退出仍可立即恢复；只有“启动失败/短命退出”才按
 * 2, 4, 8, 16, 32, 60 秒退避，防止依赖故障把 fork/exec 打满。
 */
final class ProcessRespawnBackoff {
    protected array $states = [];

    public function __construct(
        protected int $baseDelaySeconds = 2,
        protected int $maxDelaySeconds = 60,
        protected int $stableWindowSeconds = 30
    ) {
        $this->baseDelaySeconds = max(1, $this->baseDelaySeconds);
        $this->maxDelaySeconds = max($this->baseDelaySeconds, $this->maxDelaySeconds);
        $this->stableWindowSeconds = max(1, $this->stableWindowSeconds);
    }

    public function recordStarted(string $name, ?int $now = null): void {
        $now ??= time();
        $state = $this->state($name);
        $state['started_at'] = $now;
        $state['last_started_at'] = $now;
        $this->states[$name] = $state;
    }

    public function recordStartFailure(string $name, ?int $now = null): int {
        return $this->scheduleFailure($name, $now ?? time());
    }

    /**
     * @return int 下一次允许启动前需要等待的秒数；稳定退出返回 0。
     */
    public function recordExit(string $name, ?int $now = null): int {
        $now ??= time();
        $state = $this->state($name);
        $startedAt = (int)($state['started_at'] ?? 0);
        $state['last_exit_at'] = $now;
        $state['started_at'] = 0;
        $this->states[$name] = $state;
        if ($startedAt > 0 && ($now - $startedAt) >= $this->stableWindowSeconds) {
            $this->reset($name, $now);
            return 0;
        }
        return $this->scheduleFailure($name, $now);
    }

    public function canStart(string $name, ?int $now = null): bool {
        $now ??= time();
        return $now >= (int)($this->state($name)['next_retry_at'] ?? 0);
    }

    public function remainingDelay(string $name, ?int $now = null): int {
        $now ??= time();
        return max(0, (int)($this->state($name)['next_retry_at'] ?? 0) - $now);
    }

    public function markStable(string $name, ?int $now = null): bool {
        $now ??= time();
        $state = $this->state($name);
        $startedAt = (int)($state['started_at'] ?? 0);
        if (
            $startedAt > 0
            && ($now - $startedAt) >= $this->stableWindowSeconds
            && ((int)($state['attempts'] ?? 0) > 0 || (int)($state['next_retry_at'] ?? 0) > 0)
        ) {
            $state['attempts'] = 0;
            $state['next_retry_at'] = 0;
            $this->states[$name] = $state;
            return true;
        }
        return false;
    }

    public function reset(string $name, ?int $now = null): void {
        $now ??= time();
        $state = $this->state($name);
        $state['attempts'] = 0;
        $state['next_retry_at'] = $now;
        $state['started_at'] = 0;
        $this->states[$name] = $state;
    }

    public function state(string $name): array {
        return $this->states[$name] ?? [
            'attempts' => 0,
            'next_retry_at' => 0,
            'started_at' => 0,
            'last_started_at' => 0,
            'last_exit_at' => 0,
        ];
    }

    public function allStates(): array {
        return $this->states;
    }

    /**
     * 从共享运行时恢复指定进程的退避状态。
     *
     * manager 自身重建后，单靠对象内存会丢失失败次数并重新从 2 秒开始拉起。
     * 该入口只接收并归一化退避器自己产生的字段，供调用方安全地持久化/恢复。
     */
    public function restoreState(string $name, array $state): void {
        $this->states[$name] = [
            'attempts' => max(0, (int)($state['attempts'] ?? 0)),
            'next_retry_at' => max(0, (int)($state['next_retry_at'] ?? 0)),
            'started_at' => max(0, (int)($state['started_at'] ?? 0)),
            'last_started_at' => max(0, (int)($state['last_started_at'] ?? 0)),
            'last_exit_at' => max(0, (int)($state['last_exit_at'] ?? 0)),
        ];
    }

    protected function scheduleFailure(string $name, int $now): int {
        $state = $this->state($name);
        $attempts = max(0, (int)($state['attempts'] ?? 0)) + 1;
        $exponent = min(20, $attempts - 1);
        $delay = min($this->maxDelaySeconds, $this->baseDelaySeconds * (2 ** $exponent));
        $state['attempts'] = $attempts;
        $state['next_retry_at'] = $now + $delay;
        $state['started_at'] = 0;
        $this->states[$name] = $state;
        return $delay;
    }
}
