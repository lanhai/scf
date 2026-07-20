<?php

namespace Scf\Server;

use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Swoole\Process;
use Throwable;

/**
 * Swoole addProcess 根进程的跨代 crash-loop 保护器。
 *
 * Swoole 会在自定义进程意外退出后从 server manager 自动重建同一个
 * callback。普通对象属性属于旧子进程私有内存，无法把失败次数带到下一代；
 * 本类将退避状态写入预先创建的 Runtime 共享表，使 2/4/8/16/32/60 秒
 * 退避可以跨进程代际延续。父进程创建的 instance token 会随 fork 保持
 * 不变，同时隔离同一 app/role 下并行运行的多个 server 实例。
 */
final class RootProcessRespawnGuard {
    protected const STATE_TTL_SECONDS = 300;

    protected ProcessRespawnBackoff $backoff;
    protected string $identity;
    protected string $stateKey;
    protected int $startedAt = 0;
    protected int $generation = 0;
    protected string $generationToken = '';
    protected bool $active = false;
    protected bool $stable = false;

    public function __construct(
        protected string $role,
        protected string $instanceToken,
        protected int $baseDelaySeconds = 2,
        protected int $maxDelaySeconds = 60,
        protected int $stableWindowSeconds = 30
    ) {
        $this->role = trim($this->role) ?: 'custom-process';
        $this->instanceToken = trim($this->instanceToken) ?: self::newInstanceToken();
        $this->baseDelaySeconds = max(1, $this->baseDelaySeconds);
        $this->maxDelaySeconds = max($this->baseDelaySeconds, $this->maxDelaySeconds);
        $this->stableWindowSeconds = max(1, $this->stableWindowSeconds);
        $this->identity = $this->role . '|' . $this->instanceToken;
        $this->stateKey = Key::RUNTIME_ROOT_PROCESS_RESPAWN_STATE_PREFIX
            . substr(hash('sha256', $this->identity), 0, 40);
        $this->backoff = new ProcessRespawnBackoff(
            $this->baseDelaySeconds,
            $this->maxDelaySeconds,
            $this->stableWindowSeconds
        );
    }

    /**
     * 在父进程创建 Process 对象前生成；自动重建的各代子进程会继承同一 token。
     */
    public static function newInstanceToken(): string {
        try {
            return bin2hex(random_bytes(16));
        } catch (Throwable) {
            return hash('sha256', uniqid((string)getmypid(), true) . microtime(true));
        }
    }

    /**
     * 进入本代 callback；若上一代短命异常退出，会先在当前子进程内等待。
     *
     * 延迟发生在业务初始化和任何外部命令之前，因此即使 server manager 不受
     * 应用层控制地立即 fork 新一代，也不会继续制造 fork/exec 风暴。
     *
     * @return int 本次实际应用的初始退避秒数
     */
    public function begin(): int {
        // callback 对象会被 Swoole manager 跨 fork 复用，代际 token 必须在
        // 每次 callback 真正进入时重新生成，绝不能沿用构造态或旧共享状态。
        $this->generationToken = self::newInstanceToken();
        $now = time();
        $delay = 0;
        try {
            $state = $this->readState($now);
            $backoffState = (array)($state['backoff'] ?? []);
            if ($backoffState) {
                $this->backoff->restoreState($this->identity, $backoffState);
            }

            $previousPid = (int)($state['pid'] ?? 0);
            $previousRunning = (bool)($state['running'] ?? false);
            if (
                $previousRunning
                && $previousPid > 0
                && $previousPid !== getmypid()
                && !@Process::kill($previousPid, 0)
            ) {
                // SIGKILL/fatal 等路径来不及执行 finish(false)，由新一代在这里
                // 根据上一代 started_at 补记一次退出，保证失败次数不会丢失。
                $this->backoff->recordExit($this->identity, $now);
            }

            $delay = min(
                $this->maxDelaySeconds,
                $this->backoff->remainingDelay($this->identity, $now)
            );
            if ($delay > 0) {
                Console::warning("【{$this->role}】根进程连续短时退出，{$delay}s 后启动下一代", false);
                $this->waitSeconds($delay);
            }

            $this->generation = max(0, (int)($state['generation'] ?? 0)) + 1;
            $this->startedAt = time();
            $this->backoff->recordStarted($this->identity, $this->startedAt);
            $this->active = true;
            $this->stable = false;
            $this->writeState(true, $this->startedAt);
        } catch (Throwable $throwable) {
            // Runtime 表异常时仍至少保留固定最小延迟，确保保护层自身不会变成
            // “立即退出 -> 立即重建”的新风暴源；业务功能随后照常启动。
            $delay = $this->baseDelaySeconds;
            Console::warning("【{$this->role}】读取根进程退避状态失败，{$delay}s 后降级启动: " . $throwable->getMessage(), false);
            $this->waitSeconds($delay);
            $this->startedAt = time();
            $this->backoff->recordStarted($this->identity, $this->startedAt);
            $this->active = true;
            $this->stable = false;
        }
        return $delay;
    }

    /**
     * 返回本次 begin() 唯一且不可预测的代际令牌。
     */
    public function generationToken(): string {
        return $this->generationToken;
    }

    /**
     * 稳定运行达到窗口后清空失败次数；调用方可在自身心跳循环中低成本调用。
     */
    public function markStable(): bool {
        if (
            !$this->active
            || $this->stable
            || $this->startedAt <= 0
            || (time() - $this->startedAt) < $this->stableWindowSeconds
        ) {
            return false;
        }
        try {
            $this->backoff->markStable($this->identity);
            $this->stable = true;
            $this->writeState(true);
            return true;
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * 结束本代 callback。
     *
     * @param bool $intentional true 表示 shutdown/detach/正常事件循环退出；
     *                          false 表示未捕获异常导致的短命退出。
     */
    public function finish(bool $intentional): void {
        if (!$this->active) {
            return;
        }
        $now = time();
        try {
            if ($intentional) {
                $this->backoff->reset($this->identity, $now);
                $this->stable = true;
            } else {
                $delay = $this->backoff->recordExit($this->identity, $now);
                if ($delay > 0) {
                    Console::warning("【{$this->role}】根进程异常退出，下一代至少延迟 {$delay}s", false);
                }
            }
            $this->writeState(false, $now);
        } catch (Throwable) {
            // 退出路径绝不能因为遥测/共享状态写入失败而改变原始退出语义。
        } finally {
            $this->active = false;
        }
    }

    protected function readState(int $now): array {
        $state = Runtime::instance()->get($this->stateKey);
        if (!is_array($state)) {
            return [];
        }
        if (!hash_equals((string)($state['identity_hash'] ?? ''), hash('sha256', $this->identity))) {
            return [];
        }
        $updatedAt = (int)($state['updated_at'] ?? 0);
        if (
            $updatedAt <= 0
            || $updatedAt > ($now + $this->maxDelaySeconds)
            || ($now - $updatedAt) > self::STATE_TTL_SECONDS
        ) {
            return [];
        }
        return $state;
    }

    protected function writeState(bool $running, ?int $now = null): void {
        $now ??= time();
        $written = Runtime::instance()->set($this->stateKey, [
            'identity_hash' => hash('sha256', $this->identity),
            'role' => $this->role,
            'pid' => $running ? getmypid() : 0,
            'running' => $running,
            'generation' => $this->generation,
            'generation_token' => $this->generationToken,
            'backoff' => $this->backoff->state($this->identity),
            'started_at' => $this->startedAt,
            'updated_at' => $now,
        ]);
        if (!$written) {
            throw new \RuntimeException("root process respawn state write failed: {$this->stateKey}");
        }
    }

    protected function waitSeconds(int $seconds): void {
        $deadline = microtime(true) + max(0, $seconds);
        while (($remaining = $deadline - microtime(true)) > 0) {
            usleep((int)(min(0.2, $remaining) * 1000000));
        }
    }
}
