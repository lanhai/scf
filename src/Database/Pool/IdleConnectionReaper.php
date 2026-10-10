<?php

namespace Scf\Database\Pool;

use Closure;
use Scf\Core\Console;
use Swoole\Coroutine;
use Swoole\Timer;
use Throwable;
use WeakMap;

/**
 * worker 内 PDO 空闲回收的调度边界，所有连接池共用一个到期定时器。
 *
 * 只登记弱引用和到期时间，不保活连接池、不探测数据库、不预先补建连接。
 * 由 WorkerStart/WorkerExit 管理；CLI 和其他未接入生命周期的进程默认关闭。
 */
final class IdleConnectionReaper {
    private const BATCH_SIZE = 32;
    private const BATCH_DELAY_MS = 100;

    /** @var WeakMap<ConnectionPool, bool>|null */
    private static ?WeakMap $pools = null;
    private static ?Closure $mayRun = null;
    private static int $ownerPid = 0;
    private static int $timerId = 0;
    private static int $scheduledAt = 0;
    private static bool $enabled = false;
    private static bool $running = false;

    /**
     * 在 worker 启动时开放回收；没有空闲连接时不创建定时器。
     *
     * @param callable $mayRun 判断业务运行时是否仍允许维护（例如尚未排空）。
     * @return void
     */
    public static function start(callable $mayRun): void {
        self::stop();
        self::$ownerPid = getmypid();
        self::$pools = new WeakMap();
        self::$mayRun = Closure::fromCallable($mayRun);
        self::$enabled = true;
    }

    /**
     * 在 worker 退出前撤销后台维护；不等待、不遍历关闭 PDO。
     *
     * 退出期间尚未完成的业务归还连接也不能重新启动回收定时器。
     * @return void
     */
    public static function stop(): void {
        self::$enabled = false;
        if (self::$timerId > 0 && self::$ownerPid === getmypid()) {
            Timer::clear(self::$timerId);
        }
        self::$timerId = 0;
        self::$scheduledAt = 0;
        self::$pools = null;
        self::$mayRun = null;
    }

    /**
     * 登记有空闲期限的池；已有更早定时器时仅更新弱引用。
     *
     * @param ConnectionPool $pool 已实际归还连接的池。
     * @return void
     */
    public static function watch(ConnectionPool $pool): void {
        if (!self::$enabled || self::$ownerPid !== getmypid()) {
            return;
        }
        $deadline = $pool->nextIdleDeadline();
        if ($deadline === null) {
            self::forget($pool);
            return;
        }
        self::$pools[$pool] = true;
        if (!self::$running) {
            self::schedule($deadline);
        }
    }

    /**
     * 删除空池登记，保留已排定的一次唤醒，避免每次借还都清建定时器。
     *
     * @param ConnectionPool $pool 当前没有可回收空闲连接的池。
     * @return void
     */
    public static function forget(ConnectionPool $pool): void {
        // 删除当前 WeakMap 迭代项会跳过下一池；回收轮次结束后统一清理空项。
        if (self::$pools !== null && !self::$running) {
            unset(self::$pools[$pool]);
        }
    }

    /**
     * 回收前及协程切换后重新检查退出边界。
     * @return bool 是否允许继续关闭空闲连接。
     */
    private static function allowed(): bool {
        return self::$enabled && self::$ownerPid === getmypid()
            && self::$mayRun !== null && (self::$mayRun)();
    }

    /**
     * 安排唯一的一次到期唤醒；积压分批处理，避免长循环占据 worker。
     * @param int $deadline 最早空闲到期的 Unix 秒数。
     * @return void
     */
    private static function schedule(int $deadline): void {
        if (self::$timerId > 0 && self::$scheduledAt <= $deadline) {
            return;
        }
        if (self::$timerId > 0) {
            Timer::clear(self::$timerId);
        }
        self::$scheduledAt = $deadline;
        $delay = max(self::BATCH_DELAY_MS, ($deadline - time()) * 1000);
        self::$timerId = Timer::after($delay, static function (): void {
            self::reap();
        });
    }

    /**
     * 每次最多关闭 32 个到期连接；关闭可能挂起，期间禁止另起回收轮次。
     * @return void
     */
    private static function reap(): void {
        self::$timerId = 0;
        self::$scheduledAt = 0;
        if (!self::allowed() || Coroutine::getCid() < 0) {
            self::stop();
            return;
        }
        self::$running = true;
        try {
            $budget = self::BATCH_SIZE;
            foreach (self::$pools as $pool => $unused) {
                if (!self::allowed()) {
                    break;
                }
                try {
                    $budget -= $pool->reapIdleConnections(time(), $budget, static fn(): bool => self::allowed());
                } catch (Throwable $exception) {
                    Console::warning('PDO 空闲回收失败: ' . $exception->getMessage(), false);
                    // 异常前可能已消耗若干额度，停止本轮，不能突破单轮关闭上限。
                    $budget = 0;
                }
                if ($budget <= 0) {
                    break;
                }
            }
        } finally {
            self::$running = false;
        }
        if (!self::allowed()) {
            self::stop();
            return;
        }
        $next = null;
        $empty = [];
        foreach (self::$pools as $pool => $unused) {
            $deadline = $pool->nextIdleDeadline();
            if ($deadline !== null) {
                $next = $next === null ? $deadline : min($next, $deadline);
            } else {
                $empty[] = $pool;
            }
        }
        foreach ($empty as $pool) {
            self::forget($pool);
        }
        if ($next !== null) {
            self::schedule($next);
        }
    }
}
