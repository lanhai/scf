<?php

namespace Scf\Core;

use Scf\Core\Table\Counter;

/**
 * 统一维护运行期“外部 I/O 在途计数”。
 *
 * 设计意图：
 * 1. 对 MySQL / Redis / Outbound HTTP 形成统一口径；
 * 2. 给 upstream shutdown 与 gateway recycle 提供可依赖的“是否仍有在途 I/O”判定；
 * 3. 在计数异常时兜底裁剪为 0，避免负值污染回收状态机。
 */
class InflightCounter {

    /**
     * 进入一次 MySQL I/O 在途区间。
     *
     * @return void
     */
    public static function beginMysql(): void {
        self::increment(Key::COUNTER_MYSQL_INFLIGHT);
    }

    /**
     * 结束一次 MySQL I/O 在途区间。
     *
     * @return void
     */
    public static function endMysql(): void {
        self::decrement(Key::COUNTER_MYSQL_INFLIGHT);
    }

    /**
     * 进入一次 Redis I/O 在途区间。
     *
     * @return void
     */
    public static function beginRedis(): void {
        self::increment(Key::COUNTER_REDIS_INFLIGHT);
    }

    /**
     * 结束一次 Redis I/O 在途区间。
     *
     * @return void
     */
    public static function endRedis(): void {
        self::decrement(Key::COUNTER_REDIS_INFLIGHT);
    }

    /**
     * 进入一次 Outbound HTTP I/O 在途区间。
     *
     * @return void
     */
    public static function beginOutboundHttp(): void {
        self::increment(Key::COUNTER_OUTBOUND_HTTP_INFLIGHT);
    }

    /**
     * 结束一次 Outbound HTTP I/O 在途区间。
     *
     * @return void
     */
    public static function endOutboundHttp(): void {
        self::decrement(Key::COUNTER_OUTBOUND_HTTP_INFLIGHT);
    }

    /**
     * 读取当前 MySQL 在途数。
     *
     * @return int
     */
    public static function mysqlInflight(): int {
        return self::value(Key::COUNTER_MYSQL_INFLIGHT);
    }

    /**
     * 读取当前 Redis 在途数。
     *
     * @return int
     */
    public static function redisInflight(): int {
        return self::value(Key::COUNTER_REDIS_INFLIGHT);
    }

    /**
     * 读取当前 Outbound HTTP 在途数。
     *
     * @return int
     */
    public static function outboundHttpInflight(): int {
        return self::value(Key::COUNTER_OUTBOUND_HTTP_INFLIGHT);
    }

    /**
     * 原子自增计数。
     *
     * @param string $counterKey
     * @return void
     */
    protected static function increment(string $counterKey): void {
        Counter::instance()->incr($counterKey);
    }

    /**
     * 原子自减计数，并对异常负值做兜底收敛。
     *
     * @param string $counterKey
     * @return void
     */
    protected static function decrement(string $counterKey): void {
        $value = (int)Counter::instance()->decr($counterKey);
        if ($value < 0) {
            Counter::instance()->set($counterKey, 0);
        }
    }

    /**
     * 读取并归一化指定计数值。
     *
     * @param string $counterKey
     * @return int
     */
    protected static function value(string $counterKey): int {
        return max(0, (int)(Counter::instance()->get($counterKey) ?: 0));
    }
}

