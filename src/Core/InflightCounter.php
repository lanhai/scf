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
     * 进程维度 inflight 子计数键前缀。
     *
     * 键格式：`c_if_pid:{counterKey}:{pid}`。
     * 作用是把“全局 inflight”拆分为“按进程可回收的子计数”，
     * 以便在 worker 异常退出时做残留回补，避免全局计数永久悬挂。
     */
    protected const PROCESS_COUNTER_PREFIX = 'c_if_pid:';

    /**
     * inflight 计数键与诊断字段映射。
     */
    protected const COUNTER_FIELD_MAP = [
        Key::COUNTER_MYSQL_INFLIGHT => 'mysql',
        Key::COUNTER_REDIS_INFLIGHT => 'redis',
        Key::COUNTER_OUTBOUND_HTTP_INFLIGHT => 'outbound_http',
    ];

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
     * 重置全部 inflight 全局计数。
     *
     * 该方法用于 reload 收口阶段的兜底归零：
     * - 默认只重置全局汇总键；
     * - 可选清理“已退出进程”的子计数键，避免旧 pid 残留键长期驻留。
     *
     * @param bool $purgeProcessCounters 是否同时清理进程子计数键。
     * @return void
     */
    public static function resetAll(bool $purgeProcessCounters = false): void {
        foreach (array_keys(self::COUNTER_FIELD_MAP) as $counterKey) {
            Counter::instance()->set($counterKey, 0);
        }
        if ($purgeProcessCounters) {
            self::purgeExitedProcessCounters();
        }
    }

    /**
     * 回补指定进程遗留的 inflight 子计数。
     *
     * 适用场景：
     * - worker 在 begin 后未执行 finally（例如被重启/异常退出）；
     * - 通过 workerExit/workerError 钩子按 pid 把残留计数从全局扣回。
     *
     * @param int $pid 目标进程 PID。
     * @return array{pid:int,mysql:int,redis:int,outbound_http:int,total:int}
     */
    public static function cleanupProcessInflightByPid(int $pid): array {
        $released = [
            'pid' => $pid,
            'mysql' => 0,
            'redis' => 0,
            'outbound_http' => 0,
            'total' => 0,
        ];
        if ($pid <= 0) {
            return $released;
        }

        foreach (self::COUNTER_FIELD_MAP as $counterKey => $field) {
            $pidCounterKey = self::processCounterKey($counterKey, $pid);
            $leaked = self::claimProcessCounter($pidCounterKey);
            if ($leaked <= 0) {
                continue;
            }
            self::decrementCounterKey($counterKey, $leaked);
            $released[$field] = $leaked;
            $released['total'] += $leaked;
        }

        return $released;
    }

    /**
     * 扫描并回补“已退出 PID”遗留的 inflight 子计数。
     *
     * 用于兜底“未触发 workerExit/workerError 回调”的异常路径，
     * 例如极端退出时事件回调缺失。该方法不会清理存活 PID。
     *
     * @return array{mysql:int,redis:int,outbound_http:int,total:int,pids:int}
     */
    public static function cleanupExitedProcessInflight(): array {
        $released = [
            'mysql' => 0,
            'redis' => 0,
            'outbound_http' => 0,
            'total' => 0,
            'pids' => 0,
        ];
        $releasedPids = [];

        foreach (array_keys(Counter::instance()->rows()) as $key) {
            if (!str_starts_with($key, self::PROCESS_COUNTER_PREFIX)) {
                continue;
            }
            $pid = self::extractPidFromProcessCounterKey($key);
            if ($pid > 0 && self::isProcessAlive($pid)) {
                continue;
            }
            $counterKey = self::extractGlobalCounterKeyFromProcessCounterKey($key);
            if ($counterKey === '' || !isset(self::COUNTER_FIELD_MAP[$counterKey])) {
                Counter::instance()->delete($key);
                continue;
            }
            $claimed = self::claimProcessCounter($key);
            if ($claimed <= 0) {
                continue;
            }
            self::decrementCounterKey($counterKey, $claimed);
            $field = self::COUNTER_FIELD_MAP[$counterKey];
            $released[$field] += $claimed;
            $released['total'] += $claimed;
            $releasedPids[$pid] = true;
        }

        $released['pids'] = count($releasedPids);
        return $released;
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
        $pid = getmypid();
        if ($pid > 0) {
            Counter::instance()->incr(self::processCounterKey($counterKey, $pid));
        }
    }

    /**
     * 原子自减计数，并对异常负值做兜底收敛。
     *
     * @param string $counterKey
     * @return void
     */
    protected static function decrement(string $counterKey): void {
        self::decrementCounterKey($counterKey, 1);
        $pid = getmypid();
        if ($pid > 0) {
            $pidCounterKey = self::processCounterKey($counterKey, $pid);
            $pidValue = self::decrementCounterKey($pidCounterKey, 1);
            if ($pidValue <= 0) {
                Counter::instance()->delete($pidCounterKey);
            }
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

    /**
     * 生成进程子计数键。
     *
     * @param string $counterKey 全局 inflight 计数键。
     * @param int $pid 进程 PID。
     * @return string
     */
    protected static function processCounterKey(string $counterKey, int $pid): string {
        return self::PROCESS_COUNTER_PREFIX . $counterKey . ':' . $pid;
    }

    /**
     * 安全执行计数自减并做负值收敛。
     *
     * 与直接 decr 不同，这里会先检查 key 是否存在，避免在 key 不存在时触发
     * 无意义的 warning/错误日志；同时对负值强制归零，防止状态机被负数污染。
     *
     * @param string $counterKey 目标计数键。
     * @param int $step 递减步长。
     * @return int 归一化后的当前值。
     */
    protected static function decrementCounterKey(string $counterKey, int $step = 1): int {
        if ($step <= 0) {
            return self::value($counterKey);
        }
        if (!Counter::instance()->exist($counterKey)) {
            return 0;
        }
        $value = (int)Counter::instance()->decr($counterKey, '_value', $step);
        if ($value <= 0) {
            Counter::instance()->set($counterKey, 0);
            return 0;
        }
        return $value;
    }

    /**
     * 清理已退出进程的子计数键。
     *
     * 仅用于 reload 结束后的兜底收口，避免旧 PID 子键长期驻留；
     * 对仍存活的 PID 键保持不动，避免误删活跃 worker 的在途快照。
     *
     * @return void
     */
    protected static function purgeExitedProcessCounters(): void {
        foreach (array_keys(Counter::instance()->rows()) as $key) {
            if (str_starts_with($key, self::PROCESS_COUNTER_PREFIX)) {
                $pid = self::extractPidFromProcessCounterKey($key);
                if ($pid > 0 && self::isProcessAlive($pid)) {
                    continue;
                }
                Counter::instance()->delete($key);
            }
        }
    }

    /**
     * 从进程子计数键中解析 PID。
     *
     * @param string $counterKey 进程子计数键。
     * @return int
     */
    protected static function extractPidFromProcessCounterKey(string $counterKey): int {
        $parts = explode(':', $counterKey);
        if (count($parts) < 4) {
            return 0;
        }
        return max(0, (int)end($parts));
    }

    /**
     * 从进程子计数键中解析对应的全局 inflight 计数键。
     *
     * @param string $counterKey 进程子计数键。
     * @return string
     */
    protected static function extractGlobalCounterKeyFromProcessCounterKey(string $counterKey): string {
        if (!str_starts_with($counterKey, self::PROCESS_COUNTER_PREFIX)) {
            return '';
        }
        $suffix = substr($counterKey, strlen(self::PROCESS_COUNTER_PREFIX));
        if ($suffix === false || $suffix === '') {
            return '';
        }
        $pos = strrpos($suffix, ':');
        if ($pos === false) {
            return '';
        }
        return substr($suffix, 0, $pos);
    }

    /**
     * 判断目标 PID 是否仍存活。
     *
     * @param int $pid 进程 PID。
     * @return bool
     */
    protected static function isProcessAlive(int $pid): bool {
        if ($pid <= 0) {
            return false;
        }
        if (!function_exists('posix_kill')) {
            return false;
        }
        return @posix_kill($pid, 0);
    }

    /**
     * 原子认领并清空进程子计数。
     *
     * 该方法用于并发场景下避免重复回补：
     * - 只有把值一次性减到 0 的调用方才算“认领成功”；
     * - 其他并发调用即使命中同一 key，也不会重复扣减全局 inflight。
     *
     * @param string $pidCounterKey 进程子计数键。
     * @return int 成功认领的值；失败返回 0。
     */
    protected static function claimProcessCounter(string $pidCounterKey): int {
        if (!Counter::instance()->exist($pidCounterKey)) {
            return 0;
        }
        $value = max(0, (int)(Counter::instance()->get($pidCounterKey) ?: 0));
        if ($value <= 0) {
            Counter::instance()->delete($pidCounterKey);
            return 0;
        }
        $left = (int)Counter::instance()->decr($pidCounterKey, '_value', $value);
        if ($left === 0) {
            Counter::instance()->delete($pidCounterKey);
            return $value;
        }
        if ($left < 0) {
            Counter::instance()->set($pidCounterKey, 0);
            Counter::instance()->delete($pidCounterKey);
        }
        return 0;
    }
}
