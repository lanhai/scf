<?php

namespace Scf\Core;

use Scf\Core\Table\Counter;
use Scf\Core\Table\SecondWindowCounterTable;
use Swoole\Process;

/**
 * 秒窗计数协调器。
 *
 * 责任边界：
 * 1. 协调请求量 / MySQL 执行量的秒级写入与读取；
 * 2. 将“按秒拼动态 key”迁移为“固定 row + current/previous 秒窗”模型；
 * 3. 在切换新实现时回收旧版遗留的动态秒桶，避免历史脏 key 持续占用 Counter 表。
 *
 * 架构位置：
 * 位于 Core 层上方，供 listener、logger、heartbeat、dashboard 等运行态统计逻辑复用。
 */
class SecondWindowCounter {
    protected const REQUEST_ROW_PREFIX = 'request.worker:';
    protected const MYSQL_ROW_PREFIX = 'mysql.pid:';
    protected const LEGACY_SECOND_KEY_MIN_LENGTH = 10;
    protected static bool $legacyBucketsCleaned = false;
    protected static int $mysqlRowGcLastRunAt = 0;
    protected static array $requestSecondCache = [
        'second' => -1,
        'value' => 0,
    ];
    protected static array $mysqlSecondCache = [
        'second' => -1,
        'value' => 0,
    ];

    /**
     * 记录当前 worker 的请求秒窗。
     *
     * @param int $workerId worker id（1-based）。
     * @param int|null $now 当前秒时间戳。
     * @return int 当前秒累计值。
     */
    public static function incrRequestSecondForWorker(int $workerId, ?int $now = null): int {
        self::cleanupLegacyDynamicSecondBucketsOnce();
        $second = $now ?? time();
        return SecondWindowCounterTable::instance()->recordSingleWriterSecond(
            self::REQUEST_ROW_PREFIX . max(1, $workerId),
            $second
        );
    }

    /**
     * 读取指定秒的全局请求量。
     *
     * 读取口径固定为 target second 的最终值，因此同一进程内可按秒缓存，
     * 避免在每个请求都全表聚合一次 worker 行。
     *
     * @param int $second 目标秒。
     * @return int 全局请求量。
     */
    public static function requestCountOfSecond(int $second): int {
        self::cleanupLegacyDynamicSecondBucketsOnce();
        if (self::$requestSecondCache['second'] === $second) {
            return (int)self::$requestSecondCache['value'];
        }
        $value = SecondWindowCounterTable::instance()->sumSecondByPrefix(self::REQUEST_ROW_PREFIX, $second);
        self::$requestSecondCache = [
            'second' => $second,
            'value' => $value,
        ];
        return $value;
    }

    /**
     * 记录当前进程的 MySQL 执行秒窗。
     *
     * @param int|null $pid 当前进程 pid。
     * @param int|null $now 当前秒时间戳。
     * @return int 当前秒累计值。
     */
    public static function incrMysqlSecondForProcess(?int $pid = null, ?int $now = null): int {
        self::cleanupLegacyDynamicSecondBucketsOnce();
        $second = $now ?? time();
        self::cleanupMysqlProcessRowsIfNeeded($second);
        $processId = $pid ?: (int)getmypid();
        return SecondWindowCounterTable::instance()->recordSingleWriterSecond(
            self::MYSQL_ROW_PREFIX . $processId,
            $second
        );
    }

    /**
     * 读取指定秒的全局 MySQL 执行量。
     *
     * @param int $second 目标秒。
     * @return int 全局 MySQL 执行量。
     */
    public static function mysqlCountOfSecond(int $second): int {
        self::cleanupLegacyDynamicSecondBucketsOnce();
        self::cleanupMysqlProcessRowsIfNeeded(time());
        if (self::$mysqlSecondCache['second'] === $second) {
            return (int)self::$mysqlSecondCache['value'];
        }
        $value = SecondWindowCounterTable::instance()->sumSecondByPrefix(self::MYSQL_ROW_PREFIX, $second);
        self::$mysqlSecondCache = [
            'second' => $second,
            'value' => $value,
        ];
        return $value;
    }

    /**
     * 首次切换到固定秒窗实现时，回收旧版“按秒动态 key”遗留的桶。
     *
     * 旧实现把秒时间戳直接拼在 Counter key 后面，reload 后这些 key 会留在共享表里。
     * 这里按前缀 + 纯数字长后缀做一次兼容清理，不影响“总量 / 按天统计”这些稳定 key。
     *
     * @return void
     */
    protected static function cleanupLegacyDynamicSecondBucketsOnce(): void {
        if (self::$legacyBucketsCleaned) {
            return;
        }
        foreach (Counter::instance()->rows() as $key => $value) {
            $key = (string)$key;
            if (self::isLegacySecondBucketKey($key, Key::COUNTER_REQUEST)
                || self::isLegacySecondBucketKey($key, Key::COUNTER_MYSQL_PROCESSING)) {
                Counter::instance()->delete($key);
            }
        }
        self::$legacyBucketsCleaned = true;
    }

    /**
     * 判断某个 Counter key 是否属于旧版动态秒桶。
     *
     * @param string $key 完整 key。
     * @param string $prefix key 前缀。
     * @return bool
     */
    protected static function isLegacySecondBucketKey(string $key, string $prefix): bool {
        if (!str_starts_with($key, $prefix)) {
            return false;
        }
        $suffix = substr($key, strlen($prefix));
        return $suffix !== ''
            && strlen($suffix) >= self::LEGACY_SECOND_KEY_MIN_LENGTH
            && ctype_digit($suffix);
    }

    /**
     * 清理已退出进程遗留的 MySQL 秒窗行。
     *
     * MySQL 秒窗按 pid 建固定行后，行数上界取决于活跃进程数。
     * 这里按 10 秒节流扫描一次，把“进程已退出”的行删除，避免 pid 维度持续堆积。
     *
     * @param int $now 当前秒。
     * @return void
     */
    protected static function cleanupMysqlProcessRowsIfNeeded(int $now): void {
        if (($now - self::$mysqlRowGcLastRunAt) < 10) {
            return;
        }
        SecondWindowCounterTable::instance()->cleanupRows(function (string $rowKey, array $row) use ($now): bool {
            if (!str_starts_with($rowKey, self::MYSQL_ROW_PREFIX)) {
                return false;
            }
            $pid = (int)substr($rowKey, strlen(self::MYSQL_ROW_PREFIX));
            $updatedAt = (int)($row['updated_at'] ?? 0);
            if ($pid <= 0) {
                return true;
            }
            // 活跃进程保留自身行；只有“长时间未更新且 pid 已不存在”的行才回收，
            // 避免对仍在线但短期未执行 SQL 的 worker 误删统计状态。
            if (($now - $updatedAt) < 30) {
                return false;
            }
            return !@Process::kill($pid, 0);
        });
        self::$mysqlRowGcLastRunAt = $now;
    }
}
