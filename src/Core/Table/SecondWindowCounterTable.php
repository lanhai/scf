<?php

namespace Scf\Core\Table;

use Swoole\Table;

/**
 * 固定行秒窗计数表。
 *
 * 责任边界：
 * 1. 为“上一秒请求量 / 上一秒 MySQL 执行量”这类滑动秒窗统计提供共享存储；
 * 2. 采用单写者行模型，避免使用“按秒拼接动态 key”导致的行数长期增长；
 * 3. 保存 current/previous 两个秒窗，兼容读侧按 `time() - 1` 读取的口径。
 *
 * 架构位置：
 * 位于 Core Table 层，供 Http worker、task worker、subprocess 通过固定 row key
 * 写入自己的秒窗快照，再由 dashboard / heartbeat / 限流逻辑汇总读取。
 */
class SecondWindowCounterTable extends ATable {

    /**
     * 配置项。
     *
     * 行数按“worker + task worker + subprocess + 少量扩展”估算，
     * 留足余量避免运行期因为 pid/worker 变化触发表容量告警。
     *
     * @var array<string, mixed>
     */
    protected array $_config = [
        'size' => 512,
        'colums' => [
            'current_second' => ['type' => Table::TYPE_INT, 'size' => 0],
            'current_count' => ['type' => Table::TYPE_INT, 'size' => 0],
            'previous_second' => ['type' => Table::TYPE_INT, 'size' => 0],
            'previous_count' => ['type' => Table::TYPE_INT, 'size' => 0],
            'updated_at' => ['type' => Table::TYPE_INT, 'size' => 0],
        ]
    ];

    /**
     * 记录单写者当前秒的计数快照。
     *
     * 设计约束：
     * 1. 同一个 row key 只能由同一个进程写入；
     * 2. 进程跨秒时，把上一秒快照滚动到 previous_*；
     * 3. 读侧只需汇总 target second 命中的 current_* / previous_* 即可。
     *
     * @param string $rowKey 行 key。
     * @param int $second 当前秒。
     * @param int $delta 本次增加量。
     * @return int 当前秒累计值。
     */
    public function recordSingleWriterSecond(string $rowKey, int $second, int $delta = 1): int {
        $row = $this->table->get($rowKey) ?: [
            'current_second' => 0,
            'current_count' => 0,
            'previous_second' => 0,
            'previous_count' => 0,
            'updated_at' => 0,
        ];
        $currentSecond = (int)($row['current_second'] ?? 0);

        // 单写者在跨秒时把当前秒窗口滚动到 previous_*，确保读侧仍能看到上一秒完整值。
        if ($currentSecond !== $second) {
            if ($currentSecond > 0) {
                $row['previous_second'] = $currentSecond;
                $row['previous_count'] = (int)($row['current_count'] ?? 0);
            }
            $row['current_second'] = $second;
            $row['current_count'] = 0;
        }

        $row['current_count'] = max(0, (int)($row['current_count'] ?? 0) + $delta);
        $row['updated_at'] = $second;
        if (!$this->set($rowKey, $row)) {
            return 0;
        }
        return (int)$row['current_count'];
    }

    /**
     * 汇总指定前缀下某一秒的总计数。
     *
     * @param string $rowKeyPrefix 行 key 前缀。
     * @param int $second 目标秒。
     * @return int 汇总值。
     */
    public function sumSecondByPrefix(string $rowKeyPrefix, int $second): int {
        $total = 0;
        foreach ($this->table as $rowKey => $row) {
            if (!str_starts_with((string)$rowKey, $rowKeyPrefix)) {
                continue;
            }
            if ((int)($row['current_second'] ?? 0) === $second) {
                $total += (int)($row['current_count'] ?? 0);
                continue;
            }
            if ((int)($row['previous_second'] ?? 0) === $second) {
                $total += (int)($row['previous_count'] ?? 0);
            }
        }
        return $total;
    }

    /**
     * 按条件清理行。
     *
     * @param callable $matcher 返回 true 时删除该行。
     * @return int 删除的行数。
     */
    public function cleanupRows(callable $matcher): int {
        $deleted = 0;
        foreach ($this->table as $rowKey => $row) {
            if (!$matcher((string)$rowKey, $row)) {
                continue;
            }
            if ($this->delete($rowKey)) {
                $deleted++;
            }
        }
        return $deleted;
    }
}
