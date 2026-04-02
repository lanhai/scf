<?php

namespace Scf\Core\Table;

use Scf\Core\Key;
use Scf\Util\Arr;

/**
 * Gateway slave 指令历史与结果缓存表。
 *
 * 责任边界：
 * - 仅缓存 master 下发到 slave 的指令生命周期（accepted/running/success/failed/pending）；
 * - 提供 command_id -> slot 的快速定位、分页查询和状态汇总；
 * - 固定最多保留最近 1000 条，采用环形覆盖，避免内存持续膨胀。
 *
 * 架构位置：
 * - 位于 Core\Table，共享给 gateway worker 与子进程读取；
 * - 由 GatewayClusterHeartbeatTrait 负责写入、回执更新与 dashboard 推送。
 */
class GatewayCommandHistoryTable extends ATable {
    protected const MAX_HISTORY = 1000;

    protected array $_config = [
        'size' => 1024,
        'colums' => [
            '_value' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 16384],
        ],
    ];

    /**
     * 追加一条 slave 指令记录。
     *
     * @param array<string, mixed> $record 指令记录
     * @return array<string, mixed> 落盘后的最终记录
     */
    public function append(array $record): array {
        // Runtime 表的 _value 是 string 列，不支持原子 incr。
        // 指令序列必须走 Counter 表，确保并发下单调递增且无类型冲突。
        $sequence = max(1, Counter::instance()->incr(Key::COUNTER_GATEWAY_SLAVE_COMMAND_HISTORY_SEQ));
        $slot = $sequence % self::MAX_HISTORY;
        $slotKey = $this->slotKey($slot);
        $existing = $this->get($slotKey);
        if (is_array($existing) && !empty($existing['command_id'])) {
            Runtime::instance()->delete($this->commandSlotRuntimeKey((string)$existing['command_id']));
        }

        $now = time();
        $record = array_replace([
            'sequence' => $sequence,
            'command_id' => '',
            'command' => '',
            'host' => '',
            'state' => 'accepted',
            'message' => '',
            'error' => '',
            'params' => [],
            'result' => [],
            'created_at' => $now,
            'updated_at' => $now,
        ], $record);
        $record['sequence'] = $sequence;
        $record['updated_at'] = (int)($record['updated_at'] ?? $now);
        $record['created_at'] = (int)($record['created_at'] ?? $now);
        $this->set($slotKey, $record);

        $commandId = trim((string)($record['command_id'] ?? ''));
        if ($commandId !== '') {
            Runtime::instance()->set($this->commandSlotRuntimeKey($commandId), $slotKey);
        }
        return $record;
    }

    /**
     * 按 command_id 更新一条历史记录。
     *
     * @param string $commandId 指令 id
     * @param array<string, mixed> $updates 更新字段
     * @return array<string, mixed>|null 更新后的记录，未命中时返回 null
     */
    public function updateByCommandId(string $commandId, array $updates): ?array {
        $commandId = trim($commandId);
        if ($commandId === '') {
            return null;
        }
        $slotKey = $this->locateSlotKeyByCommandId($commandId);
        if ($slotKey === null) {
            return null;
        }

        $current = $this->get($slotKey);
        if (!is_array($current)) {
            return null;
        }
        $merged = Arr::merge($current, $updates);
        $merged['command_id'] = $commandId;
        $merged['updated_at'] = (int)($updates['updated_at'] ?? time());
        $merged['created_at'] = (int)($merged['created_at'] ?? time());
        $this->set($slotKey, $merged);
        Runtime::instance()->set($this->commandSlotRuntimeKey($commandId), $slotKey);
        return $merged;
    }

    /**
     * 返回分页后的指令历史。
     *
     * @param int $page 页码
     * @param int $size 每页条数
     * @param string $host 目标 host 过滤
     * @param string $state 状态过滤
     * @return array{page:int,size:int,total:int,list:array<int,array<string,mixed>>,summary:array<string,int>}
     */
    public function paginate(int $page = 1, int $size = 20, string $host = '', string $state = ''): array {
        $page = max(1, $page);
        $size = max(1, min(200, $size));
        $host = trim($host);
        $state = trim($state);
        $rows = array_values(array_filter($this->rows(), static fn($row): bool => is_array($row)));
        if ($host !== '') {
            $rows = array_values(array_filter($rows, static fn(array $row): bool => (string)($row['host'] ?? '') === $host));
        }
        if ($state !== '') {
            $rows = array_values(array_filter($rows, static fn(array $row): bool => (string)($row['state'] ?? '') === $state));
        }
        usort($rows, static function (array $a, array $b): int {
            $seqA = (int)($a['sequence'] ?? 0);
            $seqB = (int)($b['sequence'] ?? 0);
            if ($seqA === $seqB) {
                return (int)($b['updated_at'] ?? 0) <=> (int)($a['updated_at'] ?? 0);
            }
            return $seqB <=> $seqA;
        });

        $total = count($rows);
        $offset = ($page - 1) * $size;
        $list = array_slice($rows, $offset, $size);
        return [
            'page' => $page,
            'size' => $size,
            'total' => $total,
            'list' => $list,
            'summary' => $this->summary(),
        ];
    }

    /**
     * 汇总当前缓存中的指令状态统计。
     *
     * @return array{total:int,accepted:int,running:int,success:int,failed:int,pending:int,other:int}
     */
    public function summary(): array {
        $summary = [
            'total' => 0,
            'accepted' => 0,
            'running' => 0,
            'success' => 0,
            'failed' => 0,
            'pending' => 0,
            'other' => 0,
        ];
        foreach ($this->rows() as $row) {
            if (!is_array($row)) {
                continue;
            }
            $summary['total']++;
            $state = trim((string)($row['state'] ?? ''));
            if (isset($summary[$state])) {
                $summary[$state]++;
            } else {
                $summary['other']++;
            }
        }
        return $summary;
    }

    /**
     * 生成环形槽位 key。
     *
     * @param int $slot 槽位号
     * @return string
     */
    protected function slotKey(int $slot): string {
        return 'slot_' . str_pad((string)$slot, 4, '0', STR_PAD_LEFT);
    }

    /**
     * 生成 command_id 映射到槽位的 Runtime key。
     *
     * @param string $commandId 指令 id
     * @return string
     */
    protected function commandSlotRuntimeKey(string $commandId): string {
        return 'gw_cmd_slot:' . md5($commandId);
    }

    /**
     * 定位 command_id 对应的槽位 key。
     *
     * @param string $commandId 指令 id
     * @return string|null
     */
    protected function locateSlotKeyByCommandId(string $commandId): ?string {
        $mapped = Runtime::instance()->get($this->commandSlotRuntimeKey($commandId));
        if (is_string($mapped) && $mapped !== '') {
            $record = $this->get($mapped);
            if (is_array($record) && (string)($record['command_id'] ?? '') === $commandId) {
                return $mapped;
            }
            Runtime::instance()->delete($this->commandSlotRuntimeKey($commandId));
        }

        foreach ($this->rows() as $slotKey => $row) {
            if (is_array($row) && (string)($row['command_id'] ?? '') === $commandId) {
                Runtime::instance()->set($this->commandSlotRuntimeKey($commandId), (string)$slotKey);
                return (string)$slotKey;
            }
        }
        return null;
    }
}
