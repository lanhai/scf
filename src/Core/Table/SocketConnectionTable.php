<?php

namespace Scf\Core\Table;

use Swoole\Table;

class SocketConnectionTable extends ATable {

    /**
     * socket连接表
     * @var array
     */
    protected array $_config = [
        'size' => 2100,
        'colums' => [
            'route' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'worker_id' => ['type' => Table::TYPE_INT, 'size' => 0],
            'counted' => ['type' => Table::TYPE_INT, 'size' => 0],
        ]
    ];

    public function route(int $fd): bool|string {
        if (!$this->table->exist($fd)) {
            return false;
        }
        return $this->table->get($fd, 'route');
    }

    public function remember(int $fd, int $workerId, string $route = ''): bool {
        return $this->set($fd, [
            'route' => $route,
            'worker_id' => $workerId,
            'counted' => 1,
        ]);
    }

    public function connection(int $fd): array|false {
        if (!$this->table->exist($fd)) {
            return false;
        }
        return $this->get($fd);
    }

    public function workerConnectionStats(): array {
        $stats = [];
        foreach ($this->rows() as $row) {
            if (!is_array($row) || empty($row['counted'])) {
                continue;
            }
            $workerId = (int)($row['worker_id'] ?? 0);
            if ($workerId <= 0) {
                continue;
            }
            $stats[$workerId] = ($stats[$workerId] ?? 0) + 1;
        }
        return $stats;
    }
}
