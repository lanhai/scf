<?php

namespace Scf\Core\Table;

use Swoole\Table;

class MemoryMonitorTable extends ATable {

    /**
     *  进程内存占用
     * @var array
     */
    protected array $_config = [
        'size' => 520,
        'colums' => [
            'process' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'usage_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'real_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'peak_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'pid' => ['type' => Table::TYPE_INT, 'size' => 0],
            'updated' => ['type' => Table::TYPE_INT, 'size' => 0],
            'usage_updated' => ['type' => Table::TYPE_INT, 'size' => 0],
            'rss_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'pss_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'restart_ts' => ['type' => Table::TYPE_INT, 'size' => 0],
            'restart_count' => ['type' => Table::TYPE_INT, 'size' => 0],
            'limit_memory_mb' => ['type' => Table::TYPE_INT, 'size' => 0],
            'auto_restart' => ['type' => Table::TYPE_INT, 'size' => 1],
            'os_actual' => ['type' => Table::TYPE_FLOAT, 'size' => 0],
        ]
    ];
}