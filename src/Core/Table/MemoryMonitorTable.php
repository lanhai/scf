<?php

namespace Scf\Core\Table;

use Swoole\Table;

class MemoryMonitorTable extends ATable {

    /**
     *  临时日志
     * @var array
     */
    protected array $_config = [
        'size' => 1024,
        'colums' => [
            'process' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'usage_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'real_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'peak_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'pid' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'time' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'rss_mb' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'pss_mb' => ['type' => Table::TYPE_STRING, 'size' => 32]
        ]
    ];
}