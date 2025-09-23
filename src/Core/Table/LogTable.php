<?php

namespace Scf\Core\Table;

use Swoole\Table;

class LogTable extends ATable {

    /**
     *  临时日志
     * @var array
     */
    protected array $_config = [
        'size' => 256,
        'colums' => [
            'type' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'log' => ['type' => Table::TYPE_STRING, 'size' => 1024 * 300]
        ]
    ];
}