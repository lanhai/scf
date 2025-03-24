<?php

namespace Scf\Server\Table;

use Scf\Server\Runtime\Table;

class LogTable extends Table {

    /**
     *  临时日志
     * @var array
     */
    protected array $_config = [
        'size' => 1024 * 2,
        'colums' => [
            'type' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'log' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 1024 * 1024 * 2],
        ]
    ];
}