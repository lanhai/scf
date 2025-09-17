<?php

namespace Scf\Core\Table;

use Swoole\Table;

class ServerNodeTable extends ATable {

    /**
     * 子节点
     * @var array
     */
    protected array $_config = [
        'size' => 300,
        'colums' => [
            'host' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'role' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'socket_fd' => ['type' => Table::TYPE_INT, 'size' => 0],
            'connect_time' => ['type' => Table::TYPE_INT, 'size' => 0]
        ]
    ];

}