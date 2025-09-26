<?php

namespace Scf\Core\Table;

use Swoole\Table;

class RouteTable extends ATable {

    /**
     *  路由表
     * @var array
     */
    protected array $_config = [
        'size' => 1024,
        'colums' => [
            'route' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'type' => ['type' => Table::TYPE_INT, 'size' => 1],
            'method' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'action' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'module' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'controller' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'space' => ['type' => Table::TYPE_STRING, 'size' => 128]
        ]
    ];

}