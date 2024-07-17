<?php

namespace Scf\Server\Table;

use Scf\Server\Runtime\Table;

class RouteTable extends Table {

    /**
     *  临时日志
     * @var array
     */
    protected array $_config = [
        'size' => 1024,
        'colums' => [
            'route' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 1024],
            'method' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'action' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'module' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'controller' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'space' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 128]
        ]
    ];

}