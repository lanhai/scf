<?php

namespace Scf\Core\Table;

use Swoole\Table;

class SocketRouteTable extends ATable {

    /**
     *  路由表
     * @var array
     */
    protected array $_config = [
        'size' => 260,
        'colums' => [
            'entrance' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'class' => ['type' => Table::TYPE_STRING, 'size' => 256]
        ]
    ];


    public function has($route): bool {
        return $this->table->exist($route);
    }
}