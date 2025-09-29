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
        ]
    ];

    public function route(int $fd): bool|string {
        if (!$this->table->exist($fd)) {
            return false;
        }
        return $this->table->get($fd, 'route');
    }
}