<?php

namespace Scf\Server\Table;

use Scf\Server\Runtime\Table;

class PdoPoolTable extends Table {

    /**
     * Pdo连接池
     * @var array
     */
    protected array $_config = [
        'size' => 10240,
        'colums' => [
            'id' => ['type' => \Swoole\Table::TYPE_INT, 'size' => 10],
            'hash' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'object_id' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 50],
            'db' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'dsn' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 256],
            'pid' => ['type' => \Swoole\Table::TYPE_INT, 'size' => 10],
            'created' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 32],
            'timer_id' => ['type' => \Swoole\Table::TYPE_INT, 'size' => 10],
        ]
    ];

}