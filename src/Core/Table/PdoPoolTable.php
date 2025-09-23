<?php

namespace Scf\Core\Table;

use Swoole\Table;

class PdoPoolTable extends ATable {

    /**
     * Pdo连接池
     * @var array
     */
    protected array $_config = [
        'size' => 5120,
        'colums' => [
            'id' => ['type' => Table::TYPE_INT, 'size' => 10],
            'hash' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'object_id' => ['type' => Table::TYPE_STRING, 'size' => 50],
            'db' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'dsn' => ['type' => Table::TYPE_STRING, 'size' => 256],
            'pid' => ['type' => Table::TYPE_INT, 'size' => 10],
            'created' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'timer_id' => ['type' => Table::TYPE_INT, 'size' => 10],
        ]
    ];

}