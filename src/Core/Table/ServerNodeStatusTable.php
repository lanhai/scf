<?php

namespace Scf\Core\Table;

use Swoole\Table;

class ServerNodeStatusTable extends ATable {

    /**
     * 节点运行状态
     * @var array
     */
    protected array $_config = [
        'size' => 1024,
        'colums' => [
            '_value' => ['type' => Table::TYPE_STRING, 'size' => 1024 * 100]
        ]
    ];
}