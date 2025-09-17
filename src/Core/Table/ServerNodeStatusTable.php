<?php

namespace Scf\Core\Table;

use Swoole\Table;

class ServerNodeStatusTable extends ATable {

    /**
     * 配置项
     * @var array
     */
    protected array $_config = [
        'size' => 1030,
        'colums' => [
            '_value' => ['type' => Table::TYPE_STRING, 'size' => 1024 * 1024]
        ]
    ];
}