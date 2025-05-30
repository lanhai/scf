<?php

namespace Scf\Core\Table;

use Swoole\Table;

class RouteCache extends ATable {

    /**
     * 配置项
     * @var array
     */
    protected array $_config = [
        'size' => 2048,
        'colums' => [
            '_value' => ['type' => Table::TYPE_STRING, 'size' => 1024]
        ]
    ];
}