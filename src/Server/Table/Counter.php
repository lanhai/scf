<?php

namespace Scf\Server\Table;

use Scf\Server\Runtime\Table;

class Counter extends Table {

    /**
     *  配置项
     * @var array
     */
    protected array $_config = [
        'size' => 1024,
        'colums' => [
            '_value' => ['type' => \Swoole\Table::TYPE_INT, 'size' => 0],
        ]
    ];

}