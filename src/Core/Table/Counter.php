<?php

namespace Scf\Core\Table;

use Swoole\Table;

class Counter extends ATable {

    /**
     *  配置项
     * @var array
     */
    protected array $_config = [
        'size' => 1024,
        'colums' => [
            '_value' => ['type' => Table::TYPE_INT, 'size' => 0],
        ]
    ];

}