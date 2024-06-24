<?php

namespace Scf\Server\Table;

use Scf\Server\Runtime\Table;

class Runtime extends Table {

    /**
     * 配置项
     * @var array
     */
    protected array $_config = [
        'size' => 128,
        'colums' => [
            '_value' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 102400]
        ]
    ];
}