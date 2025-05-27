<?php

namespace Scf\Core\Table;

use Swoole\Table;

class CrontabTable extends ATable {

    /**
     *  协程任务列表
     * @var array
     */
    protected array $_config = [
        'size' => 64,
        'colums' => [
            '_value' => ['type' => Table::TYPE_STRING, 'size' => 2048]
        ]
    ];
}