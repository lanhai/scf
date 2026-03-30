<?php

namespace Scf\Core\Table;

use Swoole\Table;

class Counter extends ATable {

    /**
     *  配置项
     * @var array
     */
    protected array $_config = [
        // Counter 承载请求秒级桶、日志计数、运行期状态等复用 key，长期运行会持续增加。
        // 适当放大基础容量，降低冲突切片耗尽导致的 "unable to allocate memory" 概率。
        'size' => 8192,
        'colums' => [
            '_value' => ['type' => Table::TYPE_INT, 'size' => 0],
        ]
    ];

}
