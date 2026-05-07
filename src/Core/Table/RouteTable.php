<?php

namespace Scf\Core\Table;

use Swoole\Table;

class RouteTable extends ATable {

    /**
     * 路由表
     * @var array
     */
    protected array $_config = [
        'size' => 1024,
        'colums' => [
            'route' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'type' => ['type' => Table::TYPE_INT, 'size' => 1],
            'method' => ['type' => Table::TYPE_STRING, 'size' => 32],
            // 注解路由已经出现 `actionRemoteRegisterPersonCandidateTaskStart`
            // 这类长度超过 32 的方法名。这里如果继续截断 action，
            // 路由最终会命中错误的方法，表现为“同一路径执行了另一个接口”。
            'action' => ['type' => Table::TYPE_STRING, 'size' => 128],
            'module' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'controller' => ['type' => Table::TYPE_STRING, 'size' => 32],
            'space' => ['type' => Table::TYPE_STRING, 'size' => 128]
        ]
    ];

}
