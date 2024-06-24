<?php

return [
    'components' => [
        // 私有代码组件
        'HuiYun\Component\PrivateCode' => [
            'enable' => true,
        ],
        // 域名参数解析组件
        'HuiYun\Component\DomainParams' => [
            'enable' => true,
            'rule' => [
                'mch_domain' => 0,
            ],
            'merge_to_get' => true,
        ],
        // Session 配置
        'HuiYun\Component\Session' => [
            'prefix' => '_site_',
        ],
        // Cookie配置
        'HuiYun\Component\Cookie' => [
            'prefix' => ''
        ],
    ]
];
