<?php
return [
    //缓存配置
    'cache' => [
        'redis' => [
            'ttl' => 3600,//缺省生存时间
            'default_server' => 'main',
            'servers' => [
                'main' => [
                    'host' => 'master',
                    'port' => 6379,
                    'auth' => 'lkyredisPwd!@#1',
                    'db_index' => 0,
                    'time_out' => 1,//连接超时时间
                    'size' => 64,
                    'max_idle' => 32//最大闲置连接数
                ]
            ],
        ]
    ],
    //RPC配置
    'rpc' => [
        'client' => [
            //客户端的服务对应的的服务器设置
            'servers' => [
                'example_rpc_server' => [
                    'host' => '127.0.0.1',
                    'port' => 9585
                ]
            ],
            'services' => [
                'App\Rpc\Client\Demo' => [
                    'server' => 'example_rpc_server',//填写服务器名称
                    'service' => 'Rpc/Receive',
                    'appid' => 'test'//APPID作为数据加密/解密key的验证
                ],
                'App\Rpc\Client\Date' => [
                    'server' => 'example_rpc_server',//填写服务器名称
                    'service' => 'Rpc/Date',
                    'appid' => 'test'//APPID作为数据加密/解密key的验证
                ]
            ]
        ]
    ]
];