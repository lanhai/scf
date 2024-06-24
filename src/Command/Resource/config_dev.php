<?php
return [
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