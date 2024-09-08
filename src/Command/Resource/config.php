<?php

return [
    // 应用配置,在运行时赋值
    'app' => [
        'master_host' => '127.0.0.1',
        'module_style' => APP_MODULE_STYLE_MICRO
    ],
    // 类映射
    'class_map' => [],
    // 默认主题名
    'default_theme' => 'default',
    // 默认的输入过滤器
    'default_filter' => 'trim,strip_tags,htmlspecialchars',
    //阿里云SDK配置
    'aliyun' => [
        'accounts' => [
            'default' => [
                'accessId' => '',
                'accessKey' => '',
                'product' => ['oss', 'sts']
            ]
        ],
        //oss客户端
        'Scf\Cloud\Ali\Oss' => [
            'default_server' => 'oss_0',
            'server' => [
                'oss_0' => [
                    'account' => 'default',
                    'REGION_ID' => '',
                    'LOG' => false,
                    'APP_LOG_PATH' => APP_PATH . '/log/alioss/',
                    'DISPLAY_LOG' => false,
                    'OSS_HOST' => '',
                    'BUCKET' => '',
                    'IS_CNNAME' => true,
                    'ENDPOINT' => '',
                    'CDN_DOMAIN' => '',
                    'sts' => [
                        'regionId' => 'cn-wuhan',
                        'RoleArn' => '',
                        'policy' => '{"Statement": [{"Action": ["oss:*"],"Effect": "Allow","Resource": ["acs:oss:*"]}],"Version": "1"}',
                        'tokenExpire' => 900
                    ]
                ]
            ]
        ],
        //短信发送
        'Scf\Cloud\Ali\Sms' => [
            'default_server' => 'default',
            'server' => [
                'default' => [
                    'account' => 'default',
                    'regionId' => '',
                    'endpoint' => 'dysmsapi.aliyuncs.com',
                    'verify' => [
                        'sign' => '',
                        'template' => ''
                    ]
                ]
            ],
        ],
        //sts客户端
        'Scf\Cloud\Ali\Sts' => [
            'default_server' => 'default',
            'server' => [
                'default' => [
                    'account' => 'default',
                    'regionId' => 'cn-wuhan',
                    'RoleArn' => '',
                    'policy' => '{"Statement": [{"Action": ["oss:*"],"Effect": "Allow","Resource": ["acs:oss:*"]}],"Version": "1"}',
                    'tokenExpire' => 900
                ]
            ],
        ],
        //内容审核
        'Scf\Cloud\Ali\Green' => [
            'default_server' => 'default',
            'server' => [
                'default' => [
                    'account' => 'default',
                    'regionId' => 'cn-shanghai',
                ]
            ],
        ]
    ],
    // 组件配置
    'components' => [
        //日志推送服务配置
        'Scf\Component\SocketMessager' => [
            'broker' => '',
            'port' => 1883,
            'client_id' => 'logger_publisher',
            'username' => 'admin',
            'password' => 'public',
            'topic' => 'php_log',
            'enable' => false
        ],
    ],
    //简易webservice
    'simple_webservice' => [
        'debug_appid' => 'default',
        'debug_appkey' => '',
        'debug_mpappid' => '',
        'gateway' => [
            'dev' => '',
            'produce' => '',
        ],
        'apps' => [
            'default' => [
                'desc' => '',
                'key' => ''
            ]
        ]
    ],
    //数据库连接配置
    'database' => [
        'driver' => 'mysql',
        'pool' => [
            'max_open' => 32,// 最大开启连接数
            'max_idle' => 8,// 最大闲置连接数
            'task_worker_enable' => true,
            'task_worker_max_open' => 1,
            'task_worker_max_idle' => 1,
            'max_lifetime' => 3600,//连接的最长生命周期
            'wait_timeout' => 0.0,// 从池获取连接等待的时间, 0为一直等待
            'connection_auto_ping_interval' => 60,//自动ping间隔时间
            'connection_idle_timeout' => 300,//空闲回收时间
        ],
        'maintainable' => [
        ],
        'mysql' => [
            'default' => [
                'name' => '',
                'master' => [],
                'slave' => [],
                'port' => 3306,
                'username' => '',
                'password' => '',
                'charset' => 'utf8mb4',
                'prefix' => 't_',
            ],
        ]
    ],
    //缓存配置
    'cache' => [
        'redis' => [
            'ttl' => 3600,//缺省生存时间
            'default_server' => 'main',
            'servers' => [
                'main' => [
                    'host' => '',
                    'port' => 6379,
                    'auth' => '',
                    'db_index' => 0,
                    'time_out' => 1,//连接超时时间
                    'size' => 16,//连接池连接数
                    'max_idle' => 8,//最大闲置连接数
                ]
            ],
        ]
    ],
];
