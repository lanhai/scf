<?php
return [
    'dashboard_port' => 8580,
    'port' => 9580,
    'rpc_port' => 10580,
    'worker_num' => 8,
    'max_wait_time' => 60,
    'task_worker_num' => 8,
    'static_handler_locations' => ['/cp'],
    'enable_coroutine' => true,
    'max_connection' => 1024,//最大连接数
    'max_coroutine' => 10240,//最多启动多少个协程
    'max_concurrency' => 2048,//最高并发
    'max_request_limit' => 1280,//每秒最大请求量限制,超过此值将拒绝服务
    'max_mysql_execute_limit' => 1280,//每秒最大mysql处理量限制,超过此值将拒绝服务
    'package_max_length' => 10 * 1024 * 1024,//最大请求数据限制,默认:10M
    'dashboard_password' => null,//控制台超级密码
    'redis_queue_in_master' => true,//master节点运行redis队列开关
    'redis_queue_in_slave' => false,//slave节点运行redis队列开关
    'slow_log_time' => 10000,//慢日志时间
    'redis_queue_mc' => 512,//redis队列每次取出上线
    'enable_db_statistics' => false,//开启数据库统计组件,需配置statistics和history数据库
    'allow_cross_origin' => false
];