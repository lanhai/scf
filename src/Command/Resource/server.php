<?php
return [
    'module_style' => APP_MODULE_STYLE_MULTI,//模块风格 1:单模块;2:多模块
    'port' => 9580,
    'rpc_port' => 9680,
    'worker_num' => 8,
    'worker_memory_limit' => 256,//单worker内存使用限制(MB),超过设置定值将重启
    'max_wait_time' => 30,
    'task_worker_num' => 4,
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
    'redis_queue_mc' => 32,//redis队列每次取出数量
    'db_statistics_enable' => false,//开启数据库统计组件,需配置statistics和history数据库
    'db_statistics_interval' => 3,//统计数据入库扫描间隔
    'allow_cross_origin' => false,//允许跨域请求
];