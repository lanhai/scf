<?php
return [
    'port' => 9580,
    'worker_num' => 8,
    'max_wait_time' => 60,
    'task_worker_num' => 8,
    'static_handler_locations' => ['/cp'],
    'enable_coroutine' => true,
    'max_connection' => 4096,//最大连接数
    'max_coroutine' => 10240,//最多启动多少个协程
    'max_concurrency' => 2048,//最高并发
    'max_request_limit' => 1280,//每秒最大请求量限制,超过此值将拒绝服务
    'max_mysql_execute_limit' => 12800,//每秒最大mysql处理量限制,超过此值将拒绝服务
    'package_max_length' => 10 * 1024 * 1024,//最大请求数据限制 10M
    'dashboard_password' => null
];