<?php

namespace Scf\Server\Struct;

use Scf\Core\Struct;

class Node extends Struct {

    /**
     * @var string 节点ID
     */
    public string $id;
    /**
     * @var string 应用ID
     */
    public string $appid;
    /**
     * @var string 节点名称
     */
    public string $name;
    /**
     * @var string IP地址
     * @required true|节点IP不能为空
     * @rule string|节点IP地址格式错误
     */
    public string $ip = '';
    /**
     * @var int 端口号
     */
    public int $port;
    /**
     * @var int socket端口
     */
    public int $socketPort;
    /**
     * @var int 启动时间
     */
    public int $started;
    /**
     * @var int 重启次数
     */
    public int $restart_times;
    /**
     * @var int 心跳时间
     */
    public int $heart_beat;
    /**
     * @var ?string 框架版本
     */
    public ?string $framework_build_version;
    /**
     * @var bool 框架升级包是否准备就绪
     * @default bool:false
     */
    public bool $framework_update_ready;
    /**
     * @var string 节点角色类型
     */
    public string $role;
    /**
     * @var int 主进程pid
     */
    public int $master_pid;
    /**
     * @var int 管理进程pid
     */
    public int $manager_pid;
    /**
     * @var string 应用服务器指纹
     */
    public string $fingerprint;
    /**
     * /**
     * @var string 应用版本号
     */
    public string $app_version;
    /**
     * @var ?string 资源版本号
     */
    public ?string $public_version;
    /**
     * @var string 框架版本号
     */
    public string $scf_version;
    /**
     * @var string swoole版本号
     */
    public string $swoole_version;
    /**
     * @var int CPU核心数
     */
    public int $cpu_num;
    /**
     * @var ?int 栈内存使用量
     */
    public ?int $stack_useage;
    /**
     * @var ?array 内存表
     */
    public ?array $tables;
    /**
     * @var ?array
     */
    public ?array $tasks;
    /**
     * @var ?int
     */
    public ?int $threads;
    /**
     * @var ?array
     */
    public ?array $thread_status;
    /**
     * @var ?string
     */
    public ?string $server_run_mode;
    /**
     * @var ?int
     */
    public ?int $http_request_count_current;
    /**
     * @var ?int
     */
    public ?int $http_request_count_today;
    /**
     * @var ?int
     */
    public ?int $http_request_reject;
    /**
     * @var ?int
     */
    public ?int $http_request_count;
    /**
     * @var int|null
     */
    public ?int $http_request_processing;
    /**
     * @var int|null
     */
    public ?int $mysql_execute_count;
    /**
     * @var array|null
     */
    public ?array $server_stats;

}