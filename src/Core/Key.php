<?php

namespace Scf\Core;

class Key {
    public const RUNTIME_HTTP_PORT = 'http_port';
    public const RUNTIME_DASHBOARD_PORT = 'dashboard_port';
    public const RUNTIME_SOCKET_PORT = 'socket_port';
    public const RUNTIME_MASTERDB_PORT = 'master_db_port';
    public const RUNTIME_RPC_PORT = 'rpc_port';
    public const RUNTIME_SERVER_STATUS = 'server_status';
    public const RUNTIME_SERVER_DRAINING = 'server_draining';
    public const RUNTIME_REDIS_QUEUE_STATUS = 'redis_queue_status';
    public const RUNTIME_CRONTAB_STATUS = 'crontab_status';
    public const RUNTIME_CRONTAB_TASK_LIST = 'crontab_list';
    public const RUNTIME_UPSTREAM_SUPERVISOR_PID = 'upstream_supervisor_pid';
    public const RUNTIME_UPSTREAM_SUPERVISOR_STARTED_AT = 'upstream_supervisor_started_at';
    public const RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT = 'upstream_supervisor_heartbeat_at';
    public const RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID = 'gateway_business_coordinator_pid';
    public const RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT = 'gateway_business_coordinator_heartbeat_at';
    public const RUNTIME_GATEWAY_HEALTH_MONITOR_PID = 'gateway_health_monitor_pid';
    public const RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT = 'gateway_health_monitor_heartbeat_at';
    public const RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID = 'gateway_cluster_coordinator_pid';
    public const RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT = 'gateway_cluster_coordinator_heartbeat_at';
    public const RUNTIME_GATEWAY_INSTALL_TAKEOVER = 'gateway_install_takeover';
    public const RUNTIME_GATEWAY_INSTALL_UPDATING = 'gateway_install_updating';
    public const RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT = 'subprocess_manager_heartbeat_at';
    public const RUNTIME_SUBPROCESS_ALIVE_COUNT = 'subprocess_alive_count';
    public const RUNTIME_SUBPROCESS_SHUTTING_DOWN = 'subprocess_shutting_down';
    public const COUNTER_CRONTAB_PROCESS = 'crontab_manager_process';
    public const COUNTER_REDIS_QUEUE_PROCESS = 'redis_queue_manager_process';
    public const COUNTER_REQUEST = '_REQUEST_TOTAL_';
    public const COUNTER_REQUEST_PROCESSING = '_REQUEST_PROCESSING_';
    public const COUNTER_RPC_REQUEST_PROCESSING = '_RPC_REQUEST_PROCESSING_';
    public const COUNTER_REQUEST_REJECT_ = '_REQUEST_REJECT_';
    public const COUNTER_MYSQL_PROCESSING = '_MYSQL_PROCESSING_';
    public const COUNTER_REDIS_QUEUE_PROCESSING = '_REDIS_QUEUE_PROCESSING_';
    public const COUNTER_SERVER_RESTART = '_SERVER_RESTART_';
    public const COUNTER_SERVER_ISRUNNING = '_SERVER_IS_RUNNING_';


}
