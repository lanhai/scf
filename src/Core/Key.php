<?php

namespace Scf\Core;

class Key {
    public const RUNTIME_HTTP_PORT = 'r_hp';
    public const RUNTIME_DASHBOARD_PORT = 'r_dp';
    public const RUNTIME_SOCKET_PORT = 'r_sp';
    public const RUNTIME_MASTERDB_PORT = 'r_mdp';
    public const RUNTIME_RPC_PORT = 'r_rp';
    public const RUNTIME_SERVER_STARTED_AT = 'r_ssa';
    public const RUNTIME_SERVER_STATUS = 'r_ss';
    public const RUNTIME_SERVER_DRAINING = 'r_sd';
    public const RUNTIME_REDIS_QUEUE_STATUS = 'r_rqs';
    public const RUNTIME_CRONTAB_STATUS = 'r_cts';
    public const RUNTIME_CRONTAB_TASK_LIST = 'r_ctl';
    public const RUNTIME_UPSTREAM_SUPERVISOR_PID = 'r_usp';
    public const RUNTIME_UPSTREAM_SUPERVISOR_STARTED_AT = 'r_ussa';
    public const RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT = 'r_usha';
    public const RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID = 'r_gbcp';
    public const RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT = 'r_gbcha';
    public const RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE = 'r_gbcq';
    public const RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK = 'r_gbrf';
    public const RUNTIME_GATEWAY_HEALTH_MONITOR_PID = 'r_ghmp';
    public const RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT = 'r_ghmha';
    public const RUNTIME_GATEWAY_HEALTH_MONITOR_TRACE_SNAPSHOT = 'r_ghmts';
    public const RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID = 'r_gccp';
    public const RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT = 'r_gccha';
    public const RUNTIME_GATEWAY_CLUSTER_COORDINATOR_TRACE_SNAPSHOT = 'r_gccts';
    public const RUNTIME_MEMORY_MONITOR_PID = 'r_mmp';
    public const RUNTIME_MEMORY_MONITOR_HEARTBEAT_AT = 'r_mmha';
    public const RUNTIME_HEARTBEAT_PID = 'r_hbp';
    public const RUNTIME_HEARTBEAT_PROCESS_HEARTBEAT_AT = 'r_hbpha';
    public const RUNTIME_LOG_BACKUP_PID = 'r_lbp';
    public const RUNTIME_LOG_BACKUP_HEARTBEAT_AT = 'r_lbha';
    public const RUNTIME_CRONTAB_MANAGER_PID = 'r_cmp';
    public const RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT = 'r_cmha';
    public const RUNTIME_REDIS_QUEUE_MANAGER_PID = 'r_rqmp';
    public const RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT = 'r_rqmha';
    public const RUNTIME_REDIS_QUEUE_WORKER_PID = 'r_rqwp';
    public const RUNTIME_FILE_WATCHER_PID = 'r_fwp';
    public const RUNTIME_FILE_WATCHER_HEARTBEAT_AT = 'r_fwha';
    public const RUNTIME_GATEWAY_INSTALL_TAKEOVER = 'r_gitk';
    public const RUNTIME_GATEWAY_INSTALL_UPDATING = 'r_giup';
    public const RUNTIME_GATEWAY_STARTUP_READY_INSTANCES = 'gw_boot_i';
    public const RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES = 'gw_us_sync';
    public const RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS = 'gw_rm_gen';
    public const RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING = 'gw_sum_p';
    public const RUNTIME_GATEWAY_STARTUP_SUMMARY_READY = 'gw_sum_r';
    public const RUNTIME_SUBPROCESS_MANAGER_PID = 'r_smp';
    public const RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT = 'r_smha';
    public const RUNTIME_SUBPROCESS_ALIVE_COUNT = 'r_smac';
    public const RUNTIME_SUBPROCESS_SHUTTING_DOWN = 'r_smsd';
    public const RUNTIME_SUBPROCESS_CONTROL_STATE = 'r_smcs';
    public const COUNTER_CRONTAB_PROCESS = 'c_cmp';
    public const COUNTER_REDIS_QUEUE_PROCESS = 'c_rqmp';
    public const COUNTER_REQUEST = 'c_req_t';
    public const COUNTER_REQUEST_PROCESSING = 'c_req_p';
    public const COUNTER_RPC_REQUEST_PROCESSING = 'c_rreq_p';
    public const COUNTER_REQUEST_REJECT_ = 'c_req_r';
    public const COUNTER_MYSQL_PROCESSING = 'c_mysql_p';
    public const COUNTER_MYSQL_INFLIGHT = 'c_mysql_i';
    public const COUNTER_REDIS_INFLIGHT = 'c_redis_i';
    public const COUNTER_OUTBOUND_HTTP_INFLIGHT = 'c_ohttp_i';
    public const COUNTER_REDIS_QUEUE_PROCESSING = 'c_rq_p';
    public const COUNTER_SERVER_RESTART = 'c_srv_rst';
    public const COUNTER_SERVER_ISRUNNING = 'c_srv_run';


}
