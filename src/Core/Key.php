<?php

namespace Scf\Core;

class Key {
    public const RUNTIME_HTTP_PORT = 'http_port';
    public const RUNTIME_SOCKET_PORT = 'socket_port';
    public const RUNTIME_RPC_PORT = 'rpc_port';
    public const RUNTIME_SERVER_STATUS = 'server_status';
    public const RUNTIME_CRONTAB_STATUS = 'crontab_status';
    public const COUNTER_CRONTAB_PROCESS = 'crontab_manager_process';
    public const COUNTER_REQUEST = '_REQUEST_TOTAL_';
    public const COUNTER_REQUEST_PROCESSING = '_REQUEST_PROCESSING_';
    public const COUNTER_REQUEST_REJECT_ = '_REQUEST_REJECT_';
    public const COUNTER_MYSQL_PROCESSING = '_MYSQL_PROCESSING_';
    public const COUNTER_SERVER_RESTART = '_SERVER_RESTART_';
}