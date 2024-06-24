<?php
const SCF_VERSION = '1.0.0';
const APP_MODULE_STYLE_MICRO = 1;
const NETWORK_MODE_SINGLE = 'single';
const NETWORK_MODE_GROUP = 'group';

const PROTOCOL_HTTPS = 'https://';
const PROTOCOL_HTTP = 'http://';
const APP_MODULE_STYLE_LARGE = 2;
const SWOOLE_SERVER = 'server';
const SWOOLE_SERVER_HTTP = 'server_http';
const SWOOLE_SERVER_SOCKET = 'server_socket';
const REDIS_IGNORE_KEY_PREFIX = '__{IgnorePrefix}__';
const STATUS_ON = 1;
const STATUS_OFF = 0;
const MATCH_IS = 1;
const MATCH_IS_NOT = 0;
const SWITCH_ON = 'on';
const SWITCH_OFF = 'off';
const MODE_CGI = 'cgi';
const MODE_RPC = 'rpc';
const MODE_CLI = 'cli';
//主服务器,用于写入,或者单机读写
const DBS_MASTER = 1;
//从服务器,用于读取
const DBS_SLAVE = 2;
//项目顶级命名空间
const APP_TOP_NAMESPACE = 'App';