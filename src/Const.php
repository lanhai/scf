<?php
$scfVersion = 'development';
// 要检查的包名
$packageName = 'lhai/scf';
// 读取 composer.json 文件
$composerJson = file_get_contents(SCF_ROOT . '/composer.json');
// 解析 JSON 数据
$composerData = json_decode($composerJson, true);
if (json_last_error() == JSON_ERROR_NONE) {
    // 检查包是否存在于 require 或 require-dev 中
    $requires = $composerData['require'] ?? [];
    $requiresDev = $composerData['require-dev'] ?? [];
    if (array_key_exists($packageName, $requires) || array_key_exists($packageName, $requiresDev)) {
        $cmd = 'composer show --working-dir=' . SCF_ROOT . ' ' . $packageName;
        // 运行 composer show 命令
        if ($composerOutput = shell_exec($cmd)) {
            // 使用正则表达式匹配版本号
            preg_match('/versions\s*:\s*\*?\s*v([\d.]+)/', $composerOutput, $matches);
            if (isset($matches[1])) {
                $scfVersion = $matches[1];
            }
        }
    }
}
define("SCF_VERSION", $scfVersion);
const APP_MODULE_STYLE_MICRO = 1;
const APP_MODULE_STYLE_LARGE = 2;
const NETWORK_MODE_SINGLE = 'single';
const NETWORK_MODE_GROUP = 'group';
const PROTOCOL_HTTPS = 'https://';
const PROTOCOL_HTTP = 'http://';
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
const MODE_NATIVE = 'native';
const MODE_SOCKET = 'socket';
//主服务器,用于写入,或者单机读写
const DBS_MASTER = 1;
//从服务器,用于读取
const DBS_SLAVE = 2;
//项目顶级命名空间
const APP_TOP_NAMESPACE = 'App';
const NODE_ROLE_MASTER = 'master';
const NODE_ROLE_SLAVE = 'slave';