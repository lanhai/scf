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
        // 版本信息直接读取 Composer 已生成的本地元数据。Const.php 会被每个
        // Gateway/upstream/CLI 入口加载，绝不能在这里执行 Composer CLI，
        // 否则一次重拉会派生出一批 composer + php 进程。
        $prettyVersion = null;
        $installedFile = SCF_ROOT . '/vendor/composer/installed.php';
        if (is_file($installedFile)) {
            $installed = require $installedFile;
            $prettyVersion = $installed['versions'][$packageName]['pretty_version'] ?? null;
        }
        if (!is_string($prettyVersion) || $prettyVersion === '') {
            $lockFile = SCF_ROOT . '/composer.lock';
            $lockData = is_file($lockFile)
                ? json_decode((string)file_get_contents($lockFile), true)
                : null;
            if (is_array($lockData)) {
                foreach (array_merge($lockData['packages'] ?? [], $lockData['packages-dev'] ?? []) as $package) {
                    if (($package['name'] ?? '') !== $packageName) {
                        continue;
                    }
                    $prettyVersion = (string)($package['pretty_version'] ?? ($package['version'] ?? ''));
                    break;
                }
            }
        }
        if (is_string($prettyVersion) && preg_match('/v?(\d+(?:\.\d+)+)/', $prettyVersion, $matches)) {
            $scfVersion = (string)$matches[1];
        }
    }
}
defined('SCF_COMPOSER_VERSION') || define("SCF_COMPOSER_VERSION", $scfVersion);
const APP_MODULE_STYLE_SINGLE = 1;
const APP_MODULE_STYLE_MULTI = 2;
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
