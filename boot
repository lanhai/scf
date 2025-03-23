<?php
declare(strict_types=1);
version_compare(PHP_VERSION, '8.1.0', '<') and die('运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION);
//系统路径
const SCF_ROOT = __DIR__;
//TODO 根据加载的APP配置设置时区
ini_set('date.timezone', 'Asia/Shanghai');
require __DIR__ . '/vendor/autoload.php';
define('IS_DEV', in_array('-dev', $argv));
define('IS_PHAR', in_array('-phar', $argv));
define('IS_SRC', in_array('-src', $argv));
define('IS_BUILD', in_array('build', $argv));
define('IS_PUBLISH', in_array('publish', $argv));
define('IS_TOOLBOX', in_array('toolbox', $argv));
define('IS_HTTP_SERVER', in_array('server', $argv));
const FRAMEWORK_IS_PHAR = IS_PHAR || (!IS_DEV && !IS_SRC && !IS_BUILD);
//root 必须优先加载,因为含系统常量
//require __DIR__ . '/build/autoload.php';
$coreFile = __DIR__ . '/build/app.core';
$latestFile = __DIR__ . '/build/latest.core';
if (FRAMEWORK_IS_PHAR) {
    if (file_exists($latestFile)) {
        file_exists($coreFile) and unlink($coreFile);
        clearstatcache();
        if (!rename($latestFile, $coreFile)) {
            die("写入更新文件失败!");
        }
        clearstatcache();
    }
    if (!file_exists($coreFile)) {
        die("内核文件不存在");
    }
}
spl_autoload_register(function ($class) use ($coreFile) {
    // 将命名空间 Scf 映射到 PHAR 文件中的 src 目录
    if (str_starts_with($class, 'Scf\\')) {
        $classPath = str_replace('Scf\\', '', $class);
        if (FRAMEWORK_IS_PHAR) {
            $filePath = 'phar://' . $coreFile . '/' . str_replace('\\', '/', $classPath) . '.php';
        } else {
            $filePath = __DIR__ . '/src/' . str_replace('\\', '/', $classPath) . '.php';
        }
        if (file_exists($filePath)) {
            require $filePath;
        }
    }
});

use Scf\Root;

require Root::dir() . '/Const.php';
$serverBuild = require Root::dir() . '/version.php';
const FRAMEWORK_REMOTE_VERSION = 'https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/version.json';
define('FRAMEWORK_BUILD_TIME', $serverBuild['build']);
define('FRAMEWORK_BUILD_VERSION', $serverBuild['version']);

use Scf\Command\Caller;
use Scf\Command\Runner;

$caller = new Caller();
$caller->setScript(current($argv));
$caller->setCommand(next($argv));
$caller->setParams($argv);
$ret = Runner::instance()->run($caller);
if (!empty($ret->getMsg())) {
    echo $ret->getMsg() . "\n";
}