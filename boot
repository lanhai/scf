<?php
declare(strict_types=1);
version_compare(PHP_VERSION, '8.1.0', '<') and die('运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION);
//系统路径
const SCF_ROOT = __DIR__;
//exec('echo ${APP_ENV}', $output, $retval);
//$appEnv = $output[0] ?: "release";
//var_dump($appEnv);

//TODO 根据加载的APP配置设置时区
ini_set('date.timezone', 'Asia/Shanghai');
//composer自动加载
require __DIR__ . '/vendor/autoload.php';
//root 必须优先加载,因为含系统常量
use Scf\Root;
require Root::dir() . '/Const.php';

use Scf\Command\Caller;
use Scf\Command\Runner;

$caller = new Caller();
$caller->setScript(current($argv));
$caller->setCommand(next($argv));
$caller->setParams($argv);
$ret = Runner::instance()->run($caller);
//应用目录
if (!empty($ret->getMsg())) {
    echo $ret->getMsg() . "\n";
}
