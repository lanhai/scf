<?php
declare(strict_types=1);
ini_set('date.timezone', 'Asia/Shanghai');
version_compare(PHP_VERSION, '8.1.0', '<') and die('运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION);
const SCF_VERSION = '1.0.0';
const STATUS_ON = 1;
const STATUS_OFF = 0;
const SWITCH_ON = 'on';
const SWITCH_OFF = 'off';
const MODE_CGI = 'cgi';
const MODE_RPC = 'rpc';
const MODE_CLI = 'cli';
//系统路径
const SCF_ROOT = __DIR__;
//主服务器,用于写入,或者单机读写
const DBS_MASTER = 1;
//从服务器,用于读取
const DBS_SLAVE = 2;
//项目顶级命名空间
const APP_TOP_NAMESPACE = 'App';

use Scf\Command\Manager;
use Scf\Command\Runner;
use Scf\Command\Caller;

//composer自动加载
require __DIR__ . '/lib/vendor/autoload.php';
$manager = Manager::instance();
$options = $manager->getOpts();
//应用目录
define("SCF_APPS_ROOT", $options['apps_dir'] ?? SCF_ROOT . '/../apps/');

$caller = new Caller();
$caller->setScript(current($argv));
$caller->setCommand(next($argv));
$caller->setParams($argv);
reset($argv);

$ret = Runner::instance()->run($caller);
if (!empty($ret->getMsg())) {
    echo $ret->getMsg() . "\n";
}
