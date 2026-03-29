<?php

namespace Scf\Command;


require_once __DIR__ . '/Result.php';
require_once __DIR__ . '/DefaultCommand/Gateway.php';
require_once __DIR__ . '/DefaultCommand/GatewayUpstream.php';
require_once __DIR__ . '/DefaultCommand/Crontab.php';

use Scf\Command\DefaultCommand\Framework;
use Scf\Command\DefaultCommand\Gateway;
use Scf\Command\DefaultCommand\GatewayUpstream;
use Scf\Core\Traits\Singleton;
use Scf\Command\DefaultCommand\Bot;
use Scf\Command\DefaultCommand\Build;
use Scf\Command\DefaultCommand\Crontab;
use Scf\Command\DefaultCommand\Install;
use Scf\Command\DefaultCommand\Server;
use Scf\Command\DefaultCommand\Native;
use Scf\Command\DefaultCommand\Toolbox;
use Scf\Command\DefaultCommand\Run;
use Scf\Root;

class Runner {
    use Singleton;

    public function __construct() {
        Manager::instance()->addCommand(new Server());//服务器
        Manager::instance()->addCommand(new Native());//原生应用
        Manager::instance()->addCommand(new Install());//应用安装
        Manager::instance()->addCommand(new Build());//应用构建
        Manager::instance()->addCommand(new Toolbox());//cli工具箱
        Manager::instance()->addCommand(new Bot());//机器人
        Manager::instance()->addCommand(new Crontab());//一次性定时任务
        Manager::instance()->addCommand(new Run());//脚本运行
        Manager::instance()->addCommand(new Framework());//框架包管理
        Manager::instance()->addCommand(new Gateway());//代理网关
        Manager::instance()->addCommand(new GatewayUpstream());//代理业务实例
    }

    private mixed $beforeCommand;

    public function setBeforeCommand(callable $before): void {
        $this->beforeCommand = $before;
    }

    public function run(Caller $caller): Result {
        // 优先引入root，因为含系统常量
        require_once Root::dir() . '/Const.php';

        if (isset($this->beforeCommand) && is_callable($this->beforeCommand)) {
            call_user_func($this->beforeCommand, $caller);
        }
        if (function_exists('apc_clear_cache')) {
            apc_clear_cache();
        }
        if (function_exists('opcache_reset')) {
            opcache_reset();
        }

        $msg = Manager::instance()->run($caller);

        $result = new Result();
        $result->setMsg($msg);
        return $result;
    }
}
