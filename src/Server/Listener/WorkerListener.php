<?php

namespace Scf\Server\Listener;

use Exception;
use Scf\Cloud\Ali\Oss;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Table\Runtime;
use Scf\Database\Statistics\StatisticModel;
use Scf\Mode\Web\Router;
use Scf\Server\Http;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Throwable;
use Scf\Mode\Rpc\App as Rpc;

class WorkerListener extends Listener {

    /**
     */
    protected function onWorkerStart(Server $server, $workerId): void {
        Timer::after(1000, function () use ($server, $workerId) {
            if (!Process::kill($server->master_pid, 0)) {
                Process::kill($server->worker_pid, SIGKILL);
            }
        });
        //要使用app命名空间必须先加载模块
        App::mount();
        Console::enablePush();
        //给每个worker添加RPC服务
        try {
            Rpc::addService( $workerId);
        } catch (Exception $e) {
            Console::error($e->getMessage());
        }
        if ($workerId == 0) {
            $srcPath = App::src();
            $version = App::version();
            $publicVersion = App::publicVersion();
            //注册路由
            Router::instance()->loadRoutes();
            App::updateDatabase();
            $serverConfig = Config::server();
            //升级/创建统计数据表
            $enableStatistics = $serverConfig['db_statistics_enable'] ?? false;
            if ($enableStatistics && App::isMaster()) {
                StatisticModel::instance()->updateDB();
            }
            try {
                Oss::instance()->createTable();
            } catch (Throwable $throwable) {
                Console::error($throwable->getMessage());
            }
            Runtime::instance()->serverStatus(true);

            $info = <<<INFO
---------Workers启动完成---------
内核版本：{$version}
资源版本：{$publicVersion}
源码目录：{$srcPath}
---------------------------------
INFO;
            Console::write(Color::green($info));
        }
    }

    protected function onWorkerError(Server $server, int $worker_id, int $worker_pid, int $exit_code, int $signal): void {
//        if ($signal !== 2) {
//            Console::log(Color::red('#' . $worker_pid . ' worker #' . $worker_id . ' 发生致命错误!signal:' . $signal . ',exit_code:' . $exit_code));
//        }
        Timer::after(3000, function () use ($server, $worker_id) {
            if (!Process::kill($server->master_pid, 0)) {
                $server->stop($worker_id);
            }
        });
    }

    protected function onWorkerStop(Server $server, $workerId): void {

    }
}