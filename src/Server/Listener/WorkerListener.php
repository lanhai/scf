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
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Router;
use Scf\Util\MemoryMonitor;
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
        if (!(defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true)) {
            //监控内存使用
            $limitMb = Config::server()['worker_memory_limit'] ?? 256;
            MemoryMonitor::start('worker:' . ($workerId + 1), limitMb: $limitMb, autoRestart: true);
        }
        //记录致命错误
        register_shutdown_function(function () use ($workerId) {
            $error = error_get_last();
            switch ($error['type'] ?? null) {
                case E_ERROR :
                case E_PARSE :
                case E_CORE_ERROR :
                case E_COMPILE_ERROR :
                    $message = "Worker#" . ($workerId + 1) . " 发生致命错误" . ($error['message'] ?? '');
                    Console::error($message, false);
                    @file_put_contents(
                        APP_LOG_PATH . '/error.log',
                        date('m-d H:i:s') . '.' . substr((string)(microtime(true) * 1000), -3) . ' ' . $message . PHP_EOL,
                        FILE_APPEND
                    );
                    break;
            }
        });
        //要使用app命名空间必须先加载模块
        App::mount();
        //给每个worker添加RPC服务
        try {
            Rpc::addService($workerId);
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
            Runtime::instance()->serverIsDraining(false);
            Runtime::instance()->serverIsReady(true);
            if (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true) {
                \Scf\Server\Http::instance()->startLocalIpcServer();
            }
            $info = <<<INFO
---------Workers启动完成---------
应用版本：{$version}
资源版本：{$publicVersion}
应用源码：{$srcPath}
---------------------------------
INFO;
            Console::write(Color::green($info));
        }


    }

    protected function onWorkerError(Server $server, int $worker_id, int $worker_pid, int $exit_code, int $signal): void {
        Timer::after(3000, function () use ($server, $worker_id) {
            if (!Process::kill($server->master_pid, 0)) {
                $server->stop($worker_id);
            }
        });
    }

    protected function onpipeMessage(Server $server, $workerId, $data) {
    }

    protected function onWorkerStop(Server $server, $workerId): void {
        if ($workerId === 0 && defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true) {
            try {
                \Scf\Server\Http::instance()->stopLocalIpcServer();
            } catch (Throwable) {
            }
        }
    }
}
