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
        // gateway-only 模式下 worker 行会在 WorkerStart 注册，随后由
        // CgiListener + MemoryUsageCount 协同维护 usage 与 OS 实际占用。
        // 这里继续保留 worker 级致命错误记录能力。
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
        $this->registerWorkerMemoryMonitor($server, (int)$workerId);
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
            $currentPort = (int)(Runtime::instance()->httpPort() ?: 0);
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
            $info = <<<INFO
---------Workers启动完成---------
应用版本：{$version}
资源版本：{$publicVersion}
当前端口：{$currentPort}
应用源码：{$srcPath}
---------------------------------
INFO;
            Console::write(Color::green($info));
        }
    }

    /**
     * 为 upstream 的 server worker / task worker 注册内存监控行。
     *
     * upstream.memory_rows 与 heartbeat 聚合展示都依赖 MemoryMonitorTable 的进程行。
     * 这里统一在 WorkerStart 按 worker 类型落表，保证 dashboard 能同时看到：
     * - `worker:{N}`
     * - `task_worker:{N}`
     *
     * @param Server $server 当前 server 实例。
     * @param int $workerId 当前 worker id（0-based，包含 task worker）。
     * @return void
     */
    protected function registerWorkerMemoryMonitor(Server $server, int $workerId): void {
        $serverWorkerNum = (int)($server->setting['worker_num'] ?? 0);
        $taskWorkerNum = (int)($server->setting['task_worker_num'] ?? 0);
        $processName = null;
        if ($workerId >= 0 && $workerId < $serverWorkerNum) {
            $processName = "worker:" . ($workerId + 1);
        } elseif ($workerId >= $serverWorkerNum && $workerId < ($serverWorkerNum + $taskWorkerNum)) {
            $processName = "task_worker:" . (($workerId - $serverWorkerNum) + 1);
        }
        if ($processName === null) {
            return;
        }
        $serverConfig = Config::server();
        $workerMemoryLimit = max(1, (int)($serverConfig['worker_memory_limit'] ?? 256));
        $autoRestart = (bool)($serverConfig['worker_memory_auto_restart'] ?? false);
        MemoryMonitor::start($processName, 2000, $workerMemoryLimit, $autoRestart);
    }

    protected function onWorkerError(Server $server, int $worker_id, int $worker_pid, int $exit_code, int $signal): void {
        Timer::after(3000, function () use ($server, $worker_id) {
            if (!Process::kill($server->master_pid, 0)) {
                $server->stop($worker_id);
            }
        });
    }
}
