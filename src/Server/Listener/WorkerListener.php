<?php

namespace Scf\Server\Listener;

use Exception;
use Scf\Cloud\Ali\Oss;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\InflightCounter;
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
        // PID 复用场景下，上一代进程可能遗留同 PID 的 inflight 子计数。
        // worker 启动时先按当前 PID 做一次回补，确保新进程从干净状态开始统计。
        $this->cleanupInflightResidueByPid((int)getmypid(), (int)$workerId, 'worker_start');
        if ((int)$workerId === 0) {
            // worker0 启动时兜底扫描已退出 PID 的残留 inflight，覆盖极端退出场景
            // 未触发 workerExit/workerError 回调的链路，避免脏统计长期悬挂。
            $released = InflightCounter::cleanupExitedProcessInflight();
            $total = (int)($released['total'] ?? 0);
            if ($total > 0) {
                Console::warning(
                    "【Worker】启动期清理已退出进程 inflight 残留: pids=" . (int)($released['pids'] ?? 0)
                    . ", mysql=" . (int)($released['mysql'] ?? 0)
                    . ", redis=" . (int)($released['redis'] ?? 0)
                    . ", outbound_http=" . (int)($released['outbound_http'] ?? 0)
                    . ", total={$total}",
                    false
                );
            }
        }
        // upstream 业务 worker/task worker 会在这里注册 MemoryMonitorTable 行。
        // usage/real/peak 由 CgiListener/TaskListener 在请求(任务)收尾时刷新；
        // OS 视角 rss/pss/os_actual 由 gateway 汇总阶段按 pid 现采，不再依赖
        // MemoryUsageCountProcess 作为 upstream worker 的主统计链路。
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
        $this->cleanupInflightResidueByPid($worker_pid, $worker_id, 'worker_error');
        Timer::after(3000, function () use ($server, $worker_id) {
            if (!Process::kill($server->master_pid, 0)) {
                $server->stop($worker_id);
            }
        });
    }

    /**
     * worker 退出时回补该进程遗留的 inflight 计数。
     *
     * 正常路径下 finally 会把 inflight 归零；若 worker 在 I/O 中途退出，
     * begin 后未执行到 finally，会遗留“脏在途”。这里在退出钩子按 pid
     * 执行一次回补，避免全局 inflight 长期悬挂。
     *
     * @param Server $server 当前 server。
     * @param int $workerId 退出 worker id。
     * @return void
     */
    protected function onWorkerExit(Server $server, int $workerId): void {
        $pid = (int)($server->worker_pid ?? getmypid());
        $this->cleanupInflightResidueByPid($pid, $workerId, 'worker_exit');
    }

    /**
     * 按 PID 清理 inflight 残留并输出诊断日志。
     *
     * @param int $pid 目标进程 PID。
     * @param int $workerId worker id。
     * @param string $phase 调用阶段（worker_exit/worker_error）。
     * @return void
     */
    protected function cleanupInflightResidueByPid(int $pid, int $workerId, string $phase): void {
        if ($pid <= 0) {
            return;
        }
        $released = InflightCounter::cleanupProcessInflightByPid($pid);
        $total = (int)($released['total'] ?? 0);
        if ($total <= 0) {
            return;
        }
        Console::warning(
            "【Worker】检测到 inflight 残留并已回补: phase={$phase}, worker_id={$workerId}, pid={$pid}"
            . ", mysql=" . (int)($released['mysql'] ?? 0)
            . ", redis=" . (int)($released['redis'] ?? 0)
            . ", outbound_http=" . (int)($released['outbound_http'] ?? 0)
            . ", total={$total}",
            false
        );
    }
}
