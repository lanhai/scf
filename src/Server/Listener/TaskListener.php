<?php

namespace Scf\Server\Listener;

use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Log;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Util\MemoryMonitor;
use Swoole\Server\Task;
use Swoole\WebSocket\Server;
use Throwable;

class TaskListener extends Listener {

    //处理异步任务(此回调函数在task进程中执行)
    //携程模式 task_enable_coroutine=true
    protected function onTask(Server $server, Task $task): void {
        //执行任务
        //Env::isDev() and Console::info("【Task】#" . $task->id . "开始执行:" . JsonHelper::toJson($task->data));
        /** @var \Scf\Core\Task $hander */
        $hander = $task->data['_handler'];
        try {
            $hander::instance()->execute($task);
        } catch (Throwable $throwable) {
            Log::instance()->error("【Task】任务执行失败:" . $throwable->getMessage());
        } finally {
            $this->refreshTaskWorkerMemoryUsage($server);
        }
    }

    /**
     * 处理异步任务的结果(此回调函数在worker进程中执行)
     * @param Server $server
     * @param $taskId
     * @param $data
     * @return void
     */
    protected function onFinish(Server $server, $taskId, $data): void {
        Env::isDev() and Console::success("【Task】#" . $taskId . "执行完成:" . JsonHelper::toJson($data));
    }

    /**
     * 更新当前 task worker 的内存占用快照。
     *
     * task worker 不经过 CgiListener，请求结束节流逻辑不会触发。
     * 这里在 task 执行后按 5 秒节流刷新一次，保证 `task_worker:{N}` 行
     * 的 usage/real/peak 在 dashboard 中持续更新。
     *
     * @param Server $server 当前 server。
     * @return void
     */
    protected function refreshTaskWorkerMemoryUsage(Server $server): void {
        $serverWorkerNum = (int)($server->setting['worker_num'] ?? 0);
        $taskWorkerNum = (int)($server->setting['task_worker_num'] ?? 0);
        $workerId = (int)($server->worker_id ?? -1);
        if ($taskWorkerNum <= 0 || $workerId < $serverWorkerNum || $workerId >= ($serverWorkerNum + $taskWorkerNum)) {
            return;
        }
        $taskWorkerIndex = ($workerId - $serverWorkerNum) + 1;
        $updatedKey = "task_worker.memory.usage.updated:{$taskWorkerIndex}";
        $latestUpdated = (int)(Runtime::instance()->get($updatedKey) ?: 0);
        if (time() - $latestUpdated < 5) {
            return;
        }
        MemoryMonitor::updateUsage("task_worker:{$taskWorkerIndex}");
        Runtime::instance()->set($updatedKey, time());
    }
    /**
     * 非协程模式
     * @param Server $server
     * @param $taskId
     * @param $reactor_id
     * @param $data
     * @return void
     */
//    protected function onTask(Server $server, $taskId, $reactor_id, $data) {
//        //执行任务
//        $data['handler']::instance()->execute($taskId, ...$data);
//    }

}
