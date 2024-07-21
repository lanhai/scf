<?php

namespace Scf\Server\Listener;

use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Helper\JsonHelper;
use Scf\Server\Env;
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
        //Env::isDev() and Console::success("【Task】#" . $taskId . "执行完成:" . JsonHelper::toJson($data));
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