<?php

namespace Scf\Server\SubProcess;

use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Server\Task\RQueue;
use Swoole\Process;

/**
 * RedisQueue manager 子进程运行逻辑。
 *
 * 责任边界：
 * - 管理 Redis 队列消费子进程的生命周期与排空。
 */
class RedisQueueProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);

        return new Process(function (Process $process) {
            $this->call('mark_gateway_sub_process_context');
            Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID, (int)$process->pid);
            Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT, time());
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【RedisQueue】Redis队列管理PID:" . $process->pid, false);
            }
            define('IS_REDIS_QUEUE_PROCESS', true);
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $managerId = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
            $quiescing = false;
            $queueWorkerPid = 0;
            while (true) {
                Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT, time());
                while ($ret = Process::wait(false)) {
                    $pid = (int)($ret['pid'] ?? 0);
                    if ($pid > 0 && $pid === $queueWorkerPid) {
                        $queueWorkerPid = 0;
                        Runtime::instance()->redisQueueProcessStatus(false);
                    }
                }

                if (!Runtime::instance()->serverIsAlive()) {
                    Console::warning("【RedisQueue】服务器已关闭,结束运行");
                    break;
                }
                $latestManagerId = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
                if (!$quiescing && $latestManagerId !== $managerId) {
                    $quiescing = true;
                    Console::warning("【RedisQueue】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                }

                if (!$quiescing && !Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }

                if (!$quiescing && $queueWorkerPid <= 0 && !Runtime::instance()->redisQueueProcessStatus() && Runtime::instance()->serverIsAlive() && !Runtime::instance()->serverIsDraining()) {
                    Runtime::instance()->redisQueueProcessStatus(true);
                    $queueProcess = RQueue::startProcess();
                    $queueWorkerPid = (int)($queueProcess?->pid ?? 0);
                    if ($queueWorkerPid <= 0) {
                        Runtime::instance()->redisQueueProcessStatus(false);
                    }
                }

                if ($quiescing && $queueWorkerPid <= 0 && (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0) <= 0) {
                    Console::warning("【RedisQueue】#{$managerId} 管理进程排空完成,退出等待拉起");
                    break;
                }
                $cmd = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($cmd === false) {
                    $cmd = '';
                }
                if ($cmd == 'shutdown') {
                    Console::warning("【RedisQueue】#{$managerId} 服务器已关闭,结束运行");
                    break;
                }
                if ($cmd !== '') {
                    if (!$quiescing) {
                        $quiescing = true;
                        Console::warning("【RedisQueue】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                    }
                }
                sleep(1);
            }
            Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_MANAGER_HEARTBEAT_AT, 0);
            Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID, 0);
            is_resource($commandPipe) and fclose($commandPipe);
        });
    }
}
