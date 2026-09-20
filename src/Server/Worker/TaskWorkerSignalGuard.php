<?php

namespace Scf\Server\Worker;

use Swoole\Process;
use Swoole\Server;

/**
 * 保护 Swoole task worker 的进程级信号边界。
 *
 * 该组件只负责在 task worker 启动阶段建立必要的信号处置，不承接任务调度、
 * 重试或业务错误处理，避免单次 I/O 失败升级为整个 task worker 退出。
 */
final class TaskWorkerSignalGuard {
    /**
     * 为当前 task worker 安装断管信号保护。
     *
     * Swoole 5.1.x 在 macOS kqueue + task_enable_coroutine 下会把 task worker
     * 的 SIGPIPE 恢复为默认处置。这里必须注册 callable，让信号继续经过
     * Swoole reactor；不能传 SIG_IGN，因为该版本会把它转换为 null，重新落回
     * kqueue 的 SIG_DFL 分支。底层写操作仍会收到 EPIPE/false，由原调用链处理。
     *
     * @param Server $server 当前 Swoole server。
     * @param int $workerId 当前 worker id。
     * @return bool 无需保护或安装成功返回 true，安装失败返回 false。
     */
    public static function install(Server $server, int $workerId): bool {
        $workerNum = (int)($server->setting['worker_num'] ?? 0);
        $taskWorkerNum = (int)($server->setting['task_worker_num'] ?? 0);
        $isAffectedTaskWorker = PHP_OS_FAMILY === 'Darwin'
            && (bool)($server->setting['task_enable_coroutine'] ?? false)
            && $taskWorkerNum > 0
            && $workerId >= $workerNum
            && $workerId < ($workerNum + $taskWorkerNum);
        if (!$isAffectedTaskWorker) {
            return true;
        }

        $sigpipe = defined('SIGPIPE') ? (int)constant('SIGPIPE') : 0;
        if ($sigpipe <= 0) {
            return true;
        }

        return Process::signal($sigpipe, static function (int $signal): void {
            // 只消化进程级信号；产生信号的 write 仍按 EPIPE/false 返回。
        });
    }
}
