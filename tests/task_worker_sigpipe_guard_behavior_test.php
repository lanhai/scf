<?php

declare(strict_types=1);

use Scf\Server\Worker\TaskWorkerSignalGuard;
use Swoole\Http\Server;
use Swoole\Timer;

if (!extension_loaded('swoole')) {
    throw new RuntimeException('Swoole is required');
}

require_once dirname(__DIR__) . '/src/Server/Worker/TaskWorkerSignalGuard.php';

$base = sys_get_temp_dir() . '/scf-task-worker-sigpipe-' . getmypid();
$attemptFile = $base . '.attempt';
$installFile = $base . '.install';
$survivedFile = $base . '.survived';
$workerErrorFile = $base . '.worker-error';
$timeoutFile = $base . '.timeout';
$startedAt = microtime(true);

$server = new Server('127.0.0.1', 0, SWOOLE_PROCESS);
$server->set([
    'worker_num' => 1,
    'task_worker_num' => 1,
    'task_enable_coroutine' => true,
    'max_wait_time' => 2,
    'log_file' => '/dev/null',
]);
$server->on('Request', static function ($request, $response): void {
    $response->end('ok');
});
$server->on('Task', static function (): void {
});
$server->on('Finish', static function (): void {
});
$server->on('WorkerStart', static function (Server $server, int $workerId) use (
    $attemptFile,
    $installFile,
    $survivedFile,
    $workerErrorFile,
    $timeoutFile,
    $startedAt
): void {
    $workerNum = (int)($server->setting['worker_num'] ?? 0);
    if ($workerId < $workerNum) {
        Timer::tick(20, static function () use (
            $server,
            $survivedFile,
            $workerErrorFile,
            $timeoutFile,
            $startedAt
        ): void {
            if (is_file($survivedFile) || is_file($workerErrorFile)) {
                $server->shutdown();
                return;
            }
            if (microtime(true) - $startedAt >= 2) {
                file_put_contents($timeoutFile, 'timeout');
                $server->shutdown();
            }
        });
        return;
    }

    // manager 可能在失败后拉起新 task worker；只让首个进程触发一次信号。
    if (is_file($attemptFile)) {
        return;
    }
    file_put_contents($attemptFile, (string)getmypid());
    file_put_contents($installFile, TaskWorkerSignalGuard::install($server, $workerId) ? '1' : '0');
    posix_kill(getmypid(), SIGPIPE);
    Timer::after(50, static function () use ($survivedFile): void {
        file_put_contents($survivedFile, (string)getmypid());
    });
});
$server->on('WorkerError', static function (
    Server $server,
    int $workerId,
    int $workerPid,
    int $exitCode,
    int $signal
) use ($workerErrorFile): void {
    file_put_contents($workerErrorFile, json_encode([
        'worker_id' => $workerId,
        'worker_pid' => $workerPid,
        'exit_code' => $exitCode,
        'signal' => $signal,
    ], JSON_UNESCAPED_UNICODE));
});

try {
    $server->start();
    if ((string)@file_get_contents($installFile) !== '1') {
        throw new RuntimeException('Task worker SIGPIPE guard must install successfully');
    }
    if (is_file($workerErrorFile)) {
        throw new RuntimeException('Task worker exited after SIGPIPE: ' . file_get_contents($workerErrorFile));
    }
    if (!is_file($survivedFile)) {
        $reason = is_file($timeoutFile) ? 'timed out' : 'did not reach the post-signal callback';
        throw new RuntimeException('Task worker must survive SIGPIPE, but ' . $reason);
    }
    if (file_get_contents($attemptFile) !== file_get_contents($survivedFile)) {
        throw new RuntimeException('The original task worker must survive without manager respawn');
    }
    echo "PASS task worker: SIGPIPE is absorbed and the original coroutine worker stays alive\n";
} finally {
    foreach ([$attemptFile, $installFile, $survivedFile, $workerErrorFile, $timeoutFile] as $file) {
        if (is_file($file)) {
            unlink($file);
        }
    }
}
