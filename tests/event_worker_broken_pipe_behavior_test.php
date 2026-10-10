<?php

declare(strict_types=1);

use Swoole\Http\Server;
use Swoole\Process;
use Swoole\Timer;

if (!extension_loaded('swoole')) {
    throw new RuntimeException('Swoole is required');
}

// Exercise the native kqueue boundary: Swoole itself registers SIGPIPE as SIG_IGN.
$base = sys_get_temp_dir() . '/scf-event-worker-sigpipe-' . getmypid();
$attemptFile = $base . '.attempt';
$installFile = $base . '.install';
$survivedFile = $base . '.survived';
$workerErrorFile = $base . '.worker-error';
$timeoutFile = $base . '.timeout';
$callbackFile = $base . '.callback';
$startedAt = microtime(true);

$server = new Server('127.0.0.1', 0, SWOOLE_PROCESS);
$server->set([
    'worker_num' => 2,
    'task_worker_num' => 0,
    'task_enable_coroutine' => false,
    'max_wait_time' => 2,
    'log_file' => '/dev/null',
]);
$server->on('Request', static function ($request, $response): void {
    $response->end('ok');
});
$server->on('WorkerStart', static function (Server $server, int $workerId) use (
    $attemptFile,
    $installFile,
    $survivedFile,
    $workerErrorFile,
    $timeoutFile,
    $callbackFile,
    $startedAt
): void {
    if ($workerId === 0) {
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

    // manager 可能在失败后拉起新 event worker；只让首个进程触发一次信号。
    if (is_file($attemptFile)) {
        return;
    }
    file_put_contents($attemptFile, (string)getmypid());
    $installed = Process::signal(SIGUSR1, static function (int $signal) use ($callbackFile): void {
        file_put_contents($callbackFile, (string)$signal);
    });
    $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    fclose($sockets[1]);
    error_clear_last();
    $written = @fwrite($sockets[0], '12345');
    $writeError = error_get_last()['message'] ?? '';
    fclose($sockets[0]);
    file_put_contents($installFile, json_encode(['installed' => $installed, 'written' => $written, 'error' => $writeError]));
    posix_kill(getmypid(), SIGUSR1);
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
    $writeResult = json_decode((string)@file_get_contents($installFile), true);
    if (($writeResult['installed'] ?? false) !== true) {
        throw new RuntimeException('Control signal callback must install successfully');
    }
    if (($writeResult['written'] ?? null) !== false) {
        throw new RuntimeException('Broken-pipe write must still return false');
    }
    if (!str_contains((string)($writeResult['error'] ?? ''), 'Broken pipe')) {
        throw new RuntimeException('The write must reproduce the production Broken pipe error');
    }
    if (is_file($workerErrorFile)) {
        throw new RuntimeException('Event worker exited after SIGPIPE: ' . file_get_contents($workerErrorFile));
    }
    if (!is_file($survivedFile)) {
        $reason = is_file($timeoutFile) ? 'timed out' : 'did not reach the post-signal callback';
        throw new RuntimeException('Event worker must survive SIGPIPE, but ' . $reason);
    }
    if (file_get_contents($attemptFile) !== file_get_contents($survivedFile)) {
        throw new RuntimeException('The original event worker must survive without manager respawn');
    }
    if ((string)@file_get_contents($callbackFile) !== (string)SIGUSR1) {
        throw new RuntimeException('Callable signal handlers must still execute');
    }
    echo "PASS event worker: write returns false, original worker survives, callable signal still executes\n";
} finally {
    foreach ([$attemptFile, $installFile, $survivedFile, $workerErrorFile, $timeoutFile, $callbackFile] as $file) {
        if (is_file($file)) {
            unlink($file);
        }
    }
}
