<?php

namespace Scf\Util;

/**
 * 不经 shell 执行短生命周期的系统探测命令，并为整个子进程设置硬超时。
 *
 * SCF 的状态采样会调用 ps/lsof/vm_stat 等系统工具。在 macOS 的进程验证链
 * 阻塞时，子进程可能已经创建却长期停在 _dyld_start；若父进程继续阻塞读取
 * 管道，就会逐步堆积更多探测进程。这里统一使用非阻塞管道、超时终止和输出
 * 上限，把系统异常隔离为“一次采样失败”，而不是拖住 Gateway 控制面。
 */
final class BoundedProcessRunner {
    private const DEFAULT_TIMEOUT_SECONDS = 1.0;
    private const DEFAULT_MAX_OUTPUT_BYTES = 4_194_304;
    private const POLL_INTERVAL_MICROSECONDS = 10_000;
    private const TERMINATE_GRACE_SECONDS = 0.10;
    private const KILL_GRACE_SECONDS = 0.25;
    /** @var array<int, resource> */
    private static array $deferredProcesses = [];
    private static bool $runInFlight = false;

    /**
     * @param array<int, string> $command
     * @return array{output:string,error:string,exit_code:int,timed_out:bool,started:bool,truncated:bool}
     */
    public static function run(
        array $command,
        float $timeoutSeconds = self::DEFAULT_TIMEOUT_SECONDS,
        int $maxOutputBytes = self::DEFAULT_MAX_OUTPUT_BYTES
    ): array {
        $command = array_values(array_filter(
            $command,
            static fn(mixed $part): bool => is_string($part) && $part !== ''
        ));
        if (!$command) {
            return self::emptyResult();
        }

        // 若上一条探针连 SIGKILL 都无法回收，系统很可能正处于不可中断等待。
        // 此时拒绝派生更多探针，形成每个 PHP 进程最多一个遗留子进程的熔断。
        if (self::$runInFlight || self::reapDeferredProcesses() > 0) {
            return self::emptyResult(true, 'system probe circuit open');
        }

        self::$runInFlight = true;
        try {
        $timeoutSeconds = max(0.05, min(30.0, $timeoutSeconds));
        $maxOutputBytes = max(1024, min(32 * 1024 * 1024, $maxOutputBytes));
        $process = @proc_open($command, [
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ], $pipes, null, null, ['bypass_shell' => true]);
        if (!is_resource($process)) {
            return self::emptyResult();
        }

        foreach ([1, 2] as $index) {
            if (is_resource($pipes[$index] ?? null)) {
                @stream_set_blocking($pipes[$index], false);
            }
        }

        $output = '';
        $error = '';
        $exitCode = -1;
        $timedOut = false;
        $truncated = false;
        $deadline = microtime(true) + $timeoutSeconds;
        $status = null;

        while (true) {
            self::drainPipe($pipes[1] ?? null, $output, $maxOutputBytes, $truncated);
            self::drainPipe($pipes[2] ?? null, $error, $maxOutputBytes, $truncated);
            $status = @proc_get_status($process);
            if (!is_array($status) || !($status['running'] ?? false)) {
                if (is_array($status) && isset($status['exitcode']) && (int)$status['exitcode'] >= 0) {
                    $exitCode = (int)$status['exitcode'];
                }
                break;
            }
            if (microtime(true) >= $deadline) {
                $timedOut = true;
                @proc_terminate($process, 15);
                $status = self::waitForExit(
                    $process,
                    $pipes,
                    $output,
                    $error,
                    $maxOutputBytes,
                    $truncated,
                    microtime(true) + self::TERMINATE_GRACE_SECONDS
                );
                if (is_array($status) && ($status['running'] ?? false)) {
                    @proc_terminate($process, 9);
                    $status = self::waitForExit(
                        $process,
                        $pipes,
                        $output,
                        $error,
                        $maxOutputBytes,
                        $truncated,
                        microtime(true) + self::KILL_GRACE_SECONDS
                    );
                }
                if (is_array($status) && isset($status['exitcode']) && (int)$status['exitcode'] >= 0) {
                    $exitCode = (int)$status['exitcode'];
                }
                break;
            }
            usleep(self::POLL_INTERVAL_MICROSECONDS);
        }

        self::drainPipe($pipes[1] ?? null, $output, $maxOutputBytes, $truncated);
        self::drainPipe($pipes[2] ?? null, $error, $maxOutputBytes, $truncated);
        foreach ($pipes as $pipe) {
            if (is_resource($pipe)) {
                @fclose($pipe);
            }
        }
        $finalStatus = @proc_get_status($process);
        if (is_array($finalStatus) && ($finalStatus['running'] ?? false)) {
            // 极端情况下进程可能处于不可中断的内核等待。此时 proc_close 仍会
            // 等待子进程，反而破坏硬超时；保留句柄，后续调用只在确认退出后回收。
            self::$deferredProcesses[] = $process;
        } else {
            $closeCode = @proc_close($process);
            if ($exitCode < 0 && is_int($closeCode) && $closeCode >= 0) {
                $exitCode = $closeCode;
            }
        }

        return [
            'output' => $output,
            'error' => $error,
            'exit_code' => $exitCode,
            'timed_out' => $timedOut,
            'started' => true,
            'truncated' => $truncated,
        ];
        } finally {
            self::$runInFlight = false;
        }
    }

    /**
     * @param array<int, string> $command
     */
    public static function output(
        array $command,
        float $timeoutSeconds = self::DEFAULT_TIMEOUT_SECONDS,
        int $maxOutputBytes = self::DEFAULT_MAX_OUTPUT_BYTES
    ): string {
        $result = self::run($command, $timeoutSeconds, $maxOutputBytes);
        return ($result['timed_out'] || $result['truncated']) ? '' : $result['output'];
    }

    /**
     * @param resource $process
     * @param array<int, resource> $pipes
     * @return array<string, mixed>|false
     */
    private static function waitForExit(
        mixed $process,
        array $pipes,
        string &$output,
        string &$error,
        int $maxOutputBytes,
        bool &$truncated,
        float $deadline
    ): array|false {
        do {
            self::drainPipe($pipes[1] ?? null, $output, $maxOutputBytes, $truncated);
            self::drainPipe($pipes[2] ?? null, $error, $maxOutputBytes, $truncated);
            $status = @proc_get_status($process);
            if (!is_array($status) || !($status['running'] ?? false)) {
                return $status;
            }
            usleep(self::POLL_INTERVAL_MICROSECONDS);
        } while (microtime(true) < $deadline);

        return @proc_get_status($process);
    }

    /**
     * 始终把管道读空，避免子进程因输出管道写满而阻塞；超出上限的部分丢弃。
     */
    private static function drainPipe(
        mixed $pipe,
        string &$target,
        int $maxOutputBytes,
        bool &$truncated
    ): void {
        if (!is_resource($pipe)) {
            return;
        }
        while (true) {
            $chunk = @fread($pipe, 65_536);
            if (!is_string($chunk) || $chunk === '') {
                break;
            }
            $remaining = $maxOutputBytes - strlen($target);
            if ($remaining > 0) {
                $target .= substr($chunk, 0, $remaining);
            }
            if ($remaining <= 0 || strlen($chunk) > $remaining) {
                $truncated = true;
            }
        }
    }

    /**
     * @return array{output:string,error:string,exit_code:int,timed_out:bool,started:bool,truncated:bool}
     */
    private static function emptyResult(bool $timedOut = false, string $error = ''): array {
        return [
            'output' => '',
            'error' => $error,
            'exit_code' => -1,
            'timed_out' => $timedOut,
            'started' => false,
            'truncated' => false,
        ];
    }

    private static function reapDeferredProcesses(): int {
        $running = 0;
        foreach (self::$deferredProcesses as $index => $process) {
            if (!is_resource($process)) {
                unset(self::$deferredProcesses[$index]);
                continue;
            }
            $status = @proc_get_status($process);
            if (is_array($status) && ($status['running'] ?? false)) {
                $running++;
                continue;
            }
            @proc_close($process);
            unset(self::$deferredProcesses[$index]);
        }
        return $running;
    }
}
