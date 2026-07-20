<?php

namespace Scf\Database\Backup;

use Generator;
use Throwable;

/**
 * 数据库备份/恢复长任务专用外部进程执行器。
 *
 * 和状态采样用的短命令 runner 不同，数据库导出/导入可能持续数小时，不能用
 * 几十秒的探针超时直接截断。这里保留长任务窗口，同时用非阻塞管道并行排空
 * stdout/stderr，超时或取消时按 SIGTERM -> SIGKILL 收口，避免子进程或管道长期
 * 挂住 Swoole worker。
 */
final class DatabaseBackupProcessRunner {
    private const DEFAULT_MAX_CAPTURE_BYTES = 8_388_608;
    private const POLL_INTERVAL_MICROSECONDS = 20_000;
    private const TERMINATE_GRACE_SECONDS = 2.0;
    private const KILL_GRACE_SECONDS = 2.0;
    private const INPUT_CHUNK_BYTES = 65_536;

    /** @var array<int, resource> */
    private static array $deferredProcesses = [];

    /**
     * 活跃任务取消代次。
     *
     * requestCancel() 递增后，所有已经进入 run() 的任务都会在下一轮轮询中退出；
     * 后续新任务会捕获新代次，不会被历史取消请求误伤。
     */
    private int $cancelGeneration = 0;

    /**
     * 请求取消当前由该 runner 启动的全部活跃命令。
     */
    public function requestCancel(): void {
        $this->cancelGeneration++;
    }

    /**
     * 不经 shell 执行数据库长任务。
     *
     * stdinFiles 会按传入顺序流式写入同一个子进程；stdinPrefix/stdinSuffix 可用于
     * 整库恢复时在同一 mysql 会话中包裹外键开关，无需再复制一份巨型临时 SQL。
     *
     * @param array<int, string> $command 命令及参数，首项必须是可执行文件
     * @param float $timeoutSeconds 整个子进程的硬超时，最长 24 小时
     * @param array<string, string> $env 额外环境变量
     * @param array<int, string> $stdinFiles 依次写入 stdin 的文件
     * @param string|null $stdoutFile 指定时 stdout 直接流入文件，不占 PHP 内存
     * @param callable|null $shouldCancel 返回 true 时主动取消任务
     * @return array{exit_code:int,stdout:string,stderr:string,timed_out:bool,cancelled:bool,started:bool,truncated:bool}
     */
    public function run(
        array $command,
        float $timeoutSeconds,
        array $env = [],
        array $stdinFiles = [],
        ?string $stdoutFile = null,
        string $stdinPrefix = '',
        string $stdinSuffix = '',
        ?callable $shouldCancel = null,
        int $maxCaptureBytes = self::DEFAULT_MAX_CAPTURE_BYTES
    ): array {
        $command = $this->normalizeCommand($command);
        if (!$command) {
            return $this->emptyResult(false, false, '数据库命令为空');
        }
        if (self::reapDeferredProcesses() > 0) {
            return $this->emptyResult(false, false, 'database process circuit open');
        }

        foreach ($stdinFiles as $stdinFile) {
            if (!is_string($stdinFile) || !is_file($stdinFile) || !is_readable($stdinFile)) {
                return $this->emptyResult(false, false, '无法读取数据库命令输入文件: ' . (string)$stdinFile);
            }
        }
        if ($stdoutFile !== null) {
            $parent = dirname($stdoutFile);
            if (!is_dir($parent) || !is_writable($parent)) {
                return $this->emptyResult(false, false, '数据库命令输出目录不可写: ' . $parent);
            }
        }

        $timeoutSeconds = max(0.05, min(86_400.0, $timeoutSeconds));
        $maxCaptureBytes = max(1024, min(64 * 1024 * 1024, $maxCaptureBytes));
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => $stdoutFile === null ? ['pipe', 'w'] : ['file', $stdoutFile, 'w'],
            2 => ['pipe', 'w'],
        ];
        $process = @proc_open(
            $command,
            $descriptors,
            $pipes,
            null,
            $this->buildEnvironment($env),
            ['bypass_shell' => true]
        );
        if (!is_resource($process)) {
            return $this->emptyResult(false, false, '启动数据库命令失败');
        }

        foreach ([0, 1, 2] as $index) {
            if (is_resource($pipes[$index] ?? null)) {
                @stream_set_blocking($pipes[$index], false);
            }
        }

        $input = $this->inputChunks($stdinFiles, $stdinPrefix, $stdinSuffix);
        $hasInput = $stdinPrefix !== '' || $stdinSuffix !== '' || $stdinFiles !== [];
        $inputBuffer = '';
        if ($hasInput) {
            $input->rewind();
            $this->fillInputBuffer($input, $inputBuffer);
        } else {
            $this->closePipe($pipes, 0);
        }

        $stdout = '';
        $stderr = '';
        $exitCode = -1;
        $timedOut = false;
        $cancelled = false;
        $truncated = false;
        $deadline = microtime(true) + $timeoutSeconds;
        $runGeneration = $this->cancelGeneration;
        $status = null;

        while (true) {
            $this->drainPipe($pipes[1] ?? null, $stdout, $maxCaptureBytes, $truncated);
            $this->drainPipe($pipes[2] ?? null, $stderr, $maxCaptureBytes, $truncated);
            $this->writeInput($pipes, $input, $inputBuffer);

            $status = @proc_get_status($process);
            if (!is_array($status) || !($status['running'] ?? false)) {
                if (is_array($status) && (int)($status['exitcode'] ?? -1) >= 0) {
                    $exitCode = (int)$status['exitcode'];
                }
                break;
            }

            try {
                $cancelled = $runGeneration !== $this->cancelGeneration
                    || ($shouldCancel !== null && (bool)$shouldCancel());
            } catch (Throwable $throwable) {
                $cancelled = true;
                $stderr = $this->appendCaptured(
                    $stderr,
                    '取消检查失败: ' . $throwable->getMessage(),
                    $maxCaptureBytes,
                    $truncated
                );
            }
            if ($cancelled || $truncated || microtime(true) >= $deadline) {
                $timedOut = !$cancelled && !$truncated;
                $this->closePipe($pipes, 0);
                @proc_terminate($process, 15);
                $status = $this->waitForExit(
                    $process,
                    $pipes,
                    $stdout,
                    $stderr,
                    $maxCaptureBytes,
                    $truncated,
                    microtime(true) + self::TERMINATE_GRACE_SECONDS
                );
                if (is_array($status) && ($status['running'] ?? false)) {
                    @proc_terminate($process, 9);
                    $status = $this->waitForExit(
                        $process,
                        $pipes,
                        $stdout,
                        $stderr,
                        $maxCaptureBytes,
                        $truncated,
                        microtime(true) + self::KILL_GRACE_SECONDS
                    );
                }
                if (is_array($status) && (int)($status['exitcode'] ?? -1) >= 0) {
                    $exitCode = (int)$status['exitcode'];
                }
                break;
            }

            $this->pause();
        }

        $this->drainPipe($pipes[1] ?? null, $stdout, $maxCaptureBytes, $truncated);
        $this->drainPipe($pipes[2] ?? null, $stderr, $maxCaptureBytes, $truncated);
        foreach (array_keys($pipes) as $index) {
            $this->closePipe($pipes, (int)$index);
        }

        $finalStatus = @proc_get_status($process);
        if (is_array($finalStatus) && ($finalStatus['running'] ?? false)) {
            // 极端磁盘/内核等待下，proc_close 自身也会无限等待。保留一个句柄并打开
            // 熔断，后续任务只有在该子进程真正退出后才允许继续派生。
            self::$deferredProcesses[] = $process;
        } else {
            $closeCode = @proc_close($process);
            if ($exitCode < 0 && is_int($closeCode) && $closeCode >= 0) {
                $exitCode = $closeCode;
            }
        }

        return [
            'exit_code' => $exitCode,
            'stdout' => $stdout,
            'stderr' => $stderr,
            'timed_out' => $timedOut,
            'cancelled' => $cancelled,
            'started' => true,
            'truncated' => $truncated,
        ];
    }

    /**
     * @param array<int, mixed> $command
     * @return array<int, string>
     */
    private function normalizeCommand(array $command): array {
        $normalized = [];
        foreach ($command as $part) {
            if (!is_scalar($part)) {
                return [];
            }
            $part = (string)$part;
            if ($part === '') {
                return [];
            }
            $normalized[] = $part;
        }
        return $normalized;
    }

    /**
     * @param array<string, string> $env
     * @return array<string, string>
     */
    private function buildEnvironment(array $env): array {
        $inherited = getenv();
        if (!is_array($inherited)) {
            $inherited = [];
        }
        return array_merge($inherited, $_ENV, $env);
    }

    /**
     * @param array<int, string> $files
     * @return Generator<int, string>
     */
    private function inputChunks(array $files, string $prefix, string $suffix): Generator {
        if ($prefix !== '') {
            yield $prefix;
        }
        foreach ($files as $file) {
            $handle = @fopen($file, 'rb');
            if (!is_resource($handle)) {
                continue;
            }
            try {
                while (!feof($handle)) {
                    $chunk = @fread($handle, self::INPUT_CHUNK_BYTES);
                    if (!is_string($chunk) || $chunk === '') {
                        break;
                    }
                    yield $chunk;
                }
            } finally {
                @fclose($handle);
            }
            yield "\n";
        }
        if ($suffix !== '') {
            yield $suffix;
        }
    }

    private function fillInputBuffer(Generator $input, string &$buffer): void {
        while ($buffer === '' && $input->valid()) {
            $buffer = (string)$input->current();
            $input->next();
        }
    }

    /**
     * @param array<int, resource> $pipes
     */
    private function writeInput(array &$pipes, Generator $input, string &$buffer): void {
        if (!is_resource($pipes[0] ?? null)) {
            return;
        }
        $this->fillInputBuffer($input, $buffer);
        if ($buffer === '') {
            $this->closePipe($pipes, 0);
            return;
        }

        $written = @fwrite($pipes[0], $buffer);
        if (is_int($written) && $written > 0) {
            $buffer = (string)substr($buffer, $written);
        }
        if (@feof($pipes[0])) {
            $this->closePipe($pipes, 0);
        }
    }

    /**
     * @param resource $process
     * @param array<int, resource> $pipes
     * @return array<string, mixed>|false
     */
    private function waitForExit(
        mixed $process,
        array &$pipes,
        string &$stdout,
        string &$stderr,
        int $maxCaptureBytes,
        bool &$truncated,
        float $deadline
    ): array|false {
        do {
            $this->drainPipe($pipes[1] ?? null, $stdout, $maxCaptureBytes, $truncated);
            $this->drainPipe($pipes[2] ?? null, $stderr, $maxCaptureBytes, $truncated);
            $status = @proc_get_status($process);
            if (!is_array($status) || !($status['running'] ?? false)) {
                return $status;
            }
            $this->pause();
        } while (microtime(true) < $deadline);

        return @proc_get_status($process);
    }

    private function drainPipe(
        mixed $pipe,
        string &$target,
        int $maxCaptureBytes,
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
            $target = $this->appendCaptured($target, $chunk, $maxCaptureBytes, $truncated);
        }
    }

    private function appendCaptured(string $target, string $chunk, int $limit, bool &$truncated): string {
        $remaining = $limit - strlen($target);
        if ($remaining > 0) {
            $target .= substr($chunk, 0, $remaining);
        }
        if ($remaining <= 0 || strlen($chunk) > $remaining) {
            $truncated = true;
        }
        return $target;
    }

    /**
     * @param array<int, resource> $pipes
     */
    private function closePipe(array &$pipes, int $index): void {
        if (is_resource($pipes[$index] ?? null)) {
            @fclose($pipes[$index]);
        }
        unset($pipes[$index]);
    }

    private function pause(): void {
        if (class_exists(\Swoole\Coroutine::class) && \Swoole\Coroutine::getCid() > 0) {
            \Swoole\Coroutine::sleep(self::POLL_INTERVAL_MICROSECONDS / 1_000_000);
            return;
        }
        usleep(self::POLL_INTERVAL_MICROSECONDS);
    }

    /**
     * @return array{exit_code:int,stdout:string,stderr:string,timed_out:bool,cancelled:bool,started:bool,truncated:bool}
     */
    private function emptyResult(bool $timedOut, bool $cancelled, string $stderr): array {
        return [
            'exit_code' => -1,
            'stdout' => '',
            'stderr' => $stderr,
            'timed_out' => $timedOut,
            'cancelled' => $cancelled,
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
