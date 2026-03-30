<?php
declare(strict_types=1);

/**
 * boot 启动通用工具函数。
 *
 * 这批函数只服务于 boot 入口及其最小启动工具链：
 * - CLI 参数与环境判定
 * - 错误捕获与标准输出
 * - 文件锁与原子文件替换
 * - 不依赖框架其它模块的底层 I/O 操作
 *
 * 设计目标是让 framework pack 切换、server loop 和 autoload 注册都能在
 * “未加载完整框架”之前完成，避免升级窗口期混装旧框架类。
 */

function scf_safe_call(callable $callback, ?string &$error = null): mixed {
    $error = null;
    set_error_handler(static function (int $severity, string $message) use (&$error): bool {
        $error = $message;
        return true;
    });

    try {
        return $callback();
    } finally {
        restore_error_handler();
    }
}

function scf_stdout(string $message): void {
    scf_stream_write(STDOUT, $message);
}

function scf_stderr(string $message): void {
    scf_stream_write(STDERR, $message);
}

/**
 * 输出 bootstrap 日志。
 *
 * 仅对 `【Boot】` 前缀日志补齐统一时间戳，保持与运行时 Console 日志同一视觉格式；
 * 其余文本（例如业务命令原始输出）保持原样，避免误改命令返回内容。
 *
 * @param resource $stream
 * @param string $message
 * @return void
 */
function scf_stream_write($stream, string $message): void {
    $lines = preg_split('/\r\n|\r|\n/', $message) ?: [''];
    foreach ($lines as $line) {
        if (str_starts_with($line, '【Boot】')) {
            $line = scf_boot_log_prefix() . ' ' . $line;
        }
        fwrite($stream, $line . PHP_EOL);
    }
}

/**
 * 生成 bootstrap 统一时间戳。
 *
 * @return string 形如 `03-31 02:29:49.192`
 */
function scf_boot_log_prefix(): string {
    $micro = microtime(true);
    $sec = (int)$micro;
    $ms = (int)(($micro - $sec) * 1000);
    return date('m-d H:i:s', $sec) . '.' . str_pad((string)$ms, 3, '0', STR_PAD_LEFT);
}

function scf_detect_timezone(): string {
    $candidates = [
        getenv('TZ') ?: null,
        ini_get('date.timezone') ?: null,
    ];

    foreach ($candidates as $timezone) {
        if (is_string($timezone) && $timezone !== '' && in_array($timezone, timezone_identifiers_list(), true)) {
            return $timezone;
        }
    }

    return 'Asia/Shanghai';
}

function scf_has_arg(array $argv, string $needle): bool {
    return in_array($needle, $argv, true);
}

function scf_option_value(array $argv, string $name): ?string {
    foreach ($argv as $index => $arg) {
        if (!is_string($arg) || !str_starts_with($arg, '-')) {
            continue;
        }
        $option = ltrim($arg, '-');
        if (str_contains($option, '=')) {
            [$optionName, $value] = explode('=', $option, 2);
            if ($optionName === $name) {
                return $value;
            }
            continue;
        }
        if ($option === $name) {
            $next = $argv[$index + 1] ?? null;
            if (is_string($next) && !str_starts_with($next, '-')) {
                return $next;
            }
            return null;
        }
    }
    return null;
}

function scf_parse_opts(array $argv): array {
    $opts = [];
    foreach ($argv as $arg) {
        if (!is_string($arg) || !str_starts_with($arg, '-')) {
            continue;
        }
        $option = ltrim($arg, '-');
        $value = null;
        if (str_contains($option, '=')) {
            [$option, $value] = explode('=', $option, 2);
        }
        if ($option !== '') {
            $opts[$option] = $value;
        }
    }
    return $opts;
}

function scf_bool_constant(string $name): bool {
    return defined($name) && (bool)constant($name);
}

function scf_open_lock_file(string $lockFile) {
    return scf_safe_call(static fn() => fopen($lockFile, 'c'));
}

function scf_release_lock($lock): void {
    if (!is_resource($lock)) {
        return;
    }

    scf_safe_call(static fn() => flock($lock, LOCK_UN));
    scf_safe_call(static fn() => fclose($lock));
}

/**
 * 通过重命名优先、复制兜底的方式原子替换文件。
 *
 * 升级期 framework 包可能正处在“下载完成但尚未切换”的窗口。这里统一用
 * 先写临时文件再 rename 的方式，避免启动进程读到半写入的 pack。
 */
function scf_atomic_replace(string $from, string $to, ?string &$error = null): bool {
    $dir = dirname($to);
    if (!is_dir($dir)) {
        $error = '目标目录不存在:' . $dir;
        return false;
    }

    if (scf_safe_call(static fn() => rename($from, $to), $error)) {
        return true;
    }

    $tmp = $dir . '/.' . basename($to) . '.tmp.' . getmypid();
    if (!scf_safe_call(static fn() => copy($from, $tmp), $error)) {
        return false;
    }

    scf_safe_call(static fn() => chmod($tmp, 0644));
    if (!scf_safe_call(static fn() => rename($tmp, $to), $error)) {
        scf_safe_call(static fn() => unlink($tmp));
        return false;
    }

    scf_safe_call(static fn() => unlink($from));
    return true;
}

/**
 * 通过临时文件原子复制 framework 包。
 *
 * framework pack 可能被多个启动进程并发读取。复制时先写临时文件再 rename，
 * 可以避免其它进程命中未写完的目标文件。
 */
function scf_atomic_copy(string $from, string $to, ?string &$error = null): bool {
    $error = null;
    if (!is_file($from)) {
        $error = '源文件不存在:' . $from;
        return false;
    }

    $fromReal = realpath($from) ?: $from;
    $toReal = realpath($to) ?: $to;
    if ($fromReal === $toReal) {
        return true;
    }

    $dir = dirname($to);
    if (!is_dir($dir)) {
        $error = '目标目录不存在:' . $dir;
        return false;
    }

    $tmp = $dir . '/.' . basename($to) . '.tmp.' . getmypid();
    if (!scf_safe_call(static fn() => copy($from, $tmp), $error)) {
        return false;
    }

    scf_safe_call(static fn() => chmod($tmp, 0644));
    if (!scf_safe_call(static fn() => rename($tmp, $to), $error)) {
        scf_safe_call(static fn() => unlink($tmp));
        return false;
    }

    return true;
}

/**
 * 读取一个短超时的 HTTP 文本响应。
 *
 * boot 早期还不能依赖框架 HTTP client，这里只用 ext-curl / streams 做最小网络访问，
 * 供 framework 包自愈拉取使用。
 *
 * @param string $url 目标地址
 * @param int $timeoutSeconds 连接与总超时秒数
 * @param string|null $error 失败原因
 * @return string|null 成功时返回响应体
 */
function scf_http_get_text(string $url, int $timeoutSeconds = 8, ?string &$error = null): ?string {
    $error = null;
    $url = trim($url);
    if ($url === '') {
        $error = '请求地址为空';
        return null;
    }

    if (function_exists('curl_init')) {
        $ch = curl_init($url);
        if ($ch === false) {
            $error = 'curl 初始化失败';
            return null;
        }

        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_CONNECTTIMEOUT => max(1, $timeoutSeconds),
            CURLOPT_TIMEOUT => max(1, $timeoutSeconds),
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
            CURLOPT_USERAGENT => 'scf-bootstrap/1.0',
        ]);
        $response = curl_exec($ch);
        if (!is_string($response)) {
            $error = curl_error($ch) ?: 'curl 请求失败';
            curl_close($ch);
            return null;
        }
        $statusCode = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);
        if ($statusCode >= 400 || $statusCode === 0) {
            $error = 'HTTP ' . $statusCode;
            return null;
        }
        return $response;
    }

    $context = stream_context_create([
        'http' => [
            'method' => 'GET',
            'timeout' => max(1, $timeoutSeconds),
            'ignore_errors' => true,
            'header' => "User-Agent: scf-bootstrap/1.0\r\n",
        ],
        'ssl' => [
            'verify_peer' => false,
            'verify_peer_name' => false,
        ],
    ]);
    $response = @file_get_contents($url, false, $context);
    if (!is_string($response)) {
        $error = 'stream 请求失败';
        return null;
    }
    return $response;
}

/**
 * 下载远端文件到本地临时/目标路径。
 *
 * @param string $url 远端下载地址
 * @param string $targetPath 目标文件路径
 * @param int $timeoutSeconds 连接与总超时秒数
 * @param string|null $error 失败原因
 * @return bool 下载成功返回 true
 */
function scf_http_download_file(string $url, string $targetPath, int $timeoutSeconds = 20, ?string &$error = null): bool {
    $error = null;
    $body = scf_http_get_text($url, $timeoutSeconds, $error);
    if (!is_string($body)) {
        return false;
    }

    $dir = dirname($targetPath);
    if (!is_dir($dir) && !scf_ensure_dir($dir, $error)) {
        return false;
    }

    $tmp = $dir . '/.' . basename($targetPath) . '.tmp.' . getmypid();
    if (!scf_safe_call(static fn() => file_put_contents($tmp, $body), $error)) {
        return false;
    }
    scf_safe_call(static fn() => chmod($tmp, 0644));
    if (!scf_safe_call(static fn() => rename($tmp, $targetPath), $error)) {
        scf_safe_call(static fn() => unlink($tmp));
        return false;
    }
    return true;
}
