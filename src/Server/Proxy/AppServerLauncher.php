<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Scf\Core\Console;
use Scf\Core\Server as CoreServer;
use Swoole\Coroutine;
use Swoole\Coroutine\Client as CoroutineClient;
use Swoole\Coroutine\Http\Client as CoroutineHttpClient;
use Swoole\Process;
use Swoole\Server;
use Throwable;
use const SIGKILL;
use const SIGTERM;

class AppServerLauncher {

    protected const GRACEFUL_SHUTDOWN_PATH = '/_gateway/internal/upstream/shutdown';
    protected const STATUS_PATH = '/_gateway/internal/upstream/status';
    protected const HEALTH_PATH = '/_gateway/internal/upstream/health';
    protected const QUIESCE_BUSINESS_PATH = '/_gateway/internal/upstream/quiesce';
    public const NORMAL_RECYCLE_GRACE_SECONDS = 1800;
    protected const RECYCLE_WARN_AFTER_SECONDS = 60;
    protected const RECYCLE_WARN_INTERVAL_SECONDS = 60;

    public function isListening(string $host, int $port, float $timeoutSeconds = 0.3): bool {
        if ($port <= 0) {
            return false;
        }
        if (Coroutine::getCid() > 0) {
            try {
                $client = new CoroutineClient(SWOOLE_SOCK_TCP);
                $connected = $client->connect($host, $port, $timeoutSeconds);
                if ($connected) {
                    $client->close();
                    return true;
                }
            } catch (Throwable) {
            }
            return false;
        }
        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $timeoutSeconds,
            STREAM_CLIENT_CONNECT
        );
        if (is_resource($socket)) {
            fclose($socket);
            return true;
        }
        return false;
    }

    public function findAvailablePort(string $host, int $startPort, int $maxScan = 200): int {
        $port = max(1025, $startPort);
        for ($i = 0; $i < $maxScan; $i++, $port++) {
            if ($this->isListening($host, $port, 0.1)) {
                continue;
            }
            $server = @stream_socket_server("tcp://{$host}:{$port}", $errno, $errstr);
            if ($server === false) {
                continue;
            }
            fclose($server);
            return $port;
        }
        throw new RuntimeException("未找到可用端口，起始端口:{$startPort}");
    }

    public function createManagedProcess(array $options): array {
        $app = (string)($options['app'] ?? '');
        $env = (string)($options['env'] ?? 'production');
        $role = (string)($options['role'] ?? 'master');
        $port = (int)($options['port'] ?? 0);
        $rpcPort = (int)($options['rpc_port'] ?? 0);
        $src = (string)($options['src'] ?? '');
        $extra = is_array($options['extra'] ?? null) ? $options['extra'] : [];

        if ($app === '') {
            throw new RuntimeException('启动业务 server 缺少 app');
        }
        if ($port <= 0) {
            throw new RuntimeException('启动业务 server 缺少有效 port');
        }

        $command = $this->buildCommand($app, $env, $role, $port, $rpcPort, $src, $extra);
        $binary = PHP_BINARY;
        $args = array_slice($command, 1);

        $process = new Process(function (Process $process) use ($binary, $args) {
            $process->exec($binary, $args);
            exit(1);
        }, false, SOCK_DGRAM, false);

        return [
            'process' => $process,
            'port' => $port,
            'host' => (string)($options['host'] ?? '127.0.0.1'),
            'role' => $role,
            'command' => $binary . ' ' . implode(' ', array_map('escapeshellarg', $args)),
        ];
    }

    public function attachManagedProcess(Server $server, array $spec): array {
        /** @var Process|null $process */
        $process = $spec['process'] ?? null;
        if (!$process instanceof Process) {
            throw new RuntimeException('托管业务 server 缺少有效进程对象');
        }

        $server->addProcess($process);
        return $spec;
    }

    public function launch(array $options): array {
        $spec = $this->createManagedProcess($options);
        /** @var Process $process */
        $process = $spec['process'];
        $pid = $process->start();
        if ($pid <= 0) {
            throw new RuntimeException('拉起业务 server 失败:process start failed');
        }

        $spec['pid'] = $pid;
        unset($spec['process']);
        return $spec;
    }

    public function processPid(array $spec): int {
        /** @var Process|null $process */
        $process = $spec['process'] ?? null;
        if (!$process instanceof Process) {
            return 0;
        }
        return (int)($process->pid ?? 0);
    }

    protected function buildCommand(string $app, string $env, string $role, int $port, int $rpcPort, string $src, array $extra): array {
        $command = [
            PHP_BINARY,
            SCF_ROOT . '/boot',
            'gateway_upstream',
            'start',
            "-app={$app}",
            "-env={$env}",
            "-role={$role}",
            "-port={$port}",
        ];

        if ($src === 'dir') {
            $command[] = '-dir';
        } elseif ($src === 'phar') {
            $command[] = '-phar';
        }

        foreach ($this->sanitizeExtraFlags($extra) as $arg) {
            if (!is_string($arg) || $arg === '') {
                continue;
            }
            $command[] = $arg;
        }

        if ($rpcPort > 0) {
            $command[] = '-rport=' . $rpcPort;
        }

        return $command;
    }

    protected function sanitizeExtraFlags(array $extra): array {
        return array_values(array_filter($extra, static function ($arg) {
            if (!is_string($arg) || $arg === '') {
                return false;
            }
            return !str_starts_with($arg, '-rport=');
        }));
    }

    public function stop(array $instance, int $graceSeconds = self::NORMAL_RECYCLE_GRACE_SECONDS): void {
        $metadata = (array)($instance['metadata'] ?? []);
        if (($metadata['managed'] ?? false) !== true) {
            return;
        }
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $rpcPort = (int)($metadata['rpc_port'] ?? ($instance['rpc_port'] ?? 0));
        $pid = (int)($metadata['pid'] ?? 0);
        $startedAt = microtime(true);
        $deadline = $startedAt + max(1, $graceSeconds);
        $nextWarnAt = $startedAt + self::RECYCLE_WARN_AFTER_SECONDS;
        $gracefulAccepted = $this->requestGracefulShutdown($host, $port);
        $termSent = false;
        $termAt = $gracefulAccepted ? $deadline : microtime(true);

        while (microtime(true) < $deadline) {
            $httpAlive = $port > 0 && $this->isListening($host, $port, 0.2);
            $rpcAlive = $rpcPort > 0 && $this->isListening($host, $rpcPort, 0.2);
            $pidAlive = $this->isProcessRunning($pid);
            if (!$httpAlive && !$rpcAlive && !$pidAlive) {
                return;
            }
            $now = microtime(true);
            if ($now >= $nextWarnAt) {
                $elapsed = max(1, (int)floor($now - $startedAt));
                $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                Console::warning(
                    "【Gateway】业务实例仍在等待平滑回收({$elapsed}s): {$host}:{$port}{$rpcInfo}, "
                    . "pid={$pid}, http=" . ($httpAlive ? 'alive' : 'down')
                    . ", rpc=" . ($rpcAlive ? 'alive' : 'down')
                    . ", process=" . ($pidAlive ? 'alive' : 'down')
                );
                $nextWarnAt += self::RECYCLE_WARN_INTERVAL_SECONDS;
            }
            if (!$termSent && microtime(true) >= $termAt) {
                if ($this->isProcessRunning($pid)) {
                    @Process::kill($pid, SIGTERM);
                }
                $termSent = true;
            }
            usleep(200000);
        }

        $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
        Console::warning("【Gateway】业务实例回收超时，开始强制终止: {$host}:{$port}{$rpcInfo}, pid={$pid}");
        if ($this->isProcessRunning($pid)) {
            @Process::kill($pid, SIGKILL);
        }
        if ($port > 0 && $this->isListening($host, $port, 0.2)) {
            CoreServer::killProcessByPort($port);
        }
        if ($rpcPort > 0 && $this->isListening($host, $rpcPort, 0.2)) {
            CoreServer::killProcessByPort($rpcPort);
        }
    }

    protected function requestGracefulShutdown(string $host, int $port, float $timeoutSeconds = 1.0): bool {
        return $this->requestInternalPost($host, $port, self::GRACEFUL_SHUTDOWN_PATH, $timeoutSeconds);
    }

    protected function isProcessRunning(int $pid): bool {
        if ($pid <= 0 || !@Process::kill($pid, 0)) {
            return false;
        }
        $stat = @shell_exec("ps -o stat= -p " . (int)$pid . " 2>/dev/null");
        if (!is_string($stat) || trim($stat) === '') {
            return false;
        }
        return !str_contains(trim($stat), 'Z');
    }

    public function requestBusinessQuiesce(string $host, int $port, float $timeoutSeconds = 1.0): bool {
        return $this->requestInternalPost($host, $port, self::QUIESCE_BUSINESS_PATH, $timeoutSeconds);
    }

    protected function requestInternalPost(string $host, int $port, string $path, float $timeoutSeconds = 1.0): bool {
        if ($port <= 0) {
            return false;
        }
        if ($this->shouldUseLocalUnixHttpSocket($host, $port)) {
            $statusCode = $this->requestUnixHttpStatus(
                LocalIpc::upstreamHttpSocketPath($port),
                'POST',
                $path,
                $timeoutSeconds,
                [
                    'Host: ' . $host . ':' . $port,
                    'Connection: close',
                    'X-Gateway-Control: shutdown',
                    'Content-Length: 0',
                ]
            );
            if ($statusCode === 200) {
                return true;
            }
        }
        if (Coroutine::getCid() > 0) {
            try {
                $client = new CoroutineHttpClient($host, $port);
                $client->set(['timeout' => $timeoutSeconds]);
                $client->setHeaders([
                    'Host' => "{$host}:{$port}",
                    'Connection' => 'close',
                    'X-Gateway-Control' => 'shutdown',
                ]);
                $ok = $client->post($path, '');
                $statusCode = (int)($client->statusCode ?? 0);
                $client->close();
                return $ok && $statusCode === 200;
            } catch (Throwable) {
                return false;
            }
        }

        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $timeoutSeconds,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return false;
        }

        stream_set_timeout($socket, max(1, (int)ceil($timeoutSeconds)));
        $request = "POST " . $path . " HTTP/1.1\r\n"
            . "Host: {$host}:{$port}\r\n"
            . "Connection: close\r\n"
            . "Content-Length: 0\r\n"
            . "X-Gateway-Control: shutdown\r\n\r\n";
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);

        return is_string($response) && str_contains($response, ' 200 ');
    }

    public function waitUntilReady(string $host, int $port, int $timeoutSeconds = 20, int $intervalMs = 200): bool {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            if ($this->isListening($host, $port, 0.2)) {
                return true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        return false;
    }

    public function waitUntilServicesReady(string $host, int $port, int $rpcPort = 0, int $timeoutSeconds = 20, int $intervalMs = 200, bool $verbose = true): bool {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $logged = false;
        while (microtime(true) < $deadline) {
            $httpReady = $this->isListening($host, $port, 0.2);
            $rpcReady = $rpcPort <= 0 || $this->isListening($host, $rpcPort, 0.2);
            $statusReady = false;
            if ($httpReady) {
                $status = $this->fetchHealthStatus($host, $port, min(1.0, max(0.2, $intervalMs / 1000)));
                $statusReady = is_array($status)
                    && (bool)($status['server_is_ready'] ?? false) === true
                    && (bool)($status['server_is_alive'] ?? false) === true
                    && (bool)($status['server_is_draining'] ?? false) === false;
            }
            if ($httpReady && $rpcReady && $statusReady) {
                if ($verbose && $logged) {
                    $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                    echo Console::timestamp() . " 【Gateway】业务实例端口就绪: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
                }
                return true;
            }
            if ($verbose && !$logged) {
                $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                echo Console::timestamp() . " 【Gateway】等待业务实例端口就绪: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
                $logged = true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        if ($verbose && $logged) {
            $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
            echo Console::timestamp() . " 【Gateway】等待业务实例端口就绪超时: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
        }
        return false;
    }

    protected function fetchRuntimeStatus(string $host, int $port, float $timeoutSeconds = 0.5): ?array {
        return $this->fetchInternalJsonStatus($host, $port, self::STATUS_PATH, $timeoutSeconds);
    }

    protected function fetchHealthStatus(string $host, int $port, float $timeoutSeconds = 0.5): ?array {
        return $this->fetchInternalJsonStatus($host, $port, self::HEALTH_PATH, $timeoutSeconds);
    }

    protected function fetchInternalJsonStatus(string $host, int $port, string $path, float $timeoutSeconds = 0.5): ?array {
        if ($port <= 0) {
            return null;
        }
        $effectiveTimeout = $path === self::STATUS_PATH ? max($timeoutSeconds, 5.0) : $timeoutSeconds;
        if ($this->shouldUseLocalUnixHttpSocket($host, $port)) {
            $payload = $this->requestUnixHttpJson(
                LocalIpc::upstreamHttpSocketPath($port),
                'GET',
                $path,
                $effectiveTimeout,
                [
                    'Host: ' . $host . ':' . $port,
                    'Connection: close',
                ]
            );
            if (is_array($payload['data'] ?? null)) {
                return $payload['data'];
            }
        }
        if (Coroutine::getCid() > 0) {
            try {
                $client = new CoroutineHttpClient($host, $port);
                $client->set(['timeout' => $effectiveTimeout]);
                $client->setHeaders([
                    'Host' => "{$host}:{$port}",
                    'Connection' => 'close',
                ]);
                $ok = $client->get($path);
                $statusCode = (int)($client->statusCode ?? 0);
                $body = (string)($client->body ?? '');
                $client->close();
                if (!$ok || $statusCode !== 200) {
                    return null;
                }
                $decoded = json_decode($body, true);
                return is_array($decoded['data'] ?? null) ? $decoded['data'] : null;
            } catch (Throwable) {
                return null;
            }
        }

        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $effectiveTimeout,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return null;
        }

        stream_set_timeout($socket, max(1, (int)ceil($timeoutSeconds)));
        $request = "GET " . $path . " HTTP/1.1\r\n"
            . "Host: {$host}:{$port}\r\n"
            . "Connection: close\r\n\r\n";
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);
        if (!is_string($response) || !str_contains($response, "\r\n\r\n")) {
            return null;
        }
        [, $body] = explode("\r\n\r\n", $response, 2);
        $decoded = json_decode($body, true);
        return is_array($decoded['data'] ?? null) ? $decoded['data'] : null;
    }

    protected function shouldUseLocalIpc(string $host): bool {
        return in_array($host, ['127.0.0.1', 'localhost', '0.0.0.0', SERVER_HOST], true);
    }

    protected function shouldUseLocalUnixHttpSocket(string $host, int $port): bool {
        return $this->shouldUseLocalIpc($host) && $port > 0 && file_exists(LocalIpc::upstreamHttpSocketPath($port));
    }

    protected function localIpcActionForPath(string $path): ?string {
        return match ($path) {
            self::GRACEFUL_SHUTDOWN_PATH => 'upstream.shutdown',
            self::QUIESCE_BUSINESS_PATH => 'upstream.quiesce',
            self::STATUS_PATH => 'upstream.status',
            self::HEALTH_PATH => 'upstream.health',
            default => null,
        };
    }

    protected function requestUnixHttpJson(string $socketPath, string $method, string $path, float $timeoutSeconds, array $headers = []): ?array {
        $response = $this->requestUnixHttpRaw($socketPath, $method, $path, $timeoutSeconds, $headers);
        if (!is_array($response) || (int)($response['status'] ?? 0) !== 200) {
            return null;
        }
        $body = (string)($response['body'] ?? '');
        if ($body === '') {
            return null;
        }
        $decoded = json_decode($body, true);
        return is_array($decoded) ? $decoded : null;
    }

    protected function requestUnixHttpStatus(string $socketPath, string $method, string $path, float $timeoutSeconds, array $headers = []): int {
        $response = $this->requestUnixHttpRaw($socketPath, $method, $path, $timeoutSeconds, $headers);
        return (int)($response['status'] ?? 0);
    }

    protected function requestUnixHttpRaw(string $socketPath, string $method, string $path, float $timeoutSeconds, array $headers = []): ?array {
        if ($socketPath === '' || !file_exists($socketPath)) {
            return null;
        }
        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client('unix://' . $socketPath, $errno, $errstr, $timeoutSeconds, STREAM_CLIENT_CONNECT);
        if (!is_resource($socket)) {
            return null;
        }
        $seconds = max(1, (int)floor($timeoutSeconds));
        $microseconds = max(0, (int)(($timeoutSeconds - floor($timeoutSeconds)) * 1000000));
        stream_set_timeout($socket, $seconds, $microseconds);
        $headerLines = array_merge([
            strtoupper($method) . ' ' . $path . " HTTP/1.1",
        ], $headers, ['']);
        $request = implode("\r\n", $headerLines) . "\r\n";
        fwrite($socket, $request);
        $raw = stream_get_contents($socket);
        fclose($socket);
        if (!is_string($raw) || !str_contains($raw, "\r\n\r\n")) {
            return null;
        }
        [$head, $body] = explode("\r\n\r\n", $raw, 2);
        $status = 0;
        foreach (explode("\r\n", $head) as $line) {
            if (preg_match('#^HTTP/\S+\s+(\d{3})#', $line, $matches)) {
                $status = (int)$matches[1];
                break;
            }
        }
        return ['status' => $status, 'body' => $body];
    }
}
