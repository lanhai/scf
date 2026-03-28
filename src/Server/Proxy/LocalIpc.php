<?php

namespace Scf\Server\Proxy;

use Scf\Helper\JsonHelper;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Socket;
use Throwable;

class LocalIpc {
    protected const SOCKET_BASE_DIR = '/tmp/scf_ipc';

    public static function gatewaySocketPath(int $port): string {
        return self::buildSocketPath('gateway', $port);
    }

    public static function upstreamSocketPath(int $port): string {
        return self::buildSocketPath('upstream', $port);
    }

    public static function upstreamHttpSocketPath(int $port): string {
        return self::buildSocketPath('upstream_http', $port);
    }

    public static function request(string $socketPath, string $action, array $payload = [], float $timeoutSeconds = 1.0): ?array {
        if (!self::socketPathExists($socketPath)) {
            return null;
        }

        $request = JsonHelper::toJson([
            'action' => $action,
            'payload' => $payload,
        ]);
        if (!is_string($request) || $request === '') {
            return null;
        }

        try {
            $socket = @stream_socket_client(
                'unix://' . $socketPath,
                $errno,
                $errstr,
                $timeoutSeconds,
                STREAM_CLIENT_CONNECT
            );
            if (!is_resource($socket)) {
                return null;
            }
            $seconds = max(1, (int)floor($timeoutSeconds));
            $microseconds = max(0, (int)(($timeoutSeconds - floor($timeoutSeconds)) * 1000000));
            stream_set_timeout($socket, $seconds, $microseconds);
            if (!self::writeAll($socket, $request)) {
                fclose($socket);
                return null;
            }
            stream_socket_shutdown($socket, STREAM_SHUT_WR);
            $response = stream_get_contents($socket);
            fclose($socket);
            if (!is_string($response) || trim($response) === '') {
                return null;
            }
            $decoded = JsonHelper::recover(trim($response));
            if (is_array($decoded) && isset($decoded['data_file']) && is_string($decoded['data_file']) && is_file($decoded['data_file'])) {
                $data = @file_get_contents($decoded['data_file']);
                if (is_string($data) && $data !== '') {
                    $decoded['data'] = JsonHelper::recover($data);
                }
            }
            return is_array($decoded) ? $decoded : null;
        } catch (Throwable) {
            return null;
        }
    }

    public static function notify(string $socketPath, string $action, array $payload = [], float $timeoutSeconds = 1.0): bool {
        if (!self::socketPathExists($socketPath)) {
            return false;
        }

        $request = JsonHelper::toJson([
            'action' => $action,
            'payload' => $payload,
        ]);
        if (!is_string($request) || $request === '') {
            return false;
        }

        try {
            $socket = @stream_socket_client(
                'unix://' . $socketPath,
                $errno,
                $errstr,
                $timeoutSeconds,
                STREAM_CLIENT_CONNECT
            );
            if (!is_resource($socket)) {
                return false;
            }
            stream_set_timeout($socket, max(1, (int)ceil($timeoutSeconds)));
            if (!self::writeAll($socket, $request)) {
                fclose($socket);
                return false;
            }
            stream_socket_shutdown($socket, STREAM_SHUT_WR);
            fclose($socket);
            return true;
        } catch (Throwable) {
            return false;
        }
    }

    public static function ensureSocketDir(): string {
        $dir = self::SOCKET_BASE_DIR;
        if (!is_dir($dir) && !@mkdir($dir, 0777, true) && !is_dir($dir)) {
            throw new RuntimeException('无法创建本机IPC目录:' . $dir);
        }
        return $dir;
    }

    public static function spillResponseFilePath(string $socketPath, string $action): string {
        self::ensureSocketDir();
        $base = basename($socketPath, '.sock');
        $action = preg_replace('/[^a-zA-Z0-9_.-]+/', '_', $action);
        return self::SOCKET_BASE_DIR . '/' . $base . '.' . $action . '.json';
    }

    protected static function socketPathExists(string $socketPath): bool {
        return $socketPath !== '' && file_exists($socketPath);
    }

    protected static function writeAll($socket, string $buffer): bool {
        $length = strlen($buffer);
        $written = 0;
        while ($written < $length) {
            $chunk = @fwrite($socket, substr($buffer, $written));
            if (!is_int($chunk) || $chunk <= 0) {
                return false;
            }
            $written += $chunk;
        }
        return true;
    }

    protected static function buildSocketPath(string $prefix, int $port): string {
        $port = max(1, $port);
        self::ensureSocketDir();
        $app = preg_replace('/[^a-zA-Z0-9_-]+/', '_', APP_DIR_NAME ?: 'app');
        $env = preg_replace('/[^a-zA-Z0-9_-]+/', '_', SERVER_RUN_ENV ?: 'prod');
        return self::SOCKET_BASE_DIR . '/' . $prefix . '_' . $app . '_' . $env . '_' . $port . '.sock';
    }
}

class LocalIpcServer {
    protected ?Socket $server = null;
    protected array $connections = [];
    protected bool $running = false;
    protected int $acceptCoroutineId = 0;

    public function __construct(
        protected string $socketPath,
        protected \Closure $handler,
        protected string $label = 'local_ipc'
    ) {
    }

    public function start(): void {
        if ($this->running) {
            return;
        }
        $this->running = true;
        Coroutine::create(function (): void {
            try {
                $this->runServer();
            } catch (Throwable $throwable) {
                $this->running = false;
                $this->server = null;
                @unlink($this->socketPath);
                throw $throwable;
            } finally {
                $this->acceptCoroutineId = 0;
                $this->closeAllConnections();
                if ($this->server instanceof Socket) {
                    try {
                        $this->server->close();
                    } catch (Throwable) {
                    }
                    $this->server = null;
                }
                @unlink($this->socketPath);
            }
        });
    }

    public function stop(): void {
        $this->running = false;
        $this->closeAllConnections();
        if ($this->server instanceof Socket) {
            try {
                $this->server->close();
            } catch (Throwable) {
            }
            $this->server = null;
        }
        @unlink($this->socketPath);
    }

    protected function runServer(): void {
        LocalIpc::ensureSocketDir();
        @unlink($this->socketPath);
        $server = new Socket(AF_UNIX, SOCK_STREAM, 0);
        if (!$server->bind($this->socketPath)) {
            $message = $server->errMsg ?: 'bind failed';
            try {
                $server->close();
            } catch (Throwable) {
            }
            throw new RuntimeException("{$this->label} 启动失败: {$message}");
        }
        if (!$server->listen(128)) {
            $message = $server->errMsg ?: 'listen failed';
            try {
                $server->close();
            } catch (Throwable) {
            }
            throw new RuntimeException("{$this->label} 启动失败: {$message}");
        }
        @chmod($this->socketPath, 0666);
        $this->server = $server;
        $this->acceptCoroutineId = Coroutine::getCid();

        while ($this->running) {
            $connection = $server->accept(1.0);
            if (!$connection instanceof Socket) {
                if (!$this->running) {
                    break;
                }
                continue;
            }
            $id = spl_object_id($connection);
            $this->connections[$id] = $connection;
            Coroutine::create(function () use ($connection): void {
                $this->readConnection($connection);
            });
        }
    }

    protected function readConnection(Socket $connection): void {
        $id = spl_object_id($connection);
        if (!isset($this->connections[$id])) {
            return;
        }

        $body = $this->receiveRequest($connection);
        if (!is_string($body) || trim($body) === '') {
            $this->closeConnection($id);
            return;
        }

        $request = JsonHelper::recover(trim($body));
        if (!is_array($request)) {
            $this->writeAndClose($id, ['ok' => false, 'status' => 400, 'message' => 'invalid request']);
            return;
        }

        try {
            $response = ($this->handler)($request);
            if (!is_array($response)) {
                $response = ['ok' => true, 'status' => 200, 'data' => $response];
            }
        } catch (Throwable $throwable) {
            $response = [
                'ok' => false,
                'status' => 500,
                'message' => $throwable->getMessage(),
            ];
        }

        $afterWrite = null;
        if (isset($response['__after_write']) && is_callable($response['__after_write'])) {
            $afterWrite = $response['__after_write'];
            unset($response['__after_write']);
        }
        if (isset($response['__data_file_action']) && is_string($response['__data_file_action'])) {
            $filePath = $this->spillResponseData((string)$response['__data_file_action'], $response['data'] ?? null);
            unset($response['__data_file_action'], $response['data']);
            if ($filePath !== null) {
                $response['data_file'] = $filePath;
            } else {
                $response = [
                    'ok' => false,
                    'status' => 500,
                    'message' => 'ipc_spill_failed',
                ];
            }
        }

        $this->writeAndClose($id, $response);
        if ($afterWrite !== null) {
            try {
                $afterWrite();
            } catch (Throwable) {
            }
        }
    }

    protected function writeAndClose(int $id, array $response): void {
        $connection = $this->connections[$id] ?? null;
        if ($connection instanceof Socket) {
            $buffer = JsonHelper::toJson($response);
            if (!is_string($buffer)) {
                $buffer = JsonHelper::toJson([
                    'ok' => false,
                    'status' => 500,
                    'message' => 'ipc_encode_failed',
                ]);
            }
            if (is_string($buffer) && $buffer !== '') {
                try {
                    $connection->sendAll($buffer);
                } catch (Throwable) {
                }
            }
        }
        $this->closeConnection($id);
    }

    protected function receiveRequest(Socket $connection): ?string {
        $buffer = '';
        while ($this->running) {
            $chunk = $connection->recv(65536, 1.0);
            if (is_string($chunk) && $chunk !== '') {
                $buffer .= $chunk;
                continue;
            }
            if ($chunk === '') {
                break;
            }
            if ($connection->errCode === SOCKET_ETIMEDOUT) {
                if ($buffer !== '') {
                    continue;
                }
                return null;
            }
            break;
        }
        return trim($buffer) !== '' ? trim($buffer) : null;
    }

    protected function closeConnection(int $id): void {
        $connection = $this->connections[$id] ?? null;
        unset($this->connections[$id]);
        if ($connection instanceof Socket) {
            try {
                $connection->close();
            } catch (Throwable) {
            }
        }
    }

    protected function closeAllConnections(): void {
        foreach (array_keys($this->connections) as $id) {
            $this->closeConnection((int)$id);
        }
    }

    protected function spillResponseData(string $action, mixed $data): ?string {
        $encoded = JsonHelper::toJson($data);
        if (!is_string($encoded) || $encoded === '') {
            return null;
        }
        $path = LocalIpc::spillResponseFilePath($this->socketPath, $action);
        $tmpPath = $path . '.tmp';
        if (@file_put_contents($tmpPath, $encoded, LOCK_EX) === false) {
            @unlink($tmpPath);
            return null;
        }
        @rename($tmpPath, $path);
        return $path;
    }
}
