<?php

namespace Scf\Server\Gateway;

use Scf\Helper\JsonHelper;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Socket;
use Throwable;

/**
 * gateway 进程内使用的本地 IPC 工具。
 *
 * 这一层只负责 Unix Socket 路由、请求/响应封装和落盘溢出，不参与业务调度。
 */
class LocalIpc {
    protected const SOCKET_BASE_DIR = '/tmp/scf_ipc';

    /**
     * gateway 控制面 IPC socket 路径。
     *
     * @param int $port gateway 监听端口。
     * @return string 对应的 Unix socket 路径。
     */
    public static function gatewaySocketPath(int $port): string {
        return self::buildSocketPath('gateway', $port);
    }

    /**
     * upstream 管理 IPC socket 路径。
     *
     * @param int $port upstream 监听端口。
     * @return string 对应的 Unix socket 路径。
     */
    public static function upstreamSocketPath(int $port): string {
        return self::buildSocketPath('upstream', $port);
    }

    /**
     * 通过 Unix socket 发起一次带响应的 IPC 请求。
     *
     * @param string $socketPath 目标 Unix socket 路径。
     * @param string $action IPC 动作名。
     * @param array $payload 需要发送的业务载荷。
     * @param float $timeoutSeconds 连接与读写超时时间。
     * @return array|null 成功时返回解码后的响应数组，失败时返回 null。
     */
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
            stream_set_blocking($socket, false);
            if (!self::writeAll($socket, $request)) {
                fclose($socket);
                return null;
            }
            stream_socket_shutdown($socket, STREAM_SHUT_WR);
            $response = self::readAll($socket, $timeoutSeconds);
            fclose($socket);
            if (!is_string($response) || trim($response) === '') {
                return null;
            }
            $decoded = JsonHelper::recover(trim($response));
            if (is_array($decoded) && isset($decoded['data_file']) && is_string($decoded['data_file']) && is_file($decoded['data_file'])) {
                // 大响应体通过 spilled file 回传，避免 IPC 消息本身过大。
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

    /**
     * 通过 Unix socket 发送一次无响应的通知型 IPC 消息。
     *
     * @param string $socketPath 目标 Unix socket 路径。
     * @param string $action IPC 动作名。
     * @param array $payload 需要发送的业务载荷。
     * @param float $timeoutSeconds 连接与写入超时时间。
     * @return bool 发送成功时返回 true。
     */
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

    /**
     * 确保本机 IPC 目录存在。
     *
     * @return string IPC 目录路径。
     * @throws RuntimeException 当目录无法创建时抛出。
     */
    public static function ensureSocketDir(): string {
        $dir = self::SOCKET_BASE_DIR;
        if (!is_dir($dir) && !@mkdir($dir, 0777, true) && !is_dir($dir)) {
            throw new RuntimeException('无法创建本机IPC目录:' . $dir);
        }
        return $dir;
    }

    /**
     * 响应过大时，把结果落到一个稳定可寻址的文件里。
     *
     * @param string $socketPath 当前 socket 路径。
     * @param string $action IPC 动作名。
     * @return string 响应溢出文件路径。
     */
    public static function spillResponseFilePath(string $socketPath, string $action): string {
        self::ensureSocketDir();
        $base = basename($socketPath, '.sock');
        $action = preg_replace('/[^a-zA-Z0-9_.-]+/', '_', $action);
        return self::SOCKET_BASE_DIR . '/' . $base . '.' . $action . '.json';
    }

    /**
     * 判断 socket 文件是否已经存在。
     *
     * @param string $socketPath 待检查的 socket 路径。
     * @return bool 文件存在时返回 true。
     */
    protected static function socketPathExists(string $socketPath): bool {
        return $socketPath !== '' && file_exists($socketPath);
    }

    /**
     * 直到写完或写失败为止。
     *
     * @param resource $socket 已打开的 socket 资源。
     * @param string $buffer 需要写入的内容。
     * @return bool 全部写入成功时返回 true。
     */
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

    /**
     * 在受控超时窗口内读取完整响应，避免 stream_get_contents() 在 shutdown 竞态里
     * 因对端既不继续写也不及时 EOF 而把当前协程永久卡死。
     *
     * @param resource $socket 已打开的 socket 资源。
     * @param float $timeoutSeconds 本次 IPC 的总超时时间。
     * @return string 读取到的完整响应内容。
     */
    protected static function readAll($socket, float $timeoutSeconds): string {
        $deadline = microtime(true) + max(0.1, $timeoutSeconds);
        $response = '';
        while (microtime(true) < $deadline) {
            $chunk = @fread($socket, 8192);
            if (is_string($chunk) && $chunk !== '') {
                $response .= $chunk;
                continue;
            }

            if (feof($socket)) {
                break;
            }

            $meta = stream_get_meta_data($socket);
            if (($meta['timed_out'] ?? false) || ($meta['eof'] ?? false)) {
                break;
            }

            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(0.01);
            } else {
                usleep(10000);
            }
        }
        return $response;
    }

    /**
     * 统一构造带 app/env/port 的 socket 文件名。
     *
     * @param string $prefix socket 前缀，用于区分 gateway / upstream 等角色。
     * @param int $port 监听端口。
     * @return string 标准化后的 socket 路径。
     */
    protected static function buildSocketPath(string $prefix, int $port): string {
        $port = max(1, $port);
        self::ensureSocketDir();
        $app = preg_replace('/[^a-zA-Z0-9_-]+/', '_', APP_DIR_NAME ?: 'app');
        $env = preg_replace('/[^a-zA-Z0-9_-]+/', '_', SERVER_RUN_ENV ?: 'prod');
        return self::SOCKET_BASE_DIR . '/' . $prefix . '_' . $app . '_' . $env . '_' . $port . '.sock';
    }
}

/**
 * 一个基于 Unix socket 的轻量本地服务端。
 *
 * gateway / upstream 都会用它承载自己的进程间控制接口。
 */
class LocalIpcServer {
    protected ?Socket $server = null;
    protected array $connections = [];
    protected bool $running = false;
    protected int $acceptCoroutineId = 0;

    /**
     * 绑定 socket 路径和请求处理器，构成一个独立的本地 IPC 服务。
     *
     * @param string $socketPath 监听用的 Unix socket 路径。
     * @param \Closure(array):array $handler 负责处理每个 IPC 请求并返回响应。
     * @param string $label 服务标签，用于错误信息和日志定位。
     * @return void
     */
    public function __construct(
        protected string $socketPath,
        protected \Closure $handler,
        protected string $label = 'local_ipc'
    ) {
    }

    /**
     * 启动 socket 服务。
     *
     * @return void
     */
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

    /**
     * 停止 socket 服务并释放所有连接。
     *
     * @return void
     */
    public function stop(): void {
        $this->running = false;
        // accept 可能正阻塞在 Unix socket 上，先主动唤醒一次，让循环有机会观察到 running=false 后退出。
        $this->wakeAcceptLoop();
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

    /**
     * 真正创建、bind、listen 并进入 accept 循环。
     *
     * @return void
     * @throws RuntimeException 当 bind 或 listen 失败时抛出。
     */
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
            // stop() 会通过自连接唤醒 accept；如果这时服务已经进入关闭态，直接丢弃该连接并结束循环。
            if (!$this->running) {
                try {
                    $connection->close();
                } catch (Throwable) {
                }
                break;
            }
            $id = spl_object_id($connection);
            $this->connections[$id] = $connection;
            Coroutine::create(function () use ($connection): void {
                $this->readConnection($connection);
            });
        }
    }

    /**
     * 主动连接一次本地 socket，把阻塞中的 accept 协程唤醒。
     *
     * 关闭阶段不能假定 `Socket::close()` 一定能立即打断正在 accept 的协程；
     * 这里通过一次最小化的自连接，让 accept 及时返回并收敛退出路径。
     *
     * @return void
     */
    protected function wakeAcceptLoop(): void {
        if ($this->socketPath === '' || !is_file($this->socketPath)) {
            return;
        }
        try {
            $client = new Socket(AF_UNIX, SOCK_STREAM, 0);
            if ($client->connect($this->socketPath, 0.05)) {
                $client->close();
                return;
            }
            try {
                $client->close();
            } catch (Throwable) {
            }
        } catch (Throwable) {
        }
    }

    /**
     * 读取单个连接的请求、执行 handler、回写响应。
     *
     * @param Socket $connection 当前连接。
     * @return void
     */
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
            // handler 可以只声明 action，真正落盘动作由 LocalIpc 统一完成。
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

    /**
     * 把 IPC 响应发回去后关闭连接。
     *
     * @param int $id 连接标识。
     * @param array $response 要返回给客户端的响应数组。
     * @return void
     */
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

    /**
     * 读取一个完整请求体。
     *
     * @param Socket $connection 当前连接。
     * @return string|null 读取到的请求内容，超时或断开时返回 null。
     */
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

    /**
     * 关闭单个连接。
     *
     * @param int $id 连接标识。
     * @return void
     */
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

    /**
     * 关闭当前 server 持有的全部连接。
     *
     * @return void
     */
    protected function closeAllConnections(): void {
        foreach (array_keys($this->connections) as $id) {
            $this->closeConnection((int)$id);
        }
    }

    /**
     * 将过大的响应体写到文件，避免 IPC 消息本身过重。
     *
     * @param string $action IPC 动作名。
     * @param mixed $data 需要落盘的数据。
     * @return string|null 写盘成功时返回文件路径，失败时返回 null。
     */
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
