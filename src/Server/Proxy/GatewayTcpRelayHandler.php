<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Socket;
use Throwable;

/**
 * Gateway 的 TCP 直连转发器。
 *
 * 适用于不走 nginx 的场景：先根据首包判断控制面还是业务面，
 * 再把后续字节流原样透传到对应目标。
 */
class GatewayTcpRelayHandler {

    protected array $relaySockets = [];
    protected array $relayCoroutines = [];
    protected array $relayBindings = [];
    protected array $pendingBuffers = [];
    protected array $relayTargets = [];

    /**
     * 绑定 gateway 与实例管理器，负责 TCP 首包分流和字节流透传。
     *
     * @param GatewayServer $gateway 当前 gateway 运行实例。
     * @param AppInstanceManager $instanceManager 业务实例管理器。
     * @return void
     */
    public function __construct(
        protected GatewayServer $gateway,
        protected AppInstanceManager $instanceManager
    ) {
    }

    /**
     * 处理客户端收到的数据。
     *
     * 首包阶段可能还无法判断目标，因此先缓存，等能解析出 HTTP path 或
     * 可以绑定业务 upstream 后再建立 relay socket。
     *
     * @param int $fd 客户端连接 fd。
     * @param string $data 本次收到的原始字节流。
     * @param string|null $affinityKey 业务实例亲和键，允许外部指定绑定偏好。
     * @return void
     */
    public function handleReceive(int $fd, string $data, ?string $affinityKey = null): void {
        $socket = $this->relaySockets[$fd] ?? null;
        if (!$socket instanceof Socket) {
            $this->pendingBuffers[$fd] = ($this->pendingBuffers[$fd] ?? '') . $data;
            $target = $this->relayTargets[$fd] ?? $this->resolveRelayTarget($fd, $this->pendingBuffers[$fd], $affinityKey);
            if ($target === null) {
                return;
            }
            $this->relayTargets[$fd] = $target;
            if (($target['type'] ?? '') === 'upstream' && isset($target['instance']) && is_array($target['instance'])) {
                $this->relayBindings[$fd] = $target['instance'];
            }
            $socket = $this->connectRelaySocket($target);
            $this->relaySockets[$fd] = $socket;
            $this->startUpstreamPump($fd, $socket);
            $data = $this->pendingBuffers[$fd] ?? '';
            unset($this->pendingBuffers[$fd]);
        }

        try {
            if ($data !== '') {
                $ok = $socket->sendAll($data);
                if ($ok === false) {
                    throw new RuntimeException($socket->errMsg ?: 'send upstream failed');
                }
            }
        } catch (Throwable) {
            $this->disconnectClient($fd);
        }
    }

    /**
     * 记录一个新的 TCP 连接，等待首包决定转发目标。
     *
     * @param int $fd 客户端连接 fd。
     * @param string|null $affinityKey 业务实例亲和键，预留给后续首包解析使用。
     * @return void
     */
    public function handleConnect(int $fd, ?string $affinityKey = null): void {
        $this->pendingBuffers[$fd] = '';
    }

    /**
     * 当前仍处于 relay 状态或等待首包解析的连接数。
     *
     * @return int 当前活跃或待解析的 TCP 连接数。
     */
    public function activeRelayConnectionCount(): int {
        return count($this->relayTargets) + count($this->pendingBuffers);
    }

    /**
     * 清理一个 TCP relay 连接对应的 socket、协程和绑定关系。
     *
     * @param int $fd 客户端连接 fd。
     * @return void
     */
    public function handleClose(int $fd): void {
        if (isset($this->relaySockets[$fd]) && $this->relaySockets[$fd] instanceof Socket) {
            try {
                $this->relaySockets[$fd]->close();
            } catch (Throwable) {
            }
        }
        unset($this->relaySockets[$fd], $this->relayCoroutines[$fd], $this->relayBindings[$fd], $this->pendingBuffers[$fd], $this->relayTargets[$fd]);
        $this->instanceManager->releaseConnectionBinding($fd);
    }

    /**
     * 主动断开本地 TCP 客户端。
     *
     * @param int $fd 客户端连接 fd。
     * @return void
     */
    public function disconnectClient(int $fd): void {
        $server = $this->gateway->server();
        $this->handleClose($fd);
        if (!$server->exist($fd)) {
            return;
        }
        if ($server->isEstablished($fd)) {
            $server->disconnect($fd);
        } else {
            $server->close($fd);
        }
    }

    /**
     * 按目标类型建立到控制面或业务面的 TCP socket。
     *
     * @param array $target resolveRelayTarget 计算得到的目标信息。
     * @return Socket 已连接的 TCP socket。
     * @throws RuntimeException 当目标端口非法或连接失败时抛出。
     */
    protected function connectRelaySocket(array $target): Socket {
        $host = (string)($target['host'] ?? '127.0.0.1');
        $port = (int)($target['port'] ?? 0);
        if ($port <= 0) {
            throw new RuntimeException('invalid relay target port');
        }
        $socket = new Socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
        if (!$socket->connect($host, $port, $this->connectTimeout())) {
            $socket->close();
            throw new RuntimeException('connect failed');
        }
        return $socket;
    }

    /**
     * 基于首包内容决定 relay 到控制面还是业务 upstream。
     *
     * 路径命中 `/dashboard.socket`、`/_gateway` 等控制面地址时，不再把连接绑定到业务实例。
     *
     * @param int $fd 客户端连接 fd。
     * @param string $buffer 当前已缓存的首包字节流。
     * @param string|null $affinityKey 业务实例亲和键。
     * @return array|null 返回目标信息，无法判定且仍在等待首包时返回 null。
     */
    protected function resolveRelayTarget(int $fd, string $buffer, ?string $affinityKey = null): ?array {
        $path = $this->extractHttpPath($buffer);
        if ($path !== null && $this->gateway->shouldRoutePathToControlPlane($path)) {
            // 控制面请求不应该进入业务 upstream，否则 dashboard/socket 会被错误转发。
            return [
                'type' => 'control',
                'host' => $this->gateway->internalControlHost(),
                'port' => $this->gateway->controlPort(),
            ];
        }
        if ($path === null && $this->looksLikePartialHttpRequest($buffer)) {
            return null;
        }
        $upstream = $this->instanceManager->bindConnectionUpstream($fd, $affinityKey);
        if (!$upstream) {
            $this->disconnectClient($fd);
            return null;
        }
        return [
            'type' => 'upstream',
            'host' => (string)($upstream['host'] ?? '127.0.0.1'),
            'port' => (int)($upstream['port'] ?? 0),
            'instance' => $upstream,
        ];
    }

    /**
     * 尝试从首包中提取 HTTP path。
     *
     * @param string $buffer 待分析的请求缓冲。
     * @return string|null 命中 HTTP 请求行时返回 path，否则返回 null。
     */
    protected function extractHttpPath(string $buffer): ?string {
        if ($buffer === '') {
            return null;
        }
        if (!preg_match('/^[A-Z]+\\s+([^\\s]+)\\s+HTTP\\/1\\.[01]/', $buffer, $matches)) {
            return null;
        }
        return (string)($matches[1] ?? '/');
    }

    /**
     * 判断当前缓冲是否像是一个尚未收完整的 HTTP 请求行。
     *
     * @param string $buffer 待分析的请求缓冲。
     * @return bool 识别为半包 HTTP 请求行时返回 true。
     */
    protected function looksLikePartialHttpRequest(string $buffer): bool {
        if ($buffer === '') {
            return true;
        }
        $line = rtrim($buffer, "\r\n");
        return preg_match('/^[A-Z]+$/', $line) === 1
            || preg_match('/^[A-Z]+\\s+[^\\s]*$/', $line) === 1
            || preg_match('/^[A-Z]+\\s+[^\\s]+\\s+HTTP\\/1\\.[01]?$/', $line) === 1;
    }

    /**
     * 启动 upstream -> client 的读取泵。
     *
     * @param int $fd 客户端连接 fd。
     * @param Socket $socket 已连接的上游 socket。
     * @return void
     */
    protected function startUpstreamPump(int $fd, Socket $socket): void {
        $server = $this->gateway->server();
        $this->relayCoroutines[$fd] = Coroutine::create(function () use ($fd, $socket, $server) {
            try {
                $idleTimeout = $this->idleTimeout();
                while (true) {
                    $data = $idleTimeout > 0 ? $socket->recv(65535, $idleTimeout) : $socket->recv(65535);
                    if ($data === '' || $data === false) {
                        break;
                    }
                    if (!$server->exist($fd)) {
                        break;
                    }
                    if ($server->send($fd, $data) === false) {
                        break;
                    }
                }
            } catch (Throwable) {
            } finally {
                $this->disconnectClient($fd);
            }
        });
    }

    /**
     * relay 连接建立超时。
     *
     * @return float 连接超时秒数，最小为 0.1。
     */
    protected function connectTimeout(): float {
        $timeout = (float)($this->gateway->serverConfig()['gateway_tcp_relay_connect_timeout'] ?? 3);
        return max(0.1, $timeout);
    }

    /**
     * relay 空闲读超时，0 表示一直等。
     *
     * @return float 空闲读超时秒数，0 表示无限等待。
     */
    protected function idleTimeout(): float {
        $timeout = (float)($this->gateway->serverConfig()['gateway_tcp_relay_idle_timeout'] ?? 0);
        return $timeout > 0 ? $timeout : 0.0;
    }
}
