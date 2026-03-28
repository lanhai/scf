<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Socket;
use Throwable;

class GatewayTcpRelayHandler {

    protected array $relaySockets = [];
    protected array $relayCoroutines = [];
    protected array $relayBindings = [];
    protected array $pendingBuffers = [];
    protected array $relayTargets = [];

    public function __construct(
        protected GatewayServer $gateway,
        protected AppInstanceManager $instanceManager
    ) {
    }

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

    public function handleConnect(int $fd, ?string $affinityKey = null): void {
        $this->pendingBuffers[$fd] = '';
    }

    public function activeRelayConnectionCount(): int {
        return count($this->relayTargets) + count($this->pendingBuffers);
    }

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

    protected function resolveRelayTarget(int $fd, string $buffer, ?string $affinityKey = null): ?array {
        $path = $this->extractHttpPath($buffer);
        if ($path !== null && $this->gateway->shouldRoutePathToControlPlane($path)) {
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

    protected function extractHttpPath(string $buffer): ?string {
        if ($buffer === '') {
            return null;
        }
        if (!preg_match('/^[A-Z]+\\s+([^\\s]+)\\s+HTTP\\/1\\.[01]/', $buffer, $matches)) {
            return null;
        }
        return (string)($matches[1] ?? '/');
    }

    protected function looksLikePartialHttpRequest(string $buffer): bool {
        if ($buffer === '') {
            return true;
        }
        $line = rtrim($buffer, "\r\n");
        return preg_match('/^[A-Z]+$/', $line) === 1
            || preg_match('/^[A-Z]+\\s+[^\\s]*$/', $line) === 1
            || preg_match('/^[A-Z]+\\s+[^\\s]+\\s+HTTP\\/1\\.[01]?$/', $line) === 1;
    }

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

    protected function connectTimeout(): float {
        $timeout = (float)($this->gateway->serverConfig()['gateway_tcp_relay_connect_timeout'] ?? 3);
        return max(0.1, $timeout);
    }

    protected function idleTimeout(): float {
        $timeout = (float)($this->gateway->serverConfig()['gateway_tcp_relay_idle_timeout'] ?? 0);
        return $timeout > 0 ? $timeout : 0.0;
    }
}
