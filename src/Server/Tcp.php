<?php

namespace Scf\Server;

use RuntimeException;
use Scf\Server\Listener\TcpListener;
use Swoole\Coroutine\Server;
use Swoole\Server as NativeServer;
use function Swoole\Coroutine\run;

/**
 * 负责 TCP 监听配置，可挂载到 HTTP upstream 或独立启动。
 */
final class Tcp {

    /**
     * @param int|string|array $config 监听端口，或包含 port 和可选传输参数的数组
     * @throws RuntimeException 配置无效或监听失败
     */
    public function start(int|string|array $config): void {
        [$host, $port, $maxPacketLength] = $this->config($config);
        run(static function () use ($host, $port, $maxPacketLength): void {
            $server = new Server($host, $port);
            $server->handle(new TcpListener($maxPacketLength));
            if (!$server->start()) {
                throw new RuntimeException("TCP 服务启动失败 {$host}:{$port}");
            }
        });
    }
    /** 将应用 TCP 端口挂到现有 HTTP upstream，随同一 Server 启停。 */
    public function attach(NativeServer $server, int|string|array $config): void {
        [$host, $port, $maxPacketLength] = $this->config($config);
        foreach ($server->ports as $existingPort) {
            if ($existingPort->port === $port) {
                throw new RuntimeException("TCP 端口与当前 Server 的监听端口冲突 {$host}:{$port}");
            }
        }
        $listener = new TcpListener($maxPacketLength);
        $socketType = filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) ? SWOOLE_SOCK_TCP6 : SWOOLE_SOCK_TCP;
        $listenerPort = $server->listen($host, $port, $socketType);
        if ($listenerPort === false) {
            throw new RuntimeException("TCP 端口监听失败 {$host}:{$port}");
        }
        $listenerPort->set($listener->framing() + [
            'open_http_protocol' => false,
            'open_http2_protocol' => false,
            'open_websocket_protocol' => false,
        ]);
        $listener->bind($listenerPort);
    }

    /** @return array{string,int,int} 地址、端口和最大帧长度 */
    private function config(int|string|array $config): array {
        $config = is_array($config) ? $config : ['port' => $config];
        $port = filter_var($config['port'] ?? null, FILTER_VALIDATE_INT, ['options' => [
            'min_range' => 1,
            'max_range' => 65535,
        ]]);
        $host = $config['host'] ?? '127.0.0.1';
        $maxPacketLength = (int)($config['package_max_length'] ?? 2 * 1024 * 1024);
        if ($port === false || !is_string($host) || $host === '' || $maxPacketLength < 64) {
            throw new RuntimeException('TCP 监听地址、端口或最大帧长度配置无效');
        }
        return [$host, $port, $maxPacketLength];
    }
}
