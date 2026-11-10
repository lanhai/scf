<?php

namespace Scf\Client;

use RuntimeException;
use Swoole\Coroutine\Http\Client;

/** 打开节点控制通道，直接交付 Swoole 客户端及其原生帧。 */
class WebSocket {
    /**
     * @param string $url 带内部认证参数的 ws 或 wss URL。
     * @return Client 已完成协议升级的连接。
     * @throws RuntimeException URL 无效或握手失败。
     */
    public static function connect(string $url): Client {
        $parts = parse_url($url);
        $scheme = strtolower($parts['scheme'] ?? '');
        $host = trim($parts['host'] ?? '', '[]');
        if ($host === '' || !in_array($scheme, ['ws', 'wss'], true)) {
            throw new RuntimeException('Invalid node WebSocket endpoint');
        }

        $tls = $scheme === 'wss';
        $client = new Client($host, $parts['port'] ?? ($tls ? 443 : 80), $tls);
        $path = $parts['path'] ?? '/';
        if (isset($parts['query'])) {
            $path .= '?' . $parts['query'];
        }
        // Swoole 5.x can return true after receiving an HTTP rejection.
        if (!$client->upgrade($path) || !$client->connected || $client->statusCode !== 101) {
            $status = $client->statusCode;
            $error = $status >= 400 ? $status : ($client->errCode ?: $status);
            $client->close();
            throw new RuntimeException("Node WebSocket upgrade failed (code: {$error})", $error);
        }

        return $client;
    }
}
