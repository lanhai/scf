<?php

namespace Scf\Server\Proxy;

use Scf\Command\Manager;
use Scf\Core\Table\Runtime;
use Swoole\Coroutine;
use Swoole\Coroutine\Http\Client;

class ConsoleRelay {
    public const RUNTIME_LOCAL_SUBSCRIBED = 'GATEWAY_CONSOLE_LOCAL_SUBSCRIBED';
    public const RUNTIME_REMOTE_SUBSCRIBED = 'GATEWAY_CONSOLE_REMOTE_SUBSCRIBED';
    public const RUNTIME_GATEWAY_PORT = 'GATEWAY_PROXY_PORT';

    protected const INTERNAL_CONSOLE_LOG_PATH = '/_gateway/internal/console/log';
    protected const INTERNAL_CONSOLE_SUBSCRIPTION_PATH = '/_gateway/internal/console/subscription';

    public static function setGatewayPort(int $port): void {
        Runtime::instance()->set(self::RUNTIME_GATEWAY_PORT, $port);
    }

    public static function gatewayPort(): int {
        $port = (int)(Manager::instance()->getOpt('gateway_port') ?: 0);
        if ($port > 0) {
            return $port;
        }
        return (int)(Runtime::instance()->get(self::RUNTIME_GATEWAY_PORT) ?: 0);
    }

    public static function setLocalSubscribed(bool $enabled): void {
        Runtime::instance()->set(self::RUNTIME_LOCAL_SUBSCRIBED, $enabled ? 1 : 0);
    }

    public static function localSubscribed(): bool {
        return (int)(Runtime::instance()->get(self::RUNTIME_LOCAL_SUBSCRIBED) ?: 0) === 1;
    }

    public static function setRemoteSubscribed(bool $enabled): void {
        Runtime::instance()->set(self::RUNTIME_REMOTE_SUBSCRIBED, $enabled ? 1 : 0);
    }

    public static function remoteSubscribed(): bool {
        return (int)(Runtime::instance()->get(self::RUNTIME_REMOTE_SUBSCRIBED) ?: 0) === 1;
    }

    public static function shouldPushForCurrentProcess(): bool {
        if (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true) {
            return self::remoteSubscribed();
        }
        if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true) {
            return SERVER_ROLE === NODE_ROLE_MASTER ? self::localSubscribed() : self::remoteSubscribed();
        }
        return true;
    }

    public static function reportToLocalGateway(string $time, string $message, string $sourceType = 'gateway', string $node = ''): bool {
        $port = self::gatewayPort();
        if ($port <= 0 || !self::shouldPushForCurrentProcess()) {
            return false;
        }

        $payload = [
            'time' => $time,
            'message' => $message,
            'source_type' => $sourceType,
            'node' => $node ?: SERVER_HOST,
        ];

        try {
            if (Coroutine::getCid() > 0) {
                return self::postByCoroutine($port, self::INTERNAL_CONSOLE_LOG_PATH, $payload);
            }

            return self::postByStream($port, self::INTERNAL_CONSOLE_LOG_PATH, $payload);
        } catch (\Throwable) {
            return false;
        }
    }

    public static function refreshSubscriptionFromGateway(): bool {
        $port = self::gatewayPort();
        if ($port <= 0) {
            self::setRemoteSubscribed(false);
            return false;
        }

        try {
            $enabled = Coroutine::getCid() > 0
                ? self::fetchByCoroutine($port, self::INTERNAL_CONSOLE_SUBSCRIPTION_PATH)
                : self::fetchByStream($port, self::INTERNAL_CONSOLE_SUBSCRIPTION_PATH);
        } catch (\Throwable) {
            $enabled = false;
        }
        self::setRemoteSubscribed($enabled);
        return $enabled;
    }

    protected static function postByCoroutine(int $port, string $path, array $payload): bool {
        $client = new Client('127.0.0.1', $port);
        $client->setHeaders([
            'Host' => '127.0.0.1:' . $port,
            'Content-Type' => 'application/json',
            'X-Gateway-Internal' => '1',
        ]);
        $client->set(['timeout' => 1.0]);
        $ok = $client->post($path, json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        $statusCode = (int)$client->statusCode;
        $client->close();
        return $ok && $statusCode === 200;
    }

    protected static function postByStream(int $port, string $path, array $payload): bool {
        $json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            1.0,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return false;
        }

        stream_set_timeout($socket, 1);
        $request = "POST {$path} HTTP/1.1\r\n"
            . "Host: 127.0.0.1:{$port}\r\n"
            . "Content-Type: application/json\r\n"
            . "X-Gateway-Internal: 1\r\n"
            . "Connection: close\r\n"
            . "Content-Length: " . strlen($json) . "\r\n\r\n"
            . $json;
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);
        return is_string($response) && str_contains($response, ' 200 ');
    }

    protected static function fetchByCoroutine(int $port, string $path): bool {
        $client = new Client('127.0.0.1', $port);
        $client->setHeaders([
            'Host' => '127.0.0.1:' . $port,
            'X-Gateway-Internal' => '1',
        ]);
        $client->set(['timeout' => 1.0]);
        $ok = $client->get($path);
        $body = (string)$client->body;
        $statusCode = (int)$client->statusCode;
        $client->close();
        if (!$ok || $statusCode !== 200) {
            return false;
        }
        $decoded = json_decode($body, true);
        return (bool)($decoded['enabled'] ?? false);
    }

    protected static function fetchByStream(int $port, string $path): bool {
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            1.0,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return false;
        }

        stream_set_timeout($socket, 1);
        $request = "GET {$path} HTTP/1.1\r\n"
            . "Host: 127.0.0.1:{$port}\r\n"
            . "X-Gateway-Internal: 1\r\n"
            . "Connection: close\r\n\r\n";
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);
        if (!is_string($response) || !str_contains($response, "\r\n\r\n")) {
            return false;
        }
        [, $body] = explode("\r\n\r\n", $response, 2);
        $decoded = json_decode($body, true);
        return (bool)($decoded['enabled'] ?? false);
    }
}
