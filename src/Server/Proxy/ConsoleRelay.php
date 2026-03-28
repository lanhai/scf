<?php

namespace Scf\Server\Proxy;

use Scf\Command\Manager;
use Scf\Core\Table\Runtime;
use Swoole\Coroutine;
use Swoole\Coroutine\Http\Client;

/**
 * Gateway 与 dashboard 之间的控制台日志中继。
 *
 * 本地打印始终保留，websocket 订阅只决定是否额外推给 dashboard 前端。
 */
class ConsoleRelay {
    public const RUNTIME_LOCAL_SUBSCRIBED = 'GATEWAY_CONSOLE_LOCAL_SUBSCRIBED';
    public const RUNTIME_REMOTE_SUBSCRIBED = 'GATEWAY_CONSOLE_REMOTE_SUBSCRIBED';
    public const RUNTIME_GATEWAY_PORT = 'GATEWAY_PROXY_PORT';

    protected const INTERNAL_CONSOLE_LOG_PATH = '/_gateway/internal/console/log';
    protected const INTERNAL_CONSOLE_SUBSCRIPTION_PATH = '/_gateway/internal/console/subscription';

    /**
     * 记录当前 gateway 监听端口，供日志和订阅请求复用。
     *
     * @param int $port gateway 监听端口。
     * @return void
     */
    public static function setGatewayPort(int $port): void {
        Runtime::instance()->set(self::RUNTIME_GATEWAY_PORT, $port);
    }

    /**
     * 获取当前 gateway 监听端口。
     *
     * @return int 当前 gateway 端口，未设置时返回 0。
     */
    public static function gatewayPort(): int {
        $port = (int)(Manager::instance()->getOpt('gateway_port') ?: 0);
        if ($port > 0) {
            return $port;
        }
        return (int)(Runtime::instance()->get(self::RUNTIME_GATEWAY_PORT) ?: 0);
    }

    /**
     * 标记 gateway 本地是否有 dashboard 订阅者。
     *
     * @param bool $enabled 是否存在本地订阅者。
     * @return void
     */
    public static function setLocalSubscribed(bool $enabled): void {
        Runtime::instance()->set(self::RUNTIME_LOCAL_SUBSCRIBED, $enabled ? 1 : 0);
    }

    /**
     * gateway 本地订阅状态。
     *
     * @return bool 当前 gateway 是否存在本地订阅者。
     */
    public static function localSubscribed(): bool {
        return (int)(Runtime::instance()->get(self::RUNTIME_LOCAL_SUBSCRIBED) ?: 0) === 1;
    }

    /**
     * 标记上游/远端是否有订阅者。
     *
     * @param bool $enabled 是否存在远端订阅者。
     * @return void
     */
    public static function setRemoteSubscribed(bool $enabled): void {
        Runtime::instance()->set(self::RUNTIME_REMOTE_SUBSCRIBED, $enabled ? 1 : 0);
    }

    /**
     * 远端订阅状态。
     *
     * @return bool 当前是否存在远端订阅者。
     */
    public static function remoteSubscribed(): bool {
        return (int)(Runtime::instance()->get(self::RUNTIME_REMOTE_SUBSCRIBED) ?: 0) === 1;
    }

    /**
     * 判断当前进程是否应该把日志继续往外推。
     *
     * upstream 在 draining 阶段允许继续推，方便把尾流日志完整带回 gateway。
     *
     * @return bool 满足当前角色推送条件时返回 true。
     */
    public static function shouldPushForCurrentProcess(): bool {
        if (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true) {
            return self::remoteSubscribed() || Runtime::instance()->serverIsDraining();
        }
        if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true) {
            return SERVER_ROLE === NODE_ROLE_MASTER ? self::localSubscribed() : self::remoteSubscribed();
        }
        return true;
    }

    /**
     * 将一条日志回传到本机 gateway，再由 gateway 统一转发到控制台 / dashboard。
     *
     * @param string $time 日志时间戳。
     * @param string $message 日志内容。
     * @param string $sourceType 日志来源类型。
     * @param string $node 节点标识，空值时默认使用当前主机名。
     * @return bool 成功被 gateway 接收并接受时返回 true。
     */
    public static function reportToLocalGateway(string $time, string $message, string $sourceType = 'gateway', string $node = ''): bool {
        $port = self::gatewayPort();
        if ($port <= 0 || !self::shouldPushForCurrentProcess()) {
            return false;
        }

        // 本地终端打印不依赖这个开关；它只控制是否额外推送给 dashboard。
        $payload = [
            'time' => $time,
            'message' => $message,
            'source_type' => $sourceType,
            'node' => $node ?: SERVER_HOST,
            'old_instance' => (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true && Runtime::instance()->serverIsDraining()),
            'instance_label' => (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true)
                ? ('U' . ((int)(Runtime::instance()->httpPort() ?: 0)))
                : '',
        ];

        try {
            $ipcResponse = LocalIpc::request(
                LocalIpc::gatewaySocketPath($port),
                'gateway.console.log',
                $payload,
                1.0
            );
            if (($ipcResponse['ok'] ?? false) === true && (int)($ipcResponse['status'] ?? 0) === 200) {
                return (bool)($ipcResponse['data']['accepted'] ?? false);
            }
            if (Coroutine::getCid() > 0) {
                return self::postByCoroutine($port, self::INTERNAL_CONSOLE_LOG_PATH, $payload);
            }

            return self::postByStream($port, self::INTERNAL_CONSOLE_LOG_PATH, $payload);
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * 从 gateway 拉取最新订阅状态。
     *
     * @return bool 拉取成功且当前应转发日志时返回 true。
     */
    public static function refreshSubscriptionFromGateway(): bool {
        $port = self::gatewayPort();
        if ($port <= 0) {
            self::setRemoteSubscribed(false);
            return false;
        }

        try {
            $ipcResponse = LocalIpc::request(
                LocalIpc::gatewaySocketPath($port),
                'gateway.console.subscription',
                [],
                1.0
            );
            if (($ipcResponse['ok'] ?? false) === true && (int)($ipcResponse['status'] ?? 0) === 200) {
                $enabled = (bool)($ipcResponse['data']['enabled'] ?? false);
            } else {
                $enabled = Coroutine::getCid() > 0
                    ? self::fetchByCoroutine($port, self::INTERNAL_CONSOLE_SUBSCRIPTION_PATH)
                    : self::fetchByStream($port, self::INTERNAL_CONSOLE_SUBSCRIPTION_PATH);
            }
        } catch (\Throwable) {
            $enabled = false;
        }
        self::setRemoteSubscribed($enabled);
        return $enabled;
    }

    /**
     * coroutine 场景下直接通过 HTTP client 访问 gateway。
     *
     * @param int $port gateway 端口。
     * @param string $path 访问路径。
     * @param array $payload POST 请求体。
     * @return bool 请求返回 200 时返回 true。
     */
    protected static function postByCoroutine(int $port, string $path, array $payload): bool {
        $client = new Client('127.0.0.1', $port);
        $client->setHeaders([
            'Host' => '127.0.0.1:' . $port,
            'Content-Type' => 'application/json',
        ]);
        $client->set(['timeout' => 1.0]);
        $ok = $client->post($path, json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        $statusCode = (int)$client->statusCode;
        $client->close();
        return $ok && $statusCode === 200;
    }

    /**
     * 非 coroutine 场景下的 HTTP 退路实现。
     *
     * @param int $port gateway 端口。
     * @param string $path 访问路径。
     * @param array $payload POST 请求体。
     * @return bool 请求返回 200 时返回 true。
     */
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
            . "Connection: close\r\n"
            . "Content-Length: " . strlen($json) . "\r\n\r\n"
            . $json;
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);
        return is_string($response) && str_contains($response, ' 200 ');
    }

    /**
     * coroutine 场景下拉取订阅开关。
     *
     * @param int $port gateway 端口。
     * @param string $path 访问路径。
     * @return bool 订阅开关开启时返回 true。
     */
    protected static function fetchByCoroutine(int $port, string $path): bool {
        $client = new Client('127.0.0.1', $port);
        $client->setHeaders([
            'Host' => '127.0.0.1:' . $port,
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

    /**
     * 非 coroutine 场景下拉取订阅开关。
     *
     * @param int $port gateway 端口。
     * @param string $path 访问路径。
     * @return bool 订阅开关开启时返回 true。
     */
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
