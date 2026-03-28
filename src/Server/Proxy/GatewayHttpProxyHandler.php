<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Scf\Core\Config;
use Scf\Core\Console;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Http\Client;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\WebSocket\Frame;
use Throwable;

class GatewayHttpProxyHandler {

    protected array $wsClients = [];
    protected array $wsPumpCoroutines = [];
    protected array $proxyHttpClientPools = [];

    public function __construct(
        protected GatewayServer $gateway,
        protected AppInstanceManager $instanceManager
    ) {
    }

    public function proxyHttpRequest(Request $request, Response $response, array $upstream): void {
        $this->instanceManager->retainProxyHttpRequest($upstream);
        $client = null;
        $transport = null;
        $reusable = false;
        try {
            $client = $this->acquireProxyHttpClient($upstream, $transport);
            try {
                $this->forwardHttpRequestToUpstream($request, $response, $upstream, $client);
            } catch (Throwable $throwable) {
                if (
                    $client instanceof Client
                    && is_array($transport)
                    && ($transport['mode'] ?? 'tcp') === 'unix'
                    && $this->isTransientUpstreamUnavailable($throwable)
                ) {
                    $this->releaseProxyHttpClient($upstream, $client, false, $transport);
                    $client = $this->createHttpClient($upstream, [
                        'mode' => 'tcp',
                        'host' => (string)($upstream['host'] ?? '127.0.0.1'),
                        'port' => (int)($upstream['port'] ?? 0),
                    ]);
                    $transport = [
                        'mode' => 'tcp',
                        'host' => (string)($upstream['host'] ?? '127.0.0.1'),
                        'port' => (int)($upstream['port'] ?? 0),
                    ];
                    $this->forwardHttpRequestToUpstream($request, $response, $upstream, $client);
                } else {
                    throw $throwable;
                }
            }
            $reusable = $this->isReusableProxyHttpClient($client);
        } finally {
            if ($client instanceof Client) {
                $this->releaseProxyHttpClient($upstream, $client, $reusable, $transport);
            }
            $this->instanceManager->releaseProxyHttpRequest($upstream);
        }
    }

    public function handleWebSocketHandshake(Request $request, Response $response, array $upstream): bool {
        try {
            $client = $this->createHttpClient($upstream);
            $client->setHeaders($this->buildUpstreamWsHeaders($request, $upstream));
            $client->set(['timeout' => $this->proxyWsHandshakeTimeout()]);
            if (!$client->upgrade($this->buildTargetPath($request))) {
                throw new RuntimeException($client->errMsg ?: 'websocket upgrade failed');
            }

            $this->performServerHandshake($request, $response);
            $this->wsClients[$request->fd] = $client;
            $this->startUpstreamPump($request->fd, $client);
            return true;
        } catch (Throwable $e) {
            $this->instanceManager->releaseWebsocketBinding($request->fd);
            $response->status(502);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(json_encode([
                'message' => 'WebSocket 上游握手失败',
                'error' => $e->getMessage(),
            ], JSON_UNESCAPED_UNICODE));
            return false;
        }
    }

    public function handleWebSocketMessage(Frame $frame): void {
        $client = $this->wsClients[$frame->fd] ?? null;
        if (!$client) {
            $this->disconnectClient($frame->fd);
            return;
        }
        $forward = function () use ($client, $frame): void {
            try {
                $ok = $client->push($frame->data, $frame->opcode, $frame->finish ? SWOOLE_WEBSOCKET_FLAG_FIN : 0);
                if ($ok === false) {
                    throw new RuntimeException($client->errMsg ?: 'push failed');
                }
            } catch (Throwable $throwable) {
                Console::warning("【Gateway】WebSocket上游发送失败 fd={$frame->fd}: " . $throwable->getMessage());
                $this->disconnectClient($frame->fd);
            }
        };

        if (Coroutine::getCid() > 0) {
            $forward();
            return;
        }

        Coroutine::create($forward);
    }

    public function handleClientClose(int $fd): void {
        if (isset($this->wsClients[$fd])) {
            try {
                $this->wsClients[$fd]->close();
            } catch (Throwable) {
            }
            unset($this->wsClients[$fd]);
        }
        unset($this->wsPumpCoroutines[$fd]);
        $this->instanceManager->releaseWebsocketBinding($fd);
    }

    public function disconnectClient(int $fd): void {
        $server = $this->gateway->server();
        if (isset($this->wsClients[$fd])) {
            try {
                $this->wsClients[$fd]->close();
            } catch (Throwable) {
            }
            unset($this->wsClients[$fd]);
        }
        unset($this->wsPumpCoroutines[$fd]);
        $this->instanceManager->releaseWebsocketBinding($fd);
        if (!$server->exist($fd)) {
            return;
        }
        if ($server->isEstablished($fd)) {
            $server->disconnect($fd);
        } else {
            $server->close($fd);
        }
    }

    public function closeAllPooledClients(): void {
        foreach ($this->proxyHttpClientPools as $pool) {
            if (!$pool instanceof Channel) {
                continue;
            }
            while (!$pool->isEmpty()) {
                $client = $pool->pop(0.001);
                if ($client instanceof Client) {
                    try {
                        $client->close();
                    } catch (Throwable) {
                    }
                }
            }
        }
        $this->proxyHttpClientPools = [];
    }

    public function isTransientUpstreamUnavailable(?Throwable $throwable): bool {
        if (!$throwable) {
            return false;
        }
        $message = strtolower($throwable->getMessage());
        return str_contains($message, 'connection refused')
            || str_contains($message, 'connect failed')
            || str_contains($message, 'connection reset')
            || str_contains($message, 'no route to host')
            || str_contains($message, 'timed out');
    }

    protected function forwardHttpRequestToUpstream(Request $request, Response $response, array $upstream, Client $client): void {
        $method = strtoupper($request->server['request_method'] ?? 'GET');
        if ($this->shouldProxyByDownloadFile($request, $method)) {
            $this->proxyHttpRequestToUpstreamByDownloadFile($request, $response, $upstream, $client, $method);
            return;
        }

        $client->setHeaders($this->buildUpstreamHttpHeaders($request, $upstream));
        $client->set(['timeout' => $this->proxyHttpTimeout()]);
        $client->setMethod($method);
        $body = $this->requestRawBodyForProxy($request, $method);
        if ($body !== null && $body !== '' && $body !== false) {
            $client->setData($body);
        } else {
            $client->setData('');
        }
        $ok = $client->execute($this->buildTargetPath($request));
        if (!$ok) {
            throw new RuntimeException($client->errMsg ?: 'execute failed');
        }
        $response->status((int)$client->statusCode);
        foreach ($client->headers ?? [] as $key => $value) {
            if ($this->shouldSkipResponseHeader($key)) {
                continue;
            }
            $response->header($key, (string)$value);
        }
        $this->forwardResponseCookies($response, $client);
        $response->end((string)$client->body);
    }

    protected function startUpstreamPump(int $fd, Client $client): void {
        $server = $this->gateway->server();
        $this->wsPumpCoroutines[$fd] = Coroutine::create(function () use ($fd, $client, $server) {
            try {
                while (true) {
                    $frame = $client->recv(0.5);
                    if ($frame === false) {
                        if ($client->errCode === SWOOLE_ETIMEDOUT) {
                            continue;
                        }
                        break;
                    }
                    if (!isset($frame->data)) {
                        break;
                    }
                    if (!$server->exist($fd) || !$server->isEstablished($fd)) {
                        break;
                    }
                    $server->push($fd, $frame->data, $frame->opcode ?? WEBSOCKET_OPCODE_TEXT, $frame->finish ?? true);
                    if (($frame->opcode ?? 0) === WEBSOCKET_OPCODE_CLOSE) {
                        break;
                    }
                }
            } catch (Throwable $throwable) {
                Console::warning("【Gateway】WebSocket上游连接异常 fd={$fd}: " . $throwable->getMessage());
            } finally {
                unset($this->wsPumpCoroutines[$fd]);
                $this->disconnectClient($fd);
            }
        });
    }

    protected function createHttpClient(array $upstream, ?array $transport = null): Client {
        $transport ??= $this->resolveProxyHttpTransport($upstream);
        if (($transport['mode'] ?? 'tcp') === 'unix') {
            return new Client('unix:' . (string)$transport['socket'], 0, false);
        }
        return new Client(
            (string)($transport['host'] ?? ($upstream['host'] ?? '127.0.0.1')),
            (int)($transport['port'] ?? ($upstream['port'] ?? 80)),
            false
        );
    }

    protected function resolveProxyHttpTransport(array $upstream): array {
        $host = (string)($upstream['host'] ?? '127.0.0.1');
        $port = (int)($upstream['port'] ?? 0);
        if ($this->shouldUseLocalProxyHttpSocket($host, $port)) {
            $socketPath = LocalIpc::upstreamHttpSocketPath($port);
            return [
                'mode' => 'unix',
                'socket' => $socketPath,
                'host' => $host,
                'port' => $port,
            ];
        }
        return [
            'mode' => 'tcp',
            'host' => $host,
            'port' => $port,
        ];
    }

    protected function shouldUseLocalProxyHttpSocket(string $host, int $port): bool {
        if (!(bool)(Config::server()['gateway_proxy_http_use_unix_socket'] ?? true)) {
            return false;
        }
        if ($port <= 0) {
            return false;
        }
        return in_array($host, ['127.0.0.1', 'localhost'], true) && file_exists(LocalIpc::upstreamHttpSocketPath($port));
    }

    protected function requestRawBodyForProxy(Request $request, string $method): ?string {
        if (!in_array($method, ['POST', 'PUT', 'PATCH', 'DELETE'], true)) {
            return null;
        }
        $body = $request->rawContent();
        return ($body === false || $body === '') ? null : $body;
    }

    protected function shouldProxyByDownloadFile(Request $request, string $method): bool {
        if (!(bool)(Config::server()['gateway_proxy_download_offload'] ?? true)) {
            return false;
        }
        if (!in_array($method, ['GET', 'HEAD'], true)) {
            return false;
        }

        $headers = array_change_key_case((array)($request->header ?? []), CASE_LOWER);
        if (isset($headers['range']) && trim((string)$headers['range']) !== '') {
            return true;
        }

        $uri = strtolower((string)($request->server['request_uri'] ?? ''));
        foreach ((array)(Config::server()['gateway_proxy_download_offload_paths'] ?? []) as $pathPrefix) {
            $pathPrefix = strtolower(trim((string)$pathPrefix));
            if ($pathPrefix !== '' && str_starts_with($uri, $pathPrefix)) {
                return true;
            }
        }

        $accept = strtolower((string)($headers['accept'] ?? ''));
        foreach ((array)(Config::server()['gateway_proxy_download_offload_accepts'] ?? [
            'application/octet-stream',
            'application/zip',
            'application/pdf',
            'video/',
            'audio/',
            'image/',
        ]) as $needle) {
            $needle = strtolower(trim((string)$needle));
            if ($needle !== '' && str_contains($accept, $needle)) {
                return true;
            }
        }

        $path = strtolower((string)parse_url($uri, PHP_URL_PATH));
        $extension = pathinfo($path, PATHINFO_EXTENSION);
        if ($extension === '') {
            return false;
        }
        $extensions = array_map(static fn($item) => strtolower(trim((string)$item)), (array)(Config::server()['gateway_proxy_download_offload_extensions'] ?? [
            'zip', 'rar', '7z', 'pdf', 'csv', 'xls', 'xlsx',
            'mp4', 'mov', 'm3u8', 'mp3', 'wav',
            'jpg', 'jpeg', 'png', 'gif', 'webp',
        ]));
        return in_array($extension, $extensions, true);
    }

    protected function proxyHttpRequestToUpstreamByDownloadFile(Request $request, Response $response, array $upstream, Client $client, string $method): void {
        $headers = $this->buildUpstreamHttpHeaders($request, $upstream);
        $client->setHeaders($headers);
        $client->set(['timeout' => $this->proxyHttpTimeout()]);
        if ($method !== 'GET') {
            $client->setMethod($method);
        }

        $tempFile = tempnam(sys_get_temp_dir(), 'scf_gateway_dl_');
        if ($tempFile === false) {
            throw new RuntimeException('create download temp file failed');
        }

        try {
            $ok = $client->download($this->buildTargetPath($request), $tempFile);
            if (!$ok) {
                throw new RuntimeException($client->errMsg ?: 'download failed');
            }

            $response->status((int)$client->statusCode);
            foreach ($client->headers ?? [] as $key => $value) {
                if ($this->shouldSkipResponseHeader($key)) {
                    continue;
                }
                $response->header($key, (string)$value);
            }
            $this->forwardResponseCookies($response, $client);

            if ($method === 'HEAD') {
                $response->end();
                return;
            }

            if (!is_file($tempFile)) {
                throw new RuntimeException('download temp file missing');
            }

            $response->sendfile($tempFile);
        } finally {
            is_file($tempFile) && @unlink($tempFile);
        }
    }

    protected function proxyHttpClientPoolSize(): int {
        return max(0, (int)(Config::server()['gateway_proxy_http_pool_size'] ?? 8));
    }

    protected function proxyHttpClientPoolKey(array $upstream, ?array $transport = null): string {
        $transport ??= $this->resolveProxyHttpTransport($upstream);
        $mode = (string)($transport['mode'] ?? 'tcp');
        if ($mode === 'unix') {
            return 'unix:' . (string)($transport['socket'] ?? '');
        }
        return 'tcp:' . (string)($transport['host'] ?? ($upstream['host'] ?? '127.0.0.1')) . ':' . (int)($transport['port'] ?? ($upstream['port'] ?? 0));
    }

    protected function ensureProxyHttpClientPool(array $upstream, ?array $transport = null): Channel {
        $key = $this->proxyHttpClientPoolKey($upstream, $transport);
        if (!isset($this->proxyHttpClientPools[$key])) {
            $this->proxyHttpClientPools[$key] = new Channel(max(1, $this->proxyHttpClientPoolSize()));
        }
        return $this->proxyHttpClientPools[$key];
    }

    protected function acquireProxyHttpClient(array $upstream, ?array &$transport = null): Client {
        $transport ??= $this->resolveProxyHttpTransport($upstream);
        $pool = $this->ensureProxyHttpClientPool($upstream, $transport);
        $client = $pool->pop(0.001);
        if ($client instanceof Client) {
            return $client;
        }
        return $this->createHttpClient($upstream, $transport);
    }

    protected function releaseProxyHttpClient(array $upstream, Client $client, bool $reusable, ?array $transport = null): void {
        if (!$reusable || $this->proxyHttpClientPoolSize() <= 0) {
            try {
                $client->close();
            } catch (Throwable) {
            }
            return;
        }
        $pool = $this->ensureProxyHttpClientPool($upstream, $transport);
        if (!$pool->push($client, 0.001)) {
            try {
                $client->close();
            } catch (Throwable) {
            }
        }
    }

    protected function isReusableProxyHttpClient(Client $client): bool {
        $headers = array_change_key_case((array)($client->headers ?? []), CASE_LOWER);
        $connection = strtolower(trim((string)($headers['connection'] ?? '')));
        if ($connection === 'close') {
            return false;
        }
        return (int)($client->errCode ?? 0) === 0 && (int)($client->statusCode ?? 0) > 0;
    }

    protected function proxyHttpTimeout(): float|int {
        $timeout = Config::server()['gateway_proxy_http_timeout'] ?? Config::server()['proxy_http_timeout'] ?? -1;
        $timeout = (float)$timeout;
        return $timeout > 0 ? $timeout : -1;
    }

    protected function proxyWsHandshakeTimeout(): float|int {
        $timeout = Config::server()['gateway_proxy_ws_handshake_timeout'] ?? Config::server()['proxy_ws_handshake_timeout'] ?? 60;
        return max(1, (float)$timeout);
    }

    protected function buildTargetPath(Request $request): string {
        $uri = $request->server['request_uri'] ?? '/';
        $query = $request->server['query_string'] ?? '';
        if ($query === '') {
            return $uri;
        }
        return $uri . '?' . $query;
    }

    protected function buildUpstreamHttpHeaders(Request $request, array $upstream): array {
        $headers = [];
        foreach ((array)($request->header ?? []) as $key => $value) {
            $normalizedKey = (string)$key;
            if ($this->shouldSkipRequestHeader($normalizedKey)) {
                continue;
            }
            $headers[$normalizedKey] = (string)$value;
        }

        $context = $this->buildForwardedRequestContext($request);
        $headers['host'] = (string)$context['original_host'];
        $headers['x-real-ip'] = (string)$context['client_ip'];
        $headers['x-forwarded-for'] = (string)$context['forwarded_for'];
        $headers['x-forwarded-proto'] = (string)$context['forwarded_proto'];
        $headers['x-forwarded-host'] = (string)$context['original_host'];
        $headers['x-forwarded-port'] = (string)$context['original_port'];
        $headers['x-gateway-proxy'] = '1';
        return $headers;
    }

    protected function buildUpstreamWsHeaders(Request $request, array $upstream): array {
        $headers = [];
        foreach ((array)($request->header ?? []) as $key => $value) {
            $normalizedKey = (string)$key;
            if ($this->shouldSkipRequestHeader($normalizedKey)) {
                continue;
            }
            $headers[$normalizedKey] = (string)$value;
        }

        $context = $this->buildForwardedRequestContext($request);
        $headers['host'] = (string)$context['original_host'];
        $headers['x-real-ip'] = (string)$context['client_ip'];
        $headers['x-forwarded-for'] = (string)$context['forwarded_for'];
        $headers['x-forwarded-proto'] = (string)$context['ws_forwarded_proto'];
        $headers['x-forwarded-host'] = (string)$context['original_host'];
        $headers['x-forwarded-port'] = (string)$context['original_port'];
        $headers['x-gateway-proxy'] = '1';
        return $headers;
    }

    protected function buildForwardedRequestContext(Request $request): array {
        $headers = array_change_key_case((array)($request->header ?? []), CASE_LOWER);
        $server = (array)($request->server ?? []);

        $remoteAddr = (string)($server['remote_addr'] ?? '127.0.0.1');
        $forwardedFor = trim((string)($headers['x-forwarded-for'] ?? ''));
        if ($forwardedFor === '') {
            $forwardedFor = $remoteAddr;
        } else {
            $forwardedFor .= ', ' . $remoteAddr;
        }

        $originalHost = trim((string)($headers['host'] ?? 'localhost'));
        $originalPort = 0;
        if ($originalHost !== '' && str_contains($originalHost, ':')) {
            $parts = explode(':', $originalHost);
            $candidate = (int)array_pop($parts);
            if ($candidate > 0) {
                $originalPort = $candidate;
            }
        }
        if ($originalPort <= 0) {
            $originalPort = (int)($server['server_port'] ?? 80);
        }

        $scheme = 'http';
        if (!empty($server['https']) && strtolower((string)$server['https']) !== 'off') {
            $scheme = 'https';
        }
        $wsScheme = $scheme === 'https' ? 'wss' : 'ws';

        return [
            'client_ip' => $remoteAddr,
            'forwarded_for' => $forwardedFor,
            'forwarded_proto' => $scheme,
            'ws_forwarded_proto' => $wsScheme,
            'original_host' => $originalHost,
            'original_port' => $originalPort,
        ];
    }

    protected function shouldSkipRequestHeader(string $key): bool {
        return in_array(strtolower($key), [
            'connection',
            'keep-alive',
            'proxy-authenticate',
            'proxy-authorization',
            'te',
            'trailers',
            'transfer-encoding',
            'upgrade',
            'x-forwarded-for',
            'x-forwarded-proto',
            'x-forwarded-host',
            'x-forwarded-port',
            'x-real-ip',
            'content-length',
        ], true);
    }

    protected function shouldSkipResponseHeader(string $key): bool {
        return in_array(strtolower($key), [
            'content-length',
            'transfer-encoding',
            'connection',
            'keep-alive',
            'upgrade',
            'sec-websocket-accept',
            'sec-websocket-protocol',
            'sec-websocket-version',
            'set-cookie',
        ], true);
    }

    protected function forwardResponseCookies(Response $response, Client $client): void {
        $setCookie = $client->set_cookie_headers ?? null;
        if (!is_array($setCookie)) {
            return;
        }
        foreach ($setCookie as $cookieLine) {
            $parts = array_map('trim', explode(';', (string)$cookieLine));
            if (!$parts || !str_contains((string)$parts[0], '=')) {
                continue;
            }
            [$name, $value] = explode('=', (string)$parts[0], 2);
            $options = [
                'expires' => 0,
                'path' => '/',
                'domain' => '',
                'secure' => false,
                'httponly' => false,
                'samesite' => '',
            ];
            foreach (array_slice($parts, 1) as $part) {
                if ($part === '') {
                    continue;
                }
                if (!str_contains($part, '=')) {
                    $flag = strtolower($part);
                    if ($flag === 'secure') {
                        $options['secure'] = true;
                    } elseif ($flag === 'httponly') {
                        $options['httponly'] = true;
                    }
                    continue;
                }
                [$attrName, $attrValue] = explode('=', $part, 2);
                $attrName = strtolower(trim($attrName));
                $attrValue = trim($attrValue);
                match ($attrName) {
                    'expires' => $options['expires'] = strtotime($attrValue) ?: 0,
                    'path' => $options['path'] = $attrValue,
                    'domain' => $options['domain'] = $attrValue,
                    'samesite' => $options['samesite'] = $attrValue,
                    default => null,
                };
            }
            $response->cookie(
                trim($name),
                $value,
                (int)$options['expires'],
                (string)$options['path'],
                (string)$options['domain'],
                (bool)$options['secure'],
                (bool)$options['httponly'],
                (string)$options['samesite']
            );
        }
    }

    protected function performServerHandshake(Request $request, Response $response): void {
        $secWebSocketKey = $request->header['sec-websocket-key'] ?? '';
        $pattern = '#^[+/0-9A-Za-z]{21}[AQgw]==$#';
        if ($secWebSocketKey === '' || preg_match($pattern, $secWebSocketKey) !== 1 || 16 !== strlen(base64_decode($secWebSocketKey))) {
            throw new RuntimeException('非法的 websocket key');
        }
        $accept = base64_encode(sha1($secWebSocketKey . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', true));
        $response->header('Upgrade', 'websocket');
        $response->header('Connection', 'Upgrade');
        $response->header('Sec-WebSocket-Accept', $accept);
        $response->header('Sec-WebSocket-Version', '13');
        if (isset($request->header['sec-websocket-protocol'])) {
            $response->header('Sec-WebSocket-Protocol', $request->header['sec-websocket-protocol']);
        }
        $response->status(101);
        $response->end();
    }
}
