<?php

namespace Scf\Server\Gateway;

use Scf\Core\Result;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Throwable;

/**
 * Gateway 内部管理接口与本机控制命令适配层。
 *
 * 这个 trait 专门承接 `/_gateway/internal/*` 与 `/_gateway/upstreams*`
 * 这组仅服务于 gateway 控制面的内部 HTTP 接口，并负责把统一命令执行语义
 * 适配成内部 HTTP 返回体。它不引入新的运行逻辑，只把原本堆在 GatewayServer
 * 里的管理接口实现按职责单独收口，降低主类的横向噪音。
 */
trait GatewayInternalManagementTrait {

    /**
     * 判断 gateway 内部控制请求是否来自本机回环地址。
     *
     * `/ _gateway/internal/*` 是只服务于本机进程协作的控制平面入口，
     * 不能再把可伪造请求头当成放行依据。远程节点协作应继续走
     * dashboard token、node client 与本地 IPC 链路。
     *
     * @param Request $request 当前 HTTP 请求。
     * @return bool 仅当来源 IP 为 loopback 时返回 true。
     */
    protected function isInternalGatewayRequest(Request $request): bool {
        $clientIp = trim((string)($request->server['remote_addr'] ?? ''));
        return in_array($clientIp, ['127.0.0.1', '::1'], true);
    }

    /**
     * 处理 gateway 暴露给本机协作链路的内部管理请求。
     *
     * 这里覆盖 console 订阅、console log、内部命令、upstream registry
     * 操作以及健康状态读取等 HTTP 入口。真正命令执行仍回到 GatewayServer 的
     * 统一命令语义层，这里只负责鉴权、分发和返回体适配。
     *
     * @param Request $request
     * @param Response $response
     * @return void
     */
    protected function handleManagementRequest(Request $request, Response $response): void {
        $path = $request->server['request_uri'] ?? '/';
        $method = strtoupper($request->server['request_method'] ?? 'GET');
        $payload = $this->decodeJsonBody($request);

        try {
            if ($method === 'GET' && $path === '/_gateway/internal/console/subscription') {
                if (!$this->isInternalGatewayRequest($request)) {
                    $this->json($response, 403, ['message' => 'forbidden']);
                    return;
                }
                $this->json($response, 200, $this->localConsoleSubscriptionPayload());
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/internal/console/log') {
                if (!$this->isInternalGatewayRequest($request)) {
                    $this->json($response, 403, ['message' => 'forbidden']);
                    return;
                }
                $this->json($response, 200, [
                    'accepted' => $this->acceptConsolePayload([
                        'time' => (string)($payload['time'] ?? ''),
                        'message' => (string)($payload['message'] ?? ''),
                        'source_type' => (string)($payload['source_type'] ?? 'gateway'),
                        'node' => (string)($payload['node'] ?? SERVER_HOST),
                    ]),
                ]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/internal/command') {
                if (!$this->isInternalGatewayRequest($request)) {
                    $this->json($response, 403, ['message' => 'forbidden']);
                    return;
                }
                $result = $this->dispatchInternalGatewayCommand(
                    (string)($payload['command'] ?? ''),
                    (array)($payload['params'] ?? [])
                );
                $afterWrite = isset($result['__after_write']) && is_callable($result['__after_write'])
                    ? $result['__after_write']
                    : null;
                unset($result['__after_write']);
                $payload = $result['data'] ?? ['message' => (string)($result['message'] ?? 'request failed')];
                if (!is_array($payload)) {
                    $payload = [
                        'accepted' => false,
                        'message' => (string)($result['message'] ?? 'request failed'),
                        'result' => $payload,
                    ];
                }
                $this->json(
                    $response,
                    (int)($result['status'] ?? 500),
                    $payload
                );
                $afterWrite && $afterWrite();
                return;
            }

            if ($method === 'GET' && $path === '/_gateway/healthz') {
                $this->json($response, 200, [
                    'message' => 'ok',
                    'active_version' => $this->currentGatewayStateSnapshot()['active_version'] ?? null,
                ]);
                return;
            }

            if ($method === 'GET' && $path === '/_gateway/upstreams') {
                $this->json($response, 200, $this->currentGatewayStateSnapshot());
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/upstreams/register') {
                $instance = $this->instanceManager->registerUpstream(
                    (string)($payload['version'] ?? ''),
                    (string)($payload['host'] ?? '127.0.0.1'),
                    (int)($payload['port'] ?? 0),
                    (int)($payload['weight'] ?? 100),
                    (array)($payload['metadata'] ?? [])
                );
                $this->json($response, 200, [
                    'message' => 'registered',
                    'instance' => $instance,
                    'state' => $this->currentGatewayStateSnapshot(),
                ]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/versions/activate') {
                $state = $this->instanceManager->activateVersion(
                    (string)($payload['version'] ?? ''),
                    (int)($payload['grace_seconds'] ?? 30)
                );
                $this->notifyManagedUpstreamGenerationIterated((string)($payload['version'] ?? ''));
                $this->syncNginxProxyTargets('api_activate');
                $this->json($response, 200, [
                    'message' => 'activated',
                    'state' => $state,
                ]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/versions/drain') {
                $state = $this->instanceManager->drainVersion(
                    (string)($payload['version'] ?? ''),
                    (int)($payload['grace_seconds'] ?? 30)
                );
                $this->syncNginxProxyTargets('api_drain');
                $this->json($response, 200, [
                    'message' => 'draining',
                    'state' => $state,
                ]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/versions/remove') {
                $state = $this->instanceManager->removeVersion((string)($payload['version'] ?? ''));
                $this->syncNginxProxyTargets('api_remove');
                $this->json($response, 200, [
                    'message' => 'removed',
                    'state' => $state,
                ]);
                return;
            }

            $this->json($response, 404, ['message' => 'not found']);
        } catch (Throwable $e) {
            $requestUri = (string)($request->server['request_uri'] ?? '');
            if ($requestUri === '/_gateway/internal/command') {
                $command = trim((string)($payload['command'] ?? ''));
                $commandLabel = $command === '' ? 'unknown' : $command;
                \Scf\Core\Console::error(
                    "【GatewayInternal】内部命令执行异常: command={$commandLabel}, error={$e->getMessage()}, at={$e->getFile()}:{$e->getLine()}"
                );
            }
            $this->json($response, 400, [
                'message' => 'request failed',
                'error' => $e->getMessage(),
            ]);
        }
    }

    /**
     * 汇总本地 console 订阅状态。
     *
     * @return array{enabled: bool}
     */
    protected function localConsoleSubscriptionPayload(): array {
        return [
            'enabled' => $this->dashboardEnabled() ? $this->hasConsoleSubscribers() : ConsoleRelay::remoteSubscribed(),
        ];
    }

    /**
     * 将内部 HTTP 命令映射到 gateway 控制命令执行语义。
     *
     * @param string $command
     * @param array<string, mixed> $params
     * @return array<string, mixed>
     */
    protected function dispatchInternalGatewayCommand(string $command, array $params = []): array {
        return $this->formatGatewayInternalCommandExecution(
            $this->resolveGatewayControlCommandExecution($command, $params)
        );
    }

    /**
     * 将统一命令执行结果适配为内部 HTTP 命令返回体。
     *
     * @param array{
     *     result: Result,
     *     internal_error_status?: int,
     *     internal_success_status?: int,
     *     internal_message?: string,
     *     after_write?: callable|null
     * } $execution 命令执行结果
     * @return array<string, mixed>
     */
    protected function formatGatewayInternalCommandExecution(array $execution): array {
        $result = $execution['result'];
        if ($result->hasError()) {
            return [
                'ok' => false,
                'status' => (int)($execution['internal_error_status'] ?? 409),
                'message' => (string)$result->getMessage(),
                'data' => $result->getData(),
            ];
        }
        $message = $execution['internal_message'] ?? $result->getData();
        if (!is_string($message) || $message === '') {
            $message = (string)$result->getMessage();
        }
        $response = [
            'ok' => true,
            'status' => (int)($execution['internal_success_status'] ?? 200),
            'data' => [
                'accepted' => true,
                'message' => $message,
                'result' => $result->getData(),
            ],
        ];
        $afterWrite = $execution['after_write'] ?? null;
        if (is_callable($afterWrite)) {
            $response['__after_write'] = $afterWrite;
        }
        return $response;
    }

    /**
     * 从 HTTP 请求体中解析 JSON payload。
     *
     * @param Request $request
     * @return array<string, mixed>
     */
    protected function decodeJsonBody(Request $request): array {
        $raw = $request->rawContent();
        if ($raw === '' || $raw === false) {
            return [];
        }
        $decoded = json_decode($raw, true);
        return is_array($decoded) ? $decoded : [];
    }

    /**
     * 输出统一 JSON 响应。
     *
     * @param Response $response
     * @param int $status
     * @param array<string, mixed> $payload
     * @return void
     */
    protected function json(Response $response, int $status, array $payload): void {
        $response->status($status);
        $response->header('Content-Type', 'application/json;charset=utf-8');
        $response->end(json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }
}
