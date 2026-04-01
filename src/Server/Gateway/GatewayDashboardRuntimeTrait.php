<?php

namespace Scf\Server\Gateway;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Result;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App as WebApp;
use Scf\Mode\Web\Request as WebRequest;
use Scf\Mode\Web\Response as WebResponse;
use Scf\Server\DashboardAuth;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\ExitException;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;
use Throwable;

/**
 * Gateway dashboard 入口与会话交互职责。
 *
 * 该 trait 聚合 dashboard 的 HTTP API、静态资源、WebSocket 握手与消息分发逻辑，
 * 让 GatewayServer 主类保持“流程编排/运行总控”职责，避免继续吞下控制面协议细节。
 */
trait GatewayDashboardRuntimeTrait {
    protected function isDashboardHttpRequest(string $uri): bool {
        if (!str_starts_with($uri, '/~')) {
            return false;
        }
        return $this->dashboardEnabled() || $this->allowSlaveDashboardInstallRequest($uri);
    }

    protected function dashboardEnabled(): bool {
        return SERVER_ROLE === NODE_ROLE_MASTER;
    }

    protected function allowSlaveDashboardInstallRequest(string $uri): bool {
        $path = $this->normalizeDashboardPath($uri);
        return in_array($path, ['/install', '/install_check'], true);
    }

    /**
     * 判断当前是否处于“安装接管”阶段。
     *
     * 业务应用尚未安装完成时，nginx 会把业务入口先回切到 gateway 控制面。
     * 此时 control plane 需要接住普通业务路径，并把用户引导到安装页。
     *
     * @return bool
     */
    protected function installTakeoverActive(): bool {
        if (!App::isReady()) {
            return true;
        }
        return (bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER) ?? false);
    }

    /**
     * 判断当前请求是否应被引导到安装页。
     *
     * 仅在安装接管阶段对普通业务路径生效；dashboard 与内部控制面路径仍按
     * 原语义处理，避免把 `/~`、`/_gateway` 这类控制面请求错误重定向走。
     *
     * @param string $uri 当前请求 URI。
     * @return bool
     */
    protected function shouldRedirectToInstallFlow(string $uri): bool {
        if (!$this->installTakeoverActive()) {
            return false;
        }
        if (str_starts_with($uri, '/~') || str_starts_with($uri, '/_gateway')) {
            return false;
        }
        return true;
    }

    /**
     * 处理 dashboard 的 HTTP 入口。
     *
     * /~ 下既可能是静态资源，也可能是 API；这里先做路径归一化，
     * 再决定返回静态文件、index.html 还是进入 dashboard API 分发。
     *
     * @param Request $request dashboard HTTP 请求
     * @param Response $response dashboard HTTP 响应
     * @return void
     */
    protected function handleDashboardRequest(Request $request, Response $response): void {
        $path = $this->normalizeDashboardPath($request->server['request_uri'] ?? '/~');
        if (($request->server['request_method'] ?? 'GET') === 'GET' && $this->serveDashboardStaticAsset($path, $response)) {
            return;
        }

        if (!$this->isDashboardApiRequest($path, strtoupper((string)($request->server['request_method'] ?? 'GET')))) {
            $this->serveDashboardIndex($response);
            return;
        }

        WebResponse::instance()->register($response);
        WebRequest::instance()->register($request);
        $request->server['path_info'] = $path;

        try {
            WebApp::instance()->start();
            $this->dispatchDashboardApi($request, $response, $path);
        } catch (ExitException) {
            return;
        } catch (Throwable $e) {
            $this->dashboardJson($response, 'SERVICE_ERROR', 'SYSTEM ERROR:' . $e->getMessage(), '');
        }
    }

    protected function dispatchDashboardApi(Request $request, Response $response, string $path): void {
        if (!App::isReady() && !in_array($path, ['/install', '/install_check'], true)) {
            $this->dashboardJson($response, 'APP_NOT_INSTALL_YET', '应用尚未完成初始化安装', '');
            return;
        }

        $method = strtoupper((string)($request->server['request_method'] ?? 'GET'));
        $payload = $this->dashboardRequestPayload($request);

        switch ($path) {
            case '/login':
                $password = trim((string)($payload['password'] ?? ''));
                if ($password === '') {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '密码不能为空', '');
                    return;
                }
                if (!DashboardAuth::dashboardPassword()) {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '请先配置 server.dashboard_password', '');
                    return;
                }
                if (!hash_equals((string)DashboardAuth::dashboardPassword(), $password)) {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '密码错误', '');
                    return;
                }
                $token = DashboardAuth::createDashboardToken('system');
                if (!$token) {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '请先配置 server.dashboard_password', '');
                    return;
                }
                $this->dashboardJson($response, 0, 'SUCCESS', $this->dashboardLoginUser($token));
                return;
            case '/check':
                $auth = $this->requireDashboardAuth($request, $response);
                if (!$auth) {
                    return;
                }
                $token = DashboardAuth::refreshDashboardToken($auth['token']);
                if (!$token) {
                    $this->dashboardJson($response, 'LOGIN_EXPIRED', '登陆已失效', '');
                    return;
                }
                $this->dashboardJson($response, 0, 'SUCCESS', $this->dashboardLoginUser($token, (string)($auth['session']['user'] ?? 'system')));
                return;
            case '/logout':
            case '/expireToken':
                $auth = $this->parseDashboardAuth($request);
                if ($auth) {
                    DashboardAuth::expireDashboardToken($auth['token']);
                }
                $this->dashboardJson($response, 0, 'SUCCESS', '');
                return;
            case '/refreshToken':
                $auth = $this->requireDashboardAuth($request, $response);
                if (!$auth) {
                    return;
                }
                $token = DashboardAuth::refreshDashboardToken($auth['token']);
                if (!$token) {
                    $this->dashboardJson($response, 'LOGIN_EXPIRED', '登录已过期', '');
                    return;
                }
                $this->dashboardJson($response, 0, 'SUCCESS', ['token' => $token]);
                return;
            case '/server':
                $auth = $this->requireDashboardAuth($request, $response);
                if (!$auth) {
                    return;
                }
                $this->dashboardJson(
                    $response,
                    0,
                    'SUCCESS',
                    $this->dashboardServerStatus(
                        $auth['token'],
                        (string)($request->header['x-forwarded-host'] ?? $request->header['host'] ?? ''),
                        (string)($request->header['referer'] ?? ''),
                        (string)($request->header['x-forwarded-proto'] ?? '')
                    )
                );
                return;
            case '/nodes':
                $this->dashboardJson($response, 0, 'SUCCESS', $this->dashboardNodes());
                return;
            case '/command':
                $auth = $this->requireDashboardAuth($request, $response);
                if (!$auth) {
                    return;
                }
                $command = trim((string)($payload['command'] ?? ''));
                $host = trim((string)($payload['host'] ?? ''));
                if ($command === '') {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '命令不能为空', '');
                    return;
                }
                if ($host === '') {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '节点不能为空', '');
                    return;
                }
                $this->respondDashboardResult(
                    $response,
                    $this->dashboardCommand($command, $host, (array)($payload['params'] ?? []))
                );
                return;
            case '/update':
                $auth = $this->requireDashboardAuth($request, $response);
                if (!$auth) {
                    return;
                }
                $type = trim((string)($payload['type'] ?? ''));
                $version = trim((string)($payload['version'] ?? ''));
                if ($type === '') {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '更新类型错误', '');
                    return;
                }
                if ($version === '') {
                    $this->dashboardJson($response, 'SERVICE_ERROR', '版本号不能为空', '');
                    return;
                }
                $this->respondDashboardResult($response, $this->dashboardUpdate($type, $version));
                return;
            default:
                $requiresAuth = !in_array($path, ['/install', '/install_check', '/login', '/memory', '/nodes', '/update_dashboard', '/ws_document', '/ws_sign_debug'], true);
                $auth = null;
                if ($requiresAuth) {
                    $auth = $this->requireDashboardAuth($request, $response);
                    if (!$auth) {
                        return;
                    }
                }
                $this->invokeDashboardControllerAction($response, $path, $auth['token'] ?? null, (string)($auth['session']['user'] ?? 'system'));
                return;
        }
    }

    protected function handleDashboardHandshake(Request $request, Response $response): bool {
        $token = (string)($request->get['token'] ?? '');
        $auth = $token === '' ? false : DashboardAuth::validateSocketToken($token);
        if (!$auth) {
            $response->status(403);
            $response->end('forbidden');
            return false;
        }

        try {
            $this->performServerHandshake($request, $response);
            if (($auth['type'] ?? '') === 'internal_socket') {
                $this->nodeClients[$request->fd] = [
                    'connected_at' => time(),
                    'subject' => (string)($auth['subject'] ?? 'internal'),
                ];
            } else {
                $this->dashboardClients[$request->fd] = [
                    'connected_at' => time(),
                ];
                $this->syncConsoleSubscriptionState(true);
            }
            return true;
        } catch (Throwable $e) {
            $response->status(400);
            $response->end($e->getMessage());
            return false;
        }
    }

    /**
     * dashboard websocket 事件处理。
     *
     * dashboard 实时状态推送和运维命令都走这条 ws 通道，但真正执行仍然复用
     * gateway 内部已有的命令分发与 rolling 逻辑，避免再维护一套并行状态机。
     *
     * @param Server $server 当前 gateway server
     * @param Frame $frame 收到的 dashboard websocket 帧
     * @return void
     */
    protected function handleDashboardSocketMessage(Server $server, Frame $frame): void {
        if (!JsonHelper::is($frame->data)) {
            if ($server->isEstablished($frame->fd)) {
                $server->push($frame->fd, JsonHelper::toJson(['event' => 'message', 'data' => '不支持的消息']));
            }
            return;
        }

        $payload = JsonHelper::recover($frame->data);
        $event = (string)($payload['event'] ?? '');
        switch ($event) {
            case 'server_status':
                $this->pushDashboardStatus($frame->fd);
                break;
            case 'reloadAll':
                Coroutine::create(function () use ($frame) {
                    Console::info("【Gateway】Dashboard Socket命令: {$frame->data}");
                    $this->sendCommandToAllNodeClients('reload');
                    $this->pushDashboardEvent([
                        'event' => 'console',
                        'message' => ['data' => '已向所有节点发送业务重启指令，当前 Gateway 开始重启本地业务平面'],
                        'time' => Console::timestamp(),
                        'node' => SERVER_HOST,
                    ], $frame->fd);
                    $this->pushDashboardStatus();
                    $result = $this->dispatchLocalGatewayCommand('reload');
                    if ($result->hasError()) {
                        $this->pushDashboardEvent([
                            'event' => 'console',
                            'message' => ['data' => '本地业务平面重载失败: ' . $result->getMessage()],
                            'time' => Console::timestamp(),
                            'node' => SERVER_HOST,
                        ], $frame->fd);
                    }
                });
                break;
            case 'restartRedisQueueAll':
                Coroutine::create(function () use ($frame) {
                    Console::info("【Gateway】Dashboard Socket命令: {$frame->data}");
                    $this->sendCommandToAllNodeClients('restart_redisqueue');
                    $this->pushDashboardEvent([
                        'event' => 'console',
                        'message' => ['data' => '已向所有子节点发送 RedisQueue 重启指令，当前 Gateway 开始重启本地 RedisQueue 子进程'],
                        'time' => Console::timestamp(),
                        'node' => SERVER_HOST,
                    ], $frame->fd);
                    $this->pushDashboardStatus();
                    $this->dispatchLocalGatewayCommand('restart_redisqueue');
                });
                break;
            case 'restartAll':
                Coroutine::create(function () use ($frame) {
                    Console::info("【Gateway】Dashboard Socket命令: {$frame->data}");
                    $this->sendCommandToAllNodeClients('restart', ['preserve_managed_upstreams' => false]);
                    $this->pushDashboardEvent([
                        'event' => 'console',
                        'message' => ['data' => '已向所有子节点发送重启指令，当前 Gateway 开始重启'],
                        'time' => Console::timestamp(),
                        'node' => SERVER_HOST,
                    ], $frame->fd);
                    $this->scheduleGatewayShutdown(false);
                });
                break;
            case 'console_subscribe':
                $this->syncConsoleSubscriptionState(true);
                break;
            default:
                $this->pushDashboardEvent([
                    'event' => 'message',
                    'data' => '不支持的事件:' . $event,
                ], $frame->fd);
                break;
        }
    }

    /**
     * 从 dashboard Referer 中提取 websocket 对外路径前缀。
     *
     * dashboard 可能被静态目录挂在 `/cp` 这类前缀下。此时 websocket 入口也必须
     * 跟着变成 `/cp/dashboard.socket`，否则前端和 slave 节点都会连接到错误路径。
     *
     * @param string $referer dashboard 页面 Referer。
     * @return string
     */
    protected function dashboardRefererPathPrefix(string $referer): string {
        if ($referer === '') {
            return '';
        }
        $parts = parse_url($referer);
        if (!is_array($parts)) {
            return '';
        }
        return $this->normalizeDashboardPathPrefix((string)($parts['path'] ?? ''));
    }

    /**
     * 将 dashboard 页面路径归一化成 websocket 入口前缀。
     *
     * @param string $path 页面路径或静态文件路径。
     * @return string 无前缀时返回空字符串。
     */
    protected function normalizeDashboardPathPrefix(string $path): string {
        $path = trim($path);
        if ($path === '' || $path === '/' || $path === '/~' || $this->isDashboardSocketPath($path) || str_starts_with($path, '/_gateway')) {
            return '';
        }
        if (str_contains(basename($path), '.')) {
            $path = dirname($path);
        }
        $path = '/' . trim($path, '/');
        return $path === '/' ? '' : rtrim($path, '/');
    }

    /**
     * 判断路径是否命中 dashboard websocket 入口。
     *
     * 为兼容 `/cp/dashboard.socket` 这类前缀部署，不能只认根路径的精确匹配。
     *
     * @param string $path 请求路径。
     * @return bool
     */
    protected function isDashboardSocketPath(string $path): bool {
        $path = trim($path);
        return $path === '/dashboard.socket' || str_ends_with($path, '/dashboard.socket');
    }

    protected function normalizeDashboardPath(string $uri): string {
        $path = substr($uri, 2);
        if ($path === false || $path === '') {
            return '/';
        }
        return str_starts_with($path, '/') ? $path : '/' . $path;
    }

    protected function isDashboardApiRequest(string $path, string $method): bool {
        $api = [
            '/login' => ['POST'],
            '/check' => ['GET'],
            '/logout' => ['GET'],
            '/refreshToken' => ['GET'],
            '/expireToken' => ['GET'],
            '/server' => ['GET'],
            '/nodes' => ['GET'],
            '/command' => ['POST'],
            '/update' => ['POST'],
            '/versions' => ['GET'],
            '/update_dashboard' => ['POST'],
            '/routes' => ['GET'],
            '/notices' => ['GET'],
            '/logs' => ['GET'],
            '/crontabs' => ['GET'],
            '/linux_crontabs' => ['GET'],
            '/linux_crontab_installed' => ['GET'],
            '/crontab_run' => ['POST'],
            '/crontab_status' => ['POST'],
            '/crontab_override' => ['POST'],
            '/linux_crontab_save' => ['POST'],
            '/linux_crontab_delete' => ['POST'],
            '/linux_crontab_set_enabled' => ['POST'],
            '/linux_crontab_sync' => ['POST'],
            '/install' => ['POST'],
            '/install_check' => ['GET'],
            '/check_slave_node' => ['POST'],
            '/install_slave_node' => ['POST'],
            '/ws_document' => ['GET'],
            '/ws_sign_debug' => ['GET'],
        ];

        return isset($api[$path]) && in_array($method, $api[$path], true);
    }

    protected function serveDashboardStaticAsset(string $path, Response $response): bool {
        $dashboardRoot = SCF_ROOT . '/build/public/dashboard';
        if ($path === '/' || $path === '') {
            return false;
        }

        $relativePath = ltrim($path, '/');
        $filePath = realpath($dashboardRoot . '/' . $relativePath);
        $rootPath = realpath($dashboardRoot);
        if ($filePath === false || $rootPath === false || !str_starts_with($filePath, $rootPath) || !is_file($filePath)) {
            return false;
        }

        $response->header('Content-Type', $this->guessMimeType($filePath));
        $response->sendfile($filePath);
        return true;
    }

    protected function serveDashboardIndex(Response $response): void {
        $indexFile = SCF_ROOT . '/build/public/dashboard/index.html';
        if (!is_file($indexFile)) {
            $this->json($response, 404, ['message' => 'dashboard 前端资源不存在']);
            return;
        }

        $content = (string)file_get_contents($indexFile);
        $content = str_replace('./manifest.webmanifest', '/~/manifest.webmanifest', $content);
        $response->header('Content-Type', 'text/html; charset=utf-8');
        $response->end($content);
    }

    protected function guessMimeType(string $filePath): string {
        return match (strtolower(pathinfo($filePath, PATHINFO_EXTENSION))) {
            'js' => 'application/javascript; charset=utf-8',
            'css' => 'text/css; charset=utf-8',
            'json' => 'application/json; charset=utf-8',
            'svg' => 'image/svg+xml',
            'ico' => 'image/x-icon',
            'png' => 'image/png',
            'jpg', 'jpeg' => 'image/jpeg',
            'webmanifest' => 'application/manifest+json; charset=utf-8',
            default => 'text/plain; charset=utf-8',
        };
    }

    protected function dashboardJson(Response $response, string|int $errCode, string $message, mixed $data): void {
        $response->status(200);
        $response->header('Content-Type', 'application/json;charset=utf-8');
        $response->end(json_encode([
            'errCode' => $errCode,
            'message' => $message,
            'data' => $data,
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    protected function respondDashboardResult(Response $response, Result $result): void {
        if ($result->hasError()) {
            $this->dashboardJson($response, $result->getErrCode(), (string)$result->getMessage(), $result->getData());
            return;
        }
        $this->dashboardJson($response, 0, 'SUCCESS', $result->getData());
    }

    protected function invokeDashboardControllerAction(Response $response, string $path, ?string $token = null, string $currentUser = 'system'): void {
        $controller = (new GatewayDashboardController($this))->hydrateDashboardSession($token, $currentUser);
        $method = 'action' . StringHelper::lower2camel(str_replace('/', '_', substr($path, 1)));
        if (!method_exists($controller, $method)) {
            $this->dashboardJson($response, 404, 'not found', '');
            return;
        }

        $result = $controller->$method();
        if ($result instanceof Result) {
            $this->respondDashboardResult($response, $result);
            return;
        }

        $response->status(200);
        $response->end((string)$result);
    }

    protected function parseDashboardAuth(Request $request): array|false {
        $authorization = trim((string)($request->header['authorization'] ?? ''));
        if ($authorization === '' || !str_starts_with($authorization, 'Bearer ')) {
            return false;
        }
        $token = trim(substr($authorization, 7));
        if ($token === '') {
            return false;
        }
        $session = DashboardAuth::validateDashboardToken($token);
        if (!$session) {
            return false;
        }
        return [
            'token' => $token,
            'session' => $session,
        ];
    }

    protected function requireDashboardAuth(Request $request, Response $response): array|false {
        $auth = $this->parseDashboardAuth($request);
        if ($auth) {
            return $auth;
        }

        $authorization = trim((string)($request->header['authorization'] ?? ''));
        if ($authorization === '' || !str_starts_with($authorization, 'Bearer ')) {
            $this->dashboardJson($response, 'NOT_LOGIN', '需登陆后访问', '');
            return false;
        }

        $this->dashboardJson($response, 'LOGIN_EXPIRED', '登录已失效', '');
        return false;
    }

    protected function dashboardLoginUser(?string $token, string $user = 'system'): array {
        return [
            'username' => $user === 'system' ? '系统管理员' : $user,
            'avatar' => 'https://ascript.oss-cn-chengdu.aliyuncs.com/upload/20240513/04c3eeac-f118-4ea7-8665-c9bd4d20a05d.png',
            'token' => $token,
        ];
    }

    protected function dashboardRequestPayload(Request $request): array {
        $payload = [];
        if (is_array($request->get ?? null)) {
            $payload = array_merge($payload, $request->get);
        }
        if (is_array($request->post ?? null)) {
            $payload = array_merge($payload, $request->post);
        }
        $jsonPayload = $this->decodeJsonBody($request);
        if ($jsonPayload) {
            $payload = array_merge($payload, $jsonPayload);
        }
        return $payload;
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
