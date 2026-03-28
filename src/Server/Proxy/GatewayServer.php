<?php

namespace Scf\Server\Proxy;

use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Result;
use Scf\Core\Server as CoreServer;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Core\Table\ServerNodeTable;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App as WebApp;
use Scf\Mode\Web\Request as WebRequest;
use Scf\Mode\Web\Response as WebResponse;
use Scf\Server\DashboardAuth;
use Scf\Server\Manager;
use Scf\Server\SubProcessManager;
use Scf\Server\Proxy\ConsoleRelay;
use Scf\Util\File;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Http\Client;
use Swoole\Coroutine\Client as TcpClient;
use Swoole\ExitException;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\Timer;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;
use Swoole\Exception as SwooleException;
use Throwable;

class GatewayServer {
    protected const ROLLING_DRAIN_GRACE_SECONDS = 30;
    protected const INTERNAL_UPSTREAM_STATUS_PATH = '/_gateway/internal/upstream/status';
    protected const INTERNAL_UPSTREAM_HEALTH_PATH = '/_gateway/internal/upstream/health';
    protected const ACTIVE_UPSTREAM_HEALTH_CHECK_INTERVAL_SECONDS = 5;
    protected const ACTIVE_UPSTREAM_HEALTH_FAILURE_THRESHOLD = 3;
    protected const ACTIVE_UPSTREAM_HEALTH_COOLDOWN_SECONDS = 60;
    protected const ACTIVE_UPSTREAM_HEALTH_STARTUP_GRACE_SECONDS = 20;
    protected const UPSTREAM_SUPERVISOR_SYNC_INTERVAL_SECONDS = 15;
    protected const DASHBOARD_VERSION_CACHE_TTL_SECONDS = 30;
    protected const FRAMEWORK_VERSION_CACHE_TTL_SECONDS = 30;

    protected Server $server;
    protected array $dashboardClients = [];
    protected array $nodeClients = [];
    protected array $bootstrappedManagedInstances = [];
    protected ?bool $lastConsoleSubscriptionState = null;
    protected ?UpstreamSupervisor $upstreamSupervisor = null;
    protected mixed $masterGatewaySocket = null;
    protected int $rpcPort = 0;
    protected int $startedAt;
    protected int $serverMasterPid = 0;
    protected int $serverManagerPid = 0;
    protected bool $clusterControlStarted = false;
    protected bool $installWatcherStarted = false;
    protected bool $installUpdating = false;
    protected bool $gatewayShutdownScheduled = false;
    protected ?int $housekeepingTimerId = null;
    protected ?int $clusterStatusTimerId = null;
    protected ?SubProcessManager $subProcessManager = null;
    protected ?LocalIpcServer $localIpcServer = null;
    protected array $drainingGenerationWarnState = [];
    protected array $pendingManagedRecycles = [];
    protected array $pendingManagedRecycleWatchers = [];
    protected array $pendingManagedRecycleWarnState = [];
    protected array $managedUpstreamHealthState = [];
    protected bool $managedUpstreamSelfHealing = false;
    protected int $lastManagedUpstreamHealthCheckAt = 0;
    protected int $lastManagedUpstreamSelfHealAt = 0;
    protected int $lastUpstreamSupervisorSyncAt = 0;
    protected int $lastObservedUpstreamSupervisorPid = 0;
    protected int $lastObservedUpstreamSupervisorStartedAt = 0;
    protected ?string $pendingUpstreamSupervisorSyncReason = null;
    protected ?GatewayHttpProxyHandler $httpProxyHandler = null;
    protected ?GatewayTcpRelayHandler $tcpRelayHandler = null;
    protected ?GatewayNginxProxyHandler $nginxProxyHandler = null;
    protected array $dashboardVersionStatusCache = ['expires_at' => 0, 'value' => null];
    protected array $frameworkRemoteVersionCache = ['expires_at' => 0, 'value' => null];

    public function __construct(
        protected AppInstanceManager $instanceManager,
        protected string $host,
        protected int $port,
        protected int $workerNum = 1,
        protected ?AppServerLauncher $launcher = null,
        protected array $managedUpstreamPlans = [],
        protected int $rpcBindPort = 0,
        protected int $controlBindPort = 0,
        protected string $configuredTrafficMode = 'tcp'
    ) {
        $this->rpcPort = $rpcBindPort;
        $this->startedAt = time();
        $this->httpProxyHandler = new GatewayHttpProxyHandler($this, $this->instanceManager);
        $this->tcpRelayHandler = new GatewayTcpRelayHandler($this, $this->instanceManager);
        $this->nginxProxyHandler = new GatewayNginxProxyHandler($this, $this->instanceManager);
    }

    public function start(): void {
        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);
        $this->bootstrapRuntimeState();
        Console::enablePush(Config::server()['enable_log_push'] ?? STATUS_ON);
        Console::setPushHandler(function (string $time, string $message) {
            $this->pushConsoleLog($time, $message);
        });

        $this->waitForGatewayPortsReleased();
        $this->server = $this->createGatewaySocketServer();
        $this->server->set([
            'worker_num' => max(1, $this->workerNum),
            'daemonize' => false,
            'max_wait_time' => (int)(Config::server()['gateway_max_wait_time'] ?? 5),
            'reload_async' => true,
            'open_http_protocol' => true,
            'open_http2_protocol' => true,
            'open_websocket_protocol' => true,
            'package_max_length' => 20 * 1024 * 1024,
            'log_file' => APP_LOG_PATH . '/gateway.log',
            'pid_file' => $this->pidFile(),
        ]);
        $this->applyStaticHandlerSettings();
        $this->createBusinessTrafficListener();
        if ($this->nginxProxyModeEnabled()) {
            $this->syncNginxProxyTargets('gateway_prestart');
        }

        if ($this->rpcPort > 0) {
            $rpcListener = $this->server->listen($this->host, $this->rpcPort, SWOOLE_SOCK_TCP);
            if ($rpcListener === false) {
                throw new RuntimeException("Gateway RPC 监听失败: {$this->host}:{$this->rpcPort}");
            }
            $rpcListener->set([
                'package_max_length' => 20 * 1024 * 1024,
                'open_http_protocol' => false,
                'open_http2_protocol' => false,
                'open_websocket_protocol' => false,
                'open_length_check' => true,
                'package_length_type' => 'N',
                'package_length_offset' => 0,
                'package_body_offset' => 4,
            ]);
        }

        $this->attachUpstreamSupervisor();

        $this->server->on('Start', function (Server $server) {
            $this->serverMasterPid = (int)($server->master_pid ?? 0);
            $this->serverManagerPid = (int)($server->manager_pid ?? 0);
            if ($this->nginxProxyModeEnabled()) {
                $this->syncNginxProxyTargets('gateway_startup');
            }
            $dashboardPort = $this->resolvedDashboardPort();
            if ($dashboardPort > 0) {
                File::write(SERVER_DASHBOARD_PORT_FILE, (string)$dashboardPort);
            }
            $this->renderStartupInfo();
        });
        $this->server->on('BeforeReload', function (Server $server) {
            Runtime::instance()->serverIsReady(false);
            Runtime::instance()->serverIsDraining(true);
            Console::warning('【Gateway】服务即将重载');
            $disconnected = $this->disconnectAllClients($server);
            $disconnected > 0 and Console::warning("【Gateway】已断开 {$disconnected} 个客户端连接");
        });
        $this->server->on('AfterReload', function () {
            Runtime::instance()->serverIsDraining(false);
            Runtime::instance()->serverIsReady(true);
            Console::success('【Gateway】服务重载完成');
        });
        $this->server->on('BeforeShutdown', function (Server $server) {
            $this->prepareGatewayShutdown();
            Console::warning('【Gateway】服务即将关闭');
            $disconnected = $this->disconnectAllClients($server);
            $disconnected > 0 and Console::warning("【Gateway】已断开 {$disconnected} 个客户端连接");
        });
        $this->server->on('Shutdown', function () {
            Runtime::instance()->serverIsAlive(false);
        });

        $this->server->on('WorkerStart', function (Server $server, int $workerId) {
            if ($workerId !== 0) {
                return;
            }
            Runtime::instance()->serverIsDraining(false);
            Runtime::instance()->serverIsReady(true);
            $this->startClusterControlPlane();
            $tick = function () {
                if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                    $this->clearHousekeepingTimer();
                    return;
                }
                $this->ensureLocalIpcServerStarted();
                $this->ensureGatewaySubProcessManagerStarted();
                $this->ensureInstallWatcher();
                if (App::isReady()) {
                    $this->bootstrapManagedUpstreams();
                }
                $this->observeUpstreamSupervisorProcess();
                $this->syncUpstreamSupervisorState();
                $this->refreshManagedUpstreamRuntimeStates();
                $this->instanceManager->tick();
                $this->pollPendingManagedRecycles();
                $this->maintainManagedUpstreamHealth();
                $this->warnStuckDrainingManagedUpstreams();
                $this->cleanupOfflineManagedUpstreams();
            };
            $tick();
            $this->housekeepingTimerId = Timer::tick(1000, $tick);
        });

        $this->server->on('Request', function (Request $request, Response $response): void {
            $this->onRequest($request, $response);
        });
        $this->server->on('Handshake', function (Request $request, Response $response) {
            return $this->onHandshake($request, $response);
        });
        $this->server->on('Message', function (Server $server, Frame $frame): void {
            $this->onMessage($server, $frame);
        });
        $this->server->on('Connect', function (Server $server, int $fd, int $reactorId): void {
            $this->onConnect($server, $fd, $reactorId);
        });
        $this->server->on('Receive', function (Server $server, int $fd, int $reactorId, string $data): void {
            $this->onReceive($server, $fd, $reactorId, $data);
        });
        $this->server->on('Close', function (Server $server, int $fd): void {
            $this->onClose($server, $fd);
        });
        $this->ensureGatewaySubProcessManagerStarted();
        $this->server->start();
    }

    protected function onRequest(Request $request, Response $response): void {
        $uri = $request->server['request_uri'] ?? '/';
        if (str_starts_with($uri, '/~') && !$this->dashboardEnabled() && !$this->allowSlaveDashboardInstallRequest($uri)) {
            $this->json($response, 404, ['message' => 'dashboard 仅在 master 节点开放']);
            return;
        }
        if ($this->isDashboardHttpRequest($uri)) {
            $this->handleDashboardRequest($request, $response);
            return;
        }
        if (str_starts_with($uri, '/_gateway')) {
            $this->handleManagementRequest($request, $response);
            return;
        }
        if ($this->tcpRelayModeEnabled() || $this->nginxProxyModeEnabled()) {
            $this->json($response, 404, [
                'message' => $this->nginxProxyModeEnabled() ? '业务流量已由nginx接管' : '业务流量仅在业务端口开放'
            ]);
            return;
        }
        if (!App::isReady()) {
            $response->status(503);
            $response->header('Content-Type', 'text/html; charset=utf-8');
            $response->end('应用安装中...');
            return;
        }
        if ($this->shouldRejectNewProxyRequest()) {
            $this->json($response, 503, [
                'message' => '服务繁忙',
                'inflight' => $this->instanceManager->totalProxyHttpRequestCount(),
                'limit' => $this->proxyMaxInflightRequests(),
            ]);
            return;
        }

        $upstream = $this->instanceManager->pickHttpUpstream($this->resolveAffinityKey($request));
        if (!$upstream) {
            $this->json($response, 503, ['message' => '没有可用的业务实例']);
            return;
        }

        $attempted = [];
        $affinityKey = $this->resolveAffinityKey($request);
        $lastError = null;
        while ($upstream) {
            $attempted[] = (string)($upstream['id'] ?? '');
            try {
                $this->httpProxyHandler()->proxyHttpRequest($request, $response, $upstream);
                return;
            } catch (Throwable $e) {
                $lastError = $e;
                if (!$this->httpProxyHandler()->isTransientUpstreamUnavailable($e)) {
                    break;
                }
                $retryUpstream = $this->instanceManager->pickHttpUpstreamExcluding($attempted, $affinityKey);
                if (!$retryUpstream || (string)($retryUpstream['id'] ?? '') === (string)($upstream['id'] ?? '')) {
                    break;
                }
                Console::warning(
                    "【Gateway】业务实例切换窗口发生连接拒绝，改投新实例: old="
                    . $this->managedPlanDescriptor([
                        'version' => (string)($upstream['version'] ?? ''),
                        'host' => (string)($upstream['host'] ?? '127.0.0.1'),
                        'port' => (int)($upstream['port'] ?? 0),
                        'rpc_port' => (int)($upstream['metadata']['rpc_port'] ?? 0),
                    ])
                    . ', new='
                    . $this->managedPlanDescriptor([
                        'version' => (string)($retryUpstream['version'] ?? ''),
                        'host' => (string)($retryUpstream['host'] ?? '127.0.0.1'),
                        'port' => (int)($retryUpstream['port'] ?? 0),
                        'rpc_port' => (int)($retryUpstream['metadata']['rpc_port'] ?? 0),
                    ])
                );
                $upstream = $retryUpstream;
                continue;
            }
        }

        $message = $this->httpProxyHandler()->isTransientUpstreamUnavailable($lastError) ? '服务不可用' : '代理转发失败';
        $status = $this->httpProxyHandler()->isTransientUpstreamUnavailable($lastError) ? 503 : 502;
        $this->json($response, $status, [
            'message' => $message,
            'error' => $lastError?->getMessage(),
            'upstream' => $upstream,
        ]);
    }

    protected function proxyHttpRequestToUpstream(Request $request, Response $response, array $upstream): void {
        $this->httpProxyHandler()->proxyHttpRequest($request, $response, $upstream);
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

    protected function isTransientUpstreamUnavailable(?Throwable $throwable): bool {
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

    protected function onHandshake(Request $request, Response $response): bool {
        $uri = $request->server['request_uri'] ?? '/';
        if ($uri === '/dashboard.socket') {
            if (!$this->dashboardEnabled()) {
                $response->status(403);
                $response->end('dashboard disabled');
                return false;
            }
            return $this->handleDashboardHandshake($request, $response);
        }
        if (str_starts_with($uri, '/_gateway')) {
            $response->status(403);
            $response->end('forbidden');
            return false;
        }
        if ($this->tcpRelayModeEnabled()) {
            $response->status(404);
            $response->end('业务流量仅在业务端口开放');
            return false;
        }
        if (!App::isReady()) {
            $response->status(503);
            $response->end('应用安装中...');
            return false;
        }

        $upstream = $this->instanceManager->bindWebsocketUpstream($request->fd, $this->resolveAffinityKey($request));
        if (!$upstream) {
            $response->status(503);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(json_encode(['message' => '没有可用的业务实例'], JSON_UNESCAPED_UNICODE));
            return false;
        }

        return $this->httpProxyHandler()->handleWebSocketHandshake($request, $response, $upstream);
    }

    protected function onMessage(Server $server, Frame $frame): void {
        if (isset($this->nodeClients[$frame->fd])) {
            $this->handleNodeSocketMessage($server, $frame);
            return;
        }
        if (isset($this->dashboardClients[$frame->fd])) {
            $this->handleDashboardSocketMessage($server, $frame);
            return;
        }
        $this->httpProxyHandler()->handleWebSocketMessage($frame);
    }

    protected function onClose(Server $server, int $fd): void {
        unset($this->dashboardClients[$fd]);
        unset($this->nodeClients[$fd]);
        $this->tcpRelayHandler()->handleClose($fd);
        $this->httpProxyHandler()->handleClientClose($fd);
        $this->removeNodeClient($fd);
        if ($this->dashboardEnabled()) {
            $this->syncConsoleSubscriptionState();
        }
    }

    protected function bootstrapRuntimeState(): void {
        // Keep the same shared-table bootstrap order as Http::start(),
        // otherwise Gateway workers and subprocesses will each lazily
        // create private tables after fork and runtime state will drift.
        ATable::register([
            'Scf\Core\Table\PdoPoolTable',
            'Scf\Core\Table\LogTable',
            'Scf\Core\Table\Counter',
            'Scf\Core\Table\Runtime',
            'Scf\Core\Table\RouteTable',
            'Scf\Core\Table\RouteCache',
            'Scf\Core\Table\SocketRouteTable',
            'Scf\Core\Table\SocketConnectionTable',
            'Scf\Core\Table\CrontabTable',
            'Scf\Core\Table\MemoryMonitorTable',
            'Scf\Core\Table\ServerNodeTable',
            'Scf\Core\Table\ServerNodeStatusTable',
        ]);
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(false);
        Runtime::instance()->serverIsAlive(true);
        Runtime::instance()->httpPort($this->port);
        Runtime::instance()->rpcPort($this->rpcPort);
        Runtime::instance()->dashboardPort($this->resolvedDashboardPort());
        ConsoleRelay::setGatewayPort($this->port);
        ConsoleRelay::setLocalSubscribed(false);
        ConsoleRelay::setRemoteSubscribed(false);
    }

    protected function ensureLocalIpcServerStarted(): void {
        if ($this->localIpcServer instanceof LocalIpcServer) {
            return;
        }
        $socketPath = LocalIpc::gatewaySocketPath($this->port);
        $this->localIpcServer = new LocalIpcServer($socketPath, function (array $request): array {
            return $this->handleLocalIpcRequest($request);
        }, 'gateway_ipc');
        $this->localIpcServer->start();
    }

    protected function stopLocalIpcServer(): void {
        if (!$this->localIpcServer instanceof LocalIpcServer) {
            return;
        }
        $this->localIpcServer->stop();
        $this->localIpcServer = null;
    }

    protected function handleLocalIpcRequest(array $request): array {
        $action = (string)($request['action'] ?? '');
        $payload = is_array($request['payload'] ?? null) ? $request['payload'] : [];
        return match ($action) {
            'gateway.health' => [
                'ok' => true,
                'status' => 200,
                'data' => [
                    'message' => 'ok',
                    'active_version' => $this->instanceManager->snapshot()['active_version'] ?? null,
                ],
            ],
            'gateway.console.subscription' => [
                'ok' => true,
                'status' => 200,
                'data' => ['enabled' => $this->dashboardEnabled() ? $this->hasConsoleSubscribers() : ConsoleRelay::remoteSubscribed()],
            ],
            'gateway.console.log' => [
                'ok' => true,
                'status' => 200,
                'data' => [
                    'accepted' => $this->acceptConsolePayload([
                        'time' => (string)($payload['time'] ?? ''),
                        'message' => (string)($payload['message'] ?? ''),
                        'source_type' => (string)($payload['source_type'] ?? 'gateway'),
                        'node' => (string)($payload['node'] ?? SERVER_HOST),
                    ]),
                ],
            ],
            'gateway.command' => $this->dispatchInternalGatewayCommand((string)($payload['command'] ?? '')),
            default => ['ok' => false, 'status' => 404, 'message' => 'unknown action'],
        };
    }

    protected function disconnectAllClients(Server $server): int {
        $startFd = 0;
        $disconnected = 0;
        while (true) {
            $clients = $server->getClientList($startFd, 100);
            if (!$clients) {
                break;
            }
            foreach ($clients as $fd) {
                $fd = (int)$fd;
                $startFd = max($startFd, $fd);
                $this->instanceManager->releaseWebsocketBinding($fd);
                unset($this->dashboardClients[$fd], $this->nodeClients[$fd]);
                $this->tcpRelayHandler()->handleClose($fd);
                $this->httpProxyHandler()->handleClientClose($fd);
                if (!$server->exist($fd)) {
                    continue;
                }
                if ($server->isEstablished($fd)) {
                    $server->disconnect($fd);
                } else {
                    $server->close($fd);
                }
                $disconnected++;
            }
            if (count($clients) < 100) {
                break;
            }
        }
        return $disconnected;
    }

    protected function onConnect(Server $server, int $fd, int $reactorId): void {
        if (!$this->tcpRelayModeEnabled()) {
            return;
        }
        $clientInfo = $server->getClientInfo($fd);
        if (!$clientInfo || (int)($clientInfo['server_port'] ?? 0) !== $this->port) {
            return;
        }
        if ($this->shouldRejectNewRelayConnection()) {
            try {
                $server->close($fd);
            } catch (Throwable) {
            }
            return;
        }
        $this->tcpRelayHandler()->handleConnect($fd);
    }

    protected function onReceive(Server $server, int $fd, int $reactorId, string $data): void {
        $clientInfo = $server->getClientInfo($fd);
        if (!$clientInfo) {
            $server->close($fd);
            return;
        }

        $serverPort = (int)($clientInfo['server_port'] ?? 0);
        if ($this->tcpRelayModeEnabled() && $serverPort === $this->port) {
            $this->tcpRelayHandler()->handleReceive($fd, $data);
            return;
        }

        if ($serverPort !== $this->rpcPort) {
            $server->close($fd);
            return;
        }

        $upstream = $this->instanceManager->pickRpcUpstream();
        if (!$upstream) {
            $server->close($fd);
            return;
        }

        Coroutine::create(function () use ($server, $fd, $data, $upstream) {
            $client = new TcpClient(SWOOLE_SOCK_TCP);
            $client->set([
                'timeout' => 10,
                'open_length_check' => true,
                'package_length_type' => 'N',
                'package_length_offset' => 0,
                'package_body_offset' => 4,
                'package_max_length' => 20 * 1024 * 1024,
            ]);
            if (!$client->connect((string)$upstream['host'], (int)$upstream['port'], 10)) {
                if ($server->exist($fd)) {
                    $server->close($fd);
                }
                return;
            }

            try {
                if (!$client->send($data)) {
                    throw new RuntimeException($client->errMsg ?: 'rpc send failed');
                }
                $response = $client->recv();
                if ($response === '' || $response === false) {
                    throw new RuntimeException('rpc upstream response empty');
                }
                if ($server->exist($fd)) {
                    $server->send($fd, $response);
                    $server->close($fd);
                }
            } catch (Throwable) {
                if ($server->exist($fd)) {
                    $server->close($fd);
                }
            } finally {
                $client->close();
            }
        });
    }

    public function dashboardNodes(?array $upstreams = null): array {
        $nodes = [];
        $local = ServerNodeStatusTable::instance()->get('localhost');
        if (is_array($local) && $local) {
            $local['host'] = 'localhost';
            $local['online'] = true;
            $nodes[] = $local;
        } else {
            $nodes[] = $this->buildGatewayClusterNode();
        }

        foreach (ServerNodeStatusTable::instance()->rows() as $host => $status) {
            if ($host === 'localhost' || !is_array($status)) {
                continue;
            }
            $status['host'] = $host;
            $status['online'] = (time() - (int)($status['heart_beat'] ?? 0)) <= 20;
            $nodes[] = $status;
        }

        return $nodes;
    }

    public function dashboardServerStatus(string $token, string $host = '', string $referer = '', string $forwardedProto = ''): array {
        $upstreams = $this->dashboardUpstreams();
        $status = $this->buildDashboardRealtimeStatus($upstreams);
        $status['socket_host'] = $this->buildDashboardSocketHost($host, $referer, $forwardedProto) . '?token=' . rawurlencode($token);
        $status['latest_version'] = App::latestVersion();
        $status['dashboard'] = $this->buildDashboardVersionStatus();
        $status['framework'] = $this->buildFrameworkVersionStatus($upstreams);
        return $status;
    }

    public function dashboardCrontabs(): array {
        $node = $this->buildGatewayNode();
        return (array)($node['tasks'] ?? []);
    }

    public function dashboardCommand(string $command, string $host, array $params = []): Result {
        Console::info("【Gateway】Dashboard命令: {$command}, host={$host}" . $this->formatDashboardParamsLog($params));
        $resolvedNodeHost = $this->resolveGatewayCommandNodeHost($host);
        if ($resolvedNodeHost === 'localhost') {
            $result = $this->dispatchLocalGatewayCommand($command, $params);
        } elseif ($resolvedNodeHost !== null) {
            $result = $this->hasConnectedNode($resolvedNodeHost)
                ? $this->sendCommandToNodeClient($command, $resolvedNodeHost, $params)
                : Result::error('节点不在线');
        } else {
            $result = match ($command) {
                'restart', 'reload' => $this->restartUpstreamsByHost($host),
                'shutdown' => $this->shutdownUpstreamsByHost($host),
                'appoint_update' => $this->dashboardUpdate((string)($params['type'] ?? ''), (string)($params['version'] ?? '')),
                default => Result::error('暂不支持的命令:' . $command),
            };
        }
        if ($result->hasError()) {
            Console::warning("【Gateway】Dashboard命令失败: {$command}, host={$host}, error=" . $result->getMessage());
        } else {
            Console::success("【Gateway】Dashboard命令完成: {$command}, host={$host}");
        }
        return $result;
    }

    protected function isLocalGatewayHost(string $host): bool {
        $host = trim($host);
        if ($host === '') {
            return false;
        }
        return in_array($host, ['localhost', '127.0.0.1'], true);
    }

    protected function resolveGatewayCommandNodeHost(string $host): ?string {
        $host = trim($host);
        if ($host === '') {
            return null;
        }
        if ($this->isLocalGatewayHost($host)) {
            return 'localhost';
        }
        if ($this->hasConnectedNode($host)) {
            return $this->normalizeNodeHost($host);
        }
        foreach (ServerNodeStatusTable::instance()->rows() as $nodeKey => $status) {
            if ($nodeKey === 'localhost' || !is_array($status)) {
                continue;
            }
            if ($nodeKey === $host || (string)($status['ip'] ?? '') === $host) {
                return (string)$nodeKey;
            }
        }
        return null;
    }

    protected function dispatchLocalGatewayCommand(string $command, array $params = []): Result {
        switch ($command) {
            case 'reload':
                Timer::after(1, function () {
                    $this->restartGatewayBusinessPlane();
                });
                return Result::success('业务实例与业务子进程已开始重启');
            case 'restart':
                Timer::after(1, function () {
                    $this->shutdownGateway();
                });
                return Result::success('Gateway 已开始重启');
            case 'shutdown':
                Timer::after(1, function () {
                    $this->shutdownGateway();
                });
                return Result::success('Gateway 已开始关闭');
            case 'appoint_update':
                return $this->dashboardUpdate((string)($params['type'] ?? ''), (string)($params['version'] ?? ''));
            default:
                return Result::error('暂不支持的命令:' . $command);
        }
    }

    public function dashboardUpdate(string $type, string $version): Result {
        $type = trim($type);
        $version = trim($version);
        if ($type === '' || $version === '') {
            return Result::error('更新类型和版本号不能为空');
        }

        Console::info("【Gateway】开始执行升级: type={$type}, version={$version}");
        $taskId = uniqid('gateway_update_', true);
        $slaveHosts = $this->connectedSlaveHosts();
        $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
        $this->logUpdateStage($taskId, $type, $version, 'dispatch_cluster', ['slaves' => count($slaveHosts)]);
        if ($slaveHosts) {
            $this->sendCommandToAllNodeClients('appoint_update', [
                'type' => $type,
                'version' => $version,
                'task_id' => $taskId,
            ]);
        }

        $this->logUpdateStage($taskId, $type, $version, 'apply_local_package');
        if (!App::appointUpdateTo($type, $version, false)) {
            $error = App::getLastUpdateError() ?: '更新失败';
            Console::error("【Gateway】升级失败: type={$type}, version={$version}, error={$error}");
            return Result::error($error);
        }

        $restartSummary = [
            'success_count' => 0,
            'failed_nodes' => [],
        ];

        if ($type !== 'public') {
            $this->logUpdateStage($taskId, $type, $version, 'rolling_upstreams');
            $restartSummary = $this->rollingUpdateManagedUpstreams($type, $version);
        }

        if ($type !== 'public' && $restartSummary['success_count'] > 0 && !$restartSummary['failed_nodes']) {
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        }

        if (in_array($type, ['app', 'framework'], true) && !$restartSummary['failed_nodes']) {
            $this->logUpdateStage($taskId, $type, $version, 'iterate_business_processes');
            $this->iterateGatewayBusinessProcesses();
        }

        $this->logUpdateStage($taskId, $type, $version, 'wait_cluster_result');
        $summary = $this->waitForNodeUpdateSummary($taskId, $slaveHosts, 300);
        $pendingHosts = [];
        $masterState = 'success';
        $masterError = '';
        $totalNodes = max(1, count($slaveHosts) + 1);

        if ($type === 'framework') {
            $masterState = 'pending';
            $masterError = 'Gateway 需重启后才会加载新框架版本';
            $pendingHosts[] = SERVER_HOST;
        }

        $payload = [
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
            'total_nodes' => $totalNodes,
            'success_count' => $summary['success'] + ($masterState === 'success' ? 1 : 0),
            'failed_count' => count($summary['failed_nodes']) + count($restartSummary['failed_nodes']) + ($masterState === 'failed' ? 1 : 0),
            'pending_count' => count($summary['pending_hosts']) + count($pendingHosts),
            'failed_nodes' => array_merge($summary['failed_nodes'], $restartSummary['failed_nodes']),
            'pending_hosts' => array_merge($summary['pending_hosts'], $pendingHosts),
            'master' => [
                'host' => SERVER_HOST,
                'state' => $masterState,
                'error' => $masterError,
            ],
        ];
        $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);

        $this->pushDashboardStatus();
        if ($payload['failed_nodes']) {
            Console::warning("【Gateway】升级完成但存在失败实例: type={$type}, version={$version}, success={$payload['success_count']}, failed=" . count($payload['failed_nodes']));
        } else {
            Console::success("【Gateway】升级完成: type={$type}, version={$version}, success={$payload['success_count']}");
        }
        $this->logUpdateStage($taskId, $type, $version, 'completed', [
            'success' => (int)$payload['success_count'],
            'failed' => (int)$payload['failed_count'],
            'pending' => (int)$payload['pending_count'],
        ]);
        if ($payload['failed_nodes']) {
            return Result::error('部分节点升级失败', 'SERVICE_ERROR', $payload);
        }
        return Result::success($payload);
    }

    protected function logUpdateStage(string $taskId, string $type, string $version, string $stage, array $extra = []): void {
        $parts = [
            "task={$taskId}",
            "type={$type}",
            "version={$version}",
            "stage={$stage}",
        ];
        foreach ($extra as $key => $value) {
            $parts[] = $key . '=' . (is_scalar($value) ? (string)$value : JsonHelper::toJson($value));
        }
        Console::info('【Gateway】升级状态机: ' . implode(', ', $parts));
    }

    protected function iterateGatewayBusinessProcesses(): void {
        if (!$this->subProcessManager || (!$this->subProcessManager->hasProcess('CrontabManager') && !$this->subProcessManager->hasProcess('RedisQueue'))) {
            return;
        }
        Console::info('【Gateway】开始迭代业务子进程: CrontabManager, RedisQueue');
        $this->subProcessManager->iterateBusinessProcesses();
    }

    protected function restartGatewayBusinessPlane(): array {
        Console::info('【Gateway】开始重启业务平面');
        Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        $this->iterateGatewayBusinessProcesses();
        $summary = $this->restartManagedUpstreams();
        $this->pushDashboardStatus();
        if ($summary['failed_nodes']) {
            Console::warning("【Gateway】业务平面重启存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes']));
        } else {
            Console::success("【Gateway】业务平面重启完成: success={$summary['success_count']}");
        }
        return $summary;
    }

    protected function handleNodeSocketMessage(Server $server, Frame $frame): void {
        if (!JsonHelper::is($frame->data)) {
            return;
        }

        $data = JsonHelper::recover($frame->data);
        $event = (string)($data['event'] ?? '');
        switch ($event) {
            case 'slave_node_report':
                $host = (string)($data['data']['host'] ?? $data['data'] ?? '');
                $role = (string)($data['data']['role'] ?? NODE_ROLE_SLAVE);
                if ($host === '') {
                    return;
                }
                if ($this->addNodeClient($frame->fd, $host, $role)) {
                    $server->push($frame->fd, JsonHelper::toJson(['event' => 'slave_node_report_response', 'data' => $frame->fd]));
                    $this->pushConsoleSubscription($frame->fd);
                }
                break;
            case 'node_heart_beat':
                $host = (string)($data['data']['host'] ?? '');
                $status = (array)($data['data']['status'] ?? []);
                if ($host !== '' && $status) {
                    ServerNodeStatusTable::instance()->set($host, $status);
                }
                if ($server->isEstablished($frame->fd)) {
                    $server->push($frame->fd, '::pong');
                }
                break;
            case 'node_update_state':
                $payload = (array)($data['data'] ?? []);
                $taskId = (string)($payload['task_id'] ?? '');
                $host = (string)($payload['host'] ?? '');
                if ($taskId !== '' && $host !== '') {
                    Runtime::instance()->set($this->nodeUpdateTaskStateKey($taskId, $host), $payload);
                }
                if (!empty($payload['message'])) {
                    Console::info((string)$payload['message'], false);
                }
                break;
            case 'console_log':
                $this->acceptConsolePayload((array)($data['data'] ?? []));
                break;
            default:
                break;
        }
    }

    protected function startClusterControlPlane(): void {
        if ($this->clusterControlStarted) {
            return;
        }
        $this->clusterControlStarted = true;

        if ($this->dashboardEnabled()) {
            $this->refreshLocalGatewayNodeStatus();
            $this->clusterStatusTimerId = Timer::tick(5000, function () {
                $this->pruneDisconnectedNodeClients();
                if ($this->dashboardClients) {
                    $this->pushDashboardStatus();
                }
            });
            return;
        }

        Coroutine::create(function () {
            $this->runSlaveGatewayBridge();
        });
    }

    protected function runSlaveGatewayBridge(): void {
        while (true) {
            if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                $this->masterGatewaySocket = null;
                return;
            }
            try {
                $socket = Manager::instance()->getMasterSocketConnection();
                if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                    try {
                        $socket->close();
                    } catch (Throwable) {
                    }
                    $this->masterGatewaySocket = null;
                    return;
                }
                $this->masterGatewaySocket = $socket;
                $socket->push(JsonHelper::toJson(['event' => 'slave_node_report', 'data' => [
                    'host' => APP_NODE_ID,
                    'ip' => SERVER_HOST,
                    'role' => SERVER_ROLE,
                ]]));
                $sendHeartbeat = function () use ($socket): void {
                    $socket->push(JsonHelper::toJson(['event' => 'node_heart_beat', 'data' => [
                        'host' => APP_NODE_ID,
                        'status' => $this->buildGatewayClusterNode(),
                    ]]));
                };
                $sendHeartbeat();
                $heartbeatTimerId = Timer::tick(5000, function () use ($sendHeartbeat, $socket) {
                    try {
                        $sendHeartbeat();
                    } catch (Throwable) {
                        try {
                            $socket->close();
                        } catch (Throwable) {
                        }
                    }
                });
                while (true) {
                    if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                        Timer::clear($heartbeatTimerId);
                        try {
                            $socket->close();
                        } catch (Throwable) {
                        }
                        $this->masterGatewaySocket = null;
                        return;
                    }
                    $reply = $socket->recv();
                    if ($reply === false) {
                        Timer::clear($heartbeatTimerId);
                        $socket->close();
                        $this->masterGatewaySocket = null;
                        break;
                    }
                    if (!$reply || $reply->data === '' || $reply->data === '::pong') {
                        continue;
                    }
                    $this->handleMasterGatewayMessage($socket, (string)$reply->data);
                }
            } catch (Throwable $throwable) {
                $this->masterGatewaySocket = null;
                if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                    return;
                }
                Console::warning("【Gateway】与master gateway连接失败:" . $throwable->getMessage(), false);
            }
            if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                return;
            }
            Coroutine::sleep(1);
        }
    }

    protected function handleMasterGatewayMessage(object $socket, string $payload): void {
        if (!JsonHelper::is($payload)) {
            return;
        }
        $data = JsonHelper::recover($payload);
        $event = (string)($data['event'] ?? '');
        if ($event === 'slave_node_report_response') {
            Console::success('【Gateway】已与master gateway建立连接,客户端ID:' . ($data['data'] ?? ''), false);
            return;
        }
        if ($event === 'console_subscription') {
            ConsoleRelay::setRemoteSubscribed((bool)($data['data']['enabled'] ?? false));
            return;
        }
        if ($event === 'console') {
            return;
        }
        if ($event !== 'command') {
            return;
        }

        $command = (string)($data['data']['command'] ?? '');
        $params = (array)($data['data']['params'] ?? []);
        switch ($command) {
            case 'shutdown':
                Console::warning('【Gateway】收到master关闭指令', false);
                $this->shutdownGateway();
                break;
            case 'reload':
                Console::info('【Gateway】收到master业务重载指令', false);
                $this->restartGatewayBusinessPlane();
                break;
            case 'restart':
                Console::warning('【Gateway】收到master重启指令', false);
                $this->shutdownGateway();
                break;
            case 'appoint_update':
                $this->handleRemoteAppointUpdate($socket, $params);
                break;
            default:
                Console::warning("【Gateway】暂不支持的master命令:{$command}", false);
        }
    }

    protected function handleRemoteAppointUpdate(object $socket, array $params): void {
        $taskId = (string)($params['task_id'] ?? '');
        $type = (string)($params['type'] ?? '');
        $version = (string)($params['version'] ?? '');
        if ($taskId === '' || $type === '' || $version === '') {
            return;
        }

        $statePayload = [
            'event' => 'node_update_state',
            'data' => [
                'task_id' => $taskId,
                'host' => APP_NODE_ID,
                'type' => $type,
                'version' => $version,
                'updated_at' => time(),
            ]
        ];
        $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
            'data' => [
                'state' => 'running',
                'message' => "【" . SERVER_HOST . "】开始更新 {$type} => {$version}",
            ]
        ])));

        $result = $this->dashboardUpdate($type, $version);
        if ($result->hasError()) {
            $error = $result->getMessage() ?: '未知原因';
            $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                'data' => [
                    'state' => 'failed',
                    'error' => $error,
                    'message' => "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}",
                    'updated_at' => time(),
                ]
            ])));
            return;
        }

        $resultData = (array)($result->getData() ?: []);
        $masterState = (string)($resultData['master']['state'] ?? 'success');
        $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
            'data' => [
                'state' => $masterState === 'pending' ? 'pending' : 'success',
                'message' => $masterState === 'pending'
                    ? "【" . SERVER_HOST . "】版本更新已完成，等待重启生效:{$type} => {$version}"
                    : "【" . SERVER_HOST . "】版本更新成功:{$type} => {$version}",
                'updated_at' => time(),
            ]
        ])));
    }

    protected function shutdownGateway(): void {
        if ($this->gatewayShutdownScheduled) {
            return;
        }
        $this->gatewayShutdownScheduled = true;
        $this->prepareGatewayShutdown();

        $deadline = microtime(true) + 15;
        Timer::tick(200, function (int $timerId) use ($deadline) {
            if (!$this->managedUpstreamPortsReleased() && microtime(true) < $deadline) {
                return;
            }
            Timer::clear($timerId);
            $this->server->shutdown();
        });
    }

    protected function closeProxyHttpClientPools(): void {
        $this->httpProxyHandler()->closeAllPooledClients();
    }

    protected function waitForGatewayPortsReleased(int $timeoutSeconds = 15, int $intervalMs = 200): void {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $ports = array_values(array_unique(array_filter([
            $this->nginxProxyModeEnabled() ? 0 : $this->port,
            $this->rpcPort,
            ($this->tcpRelayModeEnabled() || $this->nginxProxyModeEnabled()) ? $this->controlPort() : 0,
        ], static fn(int $port) => $port > 0)));
        if (!$ports) {
            return;
        }

        $logged = false;
        while (microtime(true) < $deadline) {
            $occupied = false;
            foreach ($ports as $port) {
                if (CoreServer::isListeningPortInUse($port)) {
                    $occupied = true;
                    break;
                }
            }
            if (!$occupied) {
                if ($logged) {
                    Console::success("【Gateway】启动前端口已释放: " . implode(', ', $ports));
                }
                return;
            }
            if (!$logged) {
                Console::warning("【Gateway】启动前等待端口释放: " . implode(', ', $ports));
                $logged = true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        if ($logged) {
            Console::warning("【Gateway】等待端口释放超时，继续尝试启动: " . implode(', ', $ports));
        }
    }

    protected function createGatewaySocketServer(int $timeoutSeconds = 10, int $intervalMs = 200): Server {
        $listenPort = ($this->tcpRelayModeEnabled() || $this->nginxProxyModeEnabled()) ? $this->controlPort() : $this->port;
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $lastException = null;
        $logged = false;
        do {
            try {
                if ($logged) {
                    Console::success("【Gateway】监听端口抢占成功: {$this->host}:{$listenPort}");
                }
                return new Server($this->host, $listenPort, SWOOLE_PROCESS, SWOOLE_SOCK_TCP);
            } catch (SwooleException $exception) {
                $lastException = $exception;
                if (!str_contains($exception->getMessage(), 'Address already in use')) {
                    throw $exception;
                }
                if (!$logged) {
                    Console::warning("【Gateway】监听端口占用，等待重试: {$this->host}:{$listenPort}");
                    $logged = true;
                }
                usleep(max(50, $intervalMs) * 1000);
            }
        } while (microtime(true) < $deadline);

        throw $lastException ?: new RuntimeException("Gateway 监听失败: {$this->host}:{$listenPort}");
    }

    protected function managedUpstreamPortsReleased(): bool {
        $plans = $this->managedUpstreamPlans;
        if (!$plans) {
            return true;
        }

        foreach ($plans as $plan) {
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            $rpcPort = (int)($plan['rpc_port'] ?? 0);
            if ($port > 0 && $this->launcher && $this->launcher->isListening($host, $port, 0.2)) {
                return false;
            }
            if ($rpcPort > 0 && $this->launcher && $this->launcher->isListening($host, $rpcPort, 0.2)) {
                return false;
            }
        }

        return true;
    }

    protected function clearHousekeepingTimer(): void {
        if ($this->housekeepingTimerId) {
            Timer::clear($this->housekeepingTimerId);
            $this->housekeepingTimerId = null;
        }
    }

    protected function clearClusterStatusTimer(): void {
        if ($this->clusterStatusTimerId) {
            Timer::clear($this->clusterStatusTimerId);
            $this->clusterStatusTimerId = null;
        }
    }

    protected function clearPendingManagedRecycleWatchers(): void {
        foreach ($this->pendingManagedRecycleWatchers as $key => $timerId) {
            Timer::clear($timerId);
            unset($this->pendingManagedRecycleWatchers[$key]);
        }
    }

    protected function prepareGatewayShutdown(): void {
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        Runtime::instance()->serverIsAlive(false);
        $this->closeProxyHttpClientPools();
        $this->stopLocalIpcServer();
        $this->clearHousekeepingTimer();
        $this->clearClusterStatusTimer();
        $this->clearPendingManagedRecycleWatchers();
        isset($this->subProcessManager) && $this->subProcessManager->shutdown();
        $this->shutdownManagedUpstreams();
    }

    public function server(): Server {
        return $this->server;
    }

    public function serverConfig(): array {
        return Config::server();
    }

    public function businessPort(): int {
        return $this->port;
    }

    public function trafficMode(): string {
        $mode = strtolower(trim($this->configuredTrafficMode));
        return in_array($mode, ['http', 'tcp', 'nginx'], true) ? $mode : 'nginx';
    }

    public function tcpRelayModeEnabled(): bool {
        return $this->trafficMode() === 'tcp';
    }

    public function nginxProxyModeEnabled(): bool {
        return $this->trafficMode() === 'nginx';
    }

    public function controlPort(): int {
        return ($this->tcpRelayModeEnabled() || $this->nginxProxyModeEnabled())
            ? max(1, $this->controlBindPort ?: ($this->port + 1000))
            : $this->port;
    }

    protected function resolvedDashboardPort(): int {
        return $this->dashboardEnabled() ? $this->port : 0;
    }

    public function internalControlHost(): string {
        return in_array($this->host, ['0.0.0.0', '::', ''], true) ? '127.0.0.1' : $this->host;
    }

    public function shouldRoutePathToControlPlane(string $path): bool {
        return $path === '/dashboard.socket'
            || str_starts_with($path, '/_gateway')
            || str_starts_with($path, '/~');
    }

    protected function httpProxyHandler(): GatewayHttpProxyHandler {
        if (!$this->httpProxyHandler instanceof GatewayHttpProxyHandler) {
            $this->httpProxyHandler = new GatewayHttpProxyHandler($this, $this->instanceManager);
        }
        return $this->httpProxyHandler;
    }

    protected function tcpRelayHandler(): GatewayTcpRelayHandler {
        if (!$this->tcpRelayHandler instanceof GatewayTcpRelayHandler) {
            $this->tcpRelayHandler = new GatewayTcpRelayHandler($this, $this->instanceManager);
        }
        return $this->tcpRelayHandler;
    }

    protected function nginxProxyHandler(): GatewayNginxProxyHandler {
        if (!$this->nginxProxyHandler instanceof GatewayNginxProxyHandler) {
            $this->nginxProxyHandler = new GatewayNginxProxyHandler($this, $this->instanceManager);
        }
        return $this->nginxProxyHandler;
    }

    protected function syncNginxProxyTargets(?string $reason = null): void {
        $handler = $this->nginxProxyHandler();
        if (!$handler->enabled()) {
            return;
        }
        try {
            $result = $handler->sync($reason);
            $runtimeMeta = (array)($result['runtime_meta'] ?? []);
            if (($result['runtime_meta_changed'] ?? false) && $runtimeMeta) {
                Console::info('【Gateway】探测到 nginx: ' . ((string)($runtimeMeta['bin'] ?? 'nginx')));
                Console::info('【Gateway】探测到 nginx conf-path: ' . ((string)($runtimeMeta['conf_path'] ?? '')));
                Console::info('【Gateway】探测到 nginx conf-dir: ' . ((string)($runtimeMeta['conf_dir'] ?? '')));
            }
            $message = "【Gateway】nginx转发配置已同步";
            if (!empty($result['reason'])) {
                $message .= ': reason=' . $result['reason'];
            }
            if (!empty($result['reloaded'])) {
                $message .= ', reloaded=yes';
            } elseif (!empty($result['tested'])) {
                $message .= ', tested=yes';
            }
            $paths = array_filter([
                (string)($result['global_file'] ?? ''),
                (string)($result['upstream_file'] ?? ''),
                (string)($result['server_file'] ?? ''),
            ]);
            if ($paths) {
                $message .= ', files=' . implode(' | ', $paths);
            }
            Console::info($message);
        } catch (Throwable $throwable) {
            Console::warning('【Gateway】nginx转发配置同步失败: ' . $throwable->getMessage());
        }
    }

    protected function reloadGateway(bool $restartManagedUpstreams = true): void {
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        Console::info('【Gateway】正在重启服务');
        $this->iterateGatewayBusinessProcesses();
        if ($restartManagedUpstreams) {
            $summary = $this->restartManagedUpstreams();
            if ($summary['failed_nodes']) {
                Console::warning("【Gateway】业务实例重启存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes']));
            }
        }
        $this->server->reload();
    }

    protected function handleGatewayProcessCommand(string $command, array $params, object $socket): bool {
        switch ($command) {
            case 'shutdown':
                $socket->push("【" . SERVER_HOST . "】start shutdown");
                $this->shutdownGateway();
                return true;
            case 'reload':
                $socket->push("【" . SERVER_HOST . "】start business reload");
                $this->restartGatewayBusinessPlane();
                return true;
            case 'restart':
                $socket->push("【" . SERVER_HOST . "】start restart");
                $this->shutdownGateway();
                return true;
            case 'appoint_update':
                $taskId = (string)($params['task_id'] ?? '');
                $type = (string)($params['type'] ?? '');
                $version = (string)($params['version'] ?? '');
                $statePayload = [
                    'event' => 'node_update_state',
                    'data' => [
                        'task_id' => $taskId,
                        'host' => APP_NODE_ID,
                        'type' => $type,
                        'version' => $version,
                        'updated_at' => time(),
                    ]
                ];
                $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                    'data' => [
                        'state' => 'running',
                        'message' => "【" . SERVER_HOST . "】开始更新 {$type} => {$version}",
                    ]
                ])));
                $result = $this->dashboardUpdate($type, $version);
                if ($result->hasError()) {
                    $error = (string)($result->getMessage() ?: '未知原因');
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => 'failed',
                            'error' => $error,
                            'message' => "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}",
                            'updated_at' => time(),
                        ]
                    ])));
                } else {
                    $resultData = (array)($result->getData() ?: []);
                    $masterState = (string)($resultData['master']['state'] ?? 'success');
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => $masterState === 'pending' ? 'pending' : 'success',
                            'message' => $masterState === 'pending'
                                ? "【" . SERVER_HOST . "】版本更新已完成，等待重启生效:{$type} => {$version}"
                                : "【" . SERVER_HOST . "】版本更新成功:{$type} => {$version}",
                            'updated_at' => time(),
                        ]
                    ])));
                }
                return true;
            default:
                return false;
        }
    }

    protected function buildGatewayHeartbeatStatus(array $status): array {
        $status = $this->composeGatewayNodeRuntimeStatus($status, $this->dashboardUpstreams());
        return array_replace_recursive($status, [
            'name' => 'Gateway',
            'script' => 'gateway',
            'ip' => SERVER_HOST,
            'port' => $this->port,
            'role' => SERVER_ROLE,
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_proxy',
            'manager_pid' => (int)($this->server->manager_pid ?? $this->serverManagerPid),
            'master_pid' => (int)($this->server->master_pid ?? $this->serverMasterPid),
            'fingerprint' => APP_FINGERPRINT . ':gateway',
        ]);
    }

    protected function refreshLocalGatewayNodeStatus(): void {
        ServerNodeStatusTable::instance()->set('localhost', $this->buildGatewayClusterNode());
    }

    protected function buildRemoteGatewayNodes(): array {
        $nodes = [];
        foreach (ServerNodeStatusTable::instance()->rows() as $host => $status) {
            if ($host === 'localhost' || !is_array($status)) {
                continue;
            }
            $status['online'] = (time() - (int)($status['heart_beat'] ?? 0)) <= 20;
            $nodes[] = $status;
        }
        return $nodes;
    }

    protected function buildGatewayClusterNode(): array {
        $node = $this->buildGatewayNode();
        $node['host'] = 'localhost';
        $node['id'] = APP_NODE_ID;
        $node['appid'] = App::id() ?: 'scf_app';
        $node['heart_beat'] = time();
        $node['master_pid'] = (int)($this->server->master_pid ?? $this->serverMasterPid);
        $node['manager_pid'] = (int)($this->server->manager_pid ?? $this->serverManagerPid);
        $node['fingerprint'] = APP_FINGERPRINT;
        return $node;
    }

    protected function addNodeClient(int $fd, string $host, string $role): bool {
        return ServerNodeTable::instance()->set($host, [
            'host' => $host,
            'socket_fd' => $fd,
            'connect_time' => time(),
            'role' => $role,
        ]);
    }

    protected function removeNodeClient(int $fd): bool {
        $deleted = false;
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if ((int)($node['socket_fd'] ?? 0) !== $fd) {
                continue;
            }
            ServerNodeTable::instance()->delete((string)$node['host']);
            ServerNodeStatusTable::instance()->delete((string)$node['host']);
            $deleted = true;
        }
        return $deleted;
    }

    protected function pruneDisconnectedNodeClients(): void {
        foreach (ServerNodeTable::instance()->rows() as $node) {
            $fd = (int)($node['socket_fd'] ?? 0);
            if ($fd > 0 && $this->server->exist($fd) && $this->server->isEstablished($fd)) {
                continue;
            }
            $this->removeNodeClient($fd);
        }
    }

    protected function hasConnectedNode(string $host): bool {
        return ServerNodeTable::instance()->exist($this->normalizeNodeHost($host));
    }

    protected function sendCommandToNodeClient(string $command, string $host, array $params = []): Result {
        $normalized = $this->normalizeNodeHost($host);
        $node = ServerNodeTable::instance()->get($normalized);
        if (!$node) {
            return Result::error('节点不存在:' . $host);
        }
        $fd = (int)($node['socket_fd'] ?? 0);
        if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
            $this->removeNodeClient($fd);
            return Result::error('节点不在线');
        }
        $this->server->push($fd, JsonHelper::toJson(['event' => 'command', 'data' => [
            'command' => $command,
            'params' => $params,
        ]]));
        return Result::success();
    }

    protected function sendCommandToAllNodeClients(string $command, array $params = []): int {
        $success = 0;
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                continue;
            }
            $fd = (int)($node['socket_fd'] ?? 0);
            if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
                $this->removeNodeClient($fd);
                continue;
            }
            if ($this->server->push($fd, JsonHelper::toJson(['event' => 'command', 'data' => [
                'command' => $command,
                'params' => $params,
            ]]))) {
                $success++;
            }
        }
        return $success;
    }

    protected function connectedSlaveHosts(): array {
        $hosts = [];
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                continue;
            }
            $fd = (int)($node['socket_fd'] ?? 0);
            if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
                continue;
            }
            $hosts[] = (string)$node['host'];
        }
        return array_values(array_unique($hosts));
    }

    protected function waitForNodeUpdateSummary(string $taskId, array $hosts, int $timeout): array {
        if (!$hosts) {
            return [
                'finished' => true,
                'success' => 0,
                'failed_nodes' => [],
                'pending_hosts' => [],
            ];
        }

        $summary = $this->summarizeNodeUpdateTask($taskId, $hosts);
        if ($summary['finished']) {
            return $summary;
        }

        $waitCh = new Channel(1);
        $round = 1;
        Timer::tick(5000, function (int $timerId) use ($taskId, $hosts, $timeout, &$summary, &$round, $waitCh) {
            $summary = $this->summarizeNodeUpdateTask($taskId, $hosts);
            if ($summary['finished'] || $round >= max(1, (int)($timeout / 5))) {
                Timer::clear($timerId);
                $waitCh->push(true);
            }
            $round++;
        });
        $waitCh->pop($timeout + 3);
        return $summary;
    }

    protected function summarizeNodeUpdateTask(string $taskId, array $hosts): array {
        $success = 0;
        $failedNodes = [];
        $pendingHosts = [];
        foreach ($hosts as $host) {
            $state = Runtime::instance()->get($this->nodeUpdateTaskStateKey($taskId, $host));
            if (!$state) {
                $pendingHosts[] = $host;
                continue;
            }
            $current = $state['state'] ?? '';
            if ($current === 'success') {
                $success++;
                continue;
            }
            if ($current === 'failed') {
                $failedNodes[] = [
                    'host' => $host,
                    'error' => $state['error'] ?? '',
                    'message' => $state['message'] ?? '',
                    'updated_at' => $state['updated_at'] ?? 0,
                ];
                continue;
            }
            $pendingHosts[] = $host;
        }
        return [
            'finished' => empty($pendingHosts),
            'success' => $success,
            'failed_nodes' => $failedNodes,
            'pending_hosts' => $pendingHosts,
        ];
    }

    protected function clearNodeUpdateTaskStates(string $taskId, array $hosts): void {
        foreach ($hosts as $host) {
            Runtime::instance()->delete($this->nodeUpdateTaskStateKey($taskId, $host));
        }
    }

    protected function nodeUpdateTaskStateKey(string $taskId, string $host): string {
        return 'NODE_UPDATE_TASK:' . md5($taskId . ':' . $host);
    }

    protected function normalizeNodeHost(string $host): string {
        if (in_array($host, ['localhost', '127.0.0.1'], true)) {
            return 'localhost';
        }
        return $host;
    }

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
                    $this->restartGatewayBusinessPlane();
                });
                break;
            case 'restartAll':
                Coroutine::create(function () use ($frame) {
                    Console::info("【Gateway】Dashboard Socket命令: {$frame->data}");
                    $this->sendCommandToAllNodeClients('restart');
                    $this->pushDashboardEvent([
                        'event' => 'console',
                        'message' => ['data' => '已向所有子节点发送重启指令，当前 Gateway 开始重启'],
                        'time' => Console::timestamp(),
                        'node' => SERVER_HOST,
                    ], $frame->fd);
                    $this->shutdownGateway();
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

    protected function buildDashboardRealtimeStatus(?array $upstreams = null): array {
        $upstreams ??= $this->dashboardUpstreams();
        $nodes = $this->dashboardNodes($upstreams);
        $master = 0;
        $slave = 0;
        foreach ($nodes as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                $master++;
            } else {
                $slave++;
            }
        }

        $appInfo = App::info()?->toArray() ?: [];
        $appInfo['version'] = $this->resolveAppVersion($appInfo);
        $appInfo['public_version'] = $this->resolvePublicVersion($appInfo);
        $logger = Log::instance();

        return [
            'event' => 'server_status',
            'info' => [...$appInfo, 'master' => $master, 'slave' => $slave],
            'servers' => $nodes,
            'upstreams' => $upstreams,
            'logs' => [
                'error' => [
                    'total' => $logger->count('error', date('Y-m-d')),
                    'list' => []
                ],
                'info' => [
                    'total' => $logger->count('info', date('Y-m-d')),
                    'list' => []
                ],
                'slow' => [
                    'total' => $logger->count('slow', date('Y-m-d')),
                    'list' => []
                ]
            ]
        ];
    }

    protected function dashboardUpstreams(): array {
        $this->instanceManager->reload();
        $snapshot = $this->instanceManager->snapshot();
        $instances = [];
        foreach ($snapshot['generations'] ?? [] as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                $runtimeStatus = $this->fetchUpstreamRuntimeStatus($instance);
                $this->instanceManager->updateInstanceRuntimeStatus(
                    (string)($instance['host'] ?? '127.0.0.1'),
                    (int)($instance['port'] ?? 0),
                    $runtimeStatus
                );
                $instances[] = $this->buildUpstreamNode($generation, $instance, $runtimeStatus);
            }
        }
        return $instances;
    }

    protected function refreshManagedUpstreamRuntimeStates(): void {
        $snapshot = $this->instanceManager->snapshot();
        foreach (($snapshot['generations'] ?? []) as $generation) {
            if (!in_array((string)($generation['status'] ?? ''), ['active', 'draining', 'prepared'], true)) {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((($instance['metadata']['managed'] ?? false) !== true)) {
                    continue;
                }
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($port <= 0) {
                    continue;
                }
                $runtimeStatus = $this->fetchUpstreamRuntimeStatus($instance);
                $this->instanceManager->updateInstanceRuntimeStatus($host, $port, $runtimeStatus);
                $managerPid = (int)($runtimeStatus['manager_pid'] ?? 0);
                if ($managerPid > 0) {
                    $this->instanceManager->mergeInstanceMetadata($host, $port, ['pid' => $managerPid]);
                    $this->mergeManagedPlanMetadata($host, $port, ['pid' => $managerPid]);
                }
            }
        }
    }

    protected function syncUpstreamSupervisorState(): void {
        if (!$this->upstreamSupervisor || !$this->launcher || $this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        $now = time();
        $syncReason = $this->pendingUpstreamSupervisorSyncReason;
        if ($syncReason === null && $this->lastUpstreamSupervisorSyncAt > 0 && ($now - $this->lastUpstreamSupervisorSyncAt) < self::UPSTREAM_SUPERVISOR_SYNC_INTERVAL_SECONDS) {
            return;
        }
        $instances = $this->buildUpstreamSupervisorSyncInstances();
        if (!$this->upstreamSupervisor->sendCommand([
            'action' => 'sync_instances',
            'instances' => $instances,
        ])) {
            if ($syncReason === 'restart') {
                Console::warning('【Gateway】UpstreamSupervisor 重建后状态同步失败');
            } elseif ($syncReason === 'initial') {
                Console::warning('【Gateway】UpstreamSupervisor 初始状态同步失败');
            } else {
                Console::warning('【Gateway】业务实例管理器状态同步失败');
            }
            return;
        }
        $this->lastUpstreamSupervisorSyncAt = $now;
        if ($syncReason === 'restart') {
            Console::success('【Gateway】UpstreamSupervisor 已异常重建，状态已重新同步: pid=' . $this->lastObservedUpstreamSupervisorPid . ', instances=' . count($instances));
        } elseif ($syncReason === 'initial') {
            Console::info('【Gateway】UpstreamSupervisor 状态已同步: pid=' . $this->lastObservedUpstreamSupervisorPid . ', instances=' . count($instances));
        }
        $this->pendingUpstreamSupervisorSyncReason = null;
    }

    protected function observeUpstreamSupervisorProcess(): void {
        if (!$this->upstreamSupervisor) {
            return;
        }
        $pid = (int)(Runtime::instance()->get(Key::RUNTIME_UPSTREAM_SUPERVISOR_PID) ?? 0);
        $startedAt = (int)(Runtime::instance()->get(Key::RUNTIME_UPSTREAM_SUPERVISOR_STARTED_AT) ?? 0);
        if ($pid <= 0 || $startedAt <= 0) {
            return;
        }
        if ($this->lastObservedUpstreamSupervisorPid <= 0 || $this->lastObservedUpstreamSupervisorStartedAt <= 0) {
            $this->lastObservedUpstreamSupervisorPid = $pid;
            $this->lastObservedUpstreamSupervisorStartedAt = $startedAt;
            $this->pendingUpstreamSupervisorSyncReason ??= 'initial';
            return;
        }
        if ($pid === $this->lastObservedUpstreamSupervisorPid && $startedAt === $this->lastObservedUpstreamSupervisorStartedAt) {
            return;
        }
        Console::warning(
            '【Gateway】UpstreamSupervisor 进程PID发生变化，疑似异常重建: old='
            . $this->lastObservedUpstreamSupervisorPid . ', new=' . $pid
        );
        $this->lastObservedUpstreamSupervisorPid = $pid;
        $this->lastObservedUpstreamSupervisorStartedAt = $startedAt;
        $this->pendingUpstreamSupervisorSyncReason = 'restart';
    }

    protected function buildUpstreamSupervisorSyncInstances(): array {
        $snapshot = $this->instanceManager->snapshot();
        $instances = [];
        foreach (($snapshot['generations'] ?? []) as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((($instance['metadata']['managed'] ?? false) !== true)) {
                    continue;
                }
                $version = (string)($instance['version'] ?? $generation['version'] ?? '');
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($version === '' || $port <= 0) {
                    continue;
                }
                $rpcPort = (int)($instance['metadata']['rpc_port'] ?? 0);
                $pendingRecycle = $this->isManagedPlanRecyclePending([
                    'version' => $version,
                    'host' => $host,
                    'port' => $port,
                ]);
                $httpAlive = $this->launcher->isListening($host, $port, 0.1) || $this->launcher->isListening('0.0.0.0', $port, 0.1);
                $rpcAlive = $rpcPort <= 0 || $this->launcher->isListening($host, $rpcPort, 0.1) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.1);
                if (!$pendingRecycle && (!$httpAlive || !$rpcAlive)) {
                    continue;
                }
                $instances[] = [
                    'version' => $version,
                    'host' => $host,
                    'port' => $port,
                    'weight' => (int)($instance['weight'] ?? 100),
                    'metadata' => (array)($instance['metadata'] ?? []),
                ];
            }
        }
        return $instances;
    }

    protected function maintainManagedUpstreamHealth(): void {
        if ($this->managedUpstreamSelfHealing || !$this->launcher || !$this->upstreamSupervisor) {
            return;
        }
        if (!App::isReady() || $this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        $now = time();
        if ($this->lastManagedUpstreamHealthCheckAt > 0 && ($now - $this->lastManagedUpstreamHealthCheckAt) < self::ACTIVE_UPSTREAM_HEALTH_CHECK_INTERVAL_SECONDS) {
            return;
        }
        $this->lastManagedUpstreamHealthCheckAt = $now;
        if ($this->pendingManagedRecycles) {
            return;
        }

        $activePlans = $this->activeManagedPlans();
        if (!$activePlans) {
            $this->managedUpstreamHealthState = [];
            return;
        }

        $unhealthyPlans = [];
        $activeKeys = [];
        foreach ($activePlans as $plan) {
            $key = $this->managedPlanKey($plan);
            $activeKeys[$key] = true;

            if ($this->shouldSkipManagedUpstreamHealthCheck($plan, $now)) {
                unset($this->managedUpstreamHealthState[$key]);
                continue;
            }

            $probe = $this->probeManagedUpstreamHealth($plan);
            if ($probe['healthy']) {
                $previous = $this->managedUpstreamHealthState[$key] ?? null;
                if (is_array($previous) && (int)($previous['failures'] ?? 0) > 0) {
                    Console::success("【Gateway】active业务实例健康恢复: " . $this->managedPlanDescriptor($plan));
                }
                unset($this->managedUpstreamHealthState[$key]);
                continue;
            }

            $state = $this->managedUpstreamHealthState[$key] ?? [
                'failures' => 0,
                'reason' => '',
                'last_failed_at' => 0,
            ];
            $state['failures'] = (int)$state['failures'] + 1;
            $state['reason'] = (string)($probe['reason'] ?? 'unknown');
            $state['last_failed_at'] = $now;
            $this->managedUpstreamHealthState[$key] = $state;
            if ((int)($state['failures'] ?? 0) === 1) {
                Console::warning(
                    "【Gateway】active业务实例健康异常(1/" . self::ACTIVE_UPSTREAM_HEALTH_FAILURE_THRESHOLD . '): '
                    . $this->managedPlanDescriptor($plan)
                    . ', reason=' . $state['reason']
                );
            }

            if ($state['failures'] >= self::ACTIVE_UPSTREAM_HEALTH_FAILURE_THRESHOLD) {
                $unhealthyPlans[] = $plan;
            }
        }

        foreach (array_keys($this->managedUpstreamHealthState) as $key) {
            if (!isset($activeKeys[$key])) {
                unset($this->managedUpstreamHealthState[$key]);
            }
        }

        if (!$unhealthyPlans) {
            return;
        }
        if ($this->lastManagedUpstreamSelfHealAt > 0 && ($now - $this->lastManagedUpstreamSelfHealAt) < self::ACTIVE_UPSTREAM_HEALTH_COOLDOWN_SECONDS) {
            return;
        }

        $this->managedUpstreamSelfHealing = true;
        $descriptors = array_map(fn(array $plan) => $this->managedPlanDescriptor($plan), $unhealthyPlans);
        $recoveryPlans = $activePlans;
        Console::warning("【Gateway】检测到active业务实例连续异常，开始自动自愈: " . implode(' | ', $descriptors));
        Coroutine::create(function () use ($unhealthyPlans, $recoveryPlans) {
            try {
                $summary = $this->restartManagedUpstreams($recoveryPlans);
                if ($summary['failed_nodes']) {
                    Console::warning("【Gateway】自动自愈存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes']));
                } else {
                    Console::success("【Gateway】自动自愈完成: success={$summary['success_count']}");
                }
                foreach ($unhealthyPlans as $plan) {
                    unset($this->managedUpstreamHealthState[$this->managedPlanKey($plan)]);
                }
            } finally {
                $this->lastManagedUpstreamSelfHealAt = time();
                $this->managedUpstreamSelfHealing = false;
            }
        });
    }

    protected function shouldSkipManagedUpstreamHealthCheck(array $plan, int $now): bool {
        $startedAt = (int)($plan['metadata']['started_at'] ?? 0);
        if ($startedAt > 0 && ($now - $startedAt) < self::ACTIVE_UPSTREAM_HEALTH_STARTUP_GRACE_SECONDS) {
            return true;
        }
        return $this->isManagedPlanRecyclePending($plan);
    }

    protected function probeManagedUpstreamHealth(array $plan): array {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        if ($port <= 0) {
            return ['healthy' => false, 'reason' => 'invalid_port'];
        }

        $httpListening = $this->launcher->isListening($host, $port, 0.2) || $this->launcher->isListening('0.0.0.0', $port, 0.2);
        if (!$httpListening) {
            return ['healthy' => false, 'reason' => 'http_port_down'];
        }

        if ($rpcPort > 0) {
            $rpcListening = $this->launcher->isListening($host, $rpcPort, 0.2) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.2);
            if (!$rpcListening) {
                return ['healthy' => false, 'reason' => 'rpc_port_down'];
            }
        }

        $status = $this->fetchUpstreamHealthStatus($plan);
        if (!$status) {
            return ['healthy' => false, 'reason' => 'health_status_unreachable'];
        }
        if (!(bool)($status['server_is_alive'] ?? false)) {
            return ['healthy' => false, 'reason' => 'server_not_alive'];
        }
        if (!(bool)($status['server_is_ready'] ?? false)) {
            return ['healthy' => false, 'reason' => 'server_not_ready'];
        }
        if ((bool)($status['server_is_draining'] ?? false)) {
            return ['healthy' => false, 'reason' => 'server_draining'];
        }

        return ['healthy' => true, 'reason' => 'ok'];
    }

    protected function managedPlanKey(array $plan): string {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        return (string)($plan['version'] ?? '') . '@' . $host . ':' . $port;
    }

    protected function buildGatewayNode(?array $upstreams = null): array {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $stats = isset($this->server) ? $this->server->stats() : [];
        $node = [
            'host' => 'localhost',
            'name' => 'Gateway',
            'script' => 'gateway',
            'ip' => SERVER_HOST,
            'port' => $this->port,
            'socketPort' => $this->resolvedDashboardPort(),
            'online' => true,
            'role' => SERVER_ROLE,
            'started' => $this->startedAt,
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_proxy',
            'app_version' => $appVersion,
            'public_version' => $publicVersion,
            'framework_build_version' => FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => file_exists(SCF_ROOT . '/build/update.pack'),
            'swoole_version' => swoole_version(),
            'scf_version' => SCF_COMPOSER_VERSION,
            'manager_pid' => $this->serverManagerPid ?: '--',
            'restart_times' => Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0,
            'server_stats' => [
                'connection_num' => (int)($stats['connection_num'] ?? 0),
                'long_connection_num' => (int)($stats['connection_num'] ?? 0),
                'tasking_num' => (int)($stats['tasking_num'] ?? 0),
                'idle_worker_num' => (int)($stats['idle_worker_num'] ?? 0),
            ],
            'http_request_count_today' => 0,
            'http_request_count_current' => 0,
            'http_request_processing' => 0,
            'mysql_execute_count' => 0,
            'http_request_reject' => 0,
            'cpu_num' => function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0,
            'memory_usage' => $this->buildEmptyMemoryUsage(),
            'threads' => (int)($stats['worker_num'] ?? 0),
            'tables' => [],
            'tasks' => [],
            'fingerprint' => APP_FINGERPRINT . ':gateway',
        ];
        $heartbeatStatus = ServerNodeStatusTable::instance()->get('localhost');
        if (is_array($heartbeatStatus) && $heartbeatStatus) {
            $node = array_replace_recursive($node, $heartbeatStatus);
        }
        $upstreams ??= $this->dashboardUpstreams();
        if (!(is_array($heartbeatStatus) && $heartbeatStatus)) {
            $node = $this->composeGatewayNodeRuntimeStatus($node, $upstreams);
        }
        $node = array_replace_recursive($node, [
            'host' => 'localhost',
            'name' => 'Gateway',
            'script' => 'gateway',
            'ip' => SERVER_HOST,
            'port' => $this->port,
            'socketPort' => $this->resolvedDashboardPort(),
            'role' => SERVER_ROLE,
            'online' => true,
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_proxy',
            'manager_pid' => (int)($this->server->manager_pid ?? $this->serverManagerPid) ?: '--',
            'fingerprint' => APP_FINGERPRINT . ':gateway',
        ]);
        return $node;
    }

    protected function composeGatewayNodeRuntimeStatus(array $status, array $upstreams): array {
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        $businessOverlay = $this->buildGatewayBusinessOverlay($upstreams);
        if ($businessOverlay) {
            $status = array_replace_recursive($status, $businessOverlay);
        }
        if ($selected) {
            $primary = $selected[0];
            // Business runtime metrics should reflect the active upstream,
            // while scheduling stays on Gateway subprocesses.
            $status['started'] = (int)($primary['started'] ?? ($status['started'] ?? 0));
            $status['threads'] = (int)($primary['threads'] ?? ($status['threads'] ?? 0));
            $status['tables'] = (array)($primary['tables'] ?? ($status['tables'] ?? []));
        }
        $status['memory_usage'] = $this->buildGatewayMemoryUsage(
            (array)($status['memory_usage'] ?? []),
            $upstreams
        );
        return $status;
    }

    protected function buildUpstreamNode(array $generation, array $instance, array $runtimeStatus = []): array {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $metadata = (array)($instance['metadata'] ?? []);
        $status = (string)($generation['status'] ?? ($instance['status'] ?? 'prepared'));
        $displayVersion = (string)($metadata['display_version'] ?? $generation['version'] ?? $instance['version'] ?? 'unknown');

        $node = [
            'name' => '业务实例',
            'script' => $displayVersion,
            'ip' => $host,
            'port' => $port,
            'online' => $this->launcher ? $this->launcher->isListening($host, $port, 0.2) : true,
            'role' => (string)($metadata['role'] ?? SERVER_ROLE),
            'started' => (int)($metadata['started_at'] ?? time()),
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_upstream/' . $status,
            'app_version' => $appVersion,
            'public_version' => $publicVersion,
            'framework_build_version' => FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => file_exists(SCF_ROOT . '/build/update.pack'),
            'swoole_version' => swoole_version(),
            'scf_version' => SCF_COMPOSER_VERSION,
            'manager_pid' => (int)($metadata['pid'] ?? 0) ?: '--',
            'restart_times' => 0,
            'server_stats' => [
                'connection_num' => (int)($instance['runtime_connections'] ?? 0),
                'long_connection_num' => (int)($instance['runtime_connections'] ?? 0),
                'tasking_num' => 0,
                'idle_worker_num' => 0,
            ],
            'http_request_count_today' => 0,
            'http_request_count_current' => 0,
            'http_request_processing' => 0,
            'mysql_execute_count' => 0,
            'http_request_reject' => 0,
            'cpu_num' => function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0,
            'memory_usage' => $this->buildEmptyMemoryUsage(),
            'threads' => 0,
            'tables' => [],
            'tasks' => [],
            'fingerprint' => APP_FINGERPRINT . ':' . md5(($instance['id'] ?? '') . '@' . $port),
        ];
        if ($runtimeStatus) {
            $node = array_replace_recursive($node, $runtimeStatus);
        }
        $node = array_replace_recursive($node, [
            'name' => '业务实例',
            'script' => $displayVersion,
            'ip' => $host,
            'port' => $port,
            'online' => $this->launcher ? $this->launcher->isListening($host, $port, 0.2) : true,
            'role' => (string)($metadata['role'] ?? ($runtimeStatus['role'] ?? SERVER_ROLE)),
            'server_run_mode' => (string)($runtimeStatus['server_run_mode'] ?? APP_SRC_TYPE),
            'proxy_mode_label' => 'gateway_upstream/' . $status,
            'manager_pid' => ($runtimeStatus['manager_pid'] ?? ((int)($metadata['pid'] ?? 0) ?: '--')),
            'fingerprint' => APP_FINGERPRINT . ':' . md5(($instance['id'] ?? '') . '@' . $port),
        ]);
        $node['memory_usage'] = $this->normalizeMemoryUsage((array)($node['memory_usage'] ?? []));
        return $node;
    }

    protected function fetchUpstreamRuntimeStatus(array $instance): array {
        return $this->fetchUpstreamInternalStatus($instance, self::INTERNAL_UPSTREAM_STATUS_PATH);
    }

    protected function fetchUpstreamHealthStatus(array $instance): array {
        return $this->fetchUpstreamInternalStatus($instance, self::INTERNAL_UPSTREAM_HEALTH_PATH);
    }

    protected function fetchUpstreamInternalStatus(array $instance, string $path): array {
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $timeoutSeconds = $path === self::INTERNAL_UPSTREAM_STATUS_PATH ? 5.0 : 1.0;
        if ($host === '' || $port <= 0) {
            return [];
        }
        if ($this->launcher && !$this->launcher->isListening($host, $port, 0.2)) {
            return [];
        }
        if ($this->shouldUseLocalUpstreamUnixHttp($host, $port)) {
            $payload = $this->requestUpstreamUnixHttpJson(
                LocalIpc::upstreamHttpSocketPath($port),
                $path,
                $timeoutSeconds,
                [
                    'Host: ' . $host . ':' . $port,
                    'x-gateway-internal: 1',
                    'Connection: close',
                ]
            );
            if (is_array($payload['data'] ?? null)) {
                return $payload['data'];
            }
        }

        try {
            if (Coroutine::getCid() <= 0) {
                return $this->fetchUpstreamInternalStatusSync($host, $port, $path);
            }
            $client = new Client($host, $port, false);
            $client->set(['timeout' => 1.5]);
            $client->setHeaders([
                'host' => $host . ':' . $port,
                'x-gateway-internal' => '1',
            ]);
            if (!$client->get($path)) {
                $client->close();
                return [];
            }
            $statusCode = (int)($client->statusCode ?? 0);
            $body = (string)($client->body ?? '');
            $client->close();
            if ($statusCode !== 200 || $body === '' || !JsonHelper::is($body)) {
                return [];
            }
            $payload = JsonHelper::recover($body);
            $data = $payload['data'] ?? [];
            return is_array($data) ? $data : [];
        } catch (Throwable) {
            return [];
        }
    }

    protected function fetchUpstreamRuntimeStatusSync(string $host, int $port): array {
        return $this->fetchUpstreamInternalStatusSync($host, $port, self::INTERNAL_UPSTREAM_STATUS_PATH);
    }

    protected function fetchUpstreamHealthStatusSync(string $host, int $port): array {
        return $this->fetchUpstreamInternalStatusSync($host, $port, self::INTERNAL_UPSTREAM_HEALTH_PATH);
    }

    protected function fetchUpstreamInternalStatusSync(string $host, int $port, string $path): array {
        $context = stream_context_create([
            'http' => [
                'method' => 'GET',
                'timeout' => 1.5,
                'ignore_errors' => true,
                'header' => implode("\r\n", [
                    'Host: ' . $host . ':' . $port,
                    'x-gateway-internal: 1',
                    'Connection: close',
                ]),
            ],
        ]);
        $body = @file_get_contents('http://' . $host . ':' . $port . $path, false, $context);
        if (!is_string($body) || $body === '' || !JsonHelper::is($body)) {
            return [];
        }
        $statusCode = 0;
        foreach ((array)($http_response_header ?? []) as $headerLine) {
            if (preg_match('#^HTTP/\S+\s+(\d{3})#', (string)$headerLine, $matches)) {
                $statusCode = (int)$matches[1];
                break;
            }
        }
        if ($statusCode !== 200) {
            return [];
        }
        $payload = JsonHelper::recover($body);
        $data = $payload['data'] ?? [];
        return is_array($data) ? $data : [];
    }

    protected function shouldUseLocalUpstreamIpc(string $host): bool {
        return in_array($host, ['127.0.0.1', 'localhost', '0.0.0.0', SERVER_HOST], true);
    }

    protected function shouldUseLocalUpstreamUnixHttp(string $host, int $port): bool {
        return $this->shouldUseLocalUpstreamIpc($host) && $port > 0 && file_exists(LocalIpc::upstreamHttpSocketPath($port));
    }

    protected function localUpstreamIpcActionForPath(string $path): ?string {
        return match ($path) {
            self::INTERNAL_UPSTREAM_STATUS_PATH => 'upstream.status',
            self::INTERNAL_UPSTREAM_HEALTH_PATH => 'upstream.health',
            default => null,
        };
    }

    protected function requestUpstreamUnixHttpJson(string $socketPath, string $path, float $timeoutSeconds, array $headers = []): ?array {
        if ($socketPath === '' || !file_exists($socketPath)) {
            return null;
        }
        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client('unix://' . $socketPath, $errno, $errstr, $timeoutSeconds, STREAM_CLIENT_CONNECT);
        if (!is_resource($socket)) {
            return null;
        }
        $seconds = max(1, (int)floor($timeoutSeconds));
        $microseconds = max(0, (int)(($timeoutSeconds - floor($timeoutSeconds)) * 1000000));
        stream_set_timeout($socket, $seconds, $microseconds);
        $headerLines = array_merge([
            'GET ' . $path . ' HTTP/1.1',
        ], $headers, ['']);
        $request = implode("\r\n", $headerLines) . "\r\n";
        fwrite($socket, $request);
        $raw = stream_get_contents($socket);
        fclose($socket);
        if (!is_string($raw) || !str_contains($raw, "\r\n\r\n")) {
            return null;
        }
        [$head, $body] = explode("\r\n\r\n", $raw, 2);
        $status = 0;
        foreach (explode("\r\n", $head) as $line) {
            if (preg_match('#^HTTP/\S+\s+(\d{3})#', $line, $matches)) {
                $status = (int)$matches[1];
                break;
            }
        }
        if ($status !== 200 || $body === '') {
            return null;
        }
        $decoded = json_decode($body, true);
        return is_array($decoded) ? $decoded : null;
    }

    protected function buildGatewayBusinessOverlay(array $upstreams): array {
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        if (!$selected) {
            return [];
        }

        $base = $selected[0];
        $serverStats = [
            'connection_num' => 0,
            'long_connection_num' => 0,
            'tasking_num' => 0,
            'idle_worker_num' => 0,
        ];

        foreach ($selected as $item) {
            $stats = (array)($item['server_stats'] ?? []);
            $serverStats['connection_num'] += (int)($stats['connection_num'] ?? 0);
            $serverStats['long_connection_num'] += (int)($stats['long_connection_num'] ?? 0);
            $serverStats['tasking_num'] += (int)($stats['tasking_num'] ?? 0);
            $serverStats['idle_worker_num'] += (int)($stats['idle_worker_num'] ?? 0);
        }

        $tasks = [];
        foreach ($selected as $item) {
            foreach ((array)($item['tasks'] ?? []) as $task) {
                if (!is_array($task)) {
                    continue;
                }
                $taskId = (string)($task['id'] ?? '');
                if ($taskId !== '') {
                    $tasks[$taskId] = $task;
                    continue;
                }
                $tasks[] = $task;
            }
        }

        return [
            'app_version' => $base['app_version'] ?? '--',
            'framework_build_version' => $base['framework_build_version'] ?? FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => (bool)($base['framework_update_ready'] ?? false),
            'swoole_version' => $base['swoole_version'] ?? swoole_version(),
            'scf_version' => $base['scf_version'] ?? SCF_COMPOSER_VERSION,
            'server_stats' => $serverStats,
            'http_request_count_today' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_count_today'] ?? 0), $selected)),
            'http_request_count_current' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_count_current'] ?? 0), $selected)),
            'http_request_processing' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_processing'] ?? 0), $selected)),
            'mysql_execute_count' => array_sum(array_map(static fn(array $item) => (int)($item['mysql_execute_count'] ?? 0), $selected)),
            'http_request_reject' => array_sum(array_map(static fn(array $item) => (int)($item['http_request_reject'] ?? 0), $selected)),
            'cpu_num' => (int)($base['cpu_num'] ?? (function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0)),
            'tasks' => array_values($tasks),
        ];
    }

    protected function selectGatewayBusinessUpstreams(array $upstreams): array {
        if (!$upstreams) {
            return [];
        }

        $selected = array_values(array_filter($upstreams, static function (array $item) {
            return ($item['proxy_mode_label'] ?? '') === 'gateway_upstream/active';
        }));
        if (!$selected) {
            $selected = array_values(array_filter($upstreams, static function (array $item) {
                return (bool)($item['online'] ?? false);
            }));
        }
        return $selected;
    }

    protected function buildGatewayMemoryUsage(array $gatewayMemoryUsage, array $upstreams): array {
        $gatewayMemoryUsage = $this->normalizeMemoryUsage($gatewayMemoryUsage);
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        if (!$selected) {
            return $gatewayMemoryUsage;
        }

        $rows = [];
        $usageTotal = 0.0;
        $realTotal = 0.0;
        $peakTotal = 0.0;
        $osActualTotal = 0.0;
        $rssTotal = 0.0;
        $pssTotal = 0.0;
        $online = 0;
        $offline = 0;
        $systemTotalMemGb = (float)($gatewayMemoryUsage['system_total_mem_gb'] ?? 0);
        $systemFreeMemGb = (float)($gatewayMemoryUsage['system_free_mem_gb'] ?? 0);

        foreach ((array)($gatewayMemoryUsage['rows'] ?? []) as $row) {
            if (!$this->shouldKeepGatewayMemoryRow((array)$row)) {
                continue;
            }
            $rows[] = $row;
            $usageTotal += $this->extractMemoryValue($row['usage'] ?? null);
            $realTotal += $this->extractMemoryValue($row['real'] ?? null);
            $peakTotal += $this->extractMemoryValue($row['peak'] ?? null);
            $osActualTotal += $this->extractMemoryValue($row['os_actual'] ?? null);
            $rssTotal += $this->extractMemoryValue($row['rss'] ?? null);
            $pssTotal += $this->extractMemoryValue($row['pss'] ?? null);
            if ($this->isOnlineMemoryRow((array)$row)) {
                $online++;
            } else {
                $offline++;
            }
        }

        foreach ($selected as $item) {
            $memoryUsage = $this->normalizeMemoryUsage((array)($item['memory_usage'] ?? []));
            $systemTotalMemGb = max($systemTotalMemGb, (float)($memoryUsage['system_total_mem_gb'] ?? 0));
            $systemFreeMemGb = max($systemFreeMemGb, (float)($memoryUsage['system_free_mem_gb'] ?? 0));
            foreach ((array)($memoryUsage['rows'] ?? []) as $row) {
                $rows[] = $row;
                $usageTotal += $this->extractMemoryValue($row['usage'] ?? null);
                $realTotal += $this->extractMemoryValue($row['real'] ?? null);
                $peakTotal += $this->extractMemoryValue($row['peak'] ?? null);
                $osActualTotal += $this->extractMemoryValue($row['os_actual'] ?? null);
                $rssTotal += $this->extractMemoryValue($row['rss'] ?? null);
                $pssTotal += $this->extractMemoryValue($row['pss'] ?? null);
                if ($this->isOnlineMemoryRow((array)$row)) {
                    $online++;
                } else {
                    $offline++;
                }
            }
        }

        usort($rows, function (array $left, array $right) {
            $leftValue = $this->extractMemoryValue($left['os_actual'] ?? null);
            if ($leftValue <= 0) {
                $leftValue = $this->extractMemoryValue($left['real'] ?? null);
            }
            $rightValue = $this->extractMemoryValue($right['os_actual'] ?? null);
            if ($rightValue <= 0) {
                $rightValue = $this->extractMemoryValue($right['real'] ?? null);
            }
            return $rightValue <=> $leftValue;
        });

        return $this->normalizeMemoryUsage([
            'rows' => $rows,
            'online' => $online,
            'offline' => $offline,
            'total' => count($rows),
            'usage_total_mb' => round($usageTotal, 2),
            'real_total_mb' => round($realTotal, 2),
            'peak_total_mb' => round($peakTotal, 2),
            'os_actual_total_mb' => round($osActualTotal, 2),
            'rss_total_mb' => round($rssTotal, 2),
            'pss_total_mb' => round($pssTotal, 2),
            'system_total_mem_gb' => $systemTotalMemGb > 0 ? round($systemTotalMemGb, 2) : '--',
            'system_free_mem_gb' => $systemFreeMemGb > 0 ? round($systemFreeMemGb, 2) : '--',
        ]);
    }

    protected function shouldKeepGatewayMemoryRow(array $row): bool {
        $name = (string)($row['name'] ?? '');
        if ($name === '') {
            return false;
        }
        if (str_starts_with($name, 'worker:')) {
            return false;
        }
        return $name !== 'Server:Master';
    }

    protected function isOnlineMemoryRow(array $row): bool {
        $status = preg_replace('/\e\[[\d;]*m/', '', (string)($row['status'] ?? ''));
        return str_contains($status, '正常');
    }

    protected function extractMemoryValue(mixed $value): float {
        if (is_int($value) || is_float($value)) {
            return (float)$value;
        }
        if (!is_string($value) || $value === '' || $value === '-') {
            return 0.0;
        }
        if (preg_match('/-?\d+(?:\.\d+)?/', $value, $matches)) {
            return (float)$matches[0];
        }
        return 0.0;
    }

    protected function normalizeMemoryUsage(array $memoryUsage): array {
        if ((float)($memoryUsage['os_actual_total_mb'] ?? 0) <= 0) {
            $fallback = (float)($memoryUsage['real_total_mb'] ?? ($memoryUsage['usage_total_mb'] ?? 0));
            if ($fallback > 0) {
                $memoryUsage['os_actual_total_mb'] = round($fallback, 2);
            }
        }
        return $memoryUsage + $this->buildEmptyMemoryUsage();
    }


    protected function buildEmptyMemoryUsage(): array {
        return [
            'rows' => [],
            'system_free_mem_gb' => 0,
            'system_total_mem_gb' => 0,
            'os_actual_total_mb' => 0,
        ];
    }

    protected function buildDashboardSocketHost(string $host, string $referer, string $forwardedProto = ''): string {
        $normalizedHost = trim($host);
        $normalizedReferer = trim($referer);
        $normalizedProto = strtolower(trim($forwardedProto));

        if ($this->isLoopbackDashboardHost($normalizedHost)) {
            $refererAuthority = $this->dashboardRefererAuthority($normalizedReferer);
            if ($refererAuthority !== '') {
                $normalizedHost = $refererAuthority;
            }
        }

        if ($normalizedHost === '') {
            $normalizedHost = $this->dashboardRefererAuthority($normalizedReferer);
        }

        if ($normalizedHost === '') {
            $normalizedHost = '127.0.0.1:' . $this->resolvedDashboardPort();
        }

        $protocol = in_array($normalizedProto, ['https', 'wss'], true)
            || (!empty($normalizedReferer) && str_starts_with($normalizedReferer, 'https'))
            ? 'wss://'
            : 'ws://';

        return $protocol . $normalizedHost . '/dashboard.socket';
    }

    protected function isLoopbackDashboardHost(string $host): bool {
        if ($host === '') {
            return true;
        }
        $lowerHost = strtolower($host);
        return $lowerHost === 'localhost'
            || str_starts_with($lowerHost, 'localhost:')
            || $lowerHost === '127.0.0.1'
            || str_starts_with($lowerHost, '127.0.0.1:')
            || $lowerHost === '::1'
            || str_starts_with($lowerHost, '[::1]:');
    }

    protected function dashboardRefererAuthority(string $referer): string {
        if ($referer === '') {
            return '';
        }
        $parts = parse_url($referer);
        if (!is_array($parts)) {
            return '';
        }
        $host = trim((string)($parts['host'] ?? ''));
        if ($host === '') {
            return '';
        }
        $port = (int)($parts['port'] ?? 0);
        return $port > 0 ? ($host . ':' . $port) : $host;
    }

    protected function buildDashboardVersionStatus(): array {
        $cached = $this->dashboardVersionStatusCache['value'] ?? null;
        if (is_array($cached) && (int)($this->dashboardVersionStatusCache['expires_at'] ?? 0) > time()) {
            return $cached;
        }
        $dashboardDir = SCF_ROOT . '/build/public/dashboard';
        $versionJson = $dashboardDir . '/version.json';
        $currentDashboardVersion = ['version' => '0.0.0'];
        if (file_exists($versionJson)) {
            $currentDashboardVersion = JsonHelper::recover(File::read($versionJson)) ?: $currentDashboardVersion;
        }

        $dashboardVersion = ['version' => '--'];
        $client = \Scf\Client\Http::create(str_replace('version.json', 'dashboard-version.json', ENV_VARIABLES['scf_update_server']));
        $response = $client->get();
        if (!$response->hasError()) {
            $dashboardVersion = $response->getData();
        }

        $result = [
            'version' => $currentDashboardVersion['version'] ?? '0.0.0',
            'latest_version' => $dashboardVersion['version'] ?? '--',
        ];
        $this->dashboardVersionStatusCache = [
            'expires_at' => time() + self::DASHBOARD_VERSION_CACHE_TTL_SECONDS,
            'value' => $result,
        ];
        return $result;
    }

    protected function buildFrameworkVersionStatus(?array $upstreams = null): array {
        $remoteVersion = $this->cachedFrameworkRemoteVersion();
        $upstreams ??= $this->dashboardUpstreams();
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        $activeFrameworkVersion = (string)($selected[0]['framework_build_version'] ?? FRAMEWORK_BUILD_VERSION);
        $activeFrameworkReady = (bool)($selected[0]['framework_update_ready'] ?? file_exists(SCF_ROOT . '/build/update.pack'));

        return [
            'is_phar' => FRAMEWORK_IS_PHAR,
            'version' => $activeFrameworkVersion,
            'latest_version' => $remoteVersion['version'] ?? FRAMEWORK_BUILD_VERSION,
            'latest_build' => $remoteVersion['build'] ?? FRAMEWORK_BUILD_TIME,
            'build' => FRAMEWORK_BUILD_TIME,
            'gateway_version' => FRAMEWORK_BUILD_VERSION,
            'gateway_build' => FRAMEWORK_BUILD_TIME,
            'gateway_pending_restart' => FRAMEWORK_BUILD_VERSION !== $activeFrameworkVersion,
            'update_ready' => $activeFrameworkReady,
        ];
    }

    protected function cachedFrameworkRemoteVersion(): array {
        $cached = $this->frameworkRemoteVersionCache['value'] ?? null;
        if (is_array($cached) && (int)($this->frameworkRemoteVersionCache['expires_at'] ?? 0) > time()) {
            return $cached;
        }

        $remoteVersion = [
            'version' => FRAMEWORK_BUILD_VERSION,
            'build' => FRAMEWORK_BUILD_TIME,
        ];

        $client = \Scf\Client\Http::create(ENV_VARIABLES['scf_update_server']);
        $response = $client->get();
        if (!$response->hasError()) {
            $remoteVersion = $response->getData();
        }

        $this->frameworkRemoteVersionCache = [
            'expires_at' => time() + self::FRAMEWORK_VERSION_CACHE_TTL_SECONDS,
            'value' => $remoteVersion,
        ];

        return $remoteVersion;
    }

    protected function pushDashboardStatus(?int $fd = null): void {
        $payload = $this->buildDashboardRealtimeStatus();
        $this->pushDashboardEvent($payload, $fd);
    }

    protected function pushDashboardEvent(array $payload, ?int $fd = null): void {
        $message = JsonHelper::toJson($payload);
        if ($fd !== null) {
            if ($this->server->exist($fd) && $this->server->isEstablished($fd)) {
                $this->server->push($fd, $message);
            }
            return;
        }

        foreach (array_keys($this->dashboardClients) as $clientFd) {
            if (!$this->server->exist($clientFd) || !$this->server->isEstablished($clientFd)) {
                unset($this->dashboardClients[$clientFd]);
                $this->syncConsoleSubscriptionState();
                continue;
            }
            $this->server->push($clientFd, $message);
        }
    }

    public function pushConsoleLog(string $time, string $message): bool|int {
        if (defined('IS_GATEWAY_SUB_PROCESS') && IS_GATEWAY_SUB_PROCESS === true) {
            return ConsoleRelay::reportToLocalGateway($time, $message, 'gateway', SERVER_HOST);
        }
        return $this->acceptConsolePayload([
            'time' => $time,
            'message' => $message,
            'source_type' => 'gateway',
            'node' => SERVER_HOST,
        ]);
    }

    protected function acceptConsolePayload(array $payload): bool {
        $message = (string)($payload['message'] ?? '');
        if ($message === '') {
            return false;
        }

        $time = (string)($payload['time'] ?? Console::timestamp());
        $node = (string)($payload['node'] ?? SERVER_HOST);
        $sourceType = (string)($payload['source_type'] ?? 'gateway');
        $oldInstance = (bool)($payload['old_instance'] ?? false);

        if ($this->dashboardEnabled()) {
            if (!$this->hasConsoleSubscribers()) {
                return false;
            }
            $this->pushDashboardEvent([
                'event' => 'console',
                'message' => ['data' => $message, 'source_type' => $sourceType, 'old_instance' => $oldInstance],
                'time' => $time,
                'node' => $node,
            ]);
            return true;
        }

        if (!ConsoleRelay::remoteSubscribed()) {
            return false;
        }

        $socket = $this->masterGatewaySocket;
        if (!$socket) {
            return false;
        }

        try {
            return (bool)$socket->push(JsonHelper::toJson([
                'event' => 'console_log',
                'data' => [
                    'time' => $time,
                    'message' => $message,
                    'source_type' => $sourceType,
                    'node' => $node,
                    'old_instance' => $oldInstance,
                ],
            ]));
        } catch (Throwable) {
            return false;
        }
    }

    protected function hasConsoleSubscribers(): bool {
        return !empty($this->dashboardClients);
    }

    protected function syncConsoleSubscriptionState(bool $force = false): void {
        if (!$this->dashboardEnabled()) {
            return;
        }
        $enabled = $this->hasConsoleSubscribers();
        if (!$force && $this->lastConsoleSubscriptionState === $enabled) {
            return;
        }
        $this->lastConsoleSubscriptionState = $enabled;
        ConsoleRelay::setLocalSubscribed($enabled);
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                continue;
            }
            $this->pushConsoleSubscription((int)($node['socket_fd'] ?? 0), $force);
        }
    }

    protected function pushConsoleSubscription(int $fd, bool $force = false): void {
        if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
            return;
        }
        $enabled = $this->hasConsoleSubscribers();
        if (
            !$force
            && isset($this->nodeClients[$fd]['console_subscription'])
            && (bool)$this->nodeClients[$fd]['console_subscription'] === $enabled
        ) {
            return;
        }
        if (isset($this->nodeClients[$fd])) {
            $this->nodeClients[$fd]['console_subscription'] = $enabled;
        }
        $this->server->push($fd, JsonHelper::toJson([
            'event' => 'console_subscription',
            'data' => [
                'enabled' => $enabled,
            ],
        ]));
    }

    protected function isInternalGatewayRequest(Request $request): bool {
        $flag = (string)($request->header['x-gateway-internal'] ?? '');
        if ($flag === '1') {
            return true;
        }
        $clientIp = (string)($request->server['remote_addr'] ?? '');
        return in_array($clientIp, ['127.0.0.1', '::1'], true);
    }

    protected function restartUpstreamsByHost(string $host): Result {
        $plans = $this->matchedPlansByHost($host);
        if (!$plans) {
            return Result::error('未找到可重启的业务实例:' . $host);
        }

        Console::info("【Gateway】开始重启业务实例: host={$host}, count=" . count($plans));
        $summary = $this->restartManagedUpstreams($plans);
        $this->pushDashboardStatus();
        if ($summary['failed_nodes']) {
            Console::warning("【Gateway】业务实例重启部分失败: host={$host}, success={$summary['success_count']}, failed=" . count($summary['failed_nodes']));
            return Result::error('部分业务实例重启失败', 'SERVICE_ERROR', $summary);
        }
        Console::success("【Gateway】业务实例重启完成: host={$host}, success={$summary['success_count']}");
        return Result::success('已重启 ' . $summary['success_count'] . ' 个业务实例');
    }

    protected function shutdownUpstreamsByHost(string $host): Result {
        $plans = $this->matchedPlansByHost($host);
        if (!$plans) {
            return Result::error('未找到可关闭的业务实例:' . $host);
        }

        Console::info("【Gateway】开始关闭业务实例: host={$host}, count=" . count($plans));
        foreach ($plans as $plan) {
            $this->stopManagedPlan($plan);
        }
        $this->pushDashboardStatus();
        Console::success("【Gateway】业务实例已关闭: host={$host}, count=" . count($plans));
        return Result::success('已关闭 ' . count($plans) . ' 个业务实例');
    }

    protected function restartManagedUpstreams(?array $plans = null): array {
        return $this->rollingRestartManagedUpstreams($plans);
    }

    protected function rollingRestartManagedUpstreams(?array $plans = null): array {
        $basePlans = $plans ?? $this->activeManagedPlans();
        if (!$basePlans) {
            $basePlans = $this->managedPlansFromSnapshot();
            if ($basePlans) {
                foreach ($basePlans as $plan) {
                    $this->appendManagedPlan($plan);
                }
                Console::warning("【Gateway】业务实例计划内存为空，已从运行态快照恢复: count=" . count($basePlans));
            }
        }
        $basePlans = array_values(array_filter($basePlans, static function ($plan) {
            return (int)($plan['port'] ?? 0) > 0;
        }));

        $failedNodes = [];
        $successCount = 0;
        if (!$basePlans || !$this->upstreamSupervisor || !$this->launcher) {
            if (!$basePlans) {
                Console::warning('【Gateway】未找到可重启的业务实例计划');
            } elseif (!$this->upstreamSupervisor) {
                Console::warning('【Gateway】业务实例管理器未就绪，无法执行滚动重启');
            } elseif (!$this->launcher) {
                Console::warning('【Gateway】业务实例启动器未就绪，无法执行滚动重启');
            }
            return [
                'success_count' => 0,
                'failed_nodes' => [],
            ];
        }

        $generationVersion = $this->buildReloadGenerationVersion($basePlans);
        Console::info("【Gateway】开始滚动重启业务实例: generation={$generationVersion}, count=" . count($basePlans));
        $newPlans = [];
        $registeredPlans = [];
        $reserved = $this->collectReservedPorts();

        foreach ($basePlans as $plan) {
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $newPort = $this->allocateManagedPort(max(1025, (int)($plan['port'] ?? 0) + 1), $reserved);
            $reserved[] = $newPort;
            $newRpcPort = 0;
            if ($this->rpcPort > 0) {
                $newRpcPort = $this->allocateManagedPort(max(1025, (int)($plan['rpc_port'] ?? $this->rpcPort) + 1), $reserved);
                $reserved[] = $newRpcPort;
            }

            $newPlan = $plan;
            $newPlan['version'] = $generationVersion;
            $newPlan['port'] = $newPort;
            $newPlan['rpc_port'] = $newRpcPort;
            $newPlan['metadata'] = array_merge((array)($plan['metadata'] ?? []), [
                'managed' => true,
                'role' => (string)($plan['role'] ?? SERVER_ROLE),
                'rpc_port' => $newRpcPort,
                'started_at' => time(),
                'managed_mode' => 'gateway_supervisor',
                'rolling_from_version' => (string)($plan['version'] ?? ''),
                'display_version' => (string)($plan['metadata']['display_version'] ?? $plan['version'] ?? ''),
            ]);
            $newPlans[] = $newPlan;
            Console::info("【Gateway】启动新业务实例: " . $this->describePlan($newPlan));

            $this->upstreamSupervisor->sendCommand(['action' => 'spawn', 'plan' => $newPlan]);
            if (!$this->launcher->waitUntilServicesReady($host, $newPort, $newRpcPort, 30)) {
                $failedNodes[] = [
                    'host' => $host . ':' . $newPort,
                    'error' => '新业务实例未在预期时间内完成启动',
                ];
                continue;
            }

            $this->registerManagedPlan($newPlan, false);
            $registeredPlans[] = $newPlan;
            $successCount++;
        }

        if ($failedNodes) {
            Console::warning("【Gateway】滚动重启失败，开始回收新实例: generation={$generationVersion}, failed=" . count($failedNodes));
            foreach ($registeredPlans as $plan) {
                $this->stopManagedPlan($plan, false);
            }
            foreach ($newPlans as $plan) {
                $this->removeManagedPlan($plan);
            }
            return [
                'success_count' => $successCount,
                'failed_nodes' => $failedNodes,
            ];
        }

        $this->instanceManager->activateVersion($generationVersion, self::ROLLING_DRAIN_GRACE_SECONDS);
        $this->syncNginxProxyTargets('rolling_restart_activate');
        foreach ($basePlans as $plan) {
            $this->quiesceManagedPlanBusinessPlane($plan);
        }
        foreach ($newPlans as $plan) {
            $this->appendManagedPlan($plan);
        }
        Console::success("【Gateway】滚动重启完成并切换流量: generation={$generationVersion}, success={$successCount}, drain_grace=" . self::ROLLING_DRAIN_GRACE_SECONDS . "s");

        return [
            'success_count' => $successCount,
            'failed_nodes' => [],
        ];
    }

    protected function rollingUpdateManagedUpstreams(string $type, string $version): array {
        $basePlans = $this->activeManagedPlans();
        $failedNodes = [];
        $successCount = 0;
        if (!$basePlans || !$this->upstreamSupervisor || !$this->launcher) {
            return [
                'success_count' => 0,
                'failed_nodes' => [],
            ];
        }

        $generationVersion = $this->buildRollingGenerationVersion($type, $version);
        Console::info("【Gateway】开始滚动升级业务实例: type={$type}, version={$version}, generation={$generationVersion}, base_count=" . count($basePlans));
        $newPlans = [];
        $registeredPlans = [];
        $reserved = $this->collectReservedPorts();

        foreach ($basePlans as $plan) {
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $newPort = $this->allocateManagedPort(max(1025, (int)($plan['port'] ?? 0) + 1), $reserved);
            $reserved[] = $newPort;
            $newRpcPort = 0;
            if ($this->rpcPort > 0) {
                $newRpcPort = $this->allocateManagedPort(max(1025, (int)($plan['rpc_port'] ?? $this->rpcPort) + 1), $reserved);
                $reserved[] = $newRpcPort;
            }

            $newPlan = $plan;
            $newPlan['version'] = $generationVersion;
            $newPlan['port'] = $newPort;
            $newPlan['rpc_port'] = $newRpcPort;
            $newPlan['metadata'] = array_merge((array)($plan['metadata'] ?? []), [
                'managed' => true,
                'role' => (string)($plan['role'] ?? SERVER_ROLE),
                'rpc_port' => $newRpcPort,
                'started_at' => time(),
                'managed_mode' => 'gateway_supervisor',
                'rolling_from_version' => (string)($plan['version'] ?? ''),
            ]);
            $newPlans[] = $newPlan;
            Console::info("【Gateway】启动新业务实例: " . $this->describePlan($newPlan));

            $this->upstreamSupervisor->sendCommand(['action' => 'spawn', 'plan' => $newPlan]);
            if (!$this->launcher->waitUntilServicesReady($host, $newPort, $newRpcPort, 30)) {
                $failedNodes[] = [
                    'host' => $host . ':' . $newPort,
                    'error' => '新业务实例未在预期时间内完成启动',
                ];
                continue;
            }

            $this->registerManagedPlan($newPlan, false);
            $registeredPlans[] = $newPlan;
            $successCount++;
        }

        if ($failedNodes) {
            Console::warning("【Gateway】滚动升级失败，开始回收新实例: generation={$generationVersion}, failed=" . count($failedNodes));
            foreach ($registeredPlans as $plan) {
                $this->stopManagedPlan($plan, false);
            }
            foreach ($newPlans as $plan) {
                $this->removeManagedPlan($plan);
            }
            return [
                'success_count' => $successCount,
                'failed_nodes' => $failedNodes,
            ];
        }

        $this->instanceManager->activateVersion($generationVersion, self::ROLLING_DRAIN_GRACE_SECONDS);
        $this->syncNginxProxyTargets('rolling_update_activate');
        foreach ($basePlans as $plan) {
            $this->quiesceManagedPlanBusinessPlane($plan);
        }
        foreach ($newPlans as $plan) {
            $this->appendManagedPlan($plan);
        }
        Console::success("【Gateway】滚动升级完成并切换流量: generation={$generationVersion}, success={$successCount}, drain_grace=" . self::ROLLING_DRAIN_GRACE_SECONDS . "s");

        return [
            'success_count' => $successCount,
            'failed_nodes' => [],
        ];
    }

    protected function stopManagedPlan(array $plan, bool $removeState = true, bool $removePlan = true): void {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $plan = $this->hydrateManagedPlanRuntimeMetadata($plan);
        $key = ((string)($plan['version'] ?? '') . '@' . $host . ':' . $port);
        $descriptor = $this->managedPlanDescriptor($plan);

        if ($this->upstreamSupervisor) {
            if (isset($this->pendingManagedRecycles[$key])) {
                return;
            }
            if (!$this->upstreamSupervisor->sendCommand([
                'action' => 'stop_instance',
                'instance' => [
                    'version' => (string)($plan['version'] ?? ''),
                    'host' => $host,
                    'port' => $port,
                    'rpc_port' => (int)($plan['rpc_port'] ?? 0),
                    'metadata' => [
                        'managed' => true,
                        'rpc_port' => (int)($plan['rpc_port'] ?? 0),
                        'pid' => (int)(($plan['metadata']['pid'] ?? 0)),
                    ],
                ],
            ])) {
                Console::warning("【Gateway】旧业务实例回收命令发送失败 {$descriptor}");
                return;
            }
            $this->pendingManagedRecycles[$key] = [
                'plan' => $plan,
                'remove_state' => $removeState,
                'remove_plan' => $removePlan,
                'requested_at' => time(),
            ];
            unset($this->pendingManagedRecycleWarnState[$key]);
            $this->ensurePendingManagedRecycleWatcher($key);
            Console::info("【Gateway】开始回收旧业务实例 {$descriptor}");
            return;
        } elseif ($this->launcher) {
            $rpcPort = (int)($plan['rpc_port'] ?? 0);
            $this->launcher->stop([
                'host' => $host,
                'port' => $port,
                'rpc_port' => $rpcPort,
                'metadata' => [
                    'managed' => true,
                    'rpc_port' => $rpcPort,
                    'pid' => (int)(($plan['metadata']['pid'] ?? 0)),
                ],
            ], AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS);
        }

        if ($removeState) {
            $this->instanceManager->removeInstance($host, $port);
        }
        unset($this->bootstrappedManagedInstances[$key]);
        if ($removePlan) {
            $this->removeManagedPlan($plan);
        }
        if ($port > 0) {
            Console::success("【Gateway】旧业务实例回收完成 {$descriptor}");
        }
    }

    protected function matchedPlansByHost(string $host): array {
        $host = trim($host);
        return array_values(array_filter($this->managedUpstreamPlans, function ($plan) use ($host) {
            $planHost = (string)($plan['host'] ?? '127.0.0.1');
            if ($host === $planHost) {
                return true;
            }
            if ($host === 'localhost' && $planHost === '127.0.0.1') {
                return true;
            }
            return false;
        }));
    }

    protected function activeManagedPlans(): array {
        $activeVersion = (string)($this->instanceManager->state()['active_version'] ?? '');
        if ($activeVersion !== '') {
            $plans = array_values(array_filter($this->managedUpstreamPlans, static function ($plan) use ($activeVersion) {
                return (string)($plan['version'] ?? '') === $activeVersion;
            }));
            if ($plans) {
                return $plans;
            }
        }

        return array_values(array_filter($this->managedUpstreamPlans, static function ($plan) {
            return (int)($plan['port'] ?? 0) > 0;
        }));
    }

    protected function managedPlansFromSnapshot(): array {
        $snapshot = $this->instanceManager->snapshot();
        $activeVersion = (string)($snapshot['active_version'] ?? '');
        $plans = [];
        foreach (($snapshot['generations'] ?? []) as $generation) {
            $version = (string)($generation['version'] ?? '');
            $status = (string)($generation['status'] ?? '');
            if ($activeVersion !== '') {
                if ($version !== $activeVersion) {
                    continue;
                }
            } elseif ($status !== 'active') {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                $metadata = (array)($instance['metadata'] ?? []);
                if (($metadata['managed'] ?? false) !== true) {
                    continue;
                }
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($port <= 0) {
                    continue;
                }
                $plans[] = [
                    'app' => APP_DIR_NAME,
                    'env' => SERVER_RUN_ENV,
                    'host' => $host,
                    'version' => (string)($instance['version'] ?? $version),
                    'weight' => (int)($instance['weight'] ?? 100),
                    'role' => (string)($metadata['role'] ?? SERVER_ROLE),
                    'port' => $port,
                    'rpc_port' => (int)($metadata['rpc_port'] ?? 0),
                    'src' => APP_SRC_TYPE,
                    'metadata' => array_merge($metadata, [
                        'managed' => true,
                        'managed_mode' => (string)($metadata['managed_mode'] ?? 'gateway_supervisor'),
                        'display_version' => (string)($metadata['display_version'] ?? ($instance['version'] ?? $version)),
                    ]),
                    'start_timeout' => 25,
                    'extra' => [],
                ];
            }
        }
        return $plans;
    }

    protected function buildRollingGenerationVersion(string $type, string $version): string {
        if ($type === 'app') {
            return $version;
        }
        return $type . '-' . $version . '-' . date('YmdHis');
    }

    protected function buildReloadGenerationVersion(array $plans): string {
        $displayVersion = (string)($plans[0]['metadata']['display_version'] ?? $plans[0]['version'] ?? App::version() ?? 'reload');
        return $displayVersion . '-reload-' . date('YmdHis');
    }

    protected function collectReservedPorts(): array {
        $ports = [$this->port];
        if ($this->rpcPort > 0) {
            $ports[] = $this->rpcPort;
        }
        foreach ($this->managedUpstreamPlans as $plan) {
            $planPort = (int)($plan['port'] ?? 0);
            $planRpcPort = (int)($plan['rpc_port'] ?? 0);
            $planPort > 0 and $ports[] = $planPort;
            $planRpcPort > 0 and $ports[] = $planRpcPort;
        }
        foreach (($this->instanceManager->snapshot()['generations'] ?? []) as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                $instancePort = (int)($instance['port'] ?? 0);
                $instanceRpcPort = (int)(($instance['metadata']['rpc_port'] ?? 0));
                $instancePort > 0 and $ports[] = $instancePort;
                $instanceRpcPort > 0 and $ports[] = $instanceRpcPort;
            }
        }
        return array_values(array_unique(array_filter($ports)));
    }

    protected function allocateManagedPort(int $startPort, array $reserved): int {
        $port = max(1025, $startPort);
        while (true) {
            if (in_array($port, $reserved, true)) {
                $port++;
                continue;
            }
            if (!$this->launcher) {
                return $port;
            }
            if ($this->launcher->isListening('127.0.0.1', $port, 0.1) || $this->launcher->isListening('0.0.0.0', $port, 0.1)) {
                $port++;
                continue;
            }
            return $port;
        }
    }

    protected function registerManagedPlan(array $plan, bool $activate = false): void {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            return;
        }
        $weight = (int)($plan['weight'] ?? 100);
        $metadata = (array)($plan['metadata'] ?? []);
        $metadata['managed'] = true;
        $metadata['role'] = (string)($plan['role'] ?? ($metadata['role'] ?? SERVER_ROLE));
        $metadata['rpc_port'] = (int)($plan['rpc_port'] ?? ($metadata['rpc_port'] ?? 0));
        $metadata['started_at'] = (int)($metadata['started_at'] ?? time());
        $metadata['managed_mode'] = 'gateway_supervisor';
        $this->instanceManager->registerUpstream($version, $host, $port, $weight, $metadata);
        if ($activate) {
            $this->instanceManager->activateVersion($version, 0);
            $this->syncNginxProxyTargets('register_managed_plan_activate');
        }
        $this->bootstrappedManagedInstances[$version . '@' . $host . ':' . $port] = true;
    }

    protected function appendManagedPlan(array $plan): void {
        foreach ($this->managedUpstreamPlans as $existing) {
            if ((string)($existing['version'] ?? '') === (string)($plan['version'] ?? '')
                && (string)($existing['host'] ?? '') === (string)($plan['host'] ?? '')
                && (int)($existing['port'] ?? 0) === (int)($plan['port'] ?? 0)) {
                return;
            }
        }
        $this->managedUpstreamPlans[] = $plan;
    }

    protected function mergeManagedPlanMetadata(string $host, int $port, array $metadataPatch): void {
        if ($host === '' || $port <= 0 || !$metadataPatch) {
            return;
        }
        foreach ($this->managedUpstreamPlans as &$plan) {
            if ((string)($plan['host'] ?? '') !== $host || (int)($plan['port'] ?? 0) !== $port) {
                continue;
            }
            $plan['metadata'] = array_merge((array)($plan['metadata'] ?? []), $metadataPatch);
        }
        unset($plan);
    }

    protected function hydrateManagedPlanRuntimeMetadata(array $plan): array {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($host === '' || $port <= 0) {
            return $plan;
        }
        $snapshot = $this->instanceManager->snapshot();
        foreach (($snapshot['generations'] ?? []) as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((string)($instance['host'] ?? '') !== $host || (int)($instance['port'] ?? 0) !== $port) {
                    continue;
                }
                $plan['metadata'] = array_merge((array)($plan['metadata'] ?? []), (array)($instance['metadata'] ?? []));
                $plan['rpc_port'] = (int)($plan['rpc_port'] ?? (($instance['metadata']['rpc_port'] ?? 0)));
                return $plan;
            }
        }
        return $plan;
    }

    protected function removeManagedPlan(array $plan): void {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $this->managedUpstreamPlans = array_values(array_filter($this->managedUpstreamPlans, static function ($item) use ($version, $host, $port) {
            return !(
                (string)($item['version'] ?? '') === $version
                && (string)($item['host'] ?? '127.0.0.1') === $host
                && (int)($item['port'] ?? 0) === $port
            );
        }));
    }

    protected function managedPlanDescriptor(array $plan): string {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $parts = [];
        $version !== '' and $parts[] = "generation={$version}";
        $parts[] = "http={$host}:{$port}";
        $rpcPort > 0 and $parts[] = "rpc={$rpcPort}";
        return implode(', ', $parts);
    }

    protected function quiesceManagedPlanBusinessPlane(array $plan): void {
        if (!$this->launcher) {
            return;
        }
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($port <= 0) {
            return;
        }
        if (!$this->launcher->requestBusinessQuiesce($host, $port, 1.0)) {
            Console::warning("【Gateway】旧业务实例业务平面静默失败: " . $this->managedPlanDescriptor($plan));
            return;
        }
        $runtimeStatus = $this->fetchUpstreamRuntimeStatus($plan);
        $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
        $proxyHttpProcessing = $this->instanceManager->proxyHttpRequestCountFor($version, $host, $port);
        $httpProcessing = (int)($runtimeStatus['http_request_processing'] ?? 0) + $proxyHttpProcessing;
        $rpcProcessing = (int)($runtimeStatus['rpc_request_processing'] ?? 0);
        Console::info(
            "【Gateway】旧业务实例开始平滑回收: " . $this->managedPlanDescriptor($plan)
            . ", ws=" . $gatewayWs
            . ", http=" . $httpProcessing
            . ", rpc=" . $rpcProcessing
            . ", queue=" . (int)($runtimeStatus['redis_queue_processing'] ?? 0)
            . ", crontab=" . (int)($runtimeStatus['crontab_busy'] ?? 0)
        );
        if ($gatewayWs === 0 && $httpProcessing === 0 && $rpcProcessing === 0) {
            Console::info("【Gateway】旧业务实例已无在途请求，立即进入 shutdown: " . $this->managedPlanDescriptor($plan));
            $this->stopManagedPlan($plan, true);
        }
    }

    protected function cleanupOfflineManagedUpstreams(): void {
        $snapshot = $this->instanceManager->snapshot();
        foreach (($snapshot['generations'] ?? []) as $generation) {
            if (($generation['status'] ?? '') !== 'offline') {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                if ((($instance['metadata']['managed'] ?? false) !== true)) {
                    continue;
                }
                $plan = [
                    'version' => (string)($instance['version'] ?? $generation['version'] ?? ''),
                    'host' => (string)($instance['host'] ?? '127.0.0.1'),
                    'port' => (int)($instance['port'] ?? 0),
                    'rpc_port' => (int)(($instance['metadata']['rpc_port'] ?? 0)),
                ];
                if ($this->isManagedPlanRecyclePending($plan)) {
                    continue;
                }
                $this->stopManagedPlan($plan);
            }
            $version = (string)($generation['version'] ?? '');
            if ($this->hasPendingRecycleForVersion($version)) {
                continue;
            }
            $this->instanceManager->removeVersion($version);
            if ($version !== '') {
                Console::success("【Gateway】旧业务实例代际已移除 generation={$version}");
            }
        }
    }

    protected function pollPendingManagedRecycles(): void {
        if (!$this->launcher || !$this->pendingManagedRecycles) {
            return;
        }

        foreach (array_keys($this->pendingManagedRecycles) as $key) {
            $this->checkPendingManagedRecycle($key);
        }
    }

    protected function ensurePendingManagedRecycleWatcher(string $key): void {
        if (isset($this->pendingManagedRecycleWatchers[$key]) || !isset($this->pendingManagedRecycles[$key])) {
            return;
        }

        $timerId = Timer::tick(1000, function () use ($key) {
            if (!isset($this->pendingManagedRecycleWatchers[$key])) {
                return;
            }
            if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                $this->clearPendingManagedRecycleWatcher($key);
                return;
            }
            if ($this->checkPendingManagedRecycle($key)) {
                $this->clearPendingManagedRecycleWatcher($key);
            }
        });
        $this->pendingManagedRecycleWatchers[$key] = $timerId;
    }

    protected function clearPendingManagedRecycleWatcher(string $key): void {
        if (!isset($this->pendingManagedRecycleWatchers[$key])) {
            return;
        }
        Timer::clear($this->pendingManagedRecycleWatchers[$key]);
        unset($this->pendingManagedRecycleWatchers[$key]);
    }

    protected function checkPendingManagedRecycle(string $key): bool {
        if (!$this->launcher || !isset($this->pendingManagedRecycles[$key])) {
            return true;
        }

        $item = $this->pendingManagedRecycles[$key];
        $plan = (array)($item['plan'] ?? []);
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $httpAlive = $port > 0 && ($this->launcher->isListening($host, $port, 0.1) || $this->launcher->isListening('0.0.0.0', $port, 0.1));
        $rpcAlive = $rpcPort > 0 && ($this->launcher->isListening($host, $rpcPort, 0.1) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.1));
        if ($httpAlive || $rpcAlive) {
            $requestedAt = (int)($item['requested_at'] ?? time());
            $elapsed = max(0, time() - $requestedAt);
            if ($elapsed >= 60) {
                $lastWarnAt = (int)($this->pendingManagedRecycleWarnState[$key] ?? 0);
                if ($lastWarnAt <= 0 || (time() - $lastWarnAt) >= 60) {
                    $this->pendingManagedRecycleWarnState[$key] = time();
                    Console::warning(
                        "【Gateway】旧业务实例回收等待中: " . $this->managedPlanDescriptor($plan)
                        . ", waiting={$elapsed}s, http=" . ($httpAlive ? 'listening' : 'closed')
                        . ", rpc=" . ($rpcAlive ? 'listening' : 'closed')
                    );
                }
            }
            return false;
        }

        $removeState = (bool)($item['remove_state'] ?? true);
        $removePlan = (bool)($item['remove_plan'] ?? true);
        if ($removeState) {
            $this->instanceManager->removeInstance($host, $port);
        }
        unset($this->bootstrappedManagedInstances[$key]);
        if ($removePlan) {
            $this->removeManagedPlan($plan);
        }
        Console::success("【Gateway】旧业务实例回收完成 " . $this->managedPlanDescriptor($plan));
        unset($this->pendingManagedRecycleWarnState[$key]);
        unset($this->pendingManagedRecycles[$key]);
        return true;
    }

    protected function isManagedPlanRecyclePending(array $plan): bool {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $key = ((string)($plan['version'] ?? '') . '@' . $host . ':' . $port);
        return isset($this->pendingManagedRecycles[$key]);
    }

    protected function hasPendingRecycleForVersion(string $version): bool {
        if ($version === '') {
            return false;
        }
        foreach ($this->pendingManagedRecycles as $item) {
            $plan = (array)($item['plan'] ?? []);
            if ((string)($plan['version'] ?? '') === $version) {
                return true;
            }
        }
        return false;
    }

    protected function warnStuckDrainingManagedUpstreams(): void {
        $diagnostics = $this->instanceManager->drainingDiagnostics();
        $activeVersions = [];
        $now = time();
        foreach ($diagnostics as $item) {
            $version = (string)($item['version'] ?? '');
            if ($version === '') {
                continue;
            }
            $activeVersions[$version] = true;
            $startedAt = (int)($item['drain_started_at'] ?? 0);
            if ($startedAt <= 0) {
                continue;
            }
            $elapsed = $now - $startedAt;
            $warnAfter = max(
                self::ROLLING_DRAIN_GRACE_SECONDS,
                max(1, (int)(($item['drain_deadline_at'] ?? 0) - $startedAt))
            );
            if ($elapsed < $warnAfter) {
                continue;
            }
            $lastWarnAt = (int)($this->drainingGenerationWarnState[$version] ?? 0);
            if ($lastWarnAt > 0 && ($now - $lastWarnAt) < 60) {
                continue;
            }
            $this->drainingGenerationWarnState[$version] = $now;
            Console::warning(
                "【Gateway】旧业务实例仍在 draining，尚未进入 shutdown: generation={$version}, "
                . "draining={$elapsed}s, ws=" . (int)($item['connections'] ?? 0)
                . ", http=" . (int)($item['http_processing'] ?? 0)
                . ", rpc=" . (int)($item['rpc_processing'] ?? 0)
                . ", queue=" . (int)($item['redis_queue_processing'] ?? 0)
                . ", crontab=" . (int)($item['crontab_busy'] ?? 0)
            );
        }
        foreach (array_keys($this->drainingGenerationWarnState) as $version) {
            if (!isset($activeVersions[$version])) {
                unset($this->drainingGenerationWarnState[$version]);
            }
        }
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
            '/crontab_run' => ['POST'],
            '/crontab_status' => ['POST'],
            '/crontab_override' => ['POST'],
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
                $result = $this->dispatchInternalGatewayCommand((string)($payload['command'] ?? ''));
                $this->json($response, (int)($result['status'] ?? 500), $result['data'] ?? ['message' => (string)($result['message'] ?? 'request failed')]);
                return;
            }

            if ($method === 'GET' && $path === '/_gateway/healthz') {
                $this->json($response, 200, [
                    'message' => 'ok',
                    'active_version' => $this->instanceManager->snapshot()['active_version'] ?? null,
                ]);
                return;
            }

            if ($method === 'GET' && $path === '/_gateway/upstreams') {
                $this->json($response, 200, $this->instanceManager->snapshot());
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
                    'state' => $this->instanceManager->snapshot(),
                ]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/versions/activate') {
                $state = $this->instanceManager->activateVersion(
                    (string)($payload['version'] ?? ''),
                    (int)($payload['grace_seconds'] ?? 30)
                );
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
            $this->json($response, 400, [
                'message' => 'request failed',
                'error' => $e->getMessage(),
            ]);
        }
    }

    protected function localConsoleSubscriptionPayload(): array {
        return [
            'enabled' => $this->dashboardEnabled() ? $this->hasConsoleSubscribers() : ConsoleRelay::remoteSubscribed(),
        ];
    }

    protected function dispatchInternalGatewayCommand(string $command): array {
        $command = trim($command);
        if ($command === '') {
            return ['ok' => false, 'status' => 400, 'message' => 'command required'];
        }

        switch ($command) {
            case 'reload':
                Timer::after(1, function () {
                    $this->restartGatewayBusinessPlane();
                });
                return [
                    'ok' => true,
                    'status' => 200,
                    'data' => ['accepted' => true, 'message' => 'gateway business reload started'],
                ];
            case 'restart':
            case 'shutdown':
                Timer::after(1, function () {
                    $this->shutdownGateway();
                });
                return [
                    'ok' => true,
                    'status' => 200,
                    'data' => ['accepted' => true, 'message' => 'gateway shutdown started'],
                ];
            default:
                return ['ok' => false, 'status' => 400, 'message' => 'unsupported command'];
        }
    }

    protected function applyStaticHandlerSettings(): void {
        if ($this->trafficMode() !== 'http') {
            return;
        }
        $locations = Config::server()['static_handler_locations'] ?? [];
        if (!is_array($locations) || !$locations) {
            return;
        }
        $settings = [
            'document_root' => APP_PATH . '/public',
            'enable_static_handler' => true,
            'http_index_files' => ['index.html'],
            'static_handler_locations' => array_values($locations),
        ];
        if (Env::isDev()) {
            $settings['http_autoindex'] = true;
        }
        $this->server->set($settings);
    }

    protected function proxyMaxInflightRequests(): int {
        $configured = Config::server()['gateway_proxy_max_inflight'] ?? 1024;
        $limit = (int)$configured;
        return $limit > 0 ? $limit : 1024;
    }

    protected function shouldRejectNewProxyRequest(): bool {
        return $this->instanceManager->totalProxyHttpRequestCount() >= $this->proxyMaxInflightRequests();
    }

    protected function shouldRejectNewRelayConnection(): bool {
        if (!$this->tcpRelayModeEnabled()) {
            return false;
        }
        return $this->tcpRelayHandler()->activeRelayConnectionCount() >= $this->proxyMaxInflightRequests();
    }

    protected function decodeJsonBody(Request $request): array {
        $raw = $request->rawContent();
        if ($raw === '' || $raw === false) {
            return [];
        }
        $decoded = json_decode($raw, true);
        return is_array($decoded) ? $decoded : [];
    }

    protected function json(Response $response, int $status, array $payload): void {
        $response->status($status);
        $response->header('Content-Type', 'application/json;charset=utf-8');
        $response->end(json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
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

    protected function resolveAffinityKey(Request $request): ?string {
        $cookie = (array)($request->cookie ?? []);
        foreach (['_CID_', '_SESSIONID_'] as $cookieName) {
            $value = trim((string)($cookie[$cookieName] ?? ''));
            if ($value !== '') {
                return $cookieName . ':' . $value;
            }
        }

        $authorization = trim((string)($request->header['authorization'] ?? ''));
        if ($authorization !== '') {
            return 'authorization:' . $authorization;
        }

        $websocketKey = trim((string)($request->header['sec-websocket-key'] ?? ''));
        if ($websocketKey !== '') {
            return 'ws:' . $websocketKey;
        }

        return null;
    }

    protected function shutdownManagedUpstreams(): void {
        if ($this->upstreamSupervisor) {
            $this->upstreamSupervisor->sendCommand(['action' => 'shutdown']);
        }
        $this->instanceManager->removeManagedInstances();
    }

    protected function ensureInstallWatcher(): void {
        if (App::isReady() || $this->installWatcherStarted) {
            return;
        }
        $this->installWatcherStarted = true;
        Console::info('【Gateway】等待安装配置文件就绪...');
        Coroutine::create(function () {
            while (true) {
                if (App::isReady()) {
                    Console::success('【Gateway】应用安装成功!开始拉起业务实例');
                    $this->installWatcherStarted = false;
                    return;
                }

                $installer = App::installer();
                if ($installer->readyToInstall() && !$this->installUpdating) {
                    $this->installUpdating = true;
                    $updater = Updater::instance();
                    $version = $updater->getVersion();
                    $targetVersion = (string)($version['remote']['app']['version'] ?? '');
                    $targetVersion !== '' && Console::info('【Gateway】开始执行安装更新:' . $targetVersion);
                    if ($updater->updateApp(true)) {
                        while (!App::isReady()) {
                            Coroutine::sleep(1);
                        }
                        $this->installUpdating = false;
                        continue;
                    }
                    $message = $updater->getLastError() ?: ($targetVersion !== '' ? "更新失败:{$targetVersion}" : '更新失败');
                    Console::warning('【Gateway】' . $message);
                    $this->installUpdating = false;
                }
                Coroutine::sleep(1);
            }
        });
    }

    protected function ensureGatewaySubProcessManagerStarted(): void {
        if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        if ($this->subProcessManager) {
            return;
        }
        $this->subProcessManager = new SubProcessManager($this->server, Config::server(), [
            'exclude_processes' => ['CrontabManager', 'RedisQueue'],
            'shutdown_handler' => function (): void {
                $this->shutdownGateway();
            },
            'reload_handler' => function (): void {
                $this->restartGatewayBusinessPlane();
            },
            'command_handler' => function (string $command, array $params, object $socket): bool {
                return $this->handleGatewayProcessCommand($command, $params, $socket);
            },
            'node_status_builder' => function (array $status): array {
                return $this->buildGatewayHeartbeatStatus($status);
            },
        ]);
        $this->subProcessManager->start();
    }

    protected function attachUpstreamSupervisor(): void {
        if (!$this->launcher || !$this->managedUpstreamPlans) {
            return;
        }

        $this->upstreamSupervisor = new UpstreamSupervisor(
            $this->launcher,
            []
        );
        $this->server->addProcess($this->upstreamSupervisor->getProcess());
        $this->lastObservedUpstreamSupervisorPid = 0;
        $this->lastObservedUpstreamSupervisorStartedAt = 0;
        $this->pendingUpstreamSupervisorSyncReason = 'initial';
    }

    protected function bootstrapManagedUpstreams(): void {
        if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        if (!$this->launcher || !$this->managedUpstreamPlans) {
            return;
        }

        foreach ($this->managedUpstreamPlans as $plan) {
            $version = (string)($plan['version'] ?? '');
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            $key = ($version . '@' . $host . ':' . $port);
            if (isset($this->bootstrappedManagedInstances[$key])) {
                continue;
            }

            $rpcPort = (int)($plan['rpc_port'] ?? 0);
            if ($port <= 0) {
                continue;
            }
            if (!$this->launcher->isListening($host, $port, 0.2)) {
                if (!$this->upstreamSupervisor) {
                    continue;
                }
                Console::info("【Gateway】启动业务实例: " . $this->describePlan($plan));
                $this->upstreamSupervisor->sendCommand(['action' => 'spawn', 'plan' => $plan]);
                if (!$this->launcher->waitUntilServicesReady($host, $port, $rpcPort, (int)($plan['start_timeout'] ?? 25))) {
                    continue;
                }
            }

            if ($rpcPort > 0 && !$this->launcher->isListening($host, $rpcPort, 0.2)) {
                continue;
            }

            $weight = (int)($plan['weight'] ?? 100);
            $metadata = (array)($plan['metadata'] ?? []);
            $metadata['managed'] = true;
            $metadata['role'] = (string)($plan['role'] ?? ($metadata['role'] ?? SERVER_ROLE));
            $metadata['rpc_port'] = (int)($plan['rpc_port'] ?? ($metadata['rpc_port'] ?? 0));
            $metadata['started_at'] = (int)($metadata['started_at'] ?? time());
            $metadata['managed_mode'] = 'gateway_supervisor';

            $this->registerManagedPlan($plan, true);
            $this->instanceManager->removeOtherInstances($version, $host, $port);
            $this->syncNginxProxyTargets('bootstrap_ready');
            $rpcInfo = (int)($plan['rpc_port'] ?? 0) > 0 ? ', RPC:' . (int)$plan['rpc_port'] : '';
            Console::success("【Gateway】业务实例就绪 {$version} {$host}:{$port}{$rpcInfo}");
        }
    }

    protected function renderStartupInfo(): void {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $stateFile = $this->instanceManager->stateFile();
        Console::success("【Gateway】服务启动完成");
        if ($this->nginxProxyModeEnabled()) {
            Console::info("【Gateway】业务入口监听(nginx): {$this->host}:{$this->port}");
            Console::info("【Gateway】控制面内部监听: {$this->internalControlHost()}:{$this->controlPort()}");
        } elseif ($this->tcpRelayModeEnabled()) {
            Console::info("【Gateway】业务/控制入口监听: {$this->host}:{$this->port}");
            Console::info("【Gateway】控制面内部监听: {$this->internalControlHost()}:{$this->controlPort()}");
        } else {
            Console::info("【Gateway】监听地址: {$this->host}:{$this->port}");
        }
        if ($this->rpcPort > 0) {
            Console::info("【Gateway】RPC监听: {$this->host}:{$this->rpcPort}");
        }
        Console::info("【Gateway】流量模式: " . $this->trafficMode());
        Console::info("【Gateway】运行信息: env=" . SERVER_RUN_ENV . ", role=" . SERVER_ROLE . ", app=" . APP_DIR_NAME . ", version=" . $appVersion);
        Console::info("【Gateway】进程信息: master={$this->serverMasterPid}, manager={$this->serverManagerPid}");
        Console::info("【Gateway】状态文件: {$stateFile}");
    }

    protected function createBusinessTrafficListener(): void {
        if (!$this->tcpRelayModeEnabled()) {
            return;
        }
        if ($this->controlPort() === $this->port) {
            throw new RuntimeException('tcp流量模式下控制面端口不能与业务端口相同');
        }
        $listener = $this->server->listen($this->host, $this->port, SWOOLE_SOCK_TCP);
        if ($listener === false) {
            throw new RuntimeException("Gateway业务转发监听失败: {$this->host}:{$this->port}");
        }
        $listener->set([
            'package_max_length' => 20 * 1024 * 1024,
            'open_http_protocol' => false,
            'open_http2_protocol' => false,
            'open_websocket_protocol' => false,
        ]);
    }

    protected function resolveAppVersion(array $appInfo = []): string {
        return App::version()
            ?: (string)($appInfo['version'] ?? (App::profile()->version ?? '--'));
    }

    protected function resolvePublicVersion(array $appInfo = []): string {
        return App::publicVersion()
            ?: (string)($appInfo['public_version'] ?? (App::profile()->public_version ?? '--'));
    }

    protected function describePlan(array $plan): string {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $version = (string)($plan['version'] ?? 'unknown');
        return $rpcPort > 0
            ? "{$version} {$host}:{$port}, RPC:{$rpcPort}"
            : "{$version} {$host}:{$port}";
    }

    protected function formatDashboardParamsLog(array $params): string {
        if (!$params) {
            return '';
        }
        $json = json_encode($params, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        return $json === false ? '' : ", params={$json}";
    }

    protected function pidFile(): string {
        return dirname(SCF_ROOT) . '/var/' . APP_DIR_NAME . '_gateway_' . SERVER_ROLE . '.pid';
    }
}
