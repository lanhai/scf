<?php

namespace Scf\Server\Proxy;

use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
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

    protected Server $server;
    protected array $wsClients = [];
    protected array $wsPumpCoroutines = [];
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

    public function __construct(
        protected AppInstanceManager $instanceManager,
        protected string $host,
        protected int $port,
        protected int $workerNum = 1,
        protected ?AppServerLauncher $launcher = null,
        protected array $managedUpstreamPlans = [],
        protected int $rpcBindPort = 0
    ) {
        $this->rpcPort = $rpcBindPort;
        $this->startedAt = time();
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
            'enable_static_handler' => false,
            'package_max_length' => 20 * 1024 * 1024,
            'log_file' => APP_LOG_PATH . '/gateway.log',
            'pid_file' => $this->pidFile(),
        ]);

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
            if ($this->dashboardEnabled()) {
                File::write(SERVER_DASHBOARD_PORT_FILE, (string)$this->port);
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
                $this->ensureGatewaySubProcessManagerStarted();
                $this->ensureInstallWatcher();
                if (App::isReady()) {
                    $this->bootstrapManagedUpstreams();
                }
                $this->instanceManager->tick();
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
        if (!App::isReady()) {
            $response->status(503);
            $response->header('Content-Type', 'text/html; charset=utf-8');
            $response->end('应用安装中...');
            return;
        }

        $upstream = $this->instanceManager->pickHttpUpstream($this->resolveAffinityKey($request));
        if (!$upstream) {
            $this->json($response, 503, ['message' => '没有可用的业务实例']);
            return;
        }

        try {
            $client = $this->createHttpClient($upstream);
            $client->setHeaders($this->buildUpstreamHttpHeaders($request, $upstream));
            $client->set(['timeout' => 30]);
            $method = strtoupper($request->server['request_method'] ?? 'GET');
            $client->setMethod($method);
            $body = $request->rawContent();
            if ($body !== '' && $body !== false) {
                $client->setData($body);
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
            $client->close();
        } catch (Throwable $e) {
            $this->json($response, 502, [
                'message' => '代理转发失败',
                'error' => $e->getMessage(),
                'upstream' => $upstream,
            ]);
        }
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

        try {
            $client = $this->createHttpClient($upstream);
            $client->setHeaders($this->buildUpstreamWsHeaders($request, $upstream));
            $client->set(['timeout' => 10]);
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

    protected function onMessage(Server $server, Frame $frame): void {
        if (isset($this->nodeClients[$frame->fd])) {
            $this->handleNodeSocketMessage($server, $frame);
            return;
        }
        if (isset($this->dashboardClients[$frame->fd])) {
            $this->handleDashboardSocketMessage($server, $frame);
            return;
        }
        $client = $this->wsClients[$frame->fd] ?? null;
        if (!$client) {
            $this->disconnectClient($server, $frame->fd);
            return;
        }
        $forward = function () use ($client, $frame, $server): void {
            try {
                $ok = $client->push($frame->data, $frame->opcode, $frame->finish ? SWOOLE_WEBSOCKET_FLAG_FIN : 0);
                if ($ok === false) {
                    throw new RuntimeException($client->errMsg ?: 'push failed');
                }
            } catch (Throwable $throwable) {
                Console::warning("【Gateway】WebSocket上游发送失败 fd={$frame->fd}: " . $throwable->getMessage());
                $this->disconnectClient($server, $frame->fd);
            }
        };

        if (Coroutine::getCid() > 0) {
            $forward();
            return;
        }

        Coroutine::create($forward);
    }

    protected function onClose(Server $server, int $fd): void {
        unset($this->dashboardClients[$fd]);
        unset($this->nodeClients[$fd]);
        if (isset($this->wsClients[$fd])) {
            try {
                $this->wsClients[$fd]->close();
            } catch (Throwable) {
            }
            unset($this->wsClients[$fd]);
        }
        unset($this->wsPumpCoroutines[$fd]);
        $this->removeNodeClient($fd);
        $this->instanceManager->releaseWebsocketBinding($fd);
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
        Runtime::instance()->dashboardPort($this->dashboardEnabled() ? $this->port : 0);
        ConsoleRelay::setGatewayPort($this->port);
        ConsoleRelay::setLocalSubscribed(false);
        ConsoleRelay::setRemoteSubscribed(false);
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
                unset($this->dashboardClients[$fd], $this->nodeClients[$fd], $this->wsPumpCoroutines[$fd]);
                if (isset($this->wsClients[$fd])) {
                    try {
                        $this->wsClients[$fd]->close();
                    } catch (Throwable) {
                    }
                    unset($this->wsClients[$fd]);
                }
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

    protected function onReceive(Server $server, int $fd, int $reactorId, string $data): void {
        $clientInfo = $server->getClientInfo($fd);
        if (!$clientInfo || (int)($clientInfo['server_port'] ?? 0) !== $this->rpcPort) {
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

    public function dashboardServerStatus(string $token, string $host = '', string $referer = ''): array {
        $status = $this->buildDashboardRealtimeStatus();
        $status['socket_host'] = $this->buildDashboardSocketHost($host, $referer) . '?token=' . rawurlencode($token);
        $status['latest_version'] = App::latestVersion();
        $status['dashboard'] = $this->buildDashboardVersionStatus();
        $status['framework'] = $this->buildFrameworkVersionStatus();
        $status['upstreams'] = $this->dashboardUpstreams();
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
        if ($slaveHosts) {
            $this->sendCommandToAllNodeClients('appoint_update', [
                'type' => $type,
                'version' => $version,
                'task_id' => $taskId,
            ]);
        }

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
            $restartSummary = $this->rollingUpdateManagedUpstreams($type, $version);
        }

        if ($type !== 'public' && $restartSummary['success_count'] > 0 && !$restartSummary['failed_nodes']) {
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        }

        if (in_array($type, ['app', 'framework'], true) && !$restartSummary['failed_nodes']) {
            $this->iterateGatewayBusinessProcesses();
        }

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
        if ($payload['failed_nodes']) {
            return Result::error('部分节点升级失败', 'SERVICE_ERROR', $payload);
        }
        return Result::success($payload);
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

    protected function waitForGatewayPortsReleased(int $timeoutSeconds = 15, int $intervalMs = 200): void {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $ports = array_values(array_filter([$this->port, $this->rpcPort], static fn(int $port) => $port > 0));
        if (!$ports) {
            return;
        }

        $logged = false;
        while (microtime(true) < $deadline) {
            $occupied = false;
            foreach ($ports as $port) {
                if (CoreServer::isPortInUse($port)) {
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
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $lastException = null;
        $logged = false;
        do {
            try {
                if ($logged) {
                    Console::success("【Gateway】监听端口抢占成功: {$this->host}:{$this->port}");
                }
                return new Server($this->host, $this->port, SWOOLE_PROCESS, SWOOLE_SOCK_TCP);
            } catch (SwooleException $exception) {
                $lastException = $exception;
                if (!str_contains($exception->getMessage(), 'Address already in use')) {
                    throw $exception;
                }
                if (!$logged) {
                    Console::warning("【Gateway】监听端口占用，等待重试: {$this->host}:{$this->port}");
                    $logged = true;
                }
                usleep(max(50, $intervalMs) * 1000);
            }
        } while (microtime(true) < $deadline);

        throw $lastException ?: new RuntimeException("Gateway 监听失败: {$this->host}:{$this->port}");
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

    protected function prepareGatewayShutdown(): void {
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        Runtime::instance()->serverIsAlive(false);
        $this->clearHousekeepingTimer();
        $this->clearClusterStatusTimer();
        isset($this->subProcessManager) && $this->subProcessManager->shutdown();
        $this->shutdownManagedUpstreams();
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
                        (string)($request->header['host'] ?? ''),
                        (string)($request->header['referer'] ?? '')
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
                        'time' => date('m-d H:i:s'),
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
                        'time' => date('m-d H:i:s'),
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

    protected function buildDashboardRealtimeStatus(): array {
        $upstreams = $this->dashboardUpstreams();
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
                $instances[] = $this->buildUpstreamNode($generation, $instance, $this->fetchUpstreamRuntimeStatus($instance));
            }
        }
        return $instances;
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
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        if ($host === '' || $port <= 0) {
            return [];
        }
        if ($this->launcher && !$this->launcher->isListening($host, $port, 0.2)) {
            return [];
        }

        try {
            if (Coroutine::getCid() <= 0) {
                return $this->fetchUpstreamRuntimeStatusSync($host, $port);
            }
            $client = new Client($host, $port, false);
            $client->set(['timeout' => 1.5]);
            $client->setHeaders([
                'host' => $host . ':' . $port,
                'x-gateway-internal' => '1',
            ]);
            if (!$client->get(self::INTERNAL_UPSTREAM_STATUS_PATH)) {
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
        $body = @file_get_contents('http://' . $host . ':' . $port . self::INTERNAL_UPSTREAM_STATUS_PATH, false, $context);
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
            'public_version' => $base['public_version'] ?? '--',
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

    protected function buildDashboardSocketHost(string $host, string $referer): string {
        $protocol = (!empty($referer) && str_starts_with($referer, 'https')) ? 'wss://' : 'ws://';
        $displayHost = trim($host) !== '' ? $host : ('127.0.0.1:' . $this->port);
        return $protocol . $displayHost . '/dashboard.socket';
    }

    protected function buildDashboardVersionStatus(): array {
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

        return [
            'version' => $currentDashboardVersion['version'] ?? '0.0.0',
            'latest_version' => $dashboardVersion['version'] ?? '--',
        ];
    }

    protected function buildFrameworkVersionStatus(): array {
        $remoteVersion = [
            'version' => FRAMEWORK_BUILD_VERSION,
            'build' => FRAMEWORK_BUILD_TIME,
        ];

        $client = \Scf\Client\Http::create(ENV_VARIABLES['scf_update_server']);
        $response = $client->get();
        if (!$response->hasError()) {
            $remoteVersion = $response->getData();
        }

        $upstreams = $this->dashboardUpstreams();
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

        $time = (string)($payload['time'] ?? date('m-d H:i:s'));
        $node = (string)($payload['node'] ?? SERVER_HOST);
        $sourceType = (string)($payload['source_type'] ?? 'gateway');

        if ($this->dashboardEnabled()) {
            if (!$this->hasConsoleSubscribers()) {
                return false;
            }
            $this->pushDashboardEvent([
                'event' => 'console',
                'message' => ['data' => $message, 'source_type' => $sourceType],
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
        $basePlans = array_values(array_filter($basePlans, static function ($plan) {
            return (int)($plan['port'] ?? 0) > 0;
        }));

        $failedNodes = [];
        $successCount = 0;
        if (!$basePlans || !$this->upstreamSupervisor || !$this->launcher) {
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
        $key = ((string)($plan['version'] ?? '') . '@' . $host . ':' . $port);

        if ($this->upstreamSupervisor) {
            $this->upstreamSupervisor->sendCommand(['action' => 'stop_port', 'port' => $port]);
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
            ], 3);
        }

        if ($removeState) {
            $this->instanceManager->removeInstance($host, $port);
        }
        unset($this->bootstrappedManagedInstances[$key]);
        if ($removePlan) {
            $this->removeManagedPlan($plan);
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
                $this->stopManagedPlan($plan);
            }
            $this->instanceManager->removeVersion((string)($generation['version'] ?? ''));
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
                $this->json($response, 200, [
                    'enabled' => $this->dashboardEnabled() ? $this->hasConsoleSubscribers() : ConsoleRelay::remoteSubscribed(),
                ]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/internal/console/log') {
                if (!$this->isInternalGatewayRequest($request)) {
                    $this->json($response, 403, ['message' => 'forbidden']);
                    return;
                }
                $accepted = $this->acceptConsolePayload([
                    'time' => (string)($payload['time'] ?? ''),
                    'message' => (string)($payload['message'] ?? ''),
                    'source_type' => (string)($payload['source_type'] ?? 'gateway'),
                    'node' => (string)($payload['node'] ?? SERVER_HOST),
                ]);
                $this->json($response, 200, ['accepted' => $accepted]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/internal/command') {
                if (!$this->isInternalGatewayRequest($request)) {
                    $this->json($response, 403, ['message' => 'forbidden']);
                    return;
                }
                $command = trim((string)($payload['command'] ?? ''));
                if ($command === '') {
                    $this->json($response, 400, ['message' => 'command required']);
                    return;
                }
                switch ($command) {
                    case 'reload':
                        Timer::after(1, function () {
                            $this->restartGatewayBusinessPlane();
                        });
                        $this->json($response, 200, ['accepted' => true, 'message' => 'gateway business reload started']);
                        return;
                    case 'restart':
                    case 'shutdown':
                        Timer::after(1, function () {
                            $this->shutdownGateway();
                        });
                        $this->json($response, 200, ['accepted' => true, 'message' => 'gateway shutdown started']);
                        return;
                    default:
                        $this->json($response, 400, ['message' => 'unsupported command']);
                        return;
                }
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
                $this->json($response, 200, [
                    'message' => 'draining',
                    'state' => $state,
                ]);
                return;
            }

            if ($method === 'POST' && $path === '/_gateway/versions/remove') {
                $state = $this->instanceManager->removeVersion((string)($payload['version'] ?? ''));
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

    protected function startUpstreamPump(int $fd, Client $client): void {
        $this->wsPumpCoroutines[$fd] = Coroutine::create(function () use ($fd, $client) {
            while (true) {
                $frame = $client->recv();
                if ($frame === false || $frame === '' || $frame === null) {
                    break;
                }
                if (!isset($this->server) || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
                    break;
                }
                if ($frame instanceof Frame) {
                    if ($frame->opcode === WEBSOCKET_OPCODE_CLOSE) {
                        $this->disconnectClient($this->server, $fd);
                        break;
                    }
                    $ok = $this->server->push($fd, $frame->data, $frame->opcode);
                    if ($ok === false) {
                        Console::warning("【Gateway】WebSocket下游推送失败 fd={$fd}");
                    }
                    continue;
                }
                $ok = $this->server->push($fd, (string)$frame);
                if ($ok === false) {
                    Console::warning("【Gateway】WebSocket下游推送失败 fd={$fd}");
                }
            }

            if (isset($this->server)) {
                $this->disconnectClient($this->server, $fd);
            }
        });
    }

    protected function disconnectClient(Server $server, int $fd): void {
        if (!$server->exist($fd)) {
            return;
        }
        try {
            if ($server->isEstablished($fd)) {
                $server->disconnect($fd);
                return;
            }
            $server->close($fd);
        } catch (Throwable) {
        }
    }

    protected function createHttpClient(array $upstream): Client {
        return new Client($upstream['host'], (int)$upstream['port'], false);
    }

    protected function buildTargetPath(Request $request): string {
        $path = $request->server['request_uri'] ?? '/';
        if (!empty($request->server['query_string'])) {
            $path .= '?' . $request->server['query_string'];
        }
        return $path;
    }

    protected function buildUpstreamHttpHeaders(Request $request, array $upstream): array {
        $headers = [];
        foreach (($request->header ?? []) as $key => $value) {
            if ($this->shouldSkipRequestHeader($key)) {
                continue;
            }
            $headers[$key] = $value;
        }
        $cookieHeader = $this->buildCookieHeader($request);
        if ($cookieHeader !== null) {
            $headers['cookie'] = $cookieHeader;
        }
        $headers['host'] = $upstream['host'] . ':' . $upstream['port'];
        $headers['x-forwarded-for'] = $request->server['remote_addr'] ?? '127.0.0.1';
        $headers['x-forwarded-proto'] = 'http';
        $headers['x-forwarded-host'] = ($request->header['host'] ?? ($this->host . ':' . $this->port));
        return $headers;
    }

    protected function buildUpstreamWsHeaders(Request $request, array $upstream): array {
        $headers = [];
        foreach (($request->header ?? []) as $key => $value) {
            if (in_array(strtolower($key), ['host', 'connection'], true)) {
                continue;
            }
            $headers[$key] = $value;
        }
        $cookieHeader = $this->buildCookieHeader($request);
        if ($cookieHeader !== null) {
            $headers['cookie'] = $cookieHeader;
        }
        $headers['host'] = $upstream['host'] . ':' . $upstream['port'];
        $headers['connection'] = 'Upgrade';
        $headers['upgrade'] = 'websocket';
        $headers['x-forwarded-for'] = $request->server['remote_addr'] ?? '127.0.0.1';
        $headers['x-forwarded-proto'] = 'ws';
        return $headers;
    }

    protected function shouldSkipRequestHeader(string $key): bool {
        return in_array(strtolower($key), [
            'host',
            'connection',
            'keep-alive',
            'proxy-authenticate',
            'proxy-authorization',
            'te',
            'trailers',
            'transfer-encoding',
            'upgrade',
        ], true);
    }

    protected function shouldSkipResponseHeader(string $key): bool {
        return in_array(strtolower($key), [
            'connection',
            'keep-alive',
            'proxy-authenticate',
            'proxy-authorization',
            'set-cookie',
            'content-encoding',
            'te',
            'trailers',
            'transfer-encoding',
            'upgrade',
            'content-length',
        ], true);
    }

    protected function buildCookieHeader(Request $request): ?string {
        $cookieHeader = trim((string)($request->header['cookie'] ?? ''));
        if ($cookieHeader !== '') {
            return $cookieHeader;
        }

        $cookies = (array)($request->cookie ?? []);
        if (!$cookies) {
            return null;
        }

        $pairs = [];
        foreach ($cookies as $name => $value) {
            $name = trim((string)$name);
            if ($name === '') {
                continue;
            }
            $pairs[] = $name . '=' . rawurlencode((string)$value);
        }

        return $pairs ? implode('; ', $pairs) : null;
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

    protected function forwardResponseCookies(Response $response, Client $client): void {
        $headers = $client->set_cookie_headers ?? [];
        if (!$headers) {
            $singleHeader = $client->headers['set-cookie'] ?? null;
            if (is_string($singleHeader) && $singleHeader !== '') {
                $headers = [$singleHeader];
            }
        }

        foreach ((array)$headers as $headerValue) {
            $parsed = $this->parseSetCookieHeader((string)$headerValue);
            if ($parsed === null) {
                $response->header('Set-Cookie', (string)$headerValue, false);
                continue;
            }
            $response->cookie(
                $parsed['name'],
                $parsed['value'],
                $parsed['expires'],
                $parsed['path'],
                $parsed['domain'],
                $parsed['secure'],
                $parsed['httponly']
            );
        }
    }

    protected function parseSetCookieHeader(string $headerValue): ?array {
        $headerValue = trim($headerValue);
        if ($headerValue === '') {
            return null;
        }

        $segments = array_map('trim', explode(';', $headerValue));
        $nameValue = array_shift($segments);
        if (!$nameValue || !str_contains($nameValue, '=')) {
            return null;
        }

        [$name, $value] = explode('=', $nameValue, 2);
        $cookie = [
            'name' => trim($name),
            'value' => $value,
            'expires' => 0,
            'path' => '/',
            'domain' => '',
            'secure' => false,
            'httponly' => false,
        ];

        foreach ($segments as $segment) {
            if ($segment === '') {
                continue;
            }
            if (!str_contains($segment, '=')) {
                $flag = strtolower($segment);
                if ($flag === 'secure') {
                    $cookie['secure'] = true;
                } elseif ($flag === 'httponly') {
                    $cookie['httponly'] = true;
                }
                continue;
            }
            [$attr, $attrValue] = explode('=', $segment, 2);
            $attr = strtolower(trim($attr));
            $attrValue = trim($attrValue);
            if ($attr === 'expires') {
                $timestamp = strtotime($attrValue);
                $cookie['expires'] = $timestamp === false ? 0 : $timestamp;
            } elseif ($attr === 'max-age') {
                $cookie['expires'] = time() + max(0, (int)$attrValue);
            } elseif ($attr === 'path') {
                $cookie['path'] = $attrValue ?: '/';
            } elseif ($attr === 'domain') {
                $cookie['domain'] = $attrValue;
            }
        }

        return $cookie['name'] === '' ? null : $cookie;
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
            App::isReady() ? $this->managedUpstreamPlans : []
        );
        $this->server->addProcess($this->upstreamSupervisor->getProcess());
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
            $rpcInfo = (int)($plan['rpc_port'] ?? 0) > 0 ? ', RPC:' . (int)$plan['rpc_port'] : '';
            Console::success("【Gateway】业务实例就绪 {$version} {$host}:{$port}{$rpcInfo}");
        }
    }

    protected function renderStartupInfo(): void {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $stateFile = $this->instanceManager->stateFile();
        Console::success("【Gateway】服务启动完成");
        Console::info("【Gateway】监听地址: {$this->host}:{$this->port}");
        if ($this->rpcPort > 0) {
            Console::info("【Gateway】RPC监听: {$this->host}:{$this->rpcPort}");
        }
        Console::info("【Gateway】运行信息: env=" . SERVER_RUN_ENV . ", role=" . SERVER_ROLE . ", app=" . APP_DIR_NAME . ", version=" . $appVersion);
        Console::info("【Gateway】进程信息: master={$this->serverMasterPid}, manager={$this->serverManagerPid}");
        Console::info("【Gateway】状态文件: {$stateFile}");
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
