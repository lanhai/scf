<?php

namespace Scf\Server\Proxy;

use Scf\Command\Color;
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
use Scf\Root;
use Scf\Server\DashboardAuth;
use Scf\Server\LinuxCrontab\LinuxCrontabManager;
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
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;
use Swoole\Exception as SwooleException;
use Throwable;

/**
 * Gateway 是 proxy 模式下的总控节点。
 *
 * 它本身不承载业务 worker，而是负责：
 * - 暴露控制面 HTTP/WS 与 dashboard API；
 * - 托管 redisQueue / watcher / upstream 协调器等 gateway 子进程；
 * - 管理 managed upstream 的启动、切流、回滚、自愈与回收；
 * - 聚合集群节点和业务实例状态，提供给 dashboard 展示。
 *
 * 设计上刻意把“控制面”和“业务面”拆开：gateway 只负责调度与观测，
 * 真正承载业务请求的仍然是 upstream。
 */
class GatewayServer {

    /**
     * dashboard 触发本地升级时等待业务编排子进程返回的最长时间。
     *
     * 该等待窗口需要与 Updater 下载包的超时保持一致，否则 worker 可能先超时，
     * 而业务编排子进程还在无意义地等待下载层返回。
     */
    private const DASHBOARD_UPDATE_LOCAL_TIMEOUT_SECONDS = 300;

    /**
     * dashboard 等待 slave 节点汇总升级结果的最长时间。
     *
     * 本地更新完成后，worker 仍需给在线 slave 留出统一的收口窗口；该窗口保持
     * 与前端 longTaskTimeout 兼容，避免请求尚未返回就先在浏览器侧超时。
     */
    private const DASHBOARD_UPDATE_CLUSTER_TIMEOUT_SECONDS = 300;
    protected const ROLLING_DRAIN_GRACE_SECONDS = 30;
    protected const ROLLING_CUTOVER_VERIFY_TIMEOUT_SECONDS = 8;
    protected const ROLLING_CUTOVER_STABLE_CHECKS = 3;
    protected const INTERNAL_UPSTREAM_STATUS_PATH = '/_gateway/internal/upstream/status';
    protected const INTERNAL_UPSTREAM_HEALTH_PATH = '/_gateway/internal/upstream/health';
    protected const INTERNAL_UPSTREAM_HTTP_PROBE_PATH = '/_gateway/internal/upstream/http_probe';
    protected const ACTIVE_UPSTREAM_HEALTH_CHECK_INTERVAL_SECONDS = 5;
    protected const ACTIVE_UPSTREAM_HEALTH_FAILURE_THRESHOLD = 3;
    protected const ACTIVE_UPSTREAM_HEALTH_COOLDOWN_SECONDS = 60;
    protected const ACTIVE_UPSTREAM_HEALTH_STARTUP_GRACE_SECONDS = 20;
    protected const ACTIVE_UPSTREAM_HEALTH_ROLLING_COOLDOWN_SECONDS = 15;
    protected const UPSTREAM_SUPERVISOR_SYNC_INTERVAL_SECONDS = 15;
    protected const DASHBOARD_VERSION_CACHE_TTL_SECONDS = 30;
    protected const FRAMEWORK_VERSION_CACHE_TTL_SECONDS = 30;

    protected Server $server;
    protected array $dashboardClients = [];
    protected array $nodeClients = [];
    protected array $bootstrappedManagedInstances = [];
    protected ?bool $lastConsoleSubscriptionState = null;
    protected ?UpstreamSupervisor $upstreamSupervisor = null;
    protected int $rpcPort = 0;
    protected int $startedAt;
    protected int $serverMasterPid = 0;
    protected int $serverManagerPid = 0;
    protected bool $gatewayShutdownScheduled = false;
    protected bool $gatewayReloadScheduled = false;
    protected ?SubProcessManager $subProcessManager = null;
    protected ?LocalIpcServer $localIpcServer = null;
    protected array $drainingGenerationWarnState = [];
    protected array $pendingManagedRecycles = [];
    protected array $pendingManagedRecycleWatchers = [];
    protected array $pendingManagedRecycleWarnState = [];
    protected array $pendingManagedRecycleCompletions = [];
    protected array $managedUpstreamHealthState = [];
    protected bool $managedUpstreamSelfHealing = false;
    protected bool $managedUpstreamRolling = false;
    protected string $lastManagedHealthActiveVersion = '';
    protected int $lastManagedUpstreamRollingAt = 0;
    protected int $lastManagedUpstreamHealthCheckAt = 0;
    protected int $lastManagedUpstreamSelfHealAt = 0;
    protected int $lastUpstreamSupervisorSyncAt = 0;
    protected int $lastObservedUpstreamSupervisorPid = 0;
    protected int $lastObservedUpstreamSupervisorStartedAt = 0;
    protected ?string $pendingUpstreamSupervisorSyncReason = null;
    protected ?GatewayTcpRelayHandler $tcpRelayHandler = null;
    protected ?GatewayNginxProxyHandler $nginxProxyHandler = null;
    protected array $dashboardVersionStatusCache = ['expires_at' => 0, 'value' => null];
    protected array $frameworkRemoteVersionCache = ['expires_at' => 0, 'value' => null];
    protected ?string $lastNginxSyncLogFingerprint = null;
    protected int $lastNginxSyncLoggedAt = 0;
    protected bool $preserveManagedUpstreamsOnShutdown = false;
    protected bool $gatewayShutdownPrepared = false;

    /**
     * 创建并初始化 gateway 主服务实例。
     *
     * @param AppInstanceManager $instanceManager gateway 内存态 registry 管理器
     * @param string $host gateway 控制面绑定地址
     * @param int $port gateway 业务入口端口
     * @param int $workerNum gateway worker 数
     * @param AppServerLauncher|null $launcher managed upstream 启停器
     * @param array<int, array<string, mixed>> $managedUpstreamPlans 启动阶段需要托管的业务实例计划
     * @param int $rpcBindPort gateway RPC 端口
     * @param int $controlBindPort gateway dashboard/control 端口
     * @param string $configuredTrafficMode 入口流量模式
     * @return void
     */
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
        $this->tcpRelayHandler = new GatewayTcpRelayHandler($this, $this->instanceManager);
        $this->nginxProxyHandler = new GatewayNginxProxyHandler($this, $this->instanceManager);
    }

    /**
     * 启动 gateway 主服务。
     *
     * 启动顺序比较讲究：先等待旧端口释放，再创建控制面 server，
     * 再注册 server 托管子进程，最后由 worker#0 仅建立 socket/cluster 入口。
     * housekeep、安装接管、upstream 状态推进等后台职责统一下沉到 addProcess 子进程。
     * 与 managed upstream。
     *
     * @return void
     * @throws RuntimeException 当监听端口或附加监听创建失败时抛出
     */
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
        $this->createBusinessTrafficListener();
        $this->warmupNginxRuntimeMeta();
        if ($this->nginxProxyModeEnabled() && !App::isReady()) {
            // 只有“应用尚未安装”这个特殊场景，才需要在启动前把业务入口先切到 gateway 控制面。
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER, true);
            $this->syncNginxProxyTargets('install_takeover');
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
        });
        $this->server->on('PipeMessage', function (Server $server, int $srcWorkerId, mixed $message): void {
            $this->onPipeMessage($server, $srcWorkerId, $message);
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
        $this->ensureGatewaySubProcessManagerRegistered();
        $this->server->start();
    }

    /**
     * 处理子进程发给 worker 的 IPC 消息。
     *
     * gateway worker 只负责 socket 入口与 fd 级推送，cluster/business/health 等
     * 子进程需要通过 pipe message 通知 worker 完成 dashboard 推送或离线 fd 清理。
     *
     * @param Server $server 当前 gateway server
     * @param int $srcWorkerId 消息来源 worker/process id
     * @param mixed $message 原始 pipe 消息
     * @return void
     */
    protected function onPipeMessage(Server $server, int $srcWorkerId, mixed $message): void {
        if (!is_string($message) || !JsonHelper::is($message)) {
            return;
        }
        $payload = JsonHelper::recover($message);
        $event = (string)($payload['event'] ?? '');
        switch ($event) {
            case 'gateway_cluster_tick':
                $this->refreshLocalGatewayNodeStatus();
                $this->pruneDisconnectedNodeClients();
                if ($this->dashboardClients) {
                    $this->pushDashboardStatus();
                }
                break;
            case 'gateway_control_shutdown':
                $this->shutdownGateway((bool)($payload['preserve_managed_upstreams'] ?? false));
                break;
            default:
                break;
        }
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
        if ($this->shouldRedirectToInstallFlow($uri)) {
            $target = '/~/install';
            $query = (string)($request->server['query_string'] ?? '');
            if ($query !== '') {
                $target .= '?' . $query;
            }
            $response->status(302);
            $response->header('Location', $target);
            $response->end();
            return;
        }
        $this->json($response, 404, [
            // 这里返回统一的 404，避免把“nginx 接管”当成成功提示误导排障。
            // 如果请求没有命中 dashboard/control/install 路径，本质上就是当前
            // gateway worker 不处理这条路由，而不是对外保证 nginx 一定已生效。
            'message' => 'Not Found'
        ]);
    }

    protected function onHandshake(Request $request, Response $response): bool {
        $uri = $request->server['request_uri'] ?? '/';
        if ($this->isDashboardSocketPath($uri)) {
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
        $response->status(404);
        $response->end('Not Found');
        return false;
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
        if ($server->exist($frame->fd)) {
            $server->disconnect($frame->fd);
        }
    }

    protected function onClose(Server $server, int $fd): void {
        unset($this->dashboardClients[$fd]);
        unset($this->nodeClients[$fd]);
        $this->tcpRelayHandler()->handleClose($fd);
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
        Runtime::instance()->set(Key::RUNTIME_SERVER_STARTED_AT, $this->startedAt);
        Runtime::instance()->dashboardPort($this->resolvedDashboardPort());
        ConsoleRelay::setGatewayPort($this->port);
        ConsoleRelay::setLocalSubscribed(false);
        ConsoleRelay::setRemoteSubscribed(false);
    }

    protected function ensureLocalIpcServerStarted(): void {
        // Gateway 本地控制统一走现有内部 HTTP 接口，避免独立 LocalIpcServer 的 accept 协程
        // 在空闲时触发 "all coroutines are asleep - deadlock"。
    }

    protected function stopLocalIpcServer(): void {
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
                    'active_version' => $this->currentGatewayStateSnapshot()['active_version'] ?? null,
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

    /**
     * 返回 gateway 当前可见的最新 upstream 状态快照。
     *
     * worker 不再持有 housekeeping 定时器后，任何对外暴露的状态读取都必须先
     * 从 registry reload，再返回 snapshot，避免读到 worker 私有的旧内存视图。
     *
     * @return array<string, mixed>
     */
    protected function currentGatewayStateSnapshot(): array {
        if (App::isReady()) {
            $this->instanceManager->reload();
        }
        return $this->instanceManager->snapshot();
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
                unset($this->dashboardClients[$fd], $this->nodeClients[$fd]);
                $this->tcpRelayHandler()->handleClose($fd);
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

    /**
     * 返回 dashboard 视角下的节点列表。
     *
     * localhost 始终代表当前 gateway；其余节点来自 cluster 心跳表。
     *
     * @param array<int, array<string, mixed>>|null $upstreams 预留的 upstream 快照参数，便于后续扩展聚合来源
     * @return array<int, array<string, mixed>>
     */
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

    /**
     * 组装 dashboard 首页需要的聚合状态。
     *
     * socket_host 不能简单直接使用 Referer：
     * dev server / 反代场景下，浏览器来源地址与 gateway websocket 地址可能不同，
     * 这里必须按控制面真实可达地址重新计算。
     *
     * @param string $token dashboard websocket 鉴权 token
     * @param string $host 当前 HTTP Host 头
     * @param string $referer 前端页面 Referer
     * @param string $forwardedProto 反向代理传来的协议头
     * @return array<string, mixed>
     */
    public function dashboardServerStatus(string $token, string $host = '', string $referer = '', string $forwardedProto = ''): array {
        $upstreams = $this->dashboardUpstreams();
        $status = $this->buildDashboardRealtimeStatus($upstreams);
        $status['socket_host'] = $this->buildDashboardSocketHost($host, $referer, $forwardedProto) . '?token=' . rawurlencode($token);
        $status['latest_version'] = App::latestVersion();
        $status['dashboard'] = $this->buildDashboardVersionStatus();
        $status['framework'] = $this->buildFrameworkVersionStatus($upstreams);
        return $status;
    }

    /**
     * dashboard 的旧排程页在 gateway 模式下不再展示常驻任务。
     *
     * Linux 排程已经切换为系统 crontab 托管，因此这里统一返回空列表，
     * 避免旧页面继续把 gateway 看作 CrontabManager 的宿主。
     *
     * @return array<int, array<string, mixed>>
     */
    public function dashboardCrontabs(): array {
        return [];
    }

    /**
     * 将当前 Linux 排程配置广播到所有在线 slave 节点。
     *
     * slave 收到后会写入本地 `db/crontabs_slave.json` 并按自身 env/role
     * 重新同步系统 crontab。这里采用异步广播，结果以后续心跳与节点排程视图为准。
     *
     * @return array<string, int>
     */
    public function replicateLinuxCrontabConfigToSlaveNodes(): array {
        $targets = $this->connectedSlaveHosts();
        if (!$targets) {
            return [
                'target_count' => 0,
                'accepted_count' => 0,
            ];
        }

        // slave 节点只需要维护属于自己角色的 Linux 排程定义。
        // 这样既能减少不必要的配置噪音，也能降低本机联调时 master/slave
        // 共用同一系统用户 crontab 带来的互相覆盖风险。
        $payload = (new LinuxCrontabManager())->replicationPayload(NODE_ROLE_SLAVE);
        $accepted = $this->sendCommandToAllNodeClients('linux_crontab_sync', [
            'config' => $payload,
        ]);

        Console::info("【LinuxCrontab】已广播排程配置到 slave 节点: accepted={$accepted}, targets=" . count($targets), false);

        return [
            'target_count' => count($targets),
            'accepted_count' => $accepted,
        ];
    }

    /**
     * dashboard 命令统一入口。
     *
     * host 可能代表：
     * - 当前 gateway；
     * - cluster 内其他 gateway；
     * - 某个业务 upstream；
     * 因此这里先归一化 host，再决定走本地执行、节点转发还是直接操作业务实例。
     *
     * @param string $command dashboard 下发的命令名
     * @param string $host 命令目标，可是 gateway 节点也可是 upstream host
     * @param array<string, mixed> $params 附带参数
     * @return Result
     */
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

    /**
     * 判断 dashboard 命令中的 host 是否指向当前 gateway。
     *
     * @param string $host dashboard 上传入的 host
     * @return bool
     */
    protected function isLocalGatewayHost(string $host): bool {
        $host = trim($host);
        if ($host === '') {
            return false;
        }
        return in_array($host, ['localhost', '127.0.0.1'], true);
    }

    /**
     * 将 dashboard 里的 host 解析成 cluster 内部使用的节点标识。
     *
     * @param string $host dashboard 传入的 host / ip / localhost
     * @return string|null 返回 localhost 或标准化后的 slave host，无法识别时返回 null
     */
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

    /**
     * 执行发往当前 gateway 的本地命令。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return Result
     */
    protected function dispatchLocalGatewayCommand(string $command, array $params = []): Result {
        switch ($command) {
            case 'reload':
                return $this->dispatchGatewayBusinessCommand('reload', [], false, 0, '业务实例与业务子进程已开始重启');
            case 'reload_gateway':
                $reservation = $this->reserveGatewayReload();
                if (!$reservation['accepted']) {
                    return Result::error($reservation['message']);
                }
                if ($reservation['scheduled']) {
                    $this->scheduleReservedGatewayReload((bool)$reservation['restart_managed_upstreams']);
                }
                return Result::success($reservation['message']);
            case 'restart_crontab':
                return Result::error('Gateway 已不再托管 Crontab 子进程，请改用 Linux 排程页面管理系统 crontab');
            case 'restart_redisqueue':
                if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
                    return Result::error('RedisQueue 子进程未启用');
                }
                $this->restartGatewayRedisQueueProcess();
                return Result::success('RedisQueue 子进程已开始重启');
            case 'restart':
                $this->scheduleGatewayShutdown(true);
                return Result::success('Gateway 已开始重启');
            case 'shutdown':
                $this->scheduleGatewayShutdown();
                return Result::success('Gateway 已开始关闭');
            case 'appoint_update':
                return $this->dashboardUpdate((string)($params['type'] ?? ''), (string)($params['version'] ?? ''));
            default:
                return Result::error('暂不支持的命令:' . $command);
        }
    }

    /**
     * 生成 gateway 业务编排命令结果在 Runtime 中的存储 key。
     *
     * worker 只负责把命令投递给业务编排子进程，真实执行结果由子进程写回共享内存；
     * dashboard、本地 IPC、节点转发都通过同一条 key 读取执行结果。
     *
     * @param string $requestId 命令请求 id
     * @return string
     */
    protected function gatewayBusinessCommandResultKey(string $requestId): string {
        // Swoole\Table 的 key 长度上限较小，不能直接拼接带高精度 uniqid 的 request id。
        // 这里统一压缩成稳定长度摘要，避免滚动升级/安装等同步等待链路在写回结果时
        // 因 key 过长触发 warning，进而让 worker 端一直等到超时。
        return 'gateway_business_result:' . md5($requestId);
    }

    /**
     * 生成 UpstreamSupervisor 命令结果在 Runtime 中的存储 key。
     *
     * 安装等“由 upstream 管理子进程执行、但需要同步返回 HTTP 结果”的场景，
     * 统一通过 request_id + Runtime 的方式回传，worker 自身不直接执行安装流程。
     *
     * @param string $requestId 命令请求 id
     * @return string
     */
    protected function upstreamSupervisorCommandResultKey(string $requestId): string {
        return 'upstream_supervisor:' . md5($requestId);
    }

    /**
     * 等待 UpstreamSupervisor 返回命令执行结果。
     *
     * @param string $requestId 命令请求 id
     * @param int $timeoutSeconds 最长等待秒数
     * @return Result
     */
    protected function waitForUpstreamSupervisorCommandResult(string $requestId, int $timeoutSeconds = 120): Result {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $resultKey = $this->upstreamSupervisorCommandResultKey($requestId);
        while (microtime(true) < $deadline) {
            $payload = Runtime::instance()->get($resultKey);
            if (is_array($payload) && array_key_exists('ok', $payload)) {
                Runtime::instance()->delete($resultKey);
                $message = (string)($payload['message'] ?? ($payload['ok'] ? 'success' : 'failed'));
                $data = (array)($payload['data'] ?? []);
                return !empty($payload['ok'])
                    ? Result::success($data ?: $message)
                    : Result::error($message, 'SERVICE_ERROR', $data);
            }
            usleep(100000);
        }
        Runtime::instance()->delete($resultKey);
        return Result::error('UpstreamSupervisor 执行超时');
    }

    /**
     * 向 UpstreamSupervisor 投递命令。
     *
     * worker 只负责校验参数、投递安装/拉起类命令并等待结果；真正执行安装、
     * 下载更新包和拉起业务实例的动作都放在 UpstreamSupervisor 子进程内完成。
     *
     * @param string $action 命令动作
     * @param array<string, mixed> $params 命令参数
     * @param bool $waitForResult 是否等待执行结果
     * @param int $timeoutSeconds 最长等待秒数
     * @return Result
     */
    protected function dispatchUpstreamSupervisorCommand(
        string $action,
        array $params = [],
        bool $waitForResult = false,
        int $timeoutSeconds = 120
    ): Result {
        if (!$this->upstreamSupervisor) {
            return Result::error('UpstreamSupervisor 未启用');
        }
        $payload = ['action' => $action] + $params;
        if (!$waitForResult) {
            return $this->upstreamSupervisor->sendCommand($payload)
                ? Result::success('accepted')
                : Result::error('命令投递失败');
        }
        $requestId = uniqid('upstream_supervisor_', true);
        Runtime::instance()->delete($this->upstreamSupervisorCommandResultKey($requestId));
        $payload['request_id'] = $requestId;
        if (!$this->upstreamSupervisor->sendCommand($payload)) {
            Runtime::instance()->delete($this->upstreamSupervisorCommandResultKey($requestId));
            return Result::error('命令投递失败');
        }
        return $this->waitForUpstreamSupervisorCommandResult($requestId, $timeoutSeconds);
    }

    /**
     * 等待 gateway 业务编排子进程返回执行结果。
     *
     * 这里只有“命令分发成功但结果稍后回传”的场景，因此 worker 需要在共享内存上
     * 轮询 request_id 对应的结果，而不是直接在 server 进程里执行编排逻辑。
     *
     * @param string $requestId 命令请求 id
     * @param int $timeoutSeconds 最长等待时间
     * @return Result
     */
    protected function waitForGatewayBusinessCommandResult(string $requestId, int $timeoutSeconds = 30): Result {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
        while (microtime(true) < $deadline) {
            $payload = Runtime::instance()->get($resultKey);
            if (is_array($payload) && array_key_exists('ok', $payload)) {
                Runtime::instance()->delete($resultKey);
                $message = (string)($payload['message'] ?? ($payload['ok'] ? 'success' : 'failed'));
                $data = (array)($payload['data'] ?? []);
                return !empty($payload['ok'])
                    ? Result::success($data ?: $message)
                    : Result::error($message, 'SERVICE_ERROR', $data);
            }
            usleep(100000);
        }
        Runtime::instance()->delete($resultKey);
        return Result::error('Gateway 业务编排子进程执行超时');
    }

    /**
     * 向 gateway 业务编排子进程投递命令。
     *
     * worker 不再直接承担 bootstrap / rolling / 本地业务进程重启等职责，
     * 而是只做入口与 IPC 转发。需要同步结果时，通过 request_id 在 Runtime 上等待。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @param bool $waitForResult 是否等待子进程结果
     * @param int $timeoutSeconds 最长等待秒数
     * @param string|null $acceptedMessage 异步接受时返回给调用方的提示语
     * @return Result
     */
    protected function dispatchGatewayBusinessCommand(
        string $command,
        array $params = [],
        bool $waitForResult = false,
        int $timeoutSeconds = 30,
        ?string $acceptedMessage = null
    ): Result {
        if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('GatewayBusinessCoordinator')) {
            return Result::error('Gateway 业务编排子进程未启用');
        }
        $requestId = '';
        if ($waitForResult) {
            $requestId = uniqid('gateway_business_', true);
            Runtime::instance()->delete($this->gatewayBusinessCommandResultKey($requestId));
            $params['request_id'] = $requestId;
        }
        $this->subProcessManager->sendCommand($command, $params, ['GatewayBusinessCoordinator']);
        if (!$waitForResult) {
            return Result::success($acceptedMessage ?: 'accepted');
        }
        return $this->waitForGatewayBusinessCommandResult($requestId, $timeoutSeconds);
    }

    /**
     * 在业务编排子进程里执行一条本地控制命令。
     *
     * 这里承接的是 gateway 本机的业务编排职责，因此实现中不能依赖 dashboard fd、
     * websocket 推送或 cluster 节点连接，只返回纯命令执行结果给 worker 汇总。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return array<string, mixed>
     */
    protected function handleGatewayBusinessCommand(string $command, array $params = []): array {
        return match ($command) {
            'reload' => [
                'ok' => true,
                'message' => 'gateway business reload started',
                'data' => $this->restartGatewayBusinessPlane(false),
            ],
            'appoint_update' => $this->buildGatewayBusinessCommandResult(
                $this->executeLocalDashboardUpdate(
                    (string)($params['task_id'] ?? ''),
                    (string)($params['type'] ?? ''),
                    (string)($params['version'] ?? '')
                )
            ),
            default => [
                'ok' => false,
                'message' => '暂不支持的业务编排命令:' . $command,
                'data' => [],
            ],
        };
    }

    /**
     * 将 Result 标准化成业务编排子进程回传格式。
     *
     * @param Result $result 执行结果
     * @return array<string, mixed>
     */
    protected function buildGatewayBusinessCommandResult(Result $result): array {
        return [
            'ok' => !$result->hasError(),
            'message' => (string)$result->getMessage(),
            'data' => (array)($result->getData() ?: []),
        ];
    }

    /**
     * 通过 UpstreamSupervisor 执行安装流程。
     *
     * gateway worker 只负责接收 dashboard 或主节点转发过来的安装请求，
     * 然后把安装秘钥和目标角色通过 IPC 转给 UpstreamSupervisor。安装完成后，
     * UpstreamSupervisor 会继续下载业务包并尝试拉起业务实例，worker 只负责回包。
     *
     * @param string $key 安装秘钥
     * @param string $role 安装角色，master/slave
     * @return Result
     */
    public function dashboardInstall(string $key, string $role = 'master'): Result {
        $key = trim($key);
        $role = trim($role) ?: 'master';
        if ($key === '') {
            return Result::error('安装秘钥不能为空');
        }
        return $this->dispatchUpstreamSupervisorCommand('install', [
            'key' => $key,
            'role' => $role,
            'plans' => $this->managedUpstreamPlans,
        ], true, 300);
    }

    /**
     * 执行 dashboard 发起的版本升级流程。
     *
     * @param string $type 升级类型，例如 app/framework/public
     * @param string $version 目标版本号
     * @return Result
     */
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
        $localResult = $this->dispatchGatewayBusinessCommand('appoint_update', [
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
        ], true, self::DASHBOARD_UPDATE_LOCAL_TIMEOUT_SECONDS);
        $localData = (array)($localResult->getData() ?: []);
        if ($localResult->hasError() && !$localData) {
            $error = (string)($localResult->getMessage() ?: '更新失败');
            $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
            Console::error("【Gateway】升级失败: type={$type}, version={$version}, error={$error}");
            return Result::error($error);
        }
        $restartSummary = (array)($localData['restart_summary'] ?? [
            'success_count' => 0,
            'failed_nodes' => [],
        ]);
        if (!empty($localData['iterate_business_processes']) && !$localResult->hasError()) {
            $this->logUpdateStage($taskId, $type, $version, 'iterate_business_processes');
            $this->iterateGatewayBusinessProcesses();
        }

        $this->logUpdateStage($taskId, $type, $version, 'wait_cluster_result');
        $summary = $this->waitForNodeUpdateSummary($taskId, $slaveHosts, self::DASHBOARD_UPDATE_CLUSTER_TIMEOUT_SECONDS);
        $pendingHosts = [];
        $masterState = (string)($localData['master']['state'] ?? ($localResult->hasError() ? 'failed' : 'success'));
        $masterError = (string)($localData['master']['error'] ?? ($localResult->hasError() ? (string)$localResult->getMessage() : ''));
        $totalNodes = max(1, count($slaveHosts) + 1);
        if ($masterState === 'pending') {
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

    /**
     * 在业务编排子进程里执行本地升级。
     *
     * 这里不负责 dashboard 节点广播，也不直接碰 websocket/fd，只做：
     * 1. 本地包替换；
     * 2. managed upstream rolling update；
     * 3. 给 worker 返回“是否需要迭代 gateway 业务子进程”的信号；
     * 4. 返回给 worker 汇总的本地结果。
     *
     * @param string $taskId 升级任务 id
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @return Result
     */
    protected function executeLocalDashboardUpdate(string $taskId, string $type, string $version): Result {
        $type = trim($type);
        $version = trim($version);
        if ($type === '' || $version === '') {
            return Result::error('更新类型和版本号不能为空');
        }
        $taskId !== '' && $this->logUpdateStage($taskId, $type, $version, 'apply_local_package');
        if (!App::appointUpdateTo($type, $version, false)) {
            $error = App::getLastUpdateError() ?: '更新失败';
            return Result::error($error, 'SERVICE_ERROR', [
                'restart_summary' => [
                    'success_count' => 0,
                    'failed_nodes' => [],
                ],
                'master' => [
                    'host' => SERVER_HOST,
                    'state' => 'failed',
                    'error' => $error,
                ],
            ]);
        }

        $restartSummary = [
            'success_count' => 0,
            'failed_nodes' => [],
        ];
        if ($type !== 'public') {
            $taskId !== '' && $this->logUpdateStage($taskId, $type, $version, 'rolling_upstreams');
            $restartSummary = $this->rollingUpdateManagedUpstreams($type, $version);
        }
        if ($type !== 'public' && $restartSummary['success_count'] > 0 && !$restartSummary['failed_nodes']) {
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        }
        $iterateBusinessProcesses = in_array($type, ['app', 'framework'], true) && !$restartSummary['failed_nodes'];

        $master = [
            'host' => SERVER_HOST,
            'state' => $type === 'framework' ? 'pending' : ($restartSummary['failed_nodes'] ? 'failed' : 'success'),
            'error' => $type === 'framework'
                ? 'Gateway 需重启后才会加载新框架版本'
                : ($restartSummary['failed_nodes'] ? '部分业务实例升级失败' : ''),
        ];
        $payload = [
            'restart_summary' => $restartSummary,
            'master' => $master,
            'iterate_business_processes' => $iterateBusinessProcesses,
        ];
        if ($restartSummary['failed_nodes']) {
            return Result::error('部分业务实例升级失败', 'SERVICE_ERROR', $payload);
        }
        return Result::success($payload);
    }

    /**
     * 执行 gateway 业务编排周期任务。
     *
     * 当前这层只负责 managed upstream bootstrap，worker 不再直接推进这部分状态机。
     *
     * @return void
     */
    protected function runGatewayBusinessCoordinatorTick(): void {
        if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }

        // 业务编排子进程统一推进 gateway 的后台状态机，worker 只保留 socket 入口。
        if (!App::isReady()) {
            $this->refreshGatewayInstallTakeover();
            return;
        }
        if (App::isReady()) {
            // coordinator 在子进程内持续推进 registry / recycle 状态机，
            // 每轮先 reload，避免长生命周期子进程持有旧的 generation 视图。
            $this->instanceManager->reload();
        }
        $this->observeUpstreamSupervisorProcess();
        $this->syncUpstreamSupervisorState();
        $this->refreshManagedUpstreamRuntimeStates();
        $this->instanceManager->tick();
        $this->pollPendingManagedRecycles();
        $this->warnStuckDrainingManagedUpstreams();
        $this->cleanupOfflineManagedUpstreams();
        if (App::isReady()) {
            $this->bootstrapManagedUpstreams();
            $this->refreshGatewayInstallTakeover();
        }
    }

    /**
     * 执行 gateway 健康检查周期任务。
     *
     * active upstream 的健康探测与自愈统一下沉到单独健康子进程，worker 只保留入口职责。
     *
     * @return void
     */
    protected function runGatewayHealthMonitorTick(): void {
        $this->maintainManagedUpstreamHealth();
    }

    /**
     * 统一记录升级状态机日志，方便 dashboard 和终端串联阶段。
     *
     * @param string $taskId 升级任务 id
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @param string $stage 当前状态机阶段
     * @param array<string, mixed> $extra 额外调试字段
     * @return void
     */
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

    /**
     * 在升级 app/framework 后，单独迭代 gateway 托管的业务子进程。
     *
     * @return void
     */
    protected function iterateGatewayBusinessProcesses(): void {
        $processNames = $this->managedGatewayBusinessProcessNames();
        if (!$processNames) {
            return;
        }
        Console::info('【Gateway】开始迭代业务子进程: ' . implode(', ', $processNames));
        $this->subProcessManager->iterateBusinessProcesses();
    }

    /**
     * 返回当前 gateway 仍由 SubProcessManager 托管的业务型子进程。
     *
     * 常驻 crontab 已经迁移到 Linux 系统排程统一托管，因此这里刻意不再
     * 暴露 `CrontabManager`，避免 gateway 在升级、重载或 dashboard 命令里
     * 继续把它当成受控业务子进程处理。
     *
     * @return array<int, string>
     */
    protected function managedGatewayBusinessProcessNames(): array {
        if (!$this->subProcessManager) {
            return [];
        }

        $processNames = [];
        if ($this->subProcessManager->hasProcess('RedisQueue')) {
            $processNames[] = 'RedisQueue';
        }
        return $processNames;
    }

    /**
     * 单独重启 gateway 下的 CrontabManager。
     *
     * @return bool 是否成功发起重启
     */
    protected function restartGatewayCrontabProcess(): bool {
        Console::warning('【Gateway】Crontab 子进程已下线，改由 Linux 系统排程托管');
        return true;
    }

    /**
     * 单独重启 gateway 下的 RedisQueue 管理进程。
     *
     * @return bool 是否成功发起重启
     */
    protected function restartGatewayRedisQueueProcess(): bool {
        if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
            Console::warning('【Gateway】RedisQueue 子进程未启用，跳过重启');
            return false;
        }
        Console::info('【Gateway】开始重启 RedisQueue 子进程');
        $this->subProcessManager->iterateRedisQueueProcess();
        $this->pushDashboardStatus();
        return true;
    }

    /**
     * 重启业务平面，只滚动 managed upstream，不触碰 gateway 自身。
     *
     * worker 场景下会顺带刷新 dashboard；业务编排子进程调用时必须关闭这类
     * server fd 相关副作用，只保留纯编排逻辑。
     *
     * @param bool $emitDashboardStatus 是否推送 dashboard 状态
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function restartGatewayBusinessPlane(bool $emitDashboardStatus = true): array {
        Console::info('【Gateway】开始重启业务平面');
        Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        $summary = $this->restartManagedUpstreams();
        if ($emitDashboardStatus) {
            $this->pushDashboardStatus();
        }
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
                    $previousStatus = (array)(ServerNodeStatusTable::instance()->get($host) ?: []);

                    // Linux 排程的最近同步状态由独立 socket 事件即时回传，
                    // 普通节点心跳不能把这段运行态覆盖掉，否则 dashboard
                    // 刷新后会出现“已安装/已启用，但最近同步又变成空”的假象。
                    if (!isset($status['linux_crontab_sync']) && isset($previousStatus['linux_crontab_sync'])) {
                        $status['linux_crontab_sync'] = $previousStatus['linux_crontab_sync'];
                    }

                    // 兼容旧版 slave 心跳还没稳定带 env 的情况，避免节点弹窗里环境列反复掉成空值。
                    if (empty($status['env']) && !empty($previousStatus['env'])) {
                        $status['env'] = $previousStatus['env'];
                    }

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
                Manager::instance()->sendMessageToAllDashboardClients(JsonHelper::toJson([
                    'event' => 'node_update_state',
                    'data' => $payload,
                ]));
                if (!empty($payload['message'])) {
                    Console::info((string)$payload['message'], false);
                }
                break;
            case 'linux_crontab_sync_state':
                $payload = (array)($data['data'] ?? []);
                $this->acceptLinuxCrontabSyncState($payload);
                break;
            case 'console_log':
                $this->acceptConsolePayload((array)($data['data'] ?? []));
                break;
            default:
                break;
        }
    }

    /**
     * 接收 slave 回传的 Linux 排程同步状态。
     *
     * slave 侧完成配置落地与系统 crontab 同步后，会立即把最新节点状态推回 master。
     * 这里既要更新节点状态表，也要把最后一次同步结果挂到节点状态上，方便 dashboard
     * 直接看到这轮配置是否已经在对应节点真正生效。
     *
     * @param array<string, mixed> $payload slave 回传的同步结果
     * @return void
     */
    protected function acceptLinuxCrontabSyncState(array $payload): void {
        $host = (string)($payload['host'] ?? '');
        if ($host === '') {
            return;
        }

        $syncState = [
            'state' => (string)($payload['state'] ?? ''),
            'message' => (string)($payload['message'] ?? ''),
            'error' => (string)($payload['error'] ?? ''),
            'item_count' => (int)($payload['item_count'] ?? 0),
            'sync' => (array)($payload['sync'] ?? []),
            'updated_at' => (int)($payload['updated_at'] ?? time()),
        ];
        Runtime::instance()->set($this->nodeLinuxCrontabSyncStateKey($host), $syncState);

        $status = (array)($payload['status'] ?? []);
        if (!$status) {
            $status = (array)(ServerNodeStatusTable::instance()->get($host) ?: []);
        }
        if ($status) {
            $status['linux_crontab_sync'] = $syncState;
            ServerNodeStatusTable::instance()->set($host, $status);
        }

        if ($syncState['message'] !== '') {
            if ($syncState['state'] === 'failed') {
                Console::warning($syncState['message'], false);
            } else {
                Console::info($syncState['message'], false);
            }
        }

        $this->pushDashboardStatus();
    }

    /**
     * 生成节点最近一次 Linux 排程同步状态的运行时 key。
     *
     * @param string $host 节点 host
     * @return string
     */
    protected function nodeLinuxCrontabSyncStateKey(string $host): string {
        return 'linux_crontab_sync_state:' . $host;
    }

    /**
     * 关闭 gateway。
     *
     * preserveManagedUpstreams=true 主要用于“只重启控制面”的场景，
     * 例如 gateway 文件变动后由外层 boot 重新拉起控制面，而业务实例继续存活。
     *
     * @param bool $preserveManagedUpstreams 是否保留已托管的业务实例，仅关闭 gateway 控制面
     * @return void
     */
    protected function shutdownGateway(bool $preserveManagedUpstreams = false): void {
        if ($this->gatewayShutdownScheduled) {
            return;
        }
        $this->preserveManagedUpstreamsOnShutdown = $preserveManagedUpstreams;
        $this->gatewayShutdownScheduled = true;
        $this->prepareGatewayShutdown();
        Runtime::instance()->serverIsAlive(false);
        $masterPid = (int)($this->server->master_pid ?? 0);
        if ($masterPid > 0 && $masterPid !== getmypid() && @Process::kill($masterPid, SIGTERM)) {
            return;
        }
        $this->server->shutdown();
    }

    /**
     * 统一调度 gateway 关闭。
     *
     * 之前这里使用 `Event::defer()` 把 shutdown 推到事件循环尾部，目的是等 HTTP
     * 响应先写回客户端。但在 gateway full restart / file watcher 重启链里，这会让
     * 主进程在 shutdown 尾声仍然挂着一个 Event 回调，最终落到 Swoole 5.1 的
     * rshutdown `Event::wait()` deprecated warning。
     *
     * 现在真正需要“响应先写回”的场景都已经通过 `__after_write` 钩子在响应完成后
     * 才调用这里，因此无需再额外 defer。socket/本地命令路径也不会因为这里同步调用
     * 而阻塞发送结果，直接进入 shutdown 才是最干净的收口方式。
     *
     * @param bool $preserveManagedUpstreams 是否保留业务实例，仅重启控制面
     * @return void
     */
    protected function scheduleGatewayShutdown(bool $preserveManagedUpstreams = false): void {
        $this->shutdownGateway($preserveManagedUpstreams);
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

    /**
     * 创建 gateway 控制面 server。
     *
     * Gateway 已不再承接 HTTP 业务转发，因此控制面始终监听独立的 controlPort()。
     *
     * @param int $timeoutSeconds 最长等待监听端口释放的秒数
     * @param int $intervalMs 每次重试的毫秒间隔
     * @return Server
     * @throws RuntimeException 当底层 server 创建失败时抛出
     */
    protected function createGatewaySocketServer(int $timeoutSeconds = 10, int $intervalMs = 200): Server {
        $listenPort = $this->controlPort();
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $logged = false;

        while (microtime(true) < $deadline) {
            if (!CoreServer::isListeningPortInUse($listenPort)) {
                break;
            }
            if (!$logged) {
                Console::warning("【Gateway】监听端口占用，等待重试: {$this->host}:{$listenPort}");
                $logged = true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }

        if ($logged && !CoreServer::isListeningPortInUse($listenPort)) {
            Console::success("【Gateway】监听端口已释放，准备启动: {$this->host}:{$listenPort}");
        }

        try {
            return new Server($this->host, $listenPort, SWOOLE_PROCESS, SWOOLE_SOCK_TCP);
        } catch (SwooleException $exception) {
            if (str_contains($exception->getMessage(), 'Address already in use')) {
                throw new RuntimeException("Gateway 监听失败，端口仍被占用: {$this->host}:{$listenPort}", previous: $exception);
            }
            throw $exception;
        }
    }

    protected function managedUpstreamPortsReleased(): bool {
        if ($this->preserveManagedUpstreamsOnShutdown) {
            return true;
        }
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

    /**
     * 判断 gateway 自身托管的附属进程是否已经全部退出。
     *
     * Gateway 的 addProcess 及其派生子进程会继承 server 的监听 FD；只有等待这些
     * 进程真正退出，新的 gateway 才不会在 10580/9585 上撞到旧 FD 残留。
     *
     * @return bool
     */
    protected function gatewayAuxiliaryProcessesReleased(): bool {
        if ($this->subProcessManager) {
            $subprocessShuttingDown = (bool)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN) ?: false);
            $subprocessAliveCount = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT) ?: 0);
            // Runtime 里的 alive_count 只覆盖 manager 下面托管的直系子进程，
            // addProcess 根进程自身若仍存活，同样会继续持有旧 gateway 继承下来的监听 FD。
            if (
                $this->subProcessManager->isManagerProcessAlive()
                || $subprocessShuttingDown
                || $subprocessAliveCount > 0
            ) {
                return false;
            }
        }
        if ($this->upstreamSupervisor && $this->upstreamSupervisor->isAlive()) {
            return false;
        }
        return true;
    }

    protected function clearPendingManagedRecycleWatchers(): void {
        foreach ($this->pendingManagedRecycleWatchers as $key => $timerId) {
            Timer::clear($timerId);
            unset($this->pendingManagedRecycleWatchers[$key]);
        }
        $this->pendingManagedRecycleCompletions = [];
    }

    protected function prepareGatewayShutdown(): void {
        if ($this->gatewayShutdownPrepared) {
            return;
        }
        $this->gatewayShutdownPrepared = true;
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        $this->stopLocalIpcServer();
        $this->clearPendingManagedRecycleWatchers();
        isset($this->subProcessManager) && $this->subProcessManager->shutdown(true);
        if ($this->preserveManagedUpstreamsOnShutdown) {
            $this->detachUpstreamSupervisor();
        } else {
            $this->shutdownManagedUpstreams();
        }
    }

    /**
     * 在“只重启 gateway 控制面”场景下，让 UpstreamSupervisor 自身退出，
     * 但保留已托管的业务实例继续运行。
     *
     * @return void
     */
    protected function detachUpstreamSupervisor(): void {
        if (!$this->upstreamSupervisor) {
            return;
        }
        $result = $this->dispatchUpstreamSupervisorCommand('detach', [], true, 15);
        if ($result->hasError()) {
            Console::warning('【Gateway】等待 UpstreamSupervisor 脱离控制面失败，将继续按关停流程推进: ' . $result->getMessage());
        }
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
        return in_array($mode, ['tcp', 'nginx'], true) ? $mode : 'nginx';
    }

    public function tcpRelayModeEnabled(): bool {
        return $this->trafficMode() === 'tcp';
    }

    public function nginxProxyModeEnabled(): bool {
        return $this->trafficMode() === 'nginx';
    }

    public function controlPort(): int {
        return max(1, $this->controlBindPort ?: ($this->port + 1000));
    }

    protected function resolvedDashboardPort(): int {
        return $this->dashboardEnabled() ? $this->controlPort() : 0;
    }

    public function internalControlHost(): string {
        return in_array($this->host, ['0.0.0.0', '::', ''], true) ? '127.0.0.1' : $this->host;
    }

    public function shouldRoutePathToControlPlane(string $path): bool {
        return $this->isDashboardSocketPath($path)
            || str_starts_with($path, '/_gateway')
            || str_starts_with($path, '/~');
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
            $paths = array_filter([
                (string)($result['global_file'] ?? ''),
                (string)($result['upstream_file'] ?? ''),
                (string)($result['server_file'] ?? ''),
            ]);
            $displayPaths = array_values(array_unique(array_map(static fn(string $path): string => basename($path), $paths)));
            $fingerprint = md5(JsonHelper::toJson([
                'reason_group' => $this->isStartupNginxSyncReason((string)($result['reason'] ?? '')) ? 'startup' : (string)($result['reason'] ?? ''),
                'reloaded' => (bool)($result['reloaded'] ?? false),
                'tested' => (bool)($result['tested'] ?? false),
                'files' => $displayPaths,
            ]));
            $now = time();
            if ($fingerprint === $this->lastNginxSyncLogFingerprint && ($now - $this->lastNginxSyncLoggedAt) <= 15) {
                return;
            }
            $this->lastNginxSyncLogFingerprint = $fingerprint;
            $this->lastNginxSyncLoggedAt = $now;

            $message = "【Gateway】nginx转发配置已同步";
            if (!empty($result['reason'])) {
                $message .= ': reason=' . $result['reason'];
            }
            if (!empty($result['reloaded'])) {
                $message .= ', reloaded=yes';
            } elseif (!empty($result['tested'])) {
                $message .= ', tested=yes';
            }
            if ($displayPaths) {
                $message .= ', files=' . implode(' | ', $displayPaths);
            }
            Console::info($message);
        } catch (Throwable $throwable) {
            Console::warning('【Gateway】nginx转发配置同步失败: ' . $throwable->getMessage());
        }
    }

    /**
     * 启动早期只做 nginx 运行环境探测和缓存预热。
     *
     * 这里不触发任何配置写盘和 reload，仅把 bin/conf-path/conf-dir 等路径
     * 预先探测出来并写入缓存文件，便于后续真正 sync 时直接复用。
     *
     * @return void
     */
    protected function warmupNginxRuntimeMeta(): void {
        $handler = $this->nginxProxyHandler();
        if (!$handler->enabled()) {
            return;
        }
        try {
            $result = $handler->warmupRuntimeMeta($this->nginxProxyModeEnabled());
            $runtimeMeta = (array)($result['runtime_meta'] ?? []);
            if (($result['runtime_meta_changed'] ?? false) && $runtimeMeta) {
                Console::info('【Gateway】探测到 nginx: ' . ((string)($runtimeMeta['bin'] ?? 'nginx')));
                Console::info('【Gateway】探测到 nginx conf-path: ' . ((string)($runtimeMeta['conf_path'] ?? '')));
                Console::info('【Gateway】探测到 nginx conf-dir: ' . ((string)($runtimeMeta['conf_dir'] ?? '')));
            }
            if (!empty($result['started'])) {
                Console::info('【Gateway】nginx 未运行，已按当前主配置启动');
            }
        } catch (Throwable $throwable) {
            Console::warning('【Gateway】nginx 环境探测失败: ' . $throwable->getMessage());
        }
    }

    protected function isStartupNginxSyncReason(string $reason): bool {
        return in_array($reason, [
            'register_managed_plan_activate',
        ], true);
    }

    /**
     * 重载 gateway 控制面。
     *
     * 是否顺带滚业务面由调用方决定：普通 gateway restart 与业务 reload 的语义并不相同。
     *
     * @param bool $restartManagedUpstreams 是否顺带滚动重启业务实例
     * @return void
     */
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

    /**
     * 预留一次 gateway 重载名额，避免管理请求与 FileWatcher 重复排队。
     *
     * 当前 gateway 的 reload 链路包含业务实例滚动重启、端口就绪等待与切流校验。
     * 这类工作必须脱离触发它的管理请求协程执行，否则请求协程会卡在等待链上，
     * 最终在没有其他可唤醒事件时触发 coroutine deadlock。这里先只做“占位”，
     * 真正的 reload 交给响应写回后的异步回调。
     *
     * @param bool $restartManagedUpstreams 是否顺带滚动重启业务实例
     * @return array{accepted:bool,message:string,scheduled:bool,restart_managed_upstreams:bool}
     */
    protected function reserveGatewayReload(bool $restartManagedUpstreams = true): array {
        if ($this->gatewayShutdownScheduled) {
            return [
                'accepted' => false,
                'message' => 'Gateway 已进入关闭流程，无法再发起重载',
                'scheduled' => false,
                'restart_managed_upstreams' => $restartManagedUpstreams,
            ];
        }

        if ($this->gatewayReloadScheduled || $this->managedUpstreamRolling) {
            return [
                'accepted' => true,
                'message' => 'Gateway 重载已在进行中，已跳过重复触发',
                'scheduled' => false,
                'restart_managed_upstreams' => $restartManagedUpstreams,
            ];
        }

        $this->gatewayReloadScheduled = true;
        return [
            'accepted' => true,
            'message' => $restartManagedUpstreams
                ? 'Gateway 与业务实例已开始重载'
                : 'Gateway 已开始重载',
            'scheduled' => true,
            'restart_managed_upstreams' => $restartManagedUpstreams,
        ];
    }

    /**
     * 将预留好的 gateway reload 投递到当前 worker 的事件循环中异步执行。
     *
     * 这里使用 Timer::after 的原因不是为了延迟，而是为了确保真正的 reload
     * 发生在 HTTP 响应已经写回之后。这样 FileWatcher 或 dashboard 的控制请求
     * 只负责“投递 reload 意图”，不会再把滚动重启流程压在当前请求协程上。
     *
     * @param bool $restartManagedUpstreams 是否顺带滚动重启业务实例
     * @return void
     */
    protected function scheduleReservedGatewayReload(bool $restartManagedUpstreams = true): void {
        Timer::after(1, function () use ($restartManagedUpstreams): void {
            try {
                $this->reloadGateway($restartManagedUpstreams);
            } catch (Throwable $throwable) {
                // 异步 reload 失败时需要恢复运行态标记，避免控制面长时间停在 draining。
                Runtime::instance()->serverIsDraining(false);
                Runtime::instance()->serverIsReady(true);
                Console::error('【Gateway】异步重载失败: ' . $throwable->getMessage());
            } finally {
                $this->gatewayReloadScheduled = false;
            }
        });
    }

    protected function handleGatewayProcessCommand(string $command, array $params, object $socket): bool {
        switch ($command) {
            case 'shutdown':
                $socket->push("【" . SERVER_HOST . "】start shutdown");
                $this->scheduleGatewayShutdown();
                return true;
            case 'reload':
                $result = $this->dispatchGatewayBusinessCommand('reload', [], false, 0, 'gateway business reload started');
                $socket->push($result->hasError()
                    ? "【" . SERVER_HOST . "】business reload failed:" . $result->getMessage()
                    : "【" . SERVER_HOST . "】start business reload");
                return true;
            case 'restart_crontab':
                $socket->push("【" . SERVER_HOST . "】Crontab 已改由 Linux 系统排程托管");
                return true;
            case 'restart_redisqueue':
                if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
                    $socket->push("【" . SERVER_HOST . "】RedisQueue process unavailable");
                    return true;
                }
                $this->restartGatewayRedisQueueProcess();
                $socket->push("【" . SERVER_HOST . "】start redisqueue restart");
                return true;
            case 'restart':
                $socket->push("【" . SERVER_HOST . "】start restart");
                $this->scheduleGatewayShutdown(true);
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
                $result = $this->dispatchGatewayBusinessCommand('appoint_update', [
                    'task_id' => $taskId,
                    'type' => $type,
                    'version' => $version,
                ], true, 300);
                if ($result->hasError()) {
                    $resultData = (array)($result->getData() ?: []);
                    $masterError = (string)($resultData['master']['error'] ?? '');
                    $error = $masterError !== '' ? $masterError : (string)($result->getMessage() ?: '未知原因');
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
        // 心跳子进程的某一轮 tick 可能恰好与 gateway shutdown 交错。
        // 即便上层定时器入口已经做过一次守卫，这里仍需在真正拼装业务 overlay 之前
        // 再检查一次，避免进入 dashboardUpstreams() 时撞上 shutdown 窗口里的 upstream IPC。
        if ($this->shouldSkipGatewayBusinessOverlayDuringShutdown()) {
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
                    $this->dispatchLocalGatewayCommand('reload');
                });
                break;
            case 'restartCrontabAll':
                Coroutine::create(function () use ($frame) {
                    Console::info("【Gateway】Dashboard Socket命令: {$frame->data}");
                    $this->sendCommandToAllNodeClients('restart_crontab');
                    $this->pushDashboardEvent([
                        'event' => 'console',
                        'message' => ['data' => '已向所有子节点发送 Crontab 重启指令，当前 Gateway 开始重启本地 Crontab 子进程'],
                        'time' => Console::timestamp(),
                        'node' => SERVER_HOST,
                    ], $frame->fd);
                    $this->pushDashboardStatus();
                    $this->dispatchLocalGatewayCommand('restart_crontab');
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
                    $this->sendCommandToAllNodeClients('restart');
                    $this->pushDashboardEvent([
                        'event' => 'console',
                        'message' => ['data' => '已向所有子节点发送重启指令，当前 Gateway 开始重启'],
                        'time' => Console::timestamp(),
                        'node' => SERVER_HOST,
                    ], $frame->fd);
                    $this->scheduleGatewayShutdown(true);
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
     * 生成 dashboard 实时状态快照。
     *
     * 返回的是控制面聚合视图：
     * - servers 代表 gateway 节点；
     * - upstreams 代表当前被 gateway 选中的业务实例；
     * - 日志、版本、任务、内存等都按 dashboard 所需格式封装。
     *
     * @param array<int, array<string, mixed>>|null $upstreams 已获取的 upstream 列表，传 null 时内部自行抓取
     * @return array<string, mixed>
     */
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
        $snapshot = $this->currentGatewayStateSnapshot();
        $instances = [];
        foreach ($snapshot['generations'] ?? [] as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                $runtimeStatus = $this->fetchUpstreamRuntimeStatus($instance);
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

    /**
     * 维护 active managed upstream 的健康状态。
     *
     * 这里不是“单次失败即重启”，而是按 active generation 连续计数，
     * 只有达到阈值后才触发一次统一的 rolling self-heal，避免把瞬时抖动当成切代事件。
     *
     * @return void
     */
    protected function maintainManagedUpstreamHealth(): void {
        if ($this->managedUpstreamSelfHealing || $this->managedUpstreamRolling || !$this->launcher || !$this->upstreamSupervisor) {
            return;
        }
        if (!App::isReady() || $this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        $now = time();
        if ($this->lastManagedUpstreamRollingAt > 0 && ($now - $this->lastManagedUpstreamRollingAt) < self::ACTIVE_UPSTREAM_HEALTH_ROLLING_COOLDOWN_SECONDS) {
            return;
        }
        if ($this->lastManagedUpstreamHealthCheckAt > 0 && ($now - $this->lastManagedUpstreamHealthCheckAt) < self::ACTIVE_UPSTREAM_HEALTH_CHECK_INTERVAL_SECONDS) {
            return;
        }
        $this->lastManagedUpstreamHealthCheckAt = $now;
        if ($this->pendingManagedRecycles) {
            return;
        }

        // 健康检测统一以 registry 持久化状态为准。
        // rolling / recycle / self-heal 期间有多条协程在推进 instanceManager 内存态，
        // 这里先 reload 一次，避免使用到尚未收敛的旧代 in-memory 视图。
        $this->instanceManager->reload();
        $snapshot = $this->instanceManager->snapshot();
        $activeVersion = (string)($snapshot['active_version'] ?? '');
        if ($activeVersion === '') {
            $this->lastManagedHealthActiveVersion = '';
            $this->managedUpstreamHealthState = [];
            return;
        }
        if ($activeVersion !== $this->lastManagedHealthActiveVersion) {
            // active generation 一变，历史失败计数就没有参考意义了，需要对新代重新观察。
            $this->notifyManagedUpstreamGenerationIterated($activeVersion);
            return;
        }
        $activePlans = $this->managedPlansFromSnapshot();
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

            // 健康检查采用“端口 + internal health”双重标准，避免只看 listen 就过早判定 ready。
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
                // 自愈直接复用 rolling restart，确保切流、回滚、回收语义完全一致。
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
        if (!$this->launcher || !$this->launcher->probeHttpConnectivity($host, $port, self::INTERNAL_UPSTREAM_HTTP_PROBE_PATH, 0.5)) {
            return ['healthy' => false, 'reason' => 'http_probe_failed'];
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

    protected function notifyManagedUpstreamGenerationIterated(string $version): void {
        $version = trim($version);
        if ($version === '') {
            return;
        }
        $this->lastManagedHealthActiveVersion = $version;
        $this->managedUpstreamHealthState = [];
        $this->lastManagedUpstreamRollingAt = time();
        $this->lastManagedUpstreamHealthCheckAt = 0;
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
            'env' => SERVER_RUN_ENV ?: 'production',
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
            'env' => SERVER_RUN_ENV ?: 'production',
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
        // gateway 已进入 shutdown/restart 时，节点状态只保留本地控制面信息。
        // 这时继续向 upstream 做 memory/status 补采只会把退出路径重新拖进 IPC，
        // 既没有运维价值，也会增加旧控制面迟迟不释放监听 FD 的风险。
        if ($this->shouldSkipGatewayBusinessOverlayDuringShutdown()) {
            return $status;
        }

        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        $businessOverlay = $this->buildGatewayBusinessOverlay($upstreams);
        if ($businessOverlay) {
            if (array_key_exists('tasks', $businessOverlay)) {
                $businessOverlay['tasks'] = $this->mergeGatewayTasks(
                    (array)($status['tasks'] ?? []),
                    (array)$businessOverlay['tasks']
                );
            }
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

    /**
     * 判断 gateway 是否已经进入“只允许本地收口，不再访问 upstream”的窗口。
     *
     * @return bool 进入 shutdown/restart 收口阶段时返回 true。
     */
    protected function shouldSkipGatewayBusinessOverlayDuringShutdown(): bool {
        return $this->gatewayShutdownPrepared
            || !Runtime::instance()->serverIsAlive()
            || Runtime::instance()->serverIsDraining()
            || !Runtime::instance()->serverIsReady();
    }

    protected function mergeGatewayTasks(array $localTasks, array $businessTasks): array {
        $tasks = [];
        foreach ([$localTasks, $businessTasks] as $taskGroup) {
            foreach ($taskGroup as $task) {
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
        return array_values($tasks);
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

        try {
            $ipcAction = $this->mapUpstreamStatusPathToIpcAction($path);
            if ($ipcAction !== '') {
                $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $timeoutSeconds);
                if (is_array($ipcResponse) && (int)($ipcResponse['status'] ?? 0) === 200) {
                    $data = $ipcResponse['data'] ?? null;
                    return is_array($data) ? $data : [];
                }
                return [];
            }
            return [];
        } catch (Throwable) {
            return [];
        }
    }

    /**
     * 将 upstream 内部状态路径映射为本地 IPC action。
     *
     * @param string $path upstream 内部状态路径。
     * @return string 已知路径返回对应 action，否则返回空字符串。
     */
    protected function mapUpstreamStatusPathToIpcAction(string $path): string {
        return match ($path) {
            self::INTERNAL_UPSTREAM_STATUS_PATH => 'upstream.status',
            self::INTERNAL_UPSTREAM_HEALTH_PATH => 'upstream.health',
            default => '',
        };
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
            foreach ($this->buildGatewayUpstreamMemoryRows($item, $memoryUsage) as $row) {
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

    /**
     * 基于 upstream 上报的 memory rows 快照，在 gateway 侧补采 OS 物理内存字段。
     *
     * upstream 只负责维持 worker 活跃时写入的 usage/real/peak/pid；
     * 真正的 rss/pss/os_actual 在 gateway 汇总阶段按 pid 现采，避免 upstream 额外保留
     * MemoryUsageCount 子进程。
     *
     * @param array $instance upstream 实例描述。
     * @param array $fallbackMemoryUsage upstream runtime status 里附带的 memory_usage。
     * @return array<int, array<string, mixed>>
     */
    protected function buildGatewayUpstreamMemoryRows(array $instance, array $fallbackMemoryUsage): array {
        $fallbackRows = (array)($fallbackMemoryUsage['rows'] ?? []);
        $fallbackByName = [];
        foreach ($fallbackRows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $name = (string)($row['name'] ?? '');
            if ($name !== '') {
                $fallbackByName[$name] = $row;
            }
        }

        $snapshotRows = $this->fetchUpstreamMemoryRows($instance);
        if (!$snapshotRows) {
            return $fallbackRows;
        }

        $rows = [];
        $seen = [];
        foreach ($snapshotRows as $snapshotRow) {
            if (!is_array($snapshotRow)) {
                continue;
            }
            $name = (string)($snapshotRow['process'] ?? '');
            if ($name === '') {
                continue;
            }
            $seen[$name] = true;
            $rows[] = $this->composeGatewayUpstreamMemoryRow(
                $instance,
                $snapshotRow,
                $fallbackByName[$name] ?? []
            );
        }

        foreach ($fallbackRows as $fallbackRow) {
            if (!is_array($fallbackRow)) {
                continue;
            }
            $name = (string)($fallbackRow['name'] ?? '');
            if ($name === '' || isset($seen[$name])) {
                continue;
            }
            $rows[] = $fallbackRow;
        }

        return $rows;
    }

    /**
     * 从 upstream 本地 IPC 读取 memory rows 快照。
     *
     * @param array $instance upstream 实例描述。
     * @return array<int, array<string, mixed>>
     */
    protected function fetchUpstreamMemoryRows(array $instance): array {
        $port = (int)($instance['port'] ?? 0);
        if ($port <= 0) {
            return [];
        }
        $ipcResponse = LocalIpc::request(
            LocalIpc::upstreamSocketPath($port),
            'upstream.memory_rows',
            [],
            2.0
        );
        if (!is_array($ipcResponse) || (int)($ipcResponse['status'] ?? 0) !== 200) {
            return [];
        }
        $data = $ipcResponse['data'] ?? null;
        return is_array($data) ? $data : [];
    }

    /**
     * 组合 upstream 的逻辑内存快照和 gateway 现采的 OS 物理内存。
     *
     * @param array $instance upstream 实例描述。
     * @param array $snapshotRow upstream 本地内存表原始行。
     * @param array $fallbackRow upstream runtime status 已经格式化过的回退行。
     * @return array<string, mixed>
     */
    protected function composeGatewayUpstreamMemoryRow(array $instance, array $snapshotRow, array $fallbackRow): array {
        $name = (string)($snapshotRow['process'] ?? ($fallbackRow['name'] ?? ''));
        $pid = (int)($snapshotRow['pid'] ?? ($fallbackRow['pid'] ?? 0));
        $usage = (float)($snapshotRow['usage_mb'] ?? $this->extractMemoryValue($fallbackRow['usage'] ?? null));
        $real = (float)($snapshotRow['real_mb'] ?? $this->extractMemoryValue($fallbackRow['real'] ?? null));
        $peak = (float)($snapshotRow['peak_mb'] ?? $this->extractMemoryValue($fallbackRow['peak'] ?? null));
        $usageUpdated = (int)($snapshotRow['usage_updated'] ?? ($fallbackRow['usage_updated'] ?? 0));
        $restartTs = (int)($snapshotRow['restart_ts'] ?? ($fallbackRow['restart_ts'] ?? 0));
        $restartCount = (int)($snapshotRow['restart_count'] ?? ($fallbackRow['restart_count'] ?? 0));
        $limitMb = (int)($snapshotRow['limit_memory_mb'] ?? ($fallbackRow['limit_memory_mb'] ?? 0));
        $autoRestart = (int)($snapshotRow['auto_restart'] ?? 0);

        $alive = $pid > 0 && @Process::kill($pid, 0);
        $rssMb = null;
        $pssMb = null;
        $osActualMb = null;
        if ($alive) {
            $mem = \Scf\Util\MemoryMonitor::getPssRssByPid($pid);
            $rssMb = isset($mem['rss_kb']) && is_numeric($mem['rss_kb']) ? round(((float)$mem['rss_kb']) / 1024, 1) : null;
            $pssMb = isset($mem['pss_kb']) && is_numeric($mem['pss_kb']) ? round(((float)$mem['pss_kb']) / 1024, 1) : null;
            $osActualMb = $pssMb ?? $rssMb;
        }

        $serverConfig = Config::server();
        $autoRestartEnabled = (bool)($serverConfig['worker_memory_auto_restart'] ?? false);

        if (
            $alive
            && $autoRestartEnabled
            && $autoRestart === STATUS_ON
            && str_starts_with($name, 'worker:')
            && $limitMb > 0
            && $osActualMb !== null
            && $osActualMb > $limitMb
        ) {
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            $restartKey = "gateway.upstream.memory.restart:{$host}:{$port}:{$name}";
            $lastRestartAt = (int)(Runtime::instance()->get($restartKey) ?? 0);
            if (time() - $lastRestartAt >= 120) {
                if ($this->requestUpstreamWorkerMemoryRestart($instance, $name, $pid, $osActualMb, $limitMb)) {
                    Runtime::instance()->set($restartKey, time());
                    $restartTs = time();
                    $restartCount++;
                }
            }
        }

        return [
            'name' => $name,
            'pid' => $pid > 0 ? $pid : ($fallbackRow['pid'] ?? '--'),
            'usage' => number_format($usage, 2) . ' MB',
            'real' => number_format($real, 2) . ' MB',
            'peak' => number_format($peak, 2) . ' MB',
            'os_actual' => $osActualMb === null ? '-' : (number_format($osActualMb, 2) . ' MB'),
            'rss' => $rssMb === null ? '-' : (number_format($rssMb, 2) . ' MB'),
            'pss' => $pssMb === null ? '-' : (number_format($pssMb, 2) . ' MB'),
            'updated' => time(),
            'usage_updated' => $usageUpdated,
            'status' => $alive ? Color::green('正常') : Color::red('离线'),
            'connection' => (int)($fallbackRow['connection'] ?? 0),
            'restart_ts' => $restartTs,
            'restart_count' => $restartCount,
            'limit_memory_mb' => $limitMb,
        ];
    }

    /**
     * 通过 upstream 本地 IPC 请求其自行平滑轮换指定 worker。
     *
     * gateway 只负责发现超限和下发命令，不直接对子进程树外的 worker pid 发信号，
     * 这样 worker 的退出/补拉仍然由 upstream 自己的 Swoole 生命周期负责。
     *
     * @param array $instance upstream 实例描述。
     * @param string $processName worker 行名，例如 worker:1。
     * @param int $pid worker 当前 pid。
     * @param float $osActualMb gateway 现采到的 OS 物理内存。
     * @param int $limitMb 该 worker 的阈值。
     * @return bool 请求被 upstream 接收时返回 true。
     */
    protected function requestUpstreamWorkerMemoryRestart(array $instance, string $processName, int $pid, float $osActualMb, int $limitMb): bool {
        if (!(bool)(Config::server()['worker_memory_auto_restart'] ?? false)) {
            return false;
        }
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        if ($port <= 0 || $processName === '' || $pid <= 0) {
            return false;
        }
        $ipcResponse = LocalIpc::request(
            LocalIpc::upstreamSocketPath($port),
            'upstream.restart_worker',
            [
                'process' => $processName,
                'pid' => $pid,
            ],
            1.5
        );
        $accepted = is_array($ipcResponse) && (int)($ipcResponse['status'] ?? 0) === 200;
        if ($accepted) {
            Log::instance()->setModule('system')
                ->error("{$processName}[PID:{$pid}] 内存 {$osActualMb}MB ≥ {$limitMb}MB，Gateway 已通知 upstream 平滑轮换");
            return true;
        }
        $message = is_array($ipcResponse) ? (string)($ipcResponse['message'] ?? 'request failed') : 'ipc unavailable';
        Log::instance()->setModule('system')
            ->warning("{$processName}[PID:{$pid}] 内存 {$osActualMb}MB ≥ {$limitMb}MB，但 upstream 平滑轮换请求失败: {$message}");
        return false;
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

    /**
     * 计算 dashboard websocket 的对外地址。
     *
     * dev server、反向代理、本地 loopback 混用时，Referer/Host 很容易指向前端开发端口，
     * 这里会优先保留真正的网关地址；如果只能拿到 loopback，则回退到 gateway 自己的 dashboard 端口。
     *
     * @param string $host 当前请求的 Host 头
     * @param string $referer 当前页面 Referer
     * @param string $forwardedProto 代理透传的协议头
     * @return string
     */
    protected function buildDashboardSocketHost(string $host, string $referer, string $forwardedProto = ''): string {
        $normalizedHost = trim($host);
        $normalizedReferer = trim($referer);
        $normalizedProto = strtolower(trim($forwardedProto));
        $pathPrefix = $this->dashboardRefererPathPrefix($normalizedReferer);

        if ($this->isLoopbackDashboardHost($normalizedHost)) {
            $refererAuthority = $this->dashboardRefererAuthority($normalizedReferer);
            if ($refererAuthority !== '' && !$this->isLoopbackDashboardHost($refererAuthority)) {
                $normalizedHost = $refererAuthority;
            }
        }

        if ($normalizedHost === '') {
            $normalizedHost = $this->dashboardRefererAuthority($normalizedReferer);
        }

        if ($normalizedHost === '') {
            $normalizedHost = '127.0.0.1:' . $this->resolvedDashboardPort();
        }
        $normalizedHost = $this->normalizeDashboardSocketAuthority($normalizedHost);

        $protocol = in_array($normalizedProto, ['https', 'wss'], true)
            || (!empty($normalizedReferer) && str_starts_with($normalizedReferer, 'https'))
            ? 'wss://'
            : 'ws://';

        return $protocol . $normalizedHost . $pathPrefix . '/dashboard.socket';
    }

    protected function normalizeDashboardSocketAuthority(string $authority): string {
        $authority = trim($authority);
        if ($authority === '') {
            return '127.0.0.1:' . $this->resolvedDashboardPort();
        }
        $parts = parse_url(str_contains($authority, '://') ? $authority : ('tcp://' . $authority));
        if (!is_array($parts)) {
            return $authority;
        }
        $host = trim((string)($parts['host'] ?? ''));
        if ($host === '') {
            $host = trim((string)($parts['path'] ?? ''));
        }
        if ($host === '') {
            return $authority;
        }
        $port = (int)($parts['port'] ?? 0);
        if ($port <= 0 && $this->isLoopbackDashboardHost($host)) {
            $port = $this->resolvedDashboardPort();
        }
        return $port > 0 ? ($host . ':' . $port) : $host;
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
        $instanceLabel = trim((string)($payload['instance_label'] ?? ''));

        if ($oldInstance) {
            $this->writeRelayedOldInstanceConsole($time, $message, $instanceLabel);
        }

        if ($this->dashboardEnabled()) {
            if (!$this->hasConsoleSubscribers()) {
                return $oldInstance;
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
        if (
            !$this->subProcessManager
            || !$this->subProcessManager->hasProcess('GatewayClusterCoordinator')
        ) {
            return false;
        }
        $this->subProcessManager->sendCommand('console_log', [
            'time' => $time,
            'message' => $message,
            'source_type' => $sourceType,
            'node' => $node,
            'old_instance' => $oldInstance,
        ], ['GatewayClusterCoordinator']);
        return true;
    }

    protected function hasConsoleSubscribers(): bool {
        return !empty($this->dashboardClients);
    }

    protected function writeRelayedOldInstanceConsole(string $time, string $message, string $instanceLabel = ''): void {
        $prefix = $instanceLabel !== '' ? ('#' . $instanceLabel . ' ') : '';
        $suffix = $time !== '' ? " (source_time={$time})" : '';
        echo Console::timestamp() . ' ' . "\033[90m" . $prefix . $message . $suffix . "\e[0m" . PHP_EOL;
        flush();
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

    /**
     * 关闭指定 host 对应的 managed upstream。
     *
     * @param string $host 目标实例 host
     * @return Result
     */
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

    /**
     * 统一入口，当前默认等价于滚动重启。
     *
     * @param array<int, array<string, mixed>>|null $plans 指定的业务实例计划集合
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function restartManagedUpstreams(?array $plans = null): array {
        return $this->rollingRestartManagedUpstreams($plans);
    }

    /**
     * 对 managed upstream 做一次滚动重启。
     *
     * 顺序不能乱：
     * 1. 先拉起新 generation；
     * 2. 等待新实例 serverIsReady；
     * 3. 切 active generation 并校验切流；
     * 4. 再通知旧代进入 quiesce / recycle。
     *
     * @param array<int, array<string, mixed>>|null $plans 指定需要滚动重启的 managed plan 集合
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function rollingRestartManagedUpstreams(?array $plans = null): array {
        $this->managedUpstreamRolling = true;
        try {
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
        $previousActiveVersion = (string)($this->instanceManager->state()['active_version'] ?? ($basePlans[0]['version'] ?? ''));
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
            Console::success("【Gateway】新业务实例已就绪(serverIsReady)，等待切换流量: " . $this->describePlan($newPlan));

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

        // 只有在新代 ready 且即将切流时，旧代才允许进入 draining/recycle。
        $this->instanceManager->activateVersion($generationVersion, self::ROLLING_DRAIN_GRACE_SECONDS);
        $this->notifyManagedUpstreamGenerationIterated($generationVersion);
        $this->syncNginxProxyTargets('rolling_restart_activate');
        if (!$this->waitForManagedGenerationCutover($generationVersion, $registeredPlans, 'rolling_restart')) {
            Console::warning("【Gateway】滚动重启切换校验失败，回滚旧业务实例: generation={$generationVersion}");
            $this->rollbackManagedGenerationCutover($previousActiveVersion, $generationVersion, $registeredPlans, $newPlans, 'rolling_restart');
            return [
                'success_count' => 0,
                'failed_nodes' => [[
                    'host' => $generationVersion,
                    'error' => '新业务实例切换校验失败，已回滚，旧实例保持运行',
                ]],
            ];
        }
        foreach ($newPlans as $plan) {
            $this->appendManagedPlan($plan);
        }
        foreach ($basePlans as $plan) {
            $this->quiesceManagedPlanBusinessPlane($plan);
        }
        Console::success("【Gateway】滚动重启完成并切换流量: generation={$generationVersion}, success={$successCount}, drain_grace=" . self::ROLLING_DRAIN_GRACE_SECONDS . "s");
        $this->logPreviousGenerationTransition($previousActiveVersion);

        return [
            'success_count' => $successCount,
            'failed_nodes' => [],
        ];
        } finally {
            $this->managedUpstreamRolling = false;
            $this->lastManagedUpstreamRollingAt = time();
            $this->managedUpstreamHealthState = [];
        }
    }

    /**
     * 对 managed upstream 做滚动升级。
     *
     * 它和 rolling restart 的差异主要在 generation 标识和升级上下文，
     * 切流、回滚、回收的控制流程保持一致。
     *
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @return array{success_count:int, failed_nodes:array<int, array<string, mixed>>}
     */
    protected function rollingUpdateManagedUpstreams(string $type, string $version): array {
        $this->managedUpstreamRolling = true;
        try {
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
        $previousActiveVersion = (string)($this->instanceManager->state()['active_version'] ?? ($basePlans[0]['version'] ?? ''));
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
            Console::success("【Gateway】新业务实例已就绪(serverIsReady)，等待切换流量: " . $this->describePlan($newPlan));

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
        $this->notifyManagedUpstreamGenerationIterated($generationVersion);
        $this->syncNginxProxyTargets('rolling_update_activate');
        if (!$this->waitForManagedGenerationCutover($generationVersion, $registeredPlans, 'rolling_update')) {
            Console::warning("【Gateway】滚动升级切换校验失败，回滚旧业务实例: generation={$generationVersion}");
            $this->rollbackManagedGenerationCutover($previousActiveVersion, $generationVersion, $registeredPlans, $newPlans, 'rolling_update');
            return [
                'success_count' => 0,
                'failed_nodes' => [[
                    'host' => $generationVersion,
                    'error' => '新业务实例切换校验失败，已回滚，旧实例保持运行',
                ]],
            ];
        }
        foreach ($newPlans as $plan) {
            $this->appendManagedPlan($plan);
        }
        foreach ($basePlans as $plan) {
            $this->quiesceManagedPlanBusinessPlane($plan);
        }
        Console::success("【Gateway】滚动升级完成并切换流量: generation={$generationVersion}, success={$successCount}, drain_grace=" . self::ROLLING_DRAIN_GRACE_SECONDS . "s");
        $this->logPreviousGenerationTransition($previousActiveVersion);

        return [
            'success_count' => $successCount,
            'failed_nodes' => [],
        ];
        } finally {
            $this->managedUpstreamRolling = false;
            $this->lastManagedUpstreamRollingAt = time();
            $this->managedUpstreamHealthState = [];
        }
    }

    /**
     * 校验新 generation 是否已经真正接管流量。
     *
     * 不是只看 active_version 切换成功就算完成，还要连续多次确认新实例健康，
     * 避免“刚激活就掉线”的短抖动被误判成切流成功。
     *
     * @param string $generationVersion 新 generation 版本号
     * @param array<int, array<string, mixed>> $plans
     * @param string $stage 当前校验阶段名
     * @return bool
     */
    protected function waitForManagedGenerationCutover(string $generationVersion, array $plans, string $stage): bool {
        if ($generationVersion === '' || !$plans) {
            return false;
        }
        $deadline = microtime(true) + self::ROLLING_CUTOVER_VERIFY_TIMEOUT_SECONDS;
        $stableChecks = 0;
        $lastReason = 'unknown';
        while (microtime(true) < $deadline) {
            $snapshot = $this->instanceManager->snapshot();
            if ((string)($snapshot['active_version'] ?? '') !== $generationVersion) {
                $lastReason = 'active_version_not_switched';
                $stableChecks = 0;
                usleep(200000);
                continue;
            }
            $allHealthy = true;
            foreach ($plans as $plan) {
                $probe = $this->probeManagedUpstreamHealth($plan);
                if ($probe['healthy']) {
                    continue;
                }
                $allHealthy = false;
                $lastReason = (string)($probe['reason'] ?? 'unknown');
                $stableChecks = 0;
                break;
            }
            if (!$allHealthy) {
                usleep(200000);
                continue;
            }
            $stableChecks++;
            if ($stableChecks >= self::ROLLING_CUTOVER_STABLE_CHECKS) {
                Console::success("【Gateway】新业务实例切换校验通过: stage={$stage}, generation={$generationVersion}, checks={$stableChecks}");
                return true;
            }
            usleep(200000);
        }
        Console::warning("【Gateway】新业务实例切换校验超时: stage={$stage}, generation={$generationVersion}, reason={$lastReason}");
        return false;
    }

    /**
     * 在新 generation 切流失败时把流量恢复到旧 generation，并清理新代。
     *
     * @param string $previousActiveVersion 回滚目标 generation
     * @param string $generationVersion 本次失败的新 generation
     * @param array<int, array<string, mixed>> $registeredPlans 已注册进 registry 的新计划
     * @param array<int, array<string, mixed>> $newPlans 本轮启动过的新计划
     * @param string $stage 当前切换阶段
     * @return void
     */
    protected function rollbackManagedGenerationCutover(string $previousActiveVersion, string $generationVersion, array $registeredPlans, array $newPlans, string $stage): void {
        $snapshot = $this->instanceManager->state();
        if ($previousActiveVersion !== ''
            && $previousActiveVersion !== $generationVersion
            && isset(($snapshot['generations'] ?? [])[$previousActiveVersion])) {
            $this->instanceManager->activateVersion($previousActiveVersion, self::ROLLING_DRAIN_GRACE_SECONDS);
            $this->notifyManagedUpstreamGenerationIterated($previousActiveVersion);
            $this->syncNginxProxyTargets($stage . '_rollback');
            Console::warning("【Gateway】已回滚业务流量到旧实例: generation={$previousActiveVersion}");
        }
        foreach ($registeredPlans as $plan) {
            $this->stopManagedPlan($plan);
        }
        foreach ($newPlans as $plan) {
            $this->removeManagedPlan($plan);
        }
    }

    /**
     * 记录旧 generation 进入 draining 的起止时间窗口。
     *
     * @param string $version 旧 generation 版本号
     * @return void
     */
    protected function logDrainingGenerationSchedule(string $version): void {
        if ($version === '') {
            return;
        }
        $snapshot = $this->instanceManager->snapshot();
        $generation = (array)(($snapshot['generations'] ?? [])[$version] ?? []);
        if (($generation['status'] ?? '') !== 'draining') {
            return;
        }
        Console::info(
            "【Gateway】旧业务实例进入 draining: generation={$version}, "
            . "started_at=" . (int)($generation['drain_started_at'] ?? 0)
            . ", deadline_at=" . (int)($generation['drain_deadline_at'] ?? 0)
        );
    }

    /**
     * 记录切流后旧 generation 的状态。
     *
     * 旧代可能还在 draining，说明确实有尾流等待；
     * 也可能已经 offline，说明实例已死或已无在途请求，可以直接回收。
     *
     * @param string $version 旧 generation 版本号
     * @return void
     */
    protected function logPreviousGenerationTransition(string $version): void {
        if ($version === '') {
            return;
        }
        $snapshot = $this->instanceManager->snapshot();
        $generation = (array)(($snapshot['generations'] ?? [])[$version] ?? []);
        $status = (string)($generation['status'] ?? '');
        if ($status === 'draining') {
            Console::info("【Gateway】旧业务实例保持服务尾流，待 nginx 切换稳定且无在途请求后再进入 shutdown: previous={$version}");
            $this->logDrainingGenerationSchedule($version);
            return;
        }
        if ($status === 'offline') {
            Console::info("【Gateway】旧业务实例已不可达或已无尾流，直接进入回收: previous={$version}");
        }
    }

    /**
     * 请求 supervisor 回收一个 managed plan。
     *
     * 真正的“回收完成”不是这里发完 stop 命令，而是等 checkPendingManagedRecycle()
     * 观察到端口关闭之后，再清理 registry 和内存中的计划状态。
     *
     * @param array<string, mixed> $plan 目标 managed plan
     * @param bool $removeState 回收完成后是否从 registry 中移除实例状态
     * @param bool $removePlan 回收完成后是否从内存计划表中移除
     * @return void
     */
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
            unset($this->managedUpstreamHealthState[$key]);
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

    /**
     * 根据 host 筛选当前内存里的 managed plan。
     *
     * @param string $host 目标 host
     * @return array<int, array<string, mixed>>
     */
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

    /**
     * 返回当前 active generation 对应的 managed plan 集合。
     *
     * 当 active generation 缺失时，会退回到当前内存里的全部有效计划。
     *
     * @return array<int, array<string, mixed>>
     */
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

    /**
     * 从 instance snapshot 反推 managed plan，作为内存计划丢失时的兜底恢复。
     *
     * @return array<int, array<string, mixed>>
     */
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
                    'src' => (string)($metadata['src'] ?? APP_SRC_TYPE),
                    'metadata' => array_merge($metadata, [
                        'managed' => true,
                        'managed_mode' => (string)($metadata['managed_mode'] ?? 'gateway_supervisor'),
                        'display_version' => (string)($metadata['display_version'] ?? ($instance['version'] ?? $version)),
                    ]),
                    'start_timeout' => (int)($metadata['start_timeout'] ?? 25),
                    'extra' => $this->normalizeManagedPlanExtraFlags([
                        'src' => (string)($metadata['src'] ?? APP_SRC_TYPE),
                        'metadata' => $metadata,
                    ]),
                ];
            }
        }
        return $plans;
    }

    /**
     * 归一化计划里的额外启动参数，确保 dev/pack/src/gateway_port 语义能继承到子进程。
     *
     * @param array<string, mixed> $plan managed plan
     * @return array<int, string>
     */
    protected function normalizeManagedPlanExtraFlags(array $plan): array {
        $flags = [];
        foreach ((array)($plan['extra'] ?? []) as $flag) {
            if (is_string($flag) && $flag !== '') {
                $flags[] = $flag;
            }
        }
        foreach ((array)(($plan['metadata']['extra_flags'] ?? [])) as $flag) {
            if (is_string($flag) && $flag !== '') {
                $flags[] = $flag;
            }
        }

        $hasDev = false;
        $hasDir = false;
        $hasPhar = false;
        $hasPackMode = false;
        $hasGatewayPort = false;
        foreach ($flags as $flag) {
            $hasDev = $hasDev || $flag === '-dev';
            $hasDir = $hasDir || $flag === '-dir';
            $hasPhar = $hasPhar || $flag === '-phar';
            $hasPackMode = $hasPackMode || in_array($flag, ['-pack', '-nopack'], true);
            $hasGatewayPort = $hasGatewayPort || str_starts_with($flag, '-gateway_port=');
        }

        if (!$hasDev && SERVER_RUN_ENV === 'dev') {
            $flags[] = '-dev';
        }
        if (!$hasPackMode && !(defined('FRAMEWORK_IS_PHAR') && FRAMEWORK_IS_PHAR === true)) {
            $flags[] = '-nopack';
        }
        if (!$hasDir && !$hasPhar) {
            $src = (string)($plan['src'] ?? ($plan['metadata']['src'] ?? APP_SRC_TYPE));
            if ($src === 'dir') {
                $flags[] = '-dir';
            } elseif ($src === 'phar') {
                $flags[] = '-phar';
            }
        }
        if (!$hasGatewayPort && $this->businessPort() > 0) {
            $flags[] = '-gateway_port=' . $this->businessPort();
        }

        $normalized = [];
        foreach ($flags as $flag) {
            if (!is_string($flag) || $flag === '') {
                continue;
            }
            $normalized[$flag] = true;
        }
        return array_keys($normalized);
    }

    /**
     * 构建滚动升级使用的 generation 版本号。
     *
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @return string
     */
    protected function buildRollingGenerationVersion(string $type, string $version): string {
        if ($type === 'app') {
            return $version;
        }
        return $type . '-' . $version . '-' . date('YmdHis');
    }

    /**
     * 构建普通 reload 使用的 generation 标识。
     *
     * @param array<int, array<string, mixed>> $plans 当前 active plan 集合
     * @return string
     */
    protected function buildReloadGenerationVersion(array $plans): string {
        $displayVersion = (string)($plans[0]['metadata']['display_version'] ?? $plans[0]['version'] ?? App::version() ?? 'reload');
        return $displayVersion . '-reload-' . date('YmdHis');
    }

    /**
     * 收集 gateway 与 managed upstream 当前占用或保留的端口。
     *
     * @return array<int, int>
     */
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

    /**
     * 为新的 managed upstream 分配未占用端口。
     *
     * @param int $startPort 起始扫描端口
     * @param array<int, int> $reserved 已保留端口列表
     * @return int
     */
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

    /**
     * 将一个 managed plan 注册到 instance registry，并按需激活该 generation。
     *
     * @param array<string, mixed> $plan managed plan
     * @param bool $activate 是否立即激活该 generation
     * @return void
     */
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
        $metadata['src'] = (string)($plan['src'] ?? ($metadata['src'] ?? APP_SRC_TYPE));
        $metadata['start_timeout'] = (int)($plan['start_timeout'] ?? ($metadata['start_timeout'] ?? 25));
        $metadata['extra_flags'] = $this->normalizeManagedPlanExtraFlags($plan);
        $this->instanceManager->registerUpstream($version, $host, $port, $weight, $metadata);
        if ($activate) {
            $this->instanceManager->activateVersion($version, 0);
            $this->notifyManagedUpstreamGenerationIterated($version);
            $this->syncNginxProxyTargets('register_managed_plan_activate');
        }
        $this->bootstrappedManagedInstances[$version . '@' . $host . ':' . $port] = true;
    }

    /**
     * 把新启动的 managed plan 追加进内存计划表。
     *
     * @param array<string, mixed> $plan managed plan
     * @return void
     */
    protected function appendManagedPlan(array $plan): void {
        foreach ($this->managedUpstreamPlans as $existing) {
            if ((string)($existing['version'] ?? '') === (string)($plan['version'] ?? '')
                && (string)($existing['host'] ?? '') === (string)($plan['host'] ?? '')
                && (int)($existing['port'] ?? 0) === (int)($plan['port'] ?? 0)) {
                return;
            }
        }
        $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
        $this->managedUpstreamPlans[] = $plan;
    }

    /**
     * 将运行期探测到的 metadata 合并回内存计划表。
     *
     * @param string $host 实例 host
     * @param int $port 实例端口
     * @param array<string, mixed> $metadataPatch 需要合并的元数据补丁
     * @return void
     */
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

    /**
     * 用 registry/snapshot 中的最新运行时信息补全 managed plan。
     *
     * @param array<string, mixed> $plan 原始 managed plan
     * @return array<string, mixed>
     */
    protected function hydrateManagedPlanRuntimeMetadata(array $plan): array {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($host === '' || $port <= 0) {
            $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
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
                $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
                return $plan;
            }
        }
        $plan['extra'] = $this->normalizeManagedPlanExtraFlags($plan);
        return $plan;
    }

    /**
     * 从内存计划表移除指定的 managed plan。
     *
     * @param array<string, mixed> $plan 目标 managed plan
     * @return void
     */
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

    /**
     * 生成人类可读的 plan 描述，用于日志串联整条生命周期。
     *
     * @param array<string, mixed> $plan managed plan
     * @return string
     */
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

    /**
     * 在新代切流成功后，让旧业务实例停止接收新业务。
     *
     * 正常路径下先发 quiesce，让旧代自己排空；
     * 如果旧代已经不可达，则直接标记 offline 并进入回收，避免“死实例还挂着 draining deadline”。
     *
     * @param array<string, mixed> $plan 旧 generation 对应的 managed plan
     * @return void
     */
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
            $probe = $this->probeManagedUpstreamHealth($plan);
            $reachability = $this->managedPlanReachability($plan);
            $unrecoverableReasons = [
                'http_port_down',
                'rpc_port_down',
                'health_status_unreachable',
                'http_probe_failed',
                'server_not_alive',
                'server_not_ready',
            ];
            if (
                (!$reachability['http_listening'] && !$reachability['rpc_listening'])
                || (!$probe['healthy'] && in_array((string)($probe['reason'] ?? ''), $unrecoverableReasons, true))
            ) {
                Console::warning(
                    "【Gateway】旧业务实例已不可达，直接进入回收: " . $this->managedPlanDescriptor($plan)
                    . ", reason=" . (string)($probe['reason'] ?? 'unknown')
                    . ", http=" . ($reachability['http_listening'] ? 'listening' : 'down')
                    . ", rpc=" . ($reachability['rpc_port'] > 0 ? ($reachability['rpc_listening'] ? 'listening' : 'down') : 'n/a')
                    . ", process=" . ($reachability['pid_alive'] ? 'alive' : 'down')
                );
                $this->instanceManager->markInstanceOffline($host, $port, $version);
                $this->stopManagedPlan($plan, true);
                return;
            }
            Console::warning("【Gateway】旧业务实例业务平面静默失败: " . $this->managedPlanDescriptor($plan));
            return;
        }
        $runtimeStatus = $this->fetchUpstreamRuntimeStatus($plan);
        $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
        $httpProcessing = (int)($runtimeStatus['http_request_processing'] ?? 0);
        $rpcProcessing = (int)($runtimeStatus['rpc_request_processing'] ?? 0);
        Console::info(
            "【Gateway】旧业务实例开始平滑回收: " . $this->managedPlanDescriptor($plan)
            . ", ws=" . $gatewayWs
            . ", http=" . $httpProcessing
            . ", rpc=" . $rpcProcessing
            . ", queue=" . (int)($runtimeStatus['redis_queue_processing'] ?? 0)
            . ", crontab=" . (int)($runtimeStatus['crontab_busy'] ?? 0)
        );
        // 切流后旧实例已经不再承接新流量，剩余的 gateway 客户端连接如果继续保留，
        // WebSocket 等长连接会让旧代永远停在 draining。这里主动断开所有仍绑定在
        // 当前旧实例上的客户端 fd，让它们尽快重新连到新 active generation。
        $disconnected = $this->disconnectManagedPlanClients($plan);
        if ($disconnected > 0) {
            $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
            Console::info(
                "【Gateway】旧业务实例客户端连接已主动断开: " . $this->managedPlanDescriptor($plan)
                . ", disconnected={$disconnected}, remaining_ws={$gatewayWs}"
            );
        }
        if ($gatewayWs === 0 && $httpProcessing === 0 && $rpcProcessing === 0) {
            Console::info("【Gateway】旧业务实例已无在途请求，立即进入 shutdown: " . $this->managedPlanDescriptor($plan));
            $this->stopManagedPlan($plan, true);
        }
    }

    /**
     * 主动断开仍绑定在旧 managed plan 上的全部客户端连接。
     *
     * 切流完成后，这些连接继续留在旧实例上只会拖住 draining；尤其是 WebSocket
     * 长连接如果不主动 close，旧实例将长期无法进入 shutdown。这里按实例绑定表
     * 找到所有客户端 fd，并统一交给 tcp relay 关闭，连接计数也会随之同步释放。
     *
     * @param array<string, mixed> $plan 旧 generation 对应的 managed plan。
     * @return int 实际断开的客户端数量。
     */
    protected function disconnectManagedPlanClients(array $plan): int {
        $version = (string)($plan['version'] ?? '');
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            return 0;
        }

        $fds = $this->instanceManager->gatewayConnectionFdsFor($version, $host, $port);
        if (!$fds) {
            return 0;
        }

        $disconnected = 0;
        foreach ($fds as $fd) {
            try {
                $this->tcpRelayHandler()->disconnectClient($fd);
                $disconnected++;
            } catch (Throwable) {
            }
        }
        return $disconnected;
    }

    /**
     * 辅助判断旧实例当前是“还能服务但在排空”，还是“已经不可达”。
     *
     * @param array<string, mixed> $plan 旧实例 plan
     * @return array{http_listening: bool, rpc_listening: bool, pid_alive: bool, rpc_port: int}
     */
    protected function managedPlanReachability(array $plan): array {
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? (($plan['metadata']['rpc_port'] ?? 0)));
        $pid = (int)($plan['metadata']['pid'] ?? 0);

        $httpListening = $port > 0 && (
            $this->launcher->isListening($host, $port, 0.2)
            || $this->launcher->isListening('0.0.0.0', $port, 0.2)
        );
        $rpcListening = $rpcPort > 0 && (
            $this->launcher->isListening($host, $rpcPort, 0.2)
            || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.2)
        );
        $pidAlive = $pid > 0 && Process::kill($pid, 0);

        return [
            'http_listening' => $httpListening,
            'rpc_listening' => $rpcListening,
            'pid_alive' => $pidAlive,
            'rpc_port' => $rpcPort,
        ];
    }

    protected function cleanupOfflineManagedUpstreams(): void {
        $snapshot = $this->instanceManager->snapshot();
        $removedVersions = [];
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
                $removedVersions[] = $version;
            }
        }
        if ($removedVersions) {
            Console::success("【Gateway】旧业务实例代际已移除: count=" . count($removedVersions) . ", generations=" . implode(' | ', $removedVersions));
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

    /**
     * 轮询某个 pending recycle 的最终完成态。
     *
     * 只要端口还在监听，就说明旧实例还没真正退出；
     * 只有端口都关掉后，才会把 registry 与内存中的 managed plan 一并清理。
     *
     * @param string $key pending recycle 项唯一键
     * @return bool true 表示 watcher 可移除；false 表示仍需继续轮询
     */
    protected function checkPendingManagedRecycle(string $key): bool {
        if (!$this->launcher || !isset($this->pendingManagedRecycles[$key])) {
            return true;
        }
        if (($this->pendingManagedRecycleCompletions[$key] ?? false) === true) {
            return false;
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
        $this->pendingManagedRecycleCompletions[$key] = true;
        if ($removeState) {
            $this->instanceManager->removeInstance($host, $port);
        }
        unset($this->bootstrappedManagedInstances[$key]);
        if ($removePlan) {
            $this->removeManagedPlan($plan);
        }
        unset($this->managedUpstreamHealthState[$key]);
        $removedGeneration = '';
        if ($removeState) {
            $version = (string)($plan['version'] ?? '');
            if ($version !== '') {
                $snapshot = $this->instanceManager->snapshot();
                $generation = (array)(($snapshot['generations'] ?? [])[$version] ?? []);
                if ($generation && empty($generation['instances'] ?? [])) {
                    $this->instanceManager->removeVersion($version);
                    $removedGeneration = $version;
                }
            }
        }
        unset($this->pendingManagedRecycleWarnState[$key], $this->pendingManagedRecycles[$key], $this->pendingManagedRecycleCompletions[$key]);
        Console::success("【Gateway】旧业务实例回收完成 " . $this->managedPlanDescriptor($plan));
        if ($removedGeneration !== '') {
            Console::success("【Gateway】旧业务实例代际已移除: count=1, generations={$removedGeneration}");
        }
        $activeVersion = (string)($this->instanceManager->snapshot()['active_version'] ?? '');
        if ($activeVersion !== '') {
            $this->notifyManagedUpstreamGenerationIterated($activeVersion);
        } else {
            $this->lastManagedHealthActiveVersion = '';
            $this->managedUpstreamHealthState = [];
            $this->lastManagedUpstreamHealthCheckAt = 0;
        }
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
                . ", deadline_at=" . (int)($item['drain_deadline_at'] ?? 0)
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
                $afterWrite = isset($result['__after_write']) && is_callable($result['__after_write'])
                    ? $result['__after_write']
                    : null;
                unset($result['__after_write']);
                $this->json($response, (int)($result['status'] ?? 500), $result['data'] ?? ['message' => (string)($result['message'] ?? 'request failed')]);
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
                return $this->formatGatewayInternalCommandResult($this->dispatchLocalGatewayCommand('reload'));
            case 'reload_gateway':
                $reservation = $this->reserveGatewayReload();
                if (!$reservation['accepted']) {
                    return ['ok' => false, 'status' => 409, 'message' => $reservation['message']];
                }
                $response = [
                    'ok' => true,
                    'status' => 200,
                    'data' => [
                        'accepted' => true,
                        'message' => $reservation['message'],
                    ],
                ];
                if ($reservation['scheduled']) {
                    $response['__after_write'] = function () use ($reservation): void {
                        $this->scheduleReservedGatewayReload((bool)$reservation['restart_managed_upstreams']);
                    };
                }
                return $response;
            case 'restart_crontab':
                return ['ok' => false, 'status' => 409, 'message' => 'crontab now managed by linux scheduler'];
            case 'restart_redisqueue':
                if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
                    return ['ok' => false, 'status' => 409, 'message' => 'redisqueue process unavailable'];
                }
                return $this->formatGatewayInternalCommandResult($this->dispatchLocalGatewayCommand('restart_redisqueue'));
            case 'restart':
                return [
                    'ok' => true,
                    'status' => 200,
                    'data' => ['accepted' => true, 'message' => 'gateway restart started'],
                    '__after_write' => function (): void {
                        $this->scheduleGatewayShutdown(true);
                    },
                ];
            case 'shutdown':
                return [
                    'ok' => true,
                    'status' => 200,
                    'data' => ['accepted' => true, 'message' => 'gateway shutdown started'],
                    '__after_write' => function (): void {
                        $this->scheduleGatewayShutdown();
                    },
                ];
            default:
                return ['ok' => false, 'status' => 400, 'message' => 'unsupported command'];
        }
    }

    /**
     * 将本地命令 Result 统一转成内部 HTTP 命令返回体。
     *
     * @param Result $result 本地命令执行结果
     * @return array<string, mixed>
     */
    protected function formatGatewayInternalCommandResult(Result $result): array {
        if ($result->hasError()) {
            return [
                'ok' => false,
                'status' => 409,
                'message' => (string)$result->getMessage(),
                'data' => $result->getData(),
            ];
        }
        $message = $result->getData();
        if (!is_string($message) || $message === '') {
            $message = (string)$result->getMessage();
        }
        return [
            'ok' => true,
            'status' => 200,
            'data' => [
                'accepted' => true,
                'message' => $message,
            ],
        ];
    }

    protected function proxyMaxInflightRequests(): int {
        $configured = Config::server()['gateway_proxy_max_inflight'] ?? 1024;
        $limit = (int)$configured;
        return $limit > 0 ? $limit : 1024;
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

    /**
     * 在 gateway 关闭阶段同步回收所有托管 upstream。
     *
     * 这里不能再使用“只发 shutdown 命令就立刻清空 registry”的异步语义。
     * 否则 gateway 会先把 managed 实例从内存/状态文件里移除，而真实 upstream
     * 还在由 UpstreamSupervisor 慢慢平滑关闭，下一轮启动就会看不到这些仍存活的
     * 业务实例，从而留下端口残留和孤儿进程。
     *
     * 因此 shutdown 需要等待 UpstreamSupervisor 完成 `shutdownAll()` 并回执，
     * 只有确认监督器已经把托管实例收口完毕，才允许清理 registry。
     *
     * @return void
     */
    protected function shutdownManagedUpstreams(): void {
        if (!$this->upstreamSupervisor) {
            $this->instanceManager->removeManagedInstances();
            return;
        }

        $result = $this->dispatchUpstreamSupervisorCommand('shutdown', [], true, 120);
        if ($result->hasError()) {
            Console::warning('【Gateway】等待 UpstreamSupervisor 回收业务实例超时或失败，保留 registry 等待下次启动继续接管: ' . $result->getMessage());
            return;
        }

        $this->instanceManager->removeManagedInstances();
    }

    /**
     * 推进 gateway 的安装接管状态机。
     *
     * 当业务应用尚未安装完成时，gateway 需要先把 nginx 的业务入口接到自己，
     * 由控制面承接安装页和安装 API；待安装完成并成功拉起业务实例后，再切回
     * active upstream。整个过程必须在业务编排子进程里推进，不能再挂在 worker 定时器上。
     *
     * @return void
     */
    protected function refreshGatewayInstallTakeover(): void {
        $isReady = App::isReady();
        $takeoverActive = (bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER) ?? false);
        if (!$isReady) {
            if (!$takeoverActive) {
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER, true);
                Console::info('【Gateway】应用尚未安装，业务入口切换到 gateway 控制面承接安装流程');
                $this->syncNginxProxyTargets('install_takeover');
            }
            return;
        }

        if ($takeoverActive) {
            $snapshot = $this->instanceManager->snapshot();
            if (((string)($snapshot['active_version'] ?? '')) === '') {
                return;
            }
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER, false);
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_UPDATING, false);
            Console::success('【Gateway】应用安装完成，业务入口准备切回托管 upstream');
            $this->syncNginxProxyTargets('install_takeover_release');
        }
    }

    /**
     * 在 gateway server 启动前注册总控子进程。
     *
     * gateway 控制面下的 RedisQueue/MemoryUsageCount 等都挂在
     * SubProcessManager 这棵子进程树上，因此必须先以 addProcess 模式把
     * 根管理进程注册给 server，避免在 worker 协程环境里直接拉起新进程。
     *
     * @return void
     */
    protected function ensureGatewaySubProcessManagerRegistered(): void {
        if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            return;
        }
        if ($this->subProcessManager) {
            return;
        }
        $this->subProcessManager = new SubProcessManager($this->server, Config::server(), [
            'exclude_processes' => ['CrontabManager'],
            'shutdown_handler' => function (): void {
                $this->scheduleGatewayShutdown();
            },
            'reload_handler' => function (): void {
                $this->restartGatewayBusinessPlane();
            },
            'restart_handler' => function (): void {
                Console::warning('【Gateway】检测到控制面文件变动，准备重启 Gateway');
                $this->scheduleGatewayShutdown(true);
            },
            'command_handler' => function (string $command, array $params, object $socket): bool {
                return $this->handleGatewayProcessCommand($command, $params, $socket);
            },
            'node_status_builder' => function (array $status): array {
                return $this->buildGatewayHeartbeatStatus($status);
            },
            'gateway_business_tick_handler' => function (): void {
                $this->runGatewayBusinessCoordinatorTick();
            },
            'gateway_health_tick_handler' => function (): void {
                $this->runGatewayHealthMonitorTick();
            },
            'gateway_business_command_handler' => function (string $command, array $params): array {
                return $this->handleGatewayBusinessCommand($command, $params);
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
            $rpcInfo = (int)($plan['rpc_port'] ?? 0) > 0 ? ', RPC:' . (int)$plan['rpc_port'] : '';
            Console::success("【Gateway】业务实例就绪 {$version} {$host}:{$port}{$rpcInfo}");
        }
    }

    protected function renderStartupInfo(): void {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $stateFile = $this->instanceManager->stateFile();
        $frameworkRoot = Root::dir();
        $frameworkBuildTime = FRAMEWORK_BUILD_TIME;
        $packageVersion = $frameworkBuildTime === 'development' ? 'development' : FRAMEWORK_BUILD_VERSION;
        $loadedFiles = count(get_included_files());
        $taskWorkers = (int)(Config::server()['task_worker_num'] ?? 0);
        $listenLabel = $this->nginxProxyModeEnabled() ? '业务入口' : '业务TCP入口';
        $listenValue = "{$this->host}:{$this->port}";
        $controlValue = $this->internalControlHost() . ':' . $this->controlPort();
        $rpcValue = $this->rpcPort > 0 ? ($this->host . ':' . $this->rpcPort) : '--';
        $info = <<<INFO
------------------Gateway启动完成------------------
应用指纹：{APP_FINGERPRINT}
运行系统：{OS_ENV}
运行环境：{SERVER_RUN_ENV}
应用目录：{APP_DIR_NAME}
源码类型：{APP_SRC_TYPE}
节点角色：{SERVER_ROLE}
文件加载：{$loadedFiles}
环境版本：{SWOOLE_VERSION}
框架源码：{$frameworkRoot}
框架版本：{SCF_COMPOSER_VERSION}
打包版本：{$packageVersion}
打包时间：{$frameworkBuildTime}
工作进程：{$this->workerNum}
任务进程：{$taskWorkers}
主机地址：{SERVER_HOST}
应用版本：{$appVersion}
资源版本：{$publicVersion}
流量模式：{$this->trafficMode()}
{$listenLabel}：{$listenValue}
控制面：{$controlValue}
RPC监听：{$rpcValue}
进程信息：Master:{$this->serverMasterPid},Manager:{$this->serverManagerPid}
状态文件：{$stateFile}
--------------------------------------------------
INFO;
        $info = str_replace(
            ['{APP_FINGERPRINT}', '{OS_ENV}', '{SERVER_RUN_ENV}', '{APP_DIR_NAME}', '{APP_SRC_TYPE}', '{SERVER_ROLE}', '{SWOOLE_VERSION}', '{SCF_COMPOSER_VERSION}', '{SERVER_HOST}'],
            [APP_FINGERPRINT, OS_ENV, SERVER_RUN_ENV, APP_DIR_NAME, APP_SRC_TYPE, SERVER_ROLE, swoole_version(), SCF_COMPOSER_VERSION, SERVER_HOST],
            $info
        );
        Console::write(Color::cyan($info));
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
