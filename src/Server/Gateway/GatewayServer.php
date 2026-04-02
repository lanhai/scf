<?php

namespace Scf\Server\Gateway;

use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Server\SubProcessManager;
use Scf\Util\File;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Client as TcpClient;
use Swoole\Http\Request;
use Swoole\Http\Response;
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
    use GatewayDashboardRuntimeTrait;
    use GatewayRuntimeLifecycleTrait;
    use GatewayControlCommandTrait;
    use GatewayClusterHeartbeatTrait;
    use GatewayTelemetryTrait;
    use GatewayInternalManagementTrait;
    use GatewayManagedUpstreamLifecycleTrait;

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
    protected const INTERNAL_UPSTREAM_CUTOVER_PROBE_PATH = '/_scf_internal/upstream/cutover_probe';
    protected const ACTIVE_UPSTREAM_HEALTH_CHECK_INTERVAL_SECONDS = 5;
    protected const ACTIVE_UPSTREAM_HEALTH_FAILURE_THRESHOLD = 3;
    protected const ACTIVE_UPSTREAM_HEALTH_COOLDOWN_SECONDS = 60;
    protected const ACTIVE_UPSTREAM_HEALTH_STARTUP_GRACE_SECONDS = 20;
    protected const ACTIVE_UPSTREAM_HEALTH_ROLLING_COOLDOWN_SECONDS = 15;
    protected const UPSTREAM_SUPERVISOR_SYNC_INTERVAL_SECONDS = 15;
    protected const DASHBOARD_VERSION_CACHE_TTL_SECONDS = 30;
    protected const FRAMEWORK_VERSION_CACHE_TTL_SECONDS = 30;
    protected const REMOTE_VERSION_REQUEST_TIMEOUT_SECONDS = 3;
    protected const GATEWAY_LEASE_RENEW_INTERVAL_SECONDS = 2;
    protected const TEMP_HEARTBEAT_TRACE_ENABLED = true;
    protected const GATEWAY_HEALTH_TRACE_BUFFER_MAX_LINES = 120;

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
    protected array $pendingManagedRecycleCheckInFlight = [];
    protected array $pendingManagedRecycleEndpointReservations = [];
    protected array $quarantinedManagedRecycles = [];
    protected array $quarantinedManagedRecycleWarnState = [];
    protected array $managedRecyclePortCooldowns = [];
    protected array $managedUpstreamHealthState = [];
    protected bool $managedUpstreamSelfHealing = false;
    protected bool $managedUpstreamRolling = false;
    protected string $lastManagedHealthActiveVersion = '';
    protected int $lastManagedUpstreamRollingAt = 0;
    protected int $lastManagedUpstreamHealthCheckAt = 0;
    protected int $managedUpstreamHealthStableRounds = 0;
    protected int $managedUpstreamHealthLastFailureAt = 0;
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
    protected ?array $pendingNginxSyncSummary = null;
    protected bool $preserveManagedUpstreamsOnShutdown = false;
    protected bool $gatewayShutdownPrepared = false;
    protected string $lastFallbackBusinessReloadToken = '';
    protected int $gatewayLeaseEpoch = 0;
    protected int $gatewayLeaseTtlSeconds = 15;
    protected int $gatewayLeaseRestartTtlSeconds = 30;
    protected int $gatewayLeaseGraceSeconds = 20;
    protected int $gatewayLeaseRestartGraceSeconds = 120;
    protected int $gatewayLeaseRestartReuseGraceSeconds = 120;
    protected int $lastGatewayLeaseRenewAt = 0;
    protected bool $gatewayLeaseReusedEpoch = false;
    protected ?int $gatewayLeaseRenewTimerId = null;
    protected array $lastDisconnectedClientSummary = [];
    protected bool $gatewayHealthTraceActive = false;
    protected float $gatewayHealthTraceRoundStartedAt = 0.0;
    protected float $gatewayHealthTraceRoundUpdatedAt = 0.0;
    protected int $gatewayHealthTraceRoundSequence = 0;
    protected array $gatewayHealthTraceRoundBuffer = [];

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
        protected string             $host,
        protected int                $port,
        protected int                $workerNum = 1,
        protected ?AppServerLauncher $launcher = null,
        protected array              $managedUpstreamPlans = [],
        protected int                $rpcBindPort = 0,
        protected int                $controlBindPort = 0,
        protected string             $configuredTrafficMode = 'tcp'
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
     *
     * @return void
     * @throws RuntimeException|SwooleException 当监听端口或附加监听创建失败时抛出
     */
    public function start(): void {
        // gateway 控制面包含 lease/registry 等高频文件读写。
        // FILE/STDIO/STREAM_FUNCTION hook 会把这类 IO 变成协程异步文件语义，
        // 进而在 lease 定时器里放大 deadlock 风险。
        $hookFlags = SWOOLE_HOOK_ALL;
        foreach (['SWOOLE_HOOK_FILE', 'SWOOLE_HOOK_STDIO', 'SWOOLE_HOOK_STREAM_FUNCTION'] as $hookConst) {
            if (defined($hookConst)) {
                $hookFlags &= ~constant($hookConst);
            }
        }
        Coroutine::set(['hook_flags' => $hookFlags]);
        $this->bootstrapRuntimeState();
        $this->bootstrapGatewayLease();
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
            $this->renewGatewayLease('running', true);
            $dashboardPort = $this->resolvedDashboardPort();
            if ($dashboardPort > 0) {
                File::write(SERVER_DASHBOARD_PORT_FILE, (string)$dashboardPort);
            }
        });
        $this->server->on('BeforeReload', function (Server $server) {
            Runtime::instance()->serverIsReady(false);
            Runtime::instance()->serverIsDraining(true);
            Console::warning('【Gateway】服务即将重载');
            $disconnected = $this->disconnectAllClients($server);
            if ($disconnected > 0) {
                Console::warning("【Gateway】已断开 {$disconnected} 个客户端连接");
                $this->logDisconnectedClientSummary($disconnected);
            }
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
            if ($disconnected > 0) {
                Console::warning("【Gateway】已断开 {$disconnected} 个客户端连接");
                $this->logDisconnectedClientSummary($disconnected);
            }
        });
        $this->server->on('Shutdown', function () {
            Runtime::instance()->serverIsAlive(false);
        });

        $this->server->on('WorkerStart', function (Server $server, int $workerId) {
            // Gateway worker 也会直接执行 dashboard controller action。
            // 这些 action 里会读取应用 cache/database 配置，因此必须和 upstream worker 一样
            // 在 worker 生命周期起点完成 App::mount()，避免退回框架默认配置。
            App::mount();
            if ($workerId !== 0) {
                return;
            }
            Runtime::instance()->serverIsDraining(false);
            Runtime::instance()->serverIsReady(true);
            $this->startGatewayLeaseRenewTimer();
            $this->scheduleStartupInfoRender();
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

    /**
     * 返回 gateway nginx server_name 配置的最终值。
     *
     * 当业务未显式配置 `gateway_nginx_server_name`（或仅为 `_`）时，自动补一个
     * 当前 app/role/port 维度唯一的内部主机名，避免同机多 app 复用端口时 probe
     * 命中错误 server block。
     *
     * @return string 可直接用于 nginx `server_name` 指令的内容
     */
    public function resolvedGatewayNginxServerName(): string {
        $configured = trim((string)($this->serverConfig()['gateway_nginx_server_name'] ?? '_'));
        if ($configured !== '' && $configured !== '_') {
            return $configured;
        }
        return $this->defaultGatewayNginxServerName() . ' _';
    }

    /**
     * 返回入口切流探针应使用的 Host。
     *
     * @param int|null $port 可选端口，默认使用 businessPort()
     * @return string
     */
    public function resolvedGatewayIngressProbeHost(?int $port = null): string {
        $port = max(1, (int)($port ?: $this->businessPort()));
        $serverNames = preg_split('/\s+/', trim($this->resolvedGatewayNginxServerName())) ?: [];
        foreach ($serverNames as $token) {
            $host = trim((string)$token);
            if ($host === '' || $host === '_' || str_contains($host, '*')) {
                continue;
            }
            return str_contains($host, ':') ? $host : ($host . ':' . $port);
        }
        return '127.0.0.1:' . $port;
    }

    /**
     * 构造默认的内部 server_name（仅用于 probe/同端口去歧义）。
     *
     * @return string
     */
    protected function defaultGatewayNginxServerName(): string {
        $safeApp = preg_replace('/[^a-zA-Z0-9-]+/', '-', APP_DIR_NAME) ?: 'app';
        $safeRole = preg_replace('/[^a-zA-Z0-9-]+/', '-', SERVER_ROLE) ?: 'node';
        return strtolower("scf-gateway-{$safeApp}-{$safeRole}-" . $this->businessPort() . ".local");
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

    /**
     * 同步 gateway -> nginx 的 upstream/server 配置。
     *
     * @param string|null $reason 本轮同步原因
     * @param array<string, string> $summaryContext 额外摘要上下文
     * @param bool $deferSummary true 时只缓存“转发完成摘要”，由后续流程在校验通过后再输出
     * @return bool
     */
    protected function syncNginxProxyTargets(?string $reason = null, array $summaryContext = [], bool $deferSummary = false): bool {
        $handler = $this->nginxProxyHandler();
        if (!$handler->enabled()) {
            return false;
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
            $activeContext = $this->buildNginxActiveInstanceSummaryContext((array)($result['active_instances'] ?? []));
            $resolvedSummaryContext = $summaryContext
                ? array_merge($activeContext, $summaryContext)
                : $activeContext;
            $fingerprint = md5(JsonHelper::toJson([
                'reason_group' => $this->isStartupNginxSyncReason((string)($result['reason'] ?? '')) ? 'startup' : (string)($result['reason'] ?? ''),
                'reloaded' => (bool)($result['reloaded'] ?? false),
                'tested' => (bool)($result['tested'] ?? false),
                'files' => $displayPaths,
                'context' => $resolvedSummaryContext,
            ]));
            $now = time();
            if ($fingerprint === $this->lastNginxSyncLogFingerprint && ($now - $this->lastNginxSyncLoggedAt) <= 15) {
                return true;
            }
            $this->lastNginxSyncLogFingerprint = $fingerprint;
            $this->lastNginxSyncLoggedAt = $now;
            $syncReason = (string)($result['reason'] ?? '');
            $normalizedReason = strtolower(trim($syncReason));
            $shouldDeferSummary = $deferSummary || str_starts_with($normalizedReason, 'rolling_');

            if ($shouldDeferSummary) {
                $this->pendingNginxSyncSummary = [
                    'reason' => $syncReason,
                    'reloaded' => (bool)($result['reloaded'] ?? false),
                    'tested' => (bool)($result['tested'] ?? false),
                    'display_paths' => $displayPaths,
                    'context_lines' => $resolvedSummaryContext,
                ];
            } else {
                $this->pendingNginxSyncSummary = null;
                $this->renderNginxSyncSummary(
                    $handler,
                    (string)($result['reason'] ?? ''),
                    (bool)($result['reloaded'] ?? false),
                    (bool)($result['tested'] ?? false),
                    $displayPaths,
                    $resolvedSummaryContext
                );
            }
            return true;
        } catch (Throwable $throwable) {
            Console::warning('【Gateway】nginx转发配置同步失败: ' . $throwable->getMessage());
            return false;
        }
    }

    /**
     * 在切流校验通过后输出此前缓存的 nginx 转发完成摘要。
     *
     * @param array<string, string> $summaryContext 追加/覆盖摘要字段
     * @return void
     */
    protected function flushPendingNginxSyncSummary(array $summaryContext = []): void {
        if (!$this->pendingNginxSyncSummary) {
            return;
        }
        $payload = $this->pendingNginxSyncSummary;
        $this->pendingNginxSyncSummary = null;
        $contextLines = (array)($payload['context_lines'] ?? []);
        if ($summaryContext) {
            $contextLines = array_merge($contextLines, $summaryContext);
        }
        $this->renderNginxSyncSummary(
            $this->nginxProxyHandler(),
            (string)($payload['reason'] ?? ''),
            (bool)($payload['reloaded'] ?? false),
            (bool)($payload['tested'] ?? false),
            array_values(array_filter((array)($payload['display_paths'] ?? []), static fn($item): bool => is_string($item) && $item !== '')),
            $contextLines
        );
    }

    /**
     * 丢弃尚未输出的 nginx 转发摘要缓存。
     *
     * @return void
     */
    protected function clearPendingNginxSyncSummary(): void {
        $this->pendingNginxSyncSummary = null;
    }

    /**
     * 从 nginx 同步结果中的 active_instances 构建“切流实例”日志上下文。
     *
     * @param array<int, array<string, mixed>> $activeInstances 当前 active generation 对外承载实例。
     * @return array<string, string> 用于切流摘要的实例行。
     */
    protected function buildNginxActiveInstanceSummaryContext(array $activeInstances): array {
        $instances = [];
        foreach (array_values($activeInstances) as $instance) {
            if (!is_array($instance)) {
                continue;
            }
            $port = (int)($instance['port'] ?? 0);
            if ($port <= 0) {
                continue;
            }
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $version = trim((string)($instance['version'] ?? ''));
            $rpcPort = (int)($instance['rpc_port'] ?? 0);
            $weight = (int)($instance['weight'] ?? 0);
            $instances[] = [
                'version' => $version,
                'host' => $host,
                'port' => $port,
                'rpc_port' => $rpcPort,
                'weight' => $weight,
            ];
        }
        if (!$instances) {
            return [];
        }
        $serverConfig = $this->serverConfig();
        $context = [];
        $multiInstance = count($instances) > 1;
        foreach ($instances as $index => $instance) {
            $suffix = $multiInstance ? (string)($index + 1) : '';
            $instanceTitleKey = $multiInstance ? '切流实例' . $suffix : '切流实例';
            $portKey = $multiInstance ? '实例' . $suffix . '端口' : '实例端口';
            $rpcKey = $multiInstance ? '实例' . $suffix . 'RPC' : '实例RPC';
            $weightKey = $multiInstance ? '实例' . $suffix . '权重' : '实例权重';
            $context[$instanceTitleKey] = $instance['version'] !== '' ? (string)$instance['version'] : '--';
            $context[$portKey] = (string)$instance['host'] . ':' . (int)$instance['port'];
            (int)$instance['rpc_port'] > 0 and $context[$rpcKey] = (string)((int)$instance['rpc_port']);
            (int)$instance['weight'] > 0 and $context[$weightKey] = (string)((int)$instance['weight']);
        }
        $workerNum = (int)($serverConfig['worker_num'] ?? 0);
        $taskWorkerNum = (int)($serverConfig['task_worker_num'] ?? 0);
        $maxConnection = (int)($serverConfig['max_connection'] ?? 0);
        $maxCoroutine = (int)($serverConfig['max_coroutine'] ?? 0);
        $maxRequestLimit = (int)($serverConfig['max_request_limit'] ?? 0);
        $workerNum > 0 and $context['实例Worker'] = (string)$workerNum;
        $taskWorkerNum > 0 and $context['实例TaskWorker'] = (string)$taskWorkerNum;
        $maxConnection > 0 and $context['实例MaxConnection'] = (string)$maxConnection;
        $maxCoroutine > 0 and $context['实例MaxCoroutine'] = (string)$maxCoroutine;
        $maxRequestLimit > 0 and $context['实例MaxRequestLimit'] = (string)$maxRequestLimit;
        return $context;
    }

    /**
     * 在 nginx 转发配置真正同步完成后输出入口层参数概览。
     *
     * 这组信息不能跟 Gateway 启动摘要放在同一个时机，因为真正决定入口层生效
     * 状态的是业务实例 ready 后那次 sync/test/reload。只有在这里打印，日志时序
     * 才能准确表达“当前这些 nginx 参数已经对应到本轮转发配置”。
     *
     * @param GatewayNginxProxyHandler $handler 当前 nginx 配置同步器。
     * @return void
     */
    protected function renderNginxSyncSummary(
        GatewayNginxProxyHandler $handler,
        string                   $reason,
        bool                     $reloaded,
        bool                     $tested,
        array                    $displayPaths,
        array                    $contextLines = []
    ): void {
        $summaryData = $handler->startupSummaryData();
        $rows = [];
        foreach ($contextLines as $label => $value) {
            if (is_string($label)) {
                $rows[] = ['label' => $label, 'value' => (string)$value];
            } elseif (is_string($value) && $value !== '') {
                $rows[] = ['label' => '切流实例', 'value' => $value];
            }
        }
        $rows[] = ['label' => '切流原因', 'value' => $this->describeNginxSyncReason($reason)];
        $rows[] = ['label' => '切流执行', 'value' => 'reload:' . ($reloaded ? 'yes' : 'no') . ', test:' . ($tested ? 'yes' : 'no')];

        if ($displayPaths) {
            $fileRows = [];
            foreach (array_values($displayPaths) as $index => $path) {
                $fileRows[] = ['label' => '同步文件' . ($index + 1), 'value' => $path];
            }
            $rows = array_merge($rows, $fileRows);
        }

        $rows = array_merge($rows, [
            ['label' => 'Nginx服务', 'value' => (string)($summaryData['server'] ?? '--')],
            ['label' => 'Nginx请求体', 'value' => (string)($summaryData['body'] ?? '--')],
            ['label' => 'Nginx代理', 'value' => (string)($summaryData['proxy'] ?? '--')],
            ['label' => 'Nginx upstream', 'value' => (string)($summaryData['upstream'] ?? '--')],
            ['label' => 'Nginx日志', 'value' => (string)($summaryData['log'] ?? '--')],
            ['label' => 'Nginx真实IP', 'value' => (string)($summaryData['realip'] ?? '--')],
        ]);
        $lines = $this->formatAlignedPairs($rows);
        if (!$lines) {
            return;
        }

        $info = "------------------Nginx挂载实例完成------------------\n"
            . implode("\n", $lines)
            . "\n--------------------------------------------------";
        Console::write(Color::cyan($info));
    }

    /**
     * 把 nginx 同步原因整理成更适合运维阅读的文本。
     *
     * 内部 reason 需要保持稳定，方便流程判断；但日志摘要更关注“这次为什么切流”。
     * 这里做一层展示翻译，避免把内部枚举值直接暴露成阅读负担。
     *
     * @param string $reason 内部同步原因。
     * @return string 面向日志阅读的原因描述。
     */
    protected function describeNginxSyncReason(string $reason): string {
        return match ($reason) {
            'register_managed_plan_activate' => '业务实例激活切流',
            'install_takeover' => '应用未安装，入口切到控制面板',
            'install_takeover_release' => '应用安装完成，入口回切业务',
            'rolling_restart_activate' => '滚动重启切流',
            'rolling_update_activate' => '滚动升级切流',
            '', 'manual' => '手动同步',
            default => $reason,
        };
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

}
