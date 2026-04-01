<?php

namespace Scf\Server\Gateway;

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
use Scf\Util\File;
use RuntimeException;
use Swoole\Coroutine\Barrier;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
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
use const SIGKILL;
use const SIGTERM;

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
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING, true);
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY, false);
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES);
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES);
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS);
        Runtime::instance()->delete('gateway_lease_override_state');
        ConsoleRelay::setGatewayPort($this->port);
        ConsoleRelay::setLocalSubscribed(false);
        ConsoleRelay::setRemoteSubscribed(false);
    }

    /**
     * 初始化 gateway 租约上下文并分配 owner epoch。
     *
     * 控制面启动时统一通过 GatewayLease 持久化一份 running 租约。若上一代是
     * `restarting` 且尚未过期，本轮会复用 epoch，保证控制面短重启期间 upstream
     * 不会被错误判定为 owner 漂移。
     *
     * @return void
     */
    protected function bootstrapGatewayLease(): void {
        $serverConfig = Config::server();
        $this->gatewayLeaseTtlSeconds = max(3, (int)($serverConfig['gateway_lease_ttl'] ?? 15));
        $this->gatewayLeaseRestartTtlSeconds = max(3, (int)($serverConfig['gateway_lease_restart_ttl'] ?? 30));
        $this->gatewayLeaseGraceSeconds = max(3, (int)($serverConfig['gateway_lease_grace'] ?? 20));
        $this->gatewayLeaseRestartGraceSeconds = max(
            $this->gatewayLeaseGraceSeconds,
            (int)($serverConfig['gateway_lease_restart_grace'] ?? max(120, $this->gatewayLeaseGraceSeconds * 3))
        );
        $this->gatewayLeaseRestartReuseGraceSeconds = max(
            $this->gatewayLeaseGraceSeconds,
            (int)($serverConfig['gateway_lease_restart_reuse_grace'] ?? $this->gatewayLeaseRestartGraceSeconds)
        );
        $payload = GatewayLease::issueStartupLease($this->businessPort(), SERVER_ROLE, $this->gatewayLeaseTtlSeconds, [
            'gateway_pid' => getmypid(),
            'control_port' => $this->controlPort(),
            'rpc_port' => $this->rpcPort,
            'started_at' => $this->startedAt,
            'grace_seconds' => $this->gatewayLeaseGraceSeconds,
            'restart_grace_seconds' => $this->gatewayLeaseRestartGraceSeconds,
            'restart_reuse_grace' => $this->gatewayLeaseRestartReuseGraceSeconds,
        ]);
        $this->gatewayLeaseEpoch = (int)($payload['epoch'] ?? 0);
        $this->gatewayLeaseReusedEpoch = (bool)(($payload['meta']['reused_epoch'] ?? false) ?: false);
        $this->lastGatewayLeaseRenewAt = time();
        if ($this->gatewayLeaseEpoch <= 0) {
            throw new RuntimeException('gateway lease 初始化失败，owner epoch 无效');
        }
        if ($this->gatewayLeaseReusedEpoch) {
            Console::info("【Gateway】租约续接成功: epoch={$this->gatewayLeaseEpoch}, state=running(reused)");
            $this->reclaimStaleUpstreamProcessesByEpoch();
            return;
        }
        Console::info("【Gateway】租约初始化成功: epoch={$this->gatewayLeaseEpoch}, state=running");
        $this->reclaimStaleUpstreamProcessesByEpoch();
    }

    /**
     * 周期续租 gateway lease。
     *
     * @param string $state running/restarting/stopped。
     * @param bool $force 是否忽略最小续租间隔立即写入。
     * @return void
     */
    protected function renewGatewayLease(string $state = 'running', bool $force = false): void {
        if ($this->gatewayLeaseEpoch <= 0 || $this->businessPort() <= 0) {
            return;
        }
        $state = in_array($state, ['running', 'restarting', 'stopped'], true) ? $state : 'running';
        $now = time();
        $minInterval = self::GATEWAY_LEASE_RENEW_INTERVAL_SECONDS;
        if (!$force && $state === 'running' && ($now - $this->lastGatewayLeaseRenewAt) < $minInterval) {
            return;
        }
        $ttlSeconds = match ($state) {
            'restarting' => $this->gatewayLeaseRestartTtlSeconds,
            'stopped' => 0,
            default => $this->gatewayLeaseTtlSeconds,
        };
        $leaseMeta = [
            'gateway_pid' => $this->serverMasterPid > 0 ? $this->serverMasterPid : getmypid(),
            'control_port' => $this->controlPort(),
            'rpc_port' => $this->rpcPort,
            'started_at' => $this->startedAt,
            'grace_seconds' => $this->gatewayLeaseGraceSeconds,
            'restart_grace_seconds' => $this->gatewayLeaseRestartGraceSeconds,
        ];
        if ($state === 'restarting') {
            // 计划重启时显式写入 restart_started_at，供 upstream watcher 区分
            // “正常重启恢复窗口”与“owner 真正失联”。
            $leaseMeta['restart_started_at'] = $now;
            $leaseMeta['restart_reuse_grace'] = $this->gatewayLeaseRestartReuseGraceSeconds;
        }
        $ok = GatewayLease::renewLease(
            $this->businessPort(),
            SERVER_ROLE,
            $this->gatewayLeaseEpoch,
            $state,
            $ttlSeconds,
            $leaseMeta
        );
        if ($ok) {
            $this->lastGatewayLeaseRenewAt = $now;
            return;
        }
        Console::warning(
            "【Gateway】租约续租失败(可能被新epoch接管): epoch={$this->gatewayLeaseEpoch}, state={$state}"
        );
    }

    /**
     * 在 worker#0 启动 gateway lease 心跳定时器。
     *
     * @return void
     */
    protected function startGatewayLeaseRenewTimer(): void {
        if ($this->gatewayLeaseRenewTimerId !== null) {
            return;
        }
        $this->gatewayLeaseRenewTimerId = Timer::tick(self::GATEWAY_LEASE_RENEW_INTERVAL_SECONDS * 1000, function (int $timerId): void {
            $overrideState = (string)(Runtime::instance()->get('gateway_lease_override_state') ?? '');
            if ($overrideState !== '') {
                Timer::clear($timerId);
                $this->gatewayLeaseRenewTimerId = null;
                return;
            }
            if ($this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
                Timer::clear($timerId);
                $this->gatewayLeaseRenewTimerId = null;
                return;
            }
            $this->renewGatewayLease('running');
        });
    }

    /**
     * 停止 gateway lease 心跳定时器。
     *
     * @return void
     */
    protected function stopGatewayLeaseRenewTimer(): void {
        if ($this->gatewayLeaseRenewTimerId === null) {
            return;
        }
        Timer::clear($this->gatewayLeaseRenewTimerId);
        $this->gatewayLeaseRenewTimerId = null;
    }

    /**
     * 按当前关停语义落盘 lease 状态。
     *
     * 普通关停写 `stopped`；仅重启控制面时写 `restarting`（短 TTL）。
     *
     * @return void
     */
    protected function persistGatewayLeaseForShutdown(): void {
        $state = $this->preserveManagedUpstreamsOnShutdown ? 'restarting' : 'stopped';
        Runtime::instance()->set('gateway_lease_override_state', $state);
        $this->renewGatewayLease($state, true);
    }

    /**
     * 当前 gateway owner epoch。
     *
     * @return int
     */
    protected function gatewayLeaseEpoch(): int {
        return $this->gatewayLeaseEpoch;
    }

    /**
     * 启动阶段回收“同 gateway_port 下、但 owner_epoch 不匹配”的旧 upstream 进程。
     *
     * 这一步是 lease 自围栏之外的第二道保险：
     * - 当上一代控制面异常退出且某些 upstream watcher 没来得及执行 fence 时，
     *   新一代 gateway 启动会直接清理旧 epoch 残留；
     * - 当前 epoch（包括 restarting 复用 epoch）对应的 upstream 不会被误杀。
     *
     * @return void
     */
    protected function reclaimStaleUpstreamProcessesByEpoch(): void {
        $stale = $this->collectStaleUpstreamProcessesByEpoch();
        if (!$stale) {
            return;
        }

        $targets = array_map(static fn(array $item): int => (int)$item['pid'], $stale);
        $details = array_map(
            static fn(array $item): string => ((string)$item['pid']) . '(epoch=' . (string)$item['epoch'] . ')',
            $stale
        );
        Console::warning(
            '【Gateway】启动阶段回收旧代upstream进程: current_epoch=' . $this->gatewayLeaseEpoch
            . ', targets=' . implode(', ', $details)
        );

        foreach ($targets as $pid) {
            if ($pid > 0 && @Process::kill($pid, 0)) {
                @Process::kill($pid, SIGTERM);
            }
        }
        $remaining = $this->waitPidsExit($targets, 6) ? [] : $this->filterAlivePids($targets);
        if ($remaining) {
            Console::warning('【Gateway】旧代upstream进程未在预期时间内退出，升级为强制回收: pids=' . implode(', ', $remaining));
            foreach ($remaining as $pid) {
                if ($pid > 0 && @Process::kill($pid, 0)) {
                    @Process::kill($pid, SIGKILL);
                }
            }
            $this->waitPidsExit($remaining, 2);
        }
    }

    /**
     * 扫描并收集当前 gateway_port 下的旧代 upstream 进程。
     *
     * @return array<int, array{pid:int,epoch:int}>
     */
    protected function collectStaleUpstreamProcessesByEpoch(): array {
        if ($this->port <= 0 || $this->gatewayLeaseEpoch <= 0) {
            return [];
        }
        $output = @shell_exec('ps -axo pid=,command=');
        if (!is_string($output) || trim($output) === '') {
            return [];
        }

        $appFlag = '-app=' . APP_DIR_NAME;
        $gatewayPortFlag = '-gateway_port=' . $this->port;
        $stale = [];
        foreach (preg_split('/\r?\n/', $output) ?: [] as $line) {
            $line = trim((string)$line);
            if ($line === '' || !preg_match('/^(\d+)\s+(.+)$/', $line, $matches)) {
                continue;
            }
            $pid = (int)$matches[1];
            $command = (string)$matches[2];
            if ($pid <= 0 || $pid === getmypid()) {
                continue;
            }
            if (!str_contains($command, 'boot gateway_upstream start')) {
                continue;
            }
            if (!str_contains($command, $appFlag) || !str_contains($command, $gatewayPortFlag)) {
                continue;
            }
            $epoch = 0;
            if (preg_match('/(?:^|\s)-gateway_epoch=(\d+)(?:\s|$)/', $command, $epochMatch)) {
                $epoch = (int)($epochMatch[1] ?? 0);
            }
            if ($epoch <= 0 || $epoch !== $this->gatewayLeaseEpoch) {
                $stale[] = [
                    'pid' => $pid,
                    'epoch' => $epoch,
                ];
            }
        }

        usort($stale, static fn(array $a, array $b): int => ((int)$a['pid']) <=> ((int)$b['pid']));
        return $stale;
    }

    /**
     * 过滤仍存活的 PID。
     *
     * @param array<int, int> $pids
     * @return array<int, int>
     */
    protected function filterAlivePids(array $pids): array {
        $alive = [];
        foreach ($pids as $pid) {
            $pid = (int)$pid;
            if ($pid > 0 && @Process::kill($pid, 0)) {
                $alive[$pid] = $pid;
            }
        }
        ksort($alive);
        return array_values($alive);
    }

    /**
     * 等待一组 PID 退出。
     *
     * @param array<int, int> $pids
     * @param int $timeoutSeconds
     * @param int $intervalMs
     * @return bool
     */
    protected function waitPidsExit(array $pids, int $timeoutSeconds = 6, int $intervalMs = 200): bool {
        $targets = array_values(array_filter(array_map('intval', $pids), static fn(int $pid): bool => $pid > 0));
        if (!$targets) {
            return true;
        }
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            if (!$this->filterAlivePids($targets)) {
                return true;
            }
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(max(0.05, $intervalMs / 1000));
            } else {
                usleep(max(50, $intervalMs) * 1000);
            }
        }
        return !$this->filterAlivePids($targets);
    }

    /**
     * 判断当前是否仍处在 Gateway 启动摘要收集阶段。
     *
     * 启动早期各类子进程和结果态日志并不是同一个时机完成，所以这里用一个共享
     * Runtime 开关统一收口：未完成摘要前先写状态、不逐条打印；摘要打完后恢复
     * 正常的细粒度日志。
     *
     * @return bool
     */
    protected function startupSummaryPending(): bool {
        return (bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false);
    }

    /**
     * 记录一次启动阶段的 UpstreamSupervisor 同步结果。
     *
     * 这份数据会被 worker#0 的启动摘要直接消费，用来替代启动期那条独立的
     * “状态已同步”日志，避免摘要和单行日志重复描述同一个结果态。
     *
     * @param int $pid supervisor 进程 PID
     * @param int $instanceCount 本次同步的业务实例数量
     * @return void
     */
    protected function recordStartupUpstreamSupervisorSync(int $pid, int $instanceCount): void {
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES, [
            'pid' => $pid,
            'instances' => $instanceCount,
        ]);
    }

    /**
     * 合并记录启动阶段移除掉的旧业务代际。
     *
     * 启动早期可能会在多个分支上陆续清掉旧代，所以这里做增量合并，最终在
     * Gateway 启动摘要里一次性展示，而不是在终端里连续刷多条“代际已移除”。
     *
     * @param array<int, string> $generations 被移除的代际版本列表
     * @return void
     */
    protected function recordStartupRemovedGenerations(array $generations): void {
        $normalized = array_values(array_unique(array_filter(array_map(
            static fn(mixed $generation): string => trim((string)$generation),
            $generations
        ))));
        if (!$normalized) {
            return;
        }
        $existing = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS) ?: []);
        $merged = array_values(array_unique(array_merge(
            array_values(array_filter(array_map(
                static fn(mixed $generation): string => trim((string)$generation),
                (array)($existing['generations'] ?? [])
            ))),
            $normalized
        )));
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS, [
            'count' => count($merged),
            'generations' => $merged,
        ]);
    }

    /**
     * 记录启动阶段已经 ready 的业务实例。
     *
     * Gateway 启动时真正影响运维判断的是“最终拉起了哪个实例、监听在哪些端口”，
     * 而不是中间经历过多少次等待。这里把 ready 结果落到 Runtime，供启动摘要
     * 统一展示，避免 `启动业务实例/等待端口/端口就绪` 逐条刷屏。
     *
     * @param array<string, mixed> $plan 已 ready 的业务实例计划
     * @return void
     */
    protected function recordStartupReadyInstance(array $plan): void {
        $version = trim((string)($plan['version'] ?? ''));
        $host = trim((string)($plan['host'] ?? '127.0.0.1'));
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            return;
        }
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $descriptor = $version . ' ' . $host . ':' . $port . ($rpcPort > 0 ? ', RPC:' . $rpcPort : '');
        $existing = array_values(array_filter(array_map(
            static fn(mixed $item): string => trim((string)$item),
            (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES) ?: [])
        )));
        if (in_array($descriptor, $existing, true)) {
            return;
        }
        $existing[] = $descriptor;
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES, array_values($existing));
    }

    /**
     * 在 worker#0 上延迟输出 Gateway 启动摘要。
     *
     * 摘要不能放在 master onStart 里立即打印，因为那时子进程 PID、supervisor
     * 初始同步、旧代清理等结果态还没落到 Runtime。这里由 worker#0 轮询少量
     * 共享状态，等关键结果齐了或达到超时窗口后再统一打印。
     *
     * @return void
     */
    protected function scheduleStartupInfoRender(): void {
        if ((bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY) ?? false)) {
            return;
        }
        $scheduledAt = microtime(true);
        Timer::tick(200, function (int $timerId) use ($scheduledAt): void {
            if (!Runtime::instance()->serverIsAlive()) {
                Timer::clear($timerId);
                return;
            }
            if ((bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY) ?? false)) {
                Timer::clear($timerId);
                return;
            }

            $resultRows = $this->startupResultRows();
            if (!$this->shouldRenderStartupInfo($resultRows, microtime(true) - $scheduledAt)) {
                return;
            }

            $this->renderStartupInfo($resultRows);
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING, false);
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_READY, true);
            Timer::clear($timerId);
        });
    }

    /**
     * 判断启动摘要是否已经具备输出条件。
     *
     * 我们优先等待核心 gateway 子进程和 supervisor 初始同步结果都到齐；如果
     * 个别可选组件启动偏慢，则在超时后也要先把摘要打印出来，避免卡住整条启动链。
     *
     * @param array<int, array{section:string, label:string, value:string}> $resultRows 已收集到的结果行
     * @param float $elapsedSeconds 自调度开始后的等待秒数
     * @return bool
     */
    protected function shouldRenderStartupInfo(array $resultRows, float $elapsedSeconds): bool {
        $requiredKeys = [
            Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID,
            Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID,
            Key::RUNTIME_MEMORY_MONITOR_PID,
            Key::RUNTIME_HEARTBEAT_PID,
            Key::RUNTIME_LOG_BACKUP_PID,
        ];
        foreach ($requiredKeys as $key) {
            if ((int)(Runtime::instance()->get($key) ?? 0) <= 0) {
                return $elapsedSeconds >= 5.0;
            }
        }

        $supervisorSync = Runtime::instance()->get(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES);
        if ($supervisorSync === false || $supervisorSync === null) {
            return $elapsedSeconds >= 5.0;
        }

        if ($this->managedUpstreamPlans) {
            $readyInstances = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES) ?: []);
            if (!$readyInstances) {
                return $elapsedSeconds >= 5.0;
            }
        }

        if ((int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0) > 0
            && (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_WORKER_PID) ?? 0) <= 0
        ) {
            return $elapsedSeconds >= 5.0;
        }

        return !empty($resultRows);
    }

    /**
     * 收集 Gateway 启动摘要里的“结果态”行。
     *
     * 这里只读取启动阶段已经写入 Runtime 的最终态数据，不再重新推导业务逻辑，
     * 避免 worker#0 和业务编排子进程对同一份状态机各算各的。
     *
     * @return array<int, array{label:string, value:string}>
     */
    protected function startupResultRows(): array {
        $pidRows = [
            '业务编排' => (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID) ?? 0),
            '健康检查' => (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID) ?? 0),
            '内存监控' => (int)(Runtime::instance()->get(Key::RUNTIME_MEMORY_MONITOR_PID) ?? 0),
            '心跳进程' => (int)(Runtime::instance()->get(Key::RUNTIME_HEARTBEAT_PID) ?? 0),
            '日志备份' => (int)(Runtime::instance()->get(Key::RUNTIME_LOG_BACKUP_PID) ?? 0),
            '队列管理' => (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0),
            '队列执行' => (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_WORKER_PID) ?? 0),
            '文件监听' => (int)(Runtime::instance()->get(Key::RUNTIME_FILE_WATCHER_PID) ?? 0),
            '集群协调' => (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID) ?? 0),
        ];
        $rows = [];
        foreach ($pidRows as $label => $pid) {
            if ($pid > 0) {
                $rows[] = ['label' => $label, 'value' => 'PID:' . $pid];
            }
        }

        $readyInstances = array_values(array_filter(array_map(
            static fn(mixed $item): string => trim((string)$item),
            (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_READY_INSTANCES) ?: [])
        )));
        foreach ($readyInstances as $index => $descriptor) {
            $rows[] = [
                'label' => '业务实例' . ($index + 1),
                'value' => $descriptor,
            ];
        }

        $supervisorSync = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_UPSTREAM_SUPERVISOR_SYNC_INSTANCES) ?: []);
        if ($supervisorSync) {
            $rows[] = [
                'label' => 'Supervisor',
                'value' => 'Supervisor PID:' . (int)($supervisorSync['pid'] ?? 0) . ', 实例:' . (int)($supervisorSync['instances'] ?? 0),
            ];
        }

        $removedGenerations = (array)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_LAST_REMOVED_GENERATIONS) ?: []);
        $generationList = array_values(array_filter(array_map(
            static fn(mixed $generation): string => trim((string)$generation),
            (array)($removedGenerations['generations'] ?? [])
        )));
        if ($generationList) {
            $rows[] = [
                'label' => '代际清理',
                'value' => 'count=' . count($generationList) . ', versions=' . implode(' | ', $generationList),
            ];
        }

        return $rows;
    }

    /**
     * 把摘要结果行格式化成稳定的双列表现。
     *
     * 启动结果需要和上半区基础信息保持同一种阅读节奏，所以这里只做对齐，
     * 不再插入分组标题和空行，避免日志被割裂成多段。
     *
     * @param array<int, array{label:string, value:string}> $rows 原始结果行
     * @return array<int, string>
     */
    protected function formatStartupResultRows(array $rows): array {
        return $this->formatAlignedPairs($rows);
    }

    /**
     * 把一组标签/value 行格式化成与启动摘要上半区一致的“键：值”样式。
     *
     * @param array<int, array{label:string, value:string}> $rows
     * @return array<int, string>
     */
    protected function formatAlignedPairs(array $rows, ?int $labelWidth = null): array {
        unset($labelWidth);
        $formatted = [];
        foreach ($rows as $row) {
            $label = trim((string)($row['label'] ?? ''));
            $value = trim((string)($row['value'] ?? ''));
            if ($label === '' || $value === '') {
                continue;
            }
            $formatted[] = $label . '：' . $value;
        }
        return $formatted;
    }

    /**
     * 计算一段文本在终端中的近似显示宽度。
     *
     * 启动摘要里会混用中文和 ASCII，直接用 strlen 做补位会明显错位。这里按
     * “ASCII 算 1，非 ASCII 算 2”的终端常见宽度规则做一个轻量估算即可。
     *
     * @param string $text
     * @return int
     */
    protected function displayTextWidth(string $text): int {
        if ($text === '') {
            return 0;
        }
        $width = 0;
        foreach (preg_split('//u', $text, -1, PREG_SPLIT_NO_EMPTY) ?: [] as $char) {
            $width += strlen($char) === 1 ? 1 : 2;
        }
        return $width;
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
            'gateway.command' => $this->dispatchInternalGatewayCommand(
                (string)($payload['command'] ?? ''),
                (array)($payload['params'] ?? [])
            ),
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
        $summary = [
            'loopback' => 0,
            'dashboard' => 0,
            'node' => 0,
            'business' => 0,
            'control' => 0,
            'rpc' => 0,
            'other_port' => 0,
        ];
        while (true) {
            $clients = $server->getClientList($startFd, 100);
            if (!$clients) {
                break;
            }
            foreach ($clients as $fd) {
                $fd = (int)$fd;
                $startFd = max($startFd, $fd);
                $isDashboardClient = isset($this->dashboardClients[$fd]);
                $isNodeClient = isset($this->nodeClients[$fd]);
                $clientInfo = $server->getClientInfo($fd) ?: [];
                $remoteIp = (string)($clientInfo['remote_ip'] ?? '');
                $serverPort = (int)($clientInfo['server_port'] ?? 0);
                if (in_array($remoteIp, ['127.0.0.1', '::1'], true)) {
                    $summary['loopback']++;
                }
                if ($isDashboardClient) {
                    $summary['dashboard']++;
                }
                if ($isNodeClient) {
                    $summary['node']++;
                }
                if ($serverPort === $this->businessPort()) {
                    $summary['business']++;
                } elseif ($serverPort === $this->controlPort()) {
                    $summary['control']++;
                } elseif ($this->rpcPort > 0 && $serverPort === $this->rpcPort) {
                    $summary['rpc']++;
                } else {
                    $summary['other_port']++;
                }
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
        $this->lastDisconnectedClientSummary = $summary;
        return $disconnected;
    }

    /**
     * 打印一次连接释放明细，帮助定位“已断开 N 个客户端连接”的来源。
     *
     * @param int $total 本轮实际断开连接数
     * @return void
     */
    protected function logDisconnectedClientSummary(int $total): void {
        $summary = (array)$this->lastDisconnectedClientSummary;
        if ($total <= 0 || !$summary) {
            return;
        }
        Console::info(
            "【Gateway】连接释放明细: total={$total}, loopback=" . (int)($summary['loopback'] ?? 0)
            . ", dashboard=" . (int)($summary['dashboard'] ?? 0)
            . ", node=" . (int)($summary['node'] ?? 0)
            . ", business=" . (int)($summary['business'] ?? 0)
            . ", control=" . (int)($summary['control'] ?? 0)
            . ", rpc=" . (int)($summary['rpc'] ?? 0)
            . ", other=" . (int)($summary['other_port'] ?? 0)
        );
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
        // localhost 节点统一走 buildGatewayNode，让 1s dashboard 推送可以叠加
        // 实时 upstream 指标；否则会被 5s heartbeat 快照节奏锁住。
        $nodes[] = $this->buildGatewayNode($upstreams);

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
    public function dashboardServerStatus(string $token, string $host = '', string $referer = '', string $forwardedProto = '', bool $forceRefreshVersions = false): array {
        $upstreams = $this->dashboardUpstreams();
        $status = $this->buildDashboardRealtimeStatus($upstreams);
        $status['socket_host'] = $this->buildDashboardSocketHost($host, $referer, $forwardedProto) . '?token=' . rawurlencode($token);
        $status['latest_version'] = App::latestVersion($forceRefreshVersions);
        $status['dashboard'] = $this->buildDashboardVersionStatus($forceRefreshVersions);
        $status['framework'] = $this->buildFrameworkVersionStatus($upstreams, $forceRefreshVersions);
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
        $execution = $this->resolveGatewayControlCommandExecution($command, $params);
        $afterWrite = $execution['after_write'] ?? null;
        if (is_callable($afterWrite)) {
            $afterWrite();
        }
        return $execution['result'];
    }

    /**
     * 统一解析 gateway 控制命令的执行语义。
     *
     * dashboard、本地 IPC、gateway 内部 HTTP 命令都在做同一组控制动作：
     * 只是在“立即执行”还是“写完响应后再执行”、以及返回体格式上存在差异。
     * 这里先把真正的命令语义收敛成一处，再由不同入口做各自适配。
     *
     * @param string $command 控制命令名
     * @param array<string, mixed> $params 附带参数
     * @return array{
     *     result: Result,
     *     internal_error_status?: int,
     *     internal_success_status?: int,
     *     internal_message?: string,
     *     after_write?: callable|null
     * }
     */
    protected function resolveGatewayControlCommandExecution(string $command, array $params = []): array {
        $command = trim($command);
        if ($command === '') {
            return [
                'result' => Result::error('命令不能为空'),
                'internal_error_status' => 400,
            ];
        }

        switch ($command) {
            case 'reload':
                return [
                    'result' => $this->dispatchGatewayBusinessCommand('reload', [], false, 0, '业务实例与业务子进程已开始重启'),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'reload_gateway':
                $reservation = $this->reserveGatewayReload();
                if (!$reservation['accepted']) {
                    return [
                        'result' => Result::error($reservation['message']),
                        'internal_error_status' => 409,
                    ];
                }
                return [
                    'result' => Result::success($reservation['message']),
                    'internal_success_status' => 200,
                    'internal_message' => $reservation['message'],
                    'after_write' => $reservation['scheduled']
                        ? function () use ($reservation): void {
                            $this->scheduleReservedGatewayReload((bool)$reservation['restart_managed_upstreams']);
                        }
                        : null,
                ];
            case 'restart_redisqueue':
                if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
                    return [
                        'result' => Result::error('RedisQueue 子进程未启用'),
                        'internal_error_status' => 409,
                    ];
                }
                $restart = function (): void {
                    $this->restartGatewayRedisQueueProcess();
                };
                return [
                    'result' => Result::success('RedisQueue 子进程已开始重启'),
                    'internal_success_status' => 200,
                    'internal_message' => 'redisqueue process restart started',
                    'after_write' => $restart,
                ];
            case 'subprocess_restart':
                if (!$this->subProcessManager) {
                    return [
                        'result' => Result::error('SubProcessManager 未初始化'),
                        'internal_error_status' => 409,
                    ];
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->restartManagedProcesses($target === '' ? [] : [$target]);
                return [
                    'result' => $result['ok'] ? Result::success($result['message']) : Result::error($result['message']),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'subprocess_stop':
                if (!$this->subProcessManager) {
                    return [
                        'result' => Result::error('SubProcessManager 未初始化'),
                        'internal_error_status' => 409,
                    ];
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->stopManagedProcesses($target === '' ? [] : [$target]);
                return [
                    'result' => $result['ok'] ? Result::success($result['message']) : Result::error($result['message']),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'subprocess_restart_all':
                if (!$this->subProcessManager) {
                    return [
                        'result' => Result::error('SubProcessManager 未初始化'),
                        'internal_error_status' => 409,
                    ];
                }
                $result = $this->subProcessManager->restartAllManagedProcesses();
                return [
                    'result' => $result['ok'] ? Result::success($result['message']) : Result::error($result['message']),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'restart':
                if ($this->gatewayShutdownScheduled) {
                    return [
                        'result' => Result::error('Gateway 已进入关闭流程，无法再发起重启'),
                        'internal_error_status' => 409,
                    ];
                }
                // restart 默认走“先回收业务实例，再重启 gateway 控制面”。
                // 仅当显式传入 preserve_managed_upstreams=true 时才保留 upstream。
                $preserveManagedUpstreams = (bool)($params['preserve_managed_upstreams'] ?? false);
                return [
                    'result' => Result::success($preserveManagedUpstreams ? 'Gateway 控制面已开始重启' : 'Gateway 与业务实例已开始重启'),
                    'internal_success_status' => 200,
                    'internal_message' => $preserveManagedUpstreams ? 'gateway control-plane restart started' : 'gateway full restart started',
                    'after_write' => function () use ($preserveManagedUpstreams): void {
                        $this->scheduleGatewayShutdown($preserveManagedUpstreams);
                    },
                ];
            case 'shutdown':
                return [
                    'result' => Result::success('Gateway 已开始关闭'),
                    'internal_success_status' => 200,
                    'internal_message' => 'gateway shutdown started',
                    'after_write' => function (): void {
                        $this->scheduleGatewayShutdown();
                    },
                ];
            case 'appoint_update':
                return [
                    'result' => $this->dashboardUpdate((string)($params['type'] ?? ''), (string)($params['version'] ?? '')),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'appoint_update_remote':
                return [
                    'result' => $this->executeRemoteSlaveAppointUpdate(
                        (string)($params['task_id'] ?? ''),
                        (string)($params['type'] ?? ''),
                        (string)($params['version'] ?? '')
                    ),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            default:
                return [
                    'result' => Result::error('暂不支持的命令:' . $command),
                    'internal_error_status' => 400,
                ];
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
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(0.1);
            } else {
                usleep(100000);
            }
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
        if ($this->gatewayLeaseEpoch() <= 0) {
            return Result::error('Gateway owner epoch 未初始化');
        }
        $payload = array_merge($params, [
            'action' => $action,
            'owner_epoch' => $this->gatewayLeaseEpoch(),
        ]);
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
        Console::info("【Gateway】等待业务编排结果: request_id={$requestId}, timeout={$timeoutSeconds}s");
        while (microtime(true) < $deadline) {
            $payload = Runtime::instance()->get($resultKey);
            if (is_array($payload) && array_key_exists('ok', $payload)) {
                Runtime::instance()->delete($resultKey);
                $message = (string)($payload['message'] ?? ($payload['ok'] ? 'success' : 'failed'));
                $data = (array)($payload['data'] ?? []);
                Console::info("【Gateway】收到业务编排结果: request_id={$requestId}, ok=" . (!empty($payload['ok']) ? 'yes' : 'no') . ", message={$message}");
                return !empty($payload['ok'])
                    ? Result::success($data ?: $message)
                    : Result::error($message, 'SERVICE_ERROR', $data);
            }
            // 这条等待链会被 dashboard/internal HTTP 协程直接调用，必须使用协程友好休眠，
            // 否则在 Swoole worker 内会触发 “all coroutines are asleep - deadlock”。
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(0.1);
            } else {
                usleep(100000);
            }
        }
        Runtime::instance()->delete($resultKey);
        Console::warning("【Gateway】业务编排结果等待超时: request_id={$requestId}, timeout={$timeoutSeconds}s");
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
        // reload 是纯异步命令，入口（dashboard/filewatch/local ipc）只需要“可达且可执行”保证。
        // 这里统一走 Runtime 信号通道，让业务编排子进程在自身 tick 中消费并执行，
        // 避免依赖 worker->manager->child 的瞬时 pipe 写入结果导致“投递成功但未执行”。
        if (!$waitForResult && $command === 'reload') {
            $signal = [
                'token' => uniqid('gateway_reload_', true),
                'queued_at' => time(),
            ];
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK, $signal);
            Console::info(
                "【Gateway】投递业务编排命令: command={$command}, wait=no, mode=runtime_signal, token="
                . (string)($signal['token'] ?? '')
            );
            return Result::success($acceptedMessage ?: 'accepted');
        }
        $requestId = '';
        if ($waitForResult) {
            $requestId = uniqid('gateway_business_', true);
            Runtime::instance()->delete($this->gatewayBusinessCommandResultKey($requestId));
            $params['request_id'] = $requestId;
            Console::info("【Gateway】投递业务编排命令: command={$command}, request_id={$requestId}, wait=yes, timeout={$timeoutSeconds}s");
        } else {
            Console::info("【Gateway】投递业务编排命令: command={$command}, wait=no");
        }
        if (!$this->subProcessManager->sendCommand($command, $params, ['GatewayBusinessCoordinator'])) {
            return Result::error('Gateway 业务编排命令投递失败');
        }
        if (!$waitForResult) {
            return Result::success($acceptedMessage ?: 'accepted');
        }
        return $this->waitForGatewayBusinessCommandResult($requestId, $timeoutSeconds);
    }

    /**
     * 异步投递一条 gateway 业务编排命令，并返回共享内存 request_id。
     *
     * 升级类命令会在后台继续推进本地包替换、rolling upstream、节点汇总与重启收口，
     * HTTP 入口只负责接受任务，不能再同步把整条长链绑定在请求超时上。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return string|false 投递成功时返回 request_id，失败返回 false
     */
    protected function dispatchGatewayBusinessCommandAsync(string $command, array $params = []): string|false {
        if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('GatewayBusinessCoordinator')) {
            return false;
        }
        $requestId = uniqid('gateway_business_', true);
        Runtime::instance()->delete($this->gatewayBusinessCommandResultKey($requestId));
        $params['request_id'] = $requestId;
        $token = $this->enqueueGatewayBusinessRuntimeCommand($command, $params);
        if ($token === false) {
            Runtime::instance()->delete($this->gatewayBusinessCommandResultKey($requestId));
            Console::error("【Gateway】异步投递业务编排命令失败: command={$command}, request_id={$requestId}, reason=runtime_queue_write_failed");
            return false;
        }
        Console::info("【Gateway】异步投递业务编排命令: command={$command}, request_id={$requestId}, mode=runtime_queue, token={$token}");
        return $requestId;
    }

    /**
     * 将业务编排命令写入 Runtime 队列，交由 GatewayBusinessCoordinator 消费。
     *
     * 业务编排命令是“高价值但低频”控制指令（reload/appoint_update），
     * 不能再依赖 worker->manager->child 的瞬时 pipe 写入布尔值来判断是否送达。
     * 这里改为先落共享内存队列，再由业务编排子进程轮询消费，实现“可恢复投递”。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return string|false 本次队列投递 token，失败返回 false
     */
    protected function enqueueGatewayBusinessRuntimeCommand(string $command, array $params = []): string|false {
        $token = uniqid('gateway_business_runtime_', true);
        $queue = Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE);
        $items = (is_array($queue) && is_array($queue['items'] ?? null)) ? (array)$queue['items'] : [];
        $items[] = [
            'token' => $token,
            'command' => $command,
            'params' => $params,
            'queued_at' => time(),
        ];
        // 控制面命令是低频事件，队列保留最近 32 条足够覆盖并发场景，
        // 同时避免 Runtime 表值无限膨胀。
        if (count($items) > 32) {
            $items = array_slice($items, -32);
        }
        $written = Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE, [
            'items' => array_values($items),
            'updated_at' => time(),
        ]);
        if (!$written) {
            Console::error(
                "【Gateway】业务编排命令入队失败: command={$command}, token={$token}, runtime_count="
                . Runtime::instance()->count() . ", runtime_size=" . Runtime::instance()->size() . ", queued_items=" . count($items)
            );
            return false;
        }
        return $token;
    }

    /**
     * 消费 worker 侧写入的 reload 保底信号。
     *
     * 当 worker -> GatewayBusinessCoordinator 的 pipe 命令瞬时不可投递时，
     * worker 会把一次 reload 请求写入 Runtime。业务编排子进程在 tick
     * 中读取并执行，避免 dashboard / filewatch 的 reload 被静默丢失。
     *
     * @return bool
     */
    protected function consumeFallbackBusinessReloadSignal(): bool {
        $signal = Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK);
        if (!is_array($signal)) {
            return false;
        }
        $token = trim((string)($signal['token'] ?? ''));
        if ($token === '') {
            Runtime::instance()->delete(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK);
            return false;
        }
        if ($token === $this->lastFallbackBusinessReloadToken) {
            return false;
        }
        $this->lastFallbackBusinessReloadToken = $token;
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK);
        return true;
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
        if ($type === 'framework' && !FRAMEWORK_IS_PHAR) {
            return Result::error('当前为源码模式,框架在线升级不可用');
        }
        if ($type === 'framework' && function_exists('scf_framework_update_ready') && scf_framework_update_ready()) {
            return Result::error('正在等待升级,请重启服务器');
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

        $this->emitLocalNodeUpdateState($taskId, $type, $version, 'running', "【" . SERVER_HOST . "】开始更新 {$type} => {$version}");
        $requestId = $this->dispatchGatewayBusinessCommandAsync('appoint_update', [
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
        ]);
        if ($requestId === false) {
            $error = 'Gateway 业务编排子进程未启用';
            $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
            Console::error("【Gateway】升级失败: type={$type}, version={$version}, error={$error}");
            return Result::error($error);
        }

        Coroutine::create(function () use ($requestId, $taskId, $type, $version, $slaveHosts): void {
            $localResult = $this->waitForGatewayBusinessCommandResult($requestId, self::DASHBOARD_UPDATE_LOCAL_TIMEOUT_SECONDS);
            $localData = (array)($localResult->getData() ?: []);
            if ($localResult->hasError() && !$localData) {
                $error = (string)($localResult->getMessage() ?: '更新失败');
                $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
                $this->emitLocalNodeUpdateState($taskId, $type, $version, 'failed', "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}", $error);
                Console::error("【Gateway】升级失败: type={$type}, version={$version}, error={$error}");
                return;
            }

            $restartSummary = (array)($localData['restart_summary'] ?? [
                'success_count' => 0,
                'failed_nodes' => [],
            ]);
            $upstreamRollout = (array)($localData['upstream_rollout'] ?? [
                'attempted' => $type !== 'public',
                'success' => count((array)($restartSummary['failed_nodes'] ?? [])) === 0,
                'success_count' => (int)($restartSummary['success_count'] ?? 0),
                'failed_count' => count((array)($restartSummary['failed_nodes'] ?? [])),
                'failed_nodes' => (array)($restartSummary['failed_nodes'] ?? []),
            ]);
            $upstreamRollout['failed_nodes'] = array_values((array)($upstreamRollout['failed_nodes'] ?? []));
            $upstreamRollout['failed_count'] = (int)($upstreamRollout['failed_count'] ?? count($upstreamRollout['failed_nodes']));
            $upstreamRollout['success_count'] = (int)($upstreamRollout['success_count'] ?? 0);
            $upstreamRollout['attempted'] = (bool)($upstreamRollout['attempted'] ?? ($type !== 'public'));
            $upstreamRollout['success'] = (bool)($upstreamRollout['success'] ?? ($upstreamRollout['failed_count'] === 0));
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
            $gatewayPendingRestart = (bool)($localData['gateway_pending_restart'] ?? ($type === 'framework' && $masterState === 'pending'));

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
                'upstream_rollout' => $upstreamRollout,
                'gateway_pending_restart' => $gatewayPendingRestart,
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
                $error = $masterError !== '' ? $masterError : '部分节点升级失败';
                $this->emitLocalNodeUpdateState(
                    $taskId,
                    $type,
                    $version,
                    'failed',
                    "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}",
                    $error,
                    [
                        'upstream_rollout' => $upstreamRollout,
                        'gateway_pending_restart' => $gatewayPendingRestart,
                    ]
                );
                return;
            }

            if ($masterState === 'pending') {
                $this->emitLocalNodeUpdateState(
                    $taskId,
                    $type,
                    $version,
                    'pending',
                    "【" . SERVER_HOST . "】业务实例升级结果:成功{$upstreamRollout['success_count']},失败{$upstreamRollout['failed_count']}，Gateway等待重启生效:{$type} => {$version}",
                    '',
                    [
                        'upstream_rollout' => $upstreamRollout,
                        'gateway_pending_restart' => $gatewayPendingRestart,
                    ]
                );
                return;
            }

            $this->emitLocalNodeUpdateState(
                $taskId,
                $type,
                $version,
                'success',
                "【" . SERVER_HOST . "】版本更新成功:{$type} => {$version}",
                '',
                [
                    'upstream_rollout' => $upstreamRollout,
                    'gateway_pending_restart' => $gatewayPendingRestart,
                ]
            );
        });

        $this->logUpdateStage($taskId, $type, $version, 'accepted', [
            'slaves' => count($slaveHosts),
            'request_id' => $requestId,
        ]);
        return Result::success([
            'accepted' => true,
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
            'message' => '升级任务已开始，请留意节点状态',
            'slave_count' => count($slaveHosts),
        ]);
    }

    /**
     * 向 dashboard 广播本机 master 的升级状态。
     *
     * slave 节点是通过 websocket 把 `node_update_state` 回传给 master；
     * 本机 master 不会经过这条链，因此升级入口改成异步后，需要在本机
     * 主动补发同结构事件，保证 dashboard 仍能看到 running/success/failed/pending。
     *
     * @param string $taskId 升级任务 id
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @param string $state running/success/failed/pending
     * @param string $message 展示给 dashboard 的消息
     * @param string $error 失败时的错误文案
     * @param array $extra 追加回传给 dashboard 的扩展字段
     * @return void
     */
    protected function emitLocalNodeUpdateState(string $taskId, string $type, string $version, string $state, string $message, string $error = '', array $extra = []): void {
        $payload = [
            'task_id' => $taskId,
            'host' => APP_NODE_ID,
            'type' => $type,
            'version' => $version,
            'state' => $state,
            'message' => $message,
            'error' => $error,
            'updated_at' => time(),
        ];
        if ($extra) {
            $payload = array_merge($payload, $extra);
        }
        Runtime::instance()->set($this->nodeUpdateTaskStateKey($taskId, APP_NODE_ID), $payload);
        Manager::instance()->sendMessageToAllDashboardClients(JsonHelper::toJson([
            'event' => 'node_update_state',
            'data' => $payload,
        ]));
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
        Console::info("【Gateway】开始执行本地升级: task={$taskId}, type={$type}, version={$version}");
        if (!App::appointUpdateTo($type, $version, false)) {
            $error = App::getLastUpdateError() ?: '更新失败';
            Console::warning("【Gateway】本地升级失败: task={$taskId}, type={$type}, version={$version}, error={$error}");
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
        Console::info("【Gateway】本地包应用完成: task={$taskId}, type={$type}, version={$version}");

        $restartSummary = [
            'success_count' => 0,
            'failed_nodes' => [],
        ];
        if ($type !== 'public') {
            $taskId !== '' && $this->logUpdateStage($taskId, $type, $version, 'rolling_upstreams');
            Console::info("【Gateway】开始滚动升级业务实例: task={$taskId}, type={$type}, version={$version}");
            $restartSummary = $this->rollingUpdateManagedUpstreams($type, $version);
            Console::info("【Gateway】滚动升级业务实例结束: task={$taskId}, success=" . (int)($restartSummary['success_count'] ?? 0) . ", failed=" . count((array)($restartSummary['failed_nodes'] ?? [])));
        }
        if ($type !== 'public' && $restartSummary['success_count'] > 0 && !$restartSummary['failed_nodes']) {
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        }
        // framework 升级后 gateway 控制面只进入 pending restart，
        // 不在这一步迭代 gateway 托管子进程，避免控制面过早命中新框架包。
        $iterateBusinessProcesses = $type === 'app' && !$restartSummary['failed_nodes'];

        $master = [
            'host' => SERVER_HOST,
            'state' => $type === 'framework' ? 'pending' : ($restartSummary['failed_nodes'] ? 'failed' : 'success'),
            'error' => $type === 'framework'
                ? 'Gateway 需重启后才会加载新框架版本'
                : ($restartSummary['failed_nodes'] ? '部分业务实例升级失败' : ''),
        ];
        $upstreamFailedNodes = array_values((array)($restartSummary['failed_nodes'] ?? []));
        $upstreamRollout = [
            'attempted' => $type !== 'public',
            'success' => $type === 'public' ? true : count($upstreamFailedNodes) === 0,
            'success_count' => (int)($restartSummary['success_count'] ?? 0),
            'failed_count' => count($upstreamFailedNodes),
            'failed_nodes' => $upstreamFailedNodes,
        ];
        $payload = [
            'restart_summary' => $restartSummary,
            'master' => $master,
            'iterate_business_processes' => $iterateBusinessProcesses,
            'upstream_rollout' => $upstreamRollout,
            'gateway_pending_restart' => $type === 'framework',
        ];
        if ($restartSummary['failed_nodes']) {
            return Result::error('部分业务实例升级失败', 'SERVICE_ERROR', $payload);
        }
        Console::success("【Gateway】本地升级执行完成: task={$taskId}, type={$type}, version={$version}, state={$master['state']}");
        return Result::success($payload);
    }

    /**
     * 在 slave 本机 worker 上同步执行一轮“来自 master 的远端升级”。
     *
     * slave 收到 cluster 命令后，不能再由 cluster 协调进程自己直接碰
     * GatewayBusinessCoordinator 的兄弟进程 pipe。这里改为回到 worker，
     * 由 worker 复用与 master 本地节点相同的业务编排等待链：
     * 1. 投递 appoint_update 到业务编排子进程；
     * 2. 同步等待本地升级结果；
     * 3. 如需迭代 gateway 业务子进程，再由 worker 统一发起。
     *
     * @param string $taskId 集群升级任务 id
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @return Result
     */
    protected function executeRemoteSlaveAppointUpdate(string $taskId, string $type, string $version): Result {
        $type = trim($type);
        $version = trim($version);
        if ($type === '' || $version === '') {
            return Result::error('更新类型和版本号不能为空');
        }

        Console::info("【Gateway】开始执行远端升级: task={$taskId}, type={$type}, version={$version}");
        $requestId = $this->dispatchGatewayBusinessCommandAsync('appoint_update', [
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
        ]);
        if ($requestId === false) {
            return Result::error('Gateway 业务编排子进程未启用');
        }

        $localResult = $this->waitForGatewayBusinessCommandResult($requestId, self::DASHBOARD_UPDATE_LOCAL_TIMEOUT_SECONDS);
        $localData = (array)($localResult->getData() ?: []);
        if (!empty($localData['iterate_business_processes']) && !$localResult->hasError()) {
            $this->iterateGatewayBusinessProcesses();
        }
        return $localResult;
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
            if ($this->consumeFallbackBusinessReloadSignal()) {
                Console::info('【Gateway】检测到reload运行信号，开始执行业务平面重载');
                $this->restartGatewayBusinessPlane(false);
            }
        }
        $this->observeUpstreamSupervisorProcess();
        $this->syncUpstreamSupervisorState();
        $this->refreshManagedUpstreamRuntimeStates();
        $this->instanceManager->tick();
        $this->pollPendingManagedRecycles();
        $this->drainQuarantinedManagedRecycles();
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
        $this->gatewayHealthTraceActive = true;
        $this->beginGatewayHealthTraceRound();
        try {
            $this->maintainManagedUpstreamHealth();
        } finally {
            $this->finishGatewayHealthTraceRound();
            $this->gatewayHealthTraceActive = false;
        }
    }

    /**
     * 记录 Gateway 健康检查链路步骤耗时到“单轮缓冲池”。
     *
     * 默认不直接打印，只有当 SubProcessManager 检测到 GatewayHealthMonitor
     * 心跳超时时，才会读取该缓冲快照并一次性输出整轮明细。
     *
     * @param string $step 步骤名
     * @param float $startedAt 步骤开始时间（microtime(true)）
     * @param array<string, scalar|null> $context 追加上下文
     * @return void
     */
    protected function traceGatewayHealthStep(string $step, float $startedAt, array $context = []): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED || !$this->gatewayHealthTraceActive) {
            return;
        }
        $elapsedMs = round((microtime(true) - $startedAt) * 1000, 2);
        $stepLabel = $this->gatewayHealthStepLabel($step);
        $parts = [];
        foreach ($context as $key => $value) {
            if (is_scalar($value) || $value === null) {
                $parts[] = $key . '=' . (is_null($value) ? 'null' : (string)$value);
            }
        }
        $suffix = $parts ? (', ' . implode(', ', $parts)) : '';
        $this->appendGatewayHealthTraceEntry("{$stepLabel}, step={$step}, cost={$elapsedMs}ms{$suffix}");
    }

    /**
     * 开始一轮 GatewayHealthMonitor 探测缓冲。
     *
     * @return void
     */
    protected function beginGatewayHealthTraceRound(): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $now = microtime(true);
        $this->gatewayHealthTraceRoundStartedAt = $now;
        $this->gatewayHealthTraceRoundUpdatedAt = $now;
        $this->gatewayHealthTraceRoundSequence++;
        $this->gatewayHealthTraceRoundBuffer = [];
        $this->persistGatewayHealthTraceSnapshot(true);
    }

    /**
     * 结束一轮 GatewayHealthMonitor 探测缓冲。
     *
     * @return void
     */
    protected function finishGatewayHealthTraceRound(): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $this->gatewayHealthTraceRoundUpdatedAt = microtime(true);
        $this->persistGatewayHealthTraceSnapshot(false);
    }

    /**
     * 追加单条步骤日志到当前轮次缓冲并同步快照。
     *
     * @param string $entry
     * @return void
     */
    protected function appendGatewayHealthTraceEntry(string $entry): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $this->gatewayHealthTraceRoundUpdatedAt = microtime(true);
        $this->gatewayHealthTraceRoundBuffer[] = $entry;
        if (count($this->gatewayHealthTraceRoundBuffer) > self::GATEWAY_HEALTH_TRACE_BUFFER_MAX_LINES) {
            $this->gatewayHealthTraceRoundBuffer = array_slice($this->gatewayHealthTraceRoundBuffer, -self::GATEWAY_HEALTH_TRACE_BUFFER_MAX_LINES);
        }
        $this->persistGatewayHealthTraceSnapshot(true);
    }

    /**
     * 将当前轮次缓冲快照写入 Runtime，供超时侧读取。
     *
     * @param bool $activeRound true 表示轮次仍在执行；false 表示轮次已结束
     * @return void
     */
    protected function persistGatewayHealthTraceSnapshot(bool $activeRound): void {
        if (!self::TEMP_HEARTBEAT_TRACE_ENABLED) {
            return;
        }
        $roundStartedAt = $this->gatewayHealthTraceRoundStartedAt > 0 ? $this->gatewayHealthTraceRoundStartedAt : microtime(true);
        $updatedAt = $this->gatewayHealthTraceRoundUpdatedAt > 0 ? $this->gatewayHealthTraceRoundUpdatedAt : microtime(true);
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_TRACE_SNAPSHOT, [
            'pid' => (int)(getmypid() ?: 0),
            'active' => $activeRound ? 1 : 0,
            'round_sequence' => $this->gatewayHealthTraceRoundSequence,
            'round_started_at' => round($roundStartedAt, 6),
            'updated_at' => round($updatedAt, 6),
            'round_elapsed_ms' => round(max(0, ($updatedAt - $roundStartedAt) * 1000), 2),
            'entries' => $this->gatewayHealthTraceRoundBuffer,
        ]);
    }

    /**
     * 将健康探测步骤 key 转为中文描述，便于直接从日志读出执行动作。
     *
     * @param string $step 健康探测步骤 key
     * @return string
     */
    protected function gatewayHealthStepLabel(string $step): string {
        $map = [
            'maintain.health.guard_flags' => '维护链路-前置标志检查',
            'maintain.health.guard_runtime' => '维护链路-运行模式检查',
            'maintain.health.guard_rolling_cooldown' => '维护链路-滚动发布冷却检查',
            'maintain.health.resolve_interval' => '维护链路-解析探测间隔',
            'maintain.health.guard_interval' => '维护链路-探测间隔未到直接返回',
            'maintain.health.guard_pending_recycles' => '维护链路-待回收进程检查',
            'maintain.health.instance_reload' => '维护链路-重载实例配置',
            'maintain.health.instance_snapshot' => '维护链路-读取实例快照',
            'maintain.health.active_version_empty' => '维护链路-无活跃版本',
            'maintain.health.generation_iterated' => '维护链路-版本已进入迭代阶段',
            'maintain.health.collect_active_plans' => '维护链路-收集活跃计划',
            'maintain.health.active_plans_empty' => '维护链路-无可用计划',
            'maintain.health.plan_skip' => '维护链路-跳过计划探测',
            'maintain.health.plan_probe' => '维护链路-执行计划探测',
            'maintain.health.finish_without_unhealthy' => '维护链路-本轮无异常结束',
            'maintain.health.guard_selfheal_cooldown' => '维护链路-自愈冷却检查',
            'maintain.health.trigger_selfheal' => '维护链路-触发自愈',
            'probe.health.invalid_port' => '探测链路-端口参数非法',
            'probe.health.http_listening' => '探测链路-HTTP 监听检查',
            'probe.health.rpc_listening' => '探测链路-RPC 监听检查',
            'probe.health.fetch_internal_status' => '探测链路-拉取内部状态',
            'probe.health.http_probe' => '探测链路-HTTP 探活',
            'probe.health.barrier_wait_exception' => '探测链路-并发栅栏等待异常',
            'probe.health.total' => '探测链路-单计划总耗时',
            'fetch.internal.invalid_target' => '内部拉取-目标地址非法',
            'fetch.internal.listen_check' => '内部拉取-监听检查',
            'fetch.internal.ipc_request' => '内部拉取-IPC 请求',
            'fetch.internal.success' => '内部拉取-请求成功',
            'fetch.internal.non_200' => '内部拉取-非 200 响应',
            'fetch.internal.no_ipc_action' => '内部拉取-无可用 IPC 动作',
            'fetch.internal.exception' => '内部拉取-请求异常',
        ];
        return $map[$step] ?? ('未知步骤(' . $step . ')');
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
            Console::warning(
                "【Gateway】业务平面重启存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                . ', ' . $this->managedRestartFailureDetails($summary)
            );
        } else {
            Console::success("【Gateway】业务平面重启完成: success={$summary['success_count']}");
        }
        return $summary;
    }

    /**
     * 组装业务平面重启失败的可排障摘要。
     *
     * 输出包含两部分：
     * 1. failed_nodes 的失败原因列表；
     * 2. 旧 upstream 的连接数与 inflight 运行态快照，便于直接判断是“旧代未排空”还是“新代拉起失败”。
     *
     * @param array{success_count:int, failed_nodes:array<int, array<string, mixed>>} $summary 重启摘要。
     * @param array<int, array<string, mixed>>|null $oldPlans 可选旧代 plan 集合，不传时自动从 active/draining generation 推断。
     * @return string
     */
    protected function managedRestartFailureDetails(array $summary, ?array $oldPlans = null): string {
        $failedNodes = array_values(array_filter((array)($summary['failed_nodes'] ?? []), 'is_array'));
        $failedReasonParts = [];
        foreach (array_slice($failedNodes, 0, 5) as $node) {
            $host = trim((string)($node['host'] ?? $node['node'] ?? 'unknown'));
            $reason = trim((string)($node['error'] ?? $node['reason'] ?? $node['message'] ?? 'unknown'));
            if ($host === '') {
                $host = 'unknown';
            }
            if ($reason === '') {
                $reason = 'unknown';
            }
            $failedReasonParts[] = "{$host}:{$reason}";
        }
        if (count($failedNodes) > 5) {
            $failedReasonParts[] = '...+' . (count($failedNodes) - 5) . ' more';
        }
        $failedReasons = $failedReasonParts ? implode(' | ', $failedReasonParts) : 'none';

        return 'reasons=' . $failedReasons . ', old_upstream=' . $this->managedRestartOldUpstreamDiagnostics($oldPlans);
    }

    /**
     * 汇总旧 upstream 当前运行态诊断。
     *
     * 失败态下优先展示 active/draining generation 的旧实例指标，字段与回收状态机保持一致：
     * ws/conn/http/rpc/queue/crontab/mysql/redis/outbound_http。
     *
     * @param array<int, array<string, mixed>>|null $plans 指定旧代 plan 列表；空时自动从 snapshot 推断。
     * @return string
     */
    protected function managedRestartOldUpstreamDiagnostics(?array $plans = null): string {
        $candidates = [];
        if (is_array($plans) && $plans) {
            $candidates = $plans;
        } else {
            $snapshot = $this->instanceManager->snapshot();
            foreach ((array)($snapshot['generations'] ?? []) as $generation) {
                $status = (string)($generation['status'] ?? '');
                if (!in_array($status, ['active', 'draining'], true)) {
                    continue;
                }
                foreach ((array)($generation['instances'] ?? []) as $instance) {
                    $metadata = (array)($instance['metadata'] ?? []);
                    if (($metadata['managed'] ?? false) !== true) {
                        continue;
                    }
                    $port = (int)($instance['port'] ?? 0);
                    if ($port <= 0) {
                        continue;
                    }
                    $candidates[] = [
                        'version' => (string)($instance['version'] ?? $generation['version'] ?? ''),
                        'host' => (string)($instance['host'] ?? '127.0.0.1'),
                        'port' => $port,
                        'rpc_port' => (int)($metadata['rpc_port'] ?? 0),
                        'metadata' => $metadata,
                    ];
                }
            }
        }

        if (!$candidates) {
            return 'none';
        }

        $dedup = [];
        foreach ($candidates as $plan) {
            if (!is_array($plan)) {
                continue;
            }
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            if ($port <= 0) {
                continue;
            }
            $version = (string)($plan['version'] ?? '');
            $key = $version . '@' . $host . ':' . $port;
            $dedup[$key] = $plan;
        }
        $plans = array_values($dedup);
        if (!$plans) {
            return 'none';
        }

        $parts = [];
        foreach (array_slice($plans, 0, 3) as $plan) {
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            $version = (string)($plan['version'] ?? '');
            $runtime = $this->fetchUpstreamRuntimeStatus($plan);
            $runtimeSource = 'live';
            if ($runtime === []) {
                $cached = $this->instanceManager->instanceRuntimeStatus($host, $port, 15);
                if ($cached) {
                    $runtime = $cached;
                    $runtimeSource = 'cache';
                } else {
                    $runtimeSource = 'unavailable';
                }
            }
            $runtimeAvailable = $runtime !== [];
            $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
            $serverConnectionNum = $runtimeAvailable ? (int)(($runtime['server_stats']['connection_num'] ?? 0)) : -1;
            $httpProcessing = $runtimeAvailable ? (int)($runtime['http_request_processing'] ?? 0) : -1;
            $rpcProcessing = $runtimeAvailable ? (int)($runtime['rpc_request_processing'] ?? 0) : -1;
            $queueProcessing = $runtimeAvailable ? (int)($runtime['redis_queue_processing'] ?? 0) : -1;
            $crontabBusy = $runtimeAvailable ? (int)($runtime['crontab_busy'] ?? 0) : -1;
            $mysqlInflight = $runtimeAvailable ? (int)($runtime['mysql_inflight'] ?? 0) : -1;
            $redisInflight = $runtimeAvailable ? (int)($runtime['redis_inflight'] ?? 0) : -1;
            $outboundHttpInflight = $runtimeAvailable ? (int)($runtime['outbound_http_inflight'] ?? 0) : -1;

            $parts[] = "{$host}:{$port}(v={$version},runtime={$runtimeSource},ws={$gatewayWs},conn="
                . ($serverConnectionNum >= 0 ? (string)$serverConnectionNum : 'n/a')
                . ",http=" . ($httpProcessing >= 0 ? (string)$httpProcessing : 'n/a')
                . ",rpc=" . ($rpcProcessing >= 0 ? (string)$rpcProcessing : 'n/a')
                . ",queue=" . ($queueProcessing >= 0 ? (string)$queueProcessing : 'n/a')
                . ",crontab=" . ($crontabBusy >= 0 ? (string)$crontabBusy : 'n/a')
                . ",mysql=" . ($mysqlInflight >= 0 ? (string)$mysqlInflight : 'n/a')
                . ",redis=" . ($redisInflight >= 0 ? (string)$redisInflight : 'n/a')
                . ",outbound_http=" . ($outboundHttpInflight >= 0 ? (string)$outboundHttpInflight : 'n/a')
                . ')';
        }
        if (count($plans) > 3) {
            $parts[] = '...+' . (count($plans) - 3) . ' more';
        }
        return implode(' | ', $parts);
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
                $reportIp = (string)($data['data']['ip'] ?? '');
                if ($host === '') {
                    return;
                }
                $existingNode = ServerNodeTable::instance()->get($host);
                $previousFd = (int)($existingNode['socket_fd'] ?? 0);
                if ($this->addNodeClient($frame->fd, $host, $role)) {
                    $server->push($frame->fd, JsonHelper::toJson(['event' => 'slave_node_report_response', 'data' => $frame->fd]));
                    $this->pushConsoleSubscription($frame->fd);
                    $peerCount = 0;
                    foreach (ServerNodeTable::instance()->rows() as $node) {
                        if (($node['role'] ?? '') !== NODE_ROLE_MASTER) {
                            $peerCount++;
                        }
                    }
                    $nodeLabel = "node_id={$host}, role={$role}, fd={$frame->fd}, peers={$peerCount}";
                    if ($reportIp !== '') {
                        $nodeLabel .= ", ip={$reportIp}";
                    }
                    if ($previousFd > 0 && $previousFd !== $frame->fd) {
                        Console::warning("【GatewayCluster】节点重连加入: {$nodeLabel}, previous_fd={$previousFd}");
                    } else {
                        Console::success("【GatewayCluster】节点加入: {$nodeLabel}");
                    }
                    $this->pushDashboardStatus();
                } else {
                    $failedLabel = "node_id={$host}, role={$role}, fd={$frame->fd}";
                    if ($reportIp !== '') {
                        $failedLabel .= ", ip={$reportIp}";
                    }
                    Console::warning("【GatewayCluster】节点加入失败: {$failedLabel}");
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
        // Swoole\Table 的 row key 长度有限制（常见上限 64 字节），
        // 直接拼接较长 node_id 会触发 "key is too long" 并导致写入失败。
        // 这里统一用定长 hash，保证 Runtime key 可写且跨进程读写一致。
        return 'linux_crontab_sync_state:' . md5($host);
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
        $this->waitForGatewayShutdownDrain();
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
     * @throws RuntimeException|SwooleException 当底层 server 创建失败时抛出
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
            $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
            $managerAlive = (bool)($releaseStatus['manager_alive'] ?? false);
            $runtimeShuttingDown = (bool)($releaseStatus['runtime_shutting_down'] ?? false);
            $runtimeAliveCount = (int)($releaseStatus['runtime_alive_count'] ?? 0);
            $trackedAliveCount = (int)($releaseStatus['tracked_alive_count'] ?? 0);
            $subprocessShuttingDown = (bool)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN) ?: false);
            $subprocessAliveCount = (int)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT) ?: 0);
            $effectiveShuttingDown = $runtimeShuttingDown || $subprocessShuttingDown;
            $effectiveAliveCount = max($runtimeAliveCount, $subprocessAliveCount);

            // manager 已退出且本地句柄也确认没有托管子进程时，Runtime 里的
            // shutting_down/alive_count 可能是上一次关停残留值，这里主动清理，
            // 避免 gateway 一直卡在 aux=waiting。
            if (!$managerAlive && $trackedAliveCount <= 0 && ($effectiveShuttingDown || $effectiveAliveCount > 0)) {
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
                Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
                $effectiveShuttingDown = false;
                $effectiveAliveCount = 0;
            }

            // Runtime 里的 alive_count 只覆盖 manager 下面托管的直系子进程，
            // addProcess 根进程自身若仍存活，同样会继续持有旧 gateway 继承下来的监听 FD。
            if (
                $managerAlive
                || $effectiveAliveCount > 0
                || $effectiveShuttingDown
            ) {
                return false;
            }
        }
        if ($this->upstreamSupervisor && $this->upstreamSupervisor->isAlive()) {
            return false;
        }
        return true;
    }

    /**
     * 在 gateway 关停窗口内强制收敛附属子进程树。
     *
     * 该方法只处理控制面附属进程（SubProcessManager + 其托管子进程），
     * 不触碰业务 upstream。用于“命令已下发但附属进程未按时退出”场景，
     * 防止 gateway 长时间卡在 `aux=waiting`。
     *
     * @param string $reason 触发原因，用于日志标识。
     * @return void
     */
    protected function forceReleaseGatewayAuxiliaryProcesses(string $reason): void {
        $pidSet = [];
        $addPid = static function (array &$target, int $pid): void {
            if ($pid > 0) {
                $target[$pid] = true;
            }
        };

        $runtimePidKeys = [
            Key::RUNTIME_SUBPROCESS_MANAGER_PID,
            Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID,
            Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID,
            Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID,
            Key::RUNTIME_MEMORY_MONITOR_PID,
            Key::RUNTIME_HEARTBEAT_PID,
            Key::RUNTIME_LOG_BACKUP_PID,
            Key::RUNTIME_REDIS_QUEUE_MANAGER_PID,
            Key::RUNTIME_REDIS_QUEUE_WORKER_PID,
            Key::RUNTIME_FILE_WATCHER_PID,
        ];
        foreach ($runtimePidKeys as $key) {
            $addPid($pidSet, (int)(Runtime::instance()->get($key) ?? 0));
        }

        if ($this->subProcessManager) {
            $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
            $addPid($pidSet, (int)($releaseStatus['manager_pid'] ?? 0));
            foreach ((array)($releaseStatus['tracked_alive'] ?? []) as $pid) {
                $addPid($pidSet, (int)$pid);
            }
        }

        $selfPid = getmypid();
        $masterPid = (int)($this->server->master_pid ?? 0);
        $managerPid = (int)($this->server->manager_pid ?? 0);
        unset($pidSet[$selfPid], $pidSet[$masterPid], $pidSet[$managerPid]);
        if (!$pidSet) {
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
            return;
        }

        $termTargets = [];
        foreach (array_keys($pidSet) as $pid) {
            if (@Process::kill($pid, 0)) {
                $termTargets[] = (int)$pid;
            }
        }
        if (!$termTargets) {
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
            Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
            return;
        }

        foreach ($termTargets as $pid) {
            @Process::kill($pid, SIGTERM);
        }
        usleep(300 * 1000);

        $killTargets = [];
        foreach ($termTargets as $pid) {
            if (@Process::kill($pid, 0)) {
                $killTargets[] = $pid;
            }
        }
        foreach ($killTargets as $pid) {
            @Process::kill($pid, SIGKILL);
        }

        // 强制收敛后主动清理 runtime 关停态，避免等待环继续被旧计数卡住。
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_SHUTTING_DOWN, false);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_ALIVE_COUNT, 0);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_HEARTBEAT_AT, 0);
        Runtime::instance()->set(Key::RUNTIME_SUBPROCESS_MANAGER_PID, 0);

        Console::warning(
            '【Gateway】附属进程强制收敛完成: reason=' . $reason
            . ', term=' . count($termTargets)
            . ', kill=' . count($killTargets)
        );
    }

    /**
     * 在真正关闭 gateway 监听前，等待旧控制面附属进程释放继承下来的监听 FD。
     *
     * gateway 的 addProcess 子进程和 manager 派生树都会继承旧 server 的控制面/RPC
     * 监听句柄。如果主进程刚发出 shutdown 就立刻退出，外层 boot 守护循环会比这些
     * 旧子进程更早开始重拉新的 gateway，最终在 10580/9585 这类端口上撞出
     * `Address already in use`。
     *
     * 因此这里要在 prepareGatewayShutdown() 之后、真正触发 server->shutdown() 之前
     * 做一个有界等待，确认旧 FD 持有者已经退出；超时时仍继续关停，但会把剩余状态
     * 打进日志，方便继续定位卡住的是 manager 还是 upstream supervisor。
     *
     * @param int $timeoutSeconds 最长等待秒数
     * @param int $intervalMs 每次轮询间隔毫秒
     * @return void
     */
    protected function waitForGatewayShutdownDrain(int $timeoutSeconds = 20, int $intervalMs = 200): void {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $logged = false;
        $auxReleased = false;
        $upstreamReleased = false;
        $shutdownRetried = false;
        $forceReleased = false;
        $statusLogged = false;

        while (microtime(true) < $deadline) {
            $auxReleased = $this->gatewayAuxiliaryProcessesReleased();
            $upstreamReleased = $this->preserveManagedUpstreamsOnShutdown || $this->managedUpstreamPortsReleased();
            if ($auxReleased && $upstreamReleased) {
                if ($logged) {
                    Console::success('【Gateway】关停前附属进程与旧监听 FD 已释放');
                }
                return;
            }

            if (!$logged) {
                Console::warning(
                    '【Gateway】关停前等待附属进程释放旧监听 FD: aux=' . ($auxReleased ? 'ready' : 'waiting')
                    . ', upstream=' . ($upstreamReleased ? 'ready' : 'waiting')
                );
                $logged = true;
            }

            if (!$statusLogged && !$auxReleased && $this->subProcessManager) {
                $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
                Console::warning(
                    '【Gateway】附属进程释放状态: manager_pid=' . (int)($releaseStatus['manager_pid'] ?? 0)
                    . ', manager_alive=' . ((bool)($releaseStatus['manager_alive'] ?? false) ? 'yes' : 'no')
                    . ', manager_hb_age=' . (int)($releaseStatus['manager_heartbeat_age'] ?? -1) . 's'
                    . ', runtime_alive=' . (int)($releaseStatus['runtime_alive_count'] ?? 0)
                    . ', runtime_shutting_down=' . ((bool)($releaseStatus['runtime_shutting_down'] ?? false) ? 'yes' : 'no')
                );
                $statusLogged = true;
            }

            if (!$shutdownRetried && !$auxReleased && (microtime(true) + 8) >= $deadline && $this->subProcessManager) {
                // 关停等待接近尾声时再补发一次 subprocess shutdown，收敛“第一次投递丢失”
                // 导致的 aux=waiting 卡住场景；该操作只发生在 shutdown 流程内部。
                $this->subProcessManager->shutdown(true);
                $shutdownRetried = true;
                Console::warning('【Gateway】关停等待附属进程释放耗时较长，已重试投递子进程关停命令');
            }
            if (
                !$forceReleased
                && !$auxReleased
                && (microtime(true) + 3) >= $deadline
            ) {
                // 临近超时仍未释放时直接收敛附属进程，避免持续占用旧监听 FD。
                $this->forceReleaseGatewayAuxiliaryProcesses('shutdown_wait_timeout');
                $forceReleased = true;
            }

            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(max(0.05, $intervalMs / 1000));
            } else {
                usleep(max(50, $intervalMs) * 1000);
            }
        }

        if ($logged) {
            $detail = '';
            if ($this->subProcessManager) {
                $releaseStatus = $this->subProcessManager->managedProcessReleaseStatus();
                $detail = ', manager_pid=' . (int)($releaseStatus['manager_pid'] ?? 0)
                    . ', manager_alive=' . ((bool)($releaseStatus['manager_alive'] ?? false) ? 'yes' : 'no')
                    . ', manager_hb_age=' . (int)($releaseStatus['manager_heartbeat_age'] ?? -1) . 's'
                    . ', runtime_alive=' . (int)($releaseStatus['runtime_alive_count'] ?? 0)
                    . ', runtime_shutting_down=' . ((bool)($releaseStatus['runtime_shutting_down'] ?? false) ? 'yes' : 'no');
            }
            Console::warning(
                '【Gateway】关停等待附属进程释放超时，继续执行 shutdown: aux=' . ($auxReleased ? 'ready' : 'waiting')
                    . ', upstream=' . ($upstreamReleased ? 'ready' : 'waiting')
                    . $detail
            );
        }
    }

    protected function prepareGatewayShutdown(): void {
        if ($this->gatewayShutdownPrepared) {
            return;
        }
        $this->gatewayShutdownPrepared = true;
        $this->stopGatewayLeaseRenewTimer();
        $this->persistGatewayLeaseForShutdown();
        // 关停早期就把 serverIsAlive 拉低，避免 SubProcessManager 在 wait(false) 回收窗口里
        // 误判为“父服务仍存活”而把刚退出的子进程立即重拉。
        Runtime::instance()->serverIsAlive(false);
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
            Console::warning('【Gateway】等待 UpstreamSupervisor 脱离控制面板失败，将继续按关停流程推进: ' . $result->getMessage());
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
        string $reason,
        bool $reloaded,
        bool $tested,
        array $displayPaths,
        array $contextLines = []
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
                Console::warning(
                    "【Gateway】业务实例重启存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                    . ', ' . $this->managedRestartFailureDetails($summary)
                );
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
            case 'restart_redisqueue':
                if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
                    $socket->push("【" . SERVER_HOST . "】RedisQueue process unavailable");
                    return true;
                }
                $this->restartGatewayRedisQueueProcess();
                $socket->push("【" . SERVER_HOST . "】start redisqueue restart");
                return true;
            case 'subprocess_restart':
                if (!$this->subProcessManager) {
                    $socket->push("【" . SERVER_HOST . "】SubProcessManager unavailable");
                    return true;
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->restartManagedProcesses($target === '' ? [] : [$target]);
                $socket->push("【" . SERVER_HOST . "】" . $result['message']);
                return true;
            case 'subprocess_stop':
                if (!$this->subProcessManager) {
                    $socket->push("【" . SERVER_HOST . "】SubProcessManager unavailable");
                    return true;
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->stopManagedProcesses($target === '' ? [] : [$target]);
                $socket->push("【" . SERVER_HOST . "】" . $result['message']);
                return true;
            case 'subprocess_restart_all':
                if (!$this->subProcessManager) {
                    $socket->push("【" . SERVER_HOST . "】SubProcessManager unavailable");
                    return true;
                }
                $result = $this->subProcessManager->restartAllManagedProcesses();
                $socket->push("【" . SERVER_HOST . "】" . $result['message']);
                return true;
            case 'restart':
                if ($this->gatewayShutdownScheduled) {
                    $socket->push("【" . SERVER_HOST . "】gateway restart failed: gateway shutting down");
                    return true;
                }
                $socket->push("【" . SERVER_HOST . "】start restart");
                // 远端 restart 默认按 full restart 执行，先下线 upstream 再重启 gateway。
                $preserveManagedUpstreams = (bool)($params['preserve_managed_upstreams'] ?? false);
                $this->scheduleGatewayShutdown($preserveManagedUpstreams);
                return true;
            case 'appoint_update':
                // slave 收到 master 转发的 appoint_update 时，不能在 cluster 协调进程里
                // 再走一遍 dashboardUpdate()。那条入口会把当前节点当成“发起升级的 gateway”，
                // 重新进入 dispatch_cluster/accepted 状态机，日志里就会出现
                // `stage=dispatch_cluster, slaves=0`，同时真正的本地升级执行链反而被绕开。
                //
                // 这里显式回退给 SubProcessManager 的内置 appoint_update 分支处理，由它统一：
                // 1. 转交 GatewayBusinessCoordinator 执行本地升级；
                // 2. 在 slave 本机完成 rolling upstream / 业务子进程迭代；
                // 3. 把 running/success/failed/pending 回报给 master。
                return false;
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
        // heartbeat 只采当前 active upstream 的运行态，避免“全量并发探测”拖慢心跳循环。
        // 运行态优先走本机 IPC 实时抓取；当 IPC 短暂超时，再回退到本进程最近缓存。
        $status = $this->composeGatewayNodeRuntimeStatus($status, $this->heartbeatBusinessUpstreams());
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

    /**
     * 为 heartbeat 链路构建“当前业务面”upstream 列表。
     *
     * 设计约束：
     * 1) 仅关注 active generation，避免 heartbeat 周期探测所有历史代实例；
     * 2) 运行态优先走本机 IPC 实时读取，确保 request_today 等累计值及时更新；
     * 3) IPC 失败时回退到本进程最近缓存，避免整包节点状态直接归零。
     *
     * @return array<int, array<string, mixed>>
     */
    protected function heartbeatBusinessUpstreams(): array {
        $snapshot = $this->currentGatewayStateSnapshot();
        $activeVersion = (string)($snapshot['active_version'] ?? '');
        $plans = [];

        foreach (($snapshot['generations'] ?? []) as $generation) {
            if (!is_array($generation)) {
                continue;
            }
            $generationVersion = (string)($generation['version'] ?? '');
            $generationStatus = (string)($generation['status'] ?? '');
            if ($activeVersion !== '') {
                if ($generationVersion !== $activeVersion) {
                    continue;
                }
            } elseif ($generationStatus !== 'active') {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                if (!is_array($instance)) {
                    continue;
                }
                $metadata = (array)($instance['metadata'] ?? []);
                if (($metadata['managed'] ?? false) !== true) {
                    continue;
                }
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($host === '' || $port <= 0) {
                    continue;
                }
                $plans[] = [
                    'generation' => $generation,
                    'instance' => $instance,
                ];
            }
        }

        if (!$plans) {
            return [];
        }

        $nodes = [];
        foreach ($plans as $plan) {
            $generation = (array)($plan['generation'] ?? []);
            $instance = (array)($plan['instance'] ?? []);
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            if ($host === '' || $port <= 0) {
                continue;
            }
            $runtimeStatus = $this->fetchUpstreamInternalStatusSync(
                $host,
                $port,
                self::INTERNAL_UPSTREAM_STATUS_PATH,
                1.0
            );
            if ($runtimeStatus) {
                $this->instanceManager->updateInstanceRuntimeStatus($host, $port, $runtimeStatus);
            } else {
                $runtimeStatus = $this->instanceManager->instanceRuntimeStatus($host, $port, 30);
            }
            $nodes[] = $this->buildUpstreamNode($generation, $instance, $runtimeStatus, true);
        }

        return $nodes;
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
        $previousNode = (array)(ServerNodeStatusTable::instance()->get('localhost') ?: []);
        $node = $this->buildGatewayNode();
        $node['host'] = 'localhost';
        $node['id'] = APP_NODE_ID;
        $node['appid'] = App::id() ?: 'scf_app';
        // cluster tick 是 1s 控制面刷新，不应覆盖 heartbeat 的 5s 语义时间戳。
        // 这里单独记录 cluster tick 时间，并保留 heartbeat 原值供 dashboard 计算 HB 延迟。
        $node['cluster_tick_at'] = time();
        $node['heart_beat'] = (int)($node['heart_beat'] ?? ($previousNode['heart_beat'] ?? 0));
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
            $host = (string)$node['host'];
            $role = (string)($node['role'] ?? NODE_ROLE_SLAVE);
            ServerNodeTable::instance()->delete($host);
            ServerNodeStatusTable::instance()->delete($host);
            Console::warning("【GatewayCluster】节点断开移除: host={$host}, role={$role}, fd={$fd}");
            $deleted = true;
        }
        if ($deleted) {
            $this->pushDashboardStatus();
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
     * 生成 dashboard 实时状态快照。
     *
     * 返回的是控制面聚合视图：
     * - nodes 代表 dashboard 节点渲染数据；
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
            'nodes' => $nodes,
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

    /**
     * 构建 dashboard 视角下的业务实例列表。
     *
     * @param bool $fetchLiveRuntime true=实时抓取 upstream.status；false=仅使用 instanceManager 缓存
     * @return array<int, array<string, mixed>>
     */
    protected function dashboardUpstreams(bool $fetchLiveRuntime = true): array {
        $snapshot = $this->currentGatewayStateSnapshot();
        $plans = [];
        foreach ($snapshot['generations'] ?? [] as $generation) {
            foreach (($generation['instances'] ?? []) as $instance) {
                $plans[] = [
                    'generation' => (array)$generation,
                    'instance' => (array)$instance,
                ];
            }
        }
        if (!$plans) {
            return [];
        }

        if (!$fetchLiveRuntime) {
            $instances = [];
            foreach ($plans as $plan) {
                $generation = (array)($plan['generation'] ?? []);
                $instance = (array)($plan['instance'] ?? []);
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                $runtimeStatus = $this->instanceManager->instanceRuntimeStatus($host, $port, 30);
                $instances[] = $this->buildUpstreamNode($generation, $instance, $runtimeStatus, true);
            }
            return $instances;
        }

        $instances = array_fill(0, count($plans), null);
        $barrier = Barrier::make();
        foreach ($plans as $index => $plan) {
            Coroutine::create(function () use ($barrier, &$instances, $index, $plan): void {
                $generation = (array)($plan['generation'] ?? []);
                $instance = (array)($plan['instance'] ?? []);
                $runtimeStatus = $this->fetchUpstreamRuntimeStatus($instance);
                $instances[$index] = $this->buildUpstreamNode($generation, $instance, $runtimeStatus, false);
            });
        }
        try {
            Barrier::wait($barrier);
        } catch (Throwable) {
            // dashboard 状态链路里并发探测异常时，降级为已完成分支结果，避免整包状态失败。
        }

        return array_values(array_filter($instances, static fn($item): bool => is_array($item)));
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
                $masterPid = (int)($runtimeStatus['master_pid'] ?? 0);
                $managerPid = (int)($runtimeStatus['manager_pid'] ?? 0);
                $metadataPatch = [];
                // lifecycle 回收链必须始终用 upstream master pid 作为实例主 PID。
                // 若写成 manager pid，SIGTERM/SIGKILL 会被 manager 进程吞掉并被 master 立刻拉回，
                // 最终表现为“回收一直重试、端口迟迟不释放”。
                if ($masterPid > 0) {
                    $metadataPatch['pid'] = $masterPid;
                    $metadataPatch['master_pid'] = $masterPid;
                }
                if ($managerPid > 0) {
                    $metadataPatch['manager_pid'] = $managerPid;
                }
                if ($metadataPatch) {
                    $this->instanceManager->mergeInstanceMetadata($host, $port, $metadataPatch);
                    $this->mergeManagedPlanMetadata($host, $port, $metadataPatch);
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
            'owner_epoch' => $this->gatewayLeaseEpoch(),
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
        $this->recordStartupUpstreamSupervisorSync($this->lastObservedUpstreamSupervisorPid, count($instances));
        if ($syncReason === 'restart') {
            Console::success('【Gateway】UpstreamSupervisor 已异常重建，状态已重新同步: pid=' . $this->lastObservedUpstreamSupervisorPid . ', instances=' . count($instances));
        } elseif ($syncReason === 'initial' && !$this->startupSummaryPending()) {
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
                if ($pendingRecycle) {
                    continue;
                }
                $httpAlive = $this->launcher->isListening($host, $port, 0.1) || $this->launcher->isListening('0.0.0.0', $port, 0.1);
                $rpcAlive = $rpcPort <= 0 || $this->launcher->isListening($host, $rpcPort, 0.1) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.1);
                if (!$httpAlive || !$rpcAlive) {
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
        $maintainStartedAt = microtime(true);
        if ($this->managedUpstreamSelfHealing || $this->managedUpstreamRolling || !$this->launcher || !$this->upstreamSupervisor) {
            $this->traceGatewayHealthStep('maintain.health.guard_flags', $maintainStartedAt, [
                'return' => 'self_healing_or_rolling_or_dependency_missing',
            ]);
            return;
        }
        if (!App::isReady() || $this->gatewayShutdownScheduled || !Runtime::instance()->serverIsAlive()) {
            $this->traceGatewayHealthStep('maintain.health.guard_runtime', $maintainStartedAt, [
                'return' => 'app_not_ready_or_shutdown_or_server_down',
            ]);
            return;
        }
        $now = time();
        if ($this->lastManagedUpstreamRollingAt > 0 && ($now - $this->lastManagedUpstreamRollingAt) < self::ACTIVE_UPSTREAM_HEALTH_ROLLING_COOLDOWN_SECONDS) {
            $this->traceGatewayHealthStep('maintain.health.guard_rolling_cooldown', $maintainStartedAt, [
                'return' => 'rolling_cooldown',
            ]);
            return;
        }
        $intervalResolveStartedAt = microtime(true);
        $healthCheckInterval = $this->resolveManagedUpstreamHealthCheckIntervalSeconds($now);
        $elapsedSinceLastRound = $this->lastManagedUpstreamHealthCheckAt > 0
            ? ($now - $this->lastManagedUpstreamHealthCheckAt)
            : -1;
        if ($elapsedSinceLastRound >= 0 && $elapsedSinceLastRound < $healthCheckInterval) {
            return;
        }
        $this->traceGatewayHealthStep('maintain.health.resolve_interval', $intervalResolveStartedAt, [
            'interval' => $healthCheckInterval,
            'elapsed' => $elapsedSinceLastRound >= 0 ? $elapsedSinceLastRound : 'first_round',
        ]);
        $this->lastManagedUpstreamHealthCheckAt = $now;
        if ($this->pendingManagedRecycles) {
            $this->traceGatewayHealthStep('maintain.health.guard_pending_recycles', $maintainStartedAt, [
                'return' => 'pending_recycles',
                'pending_count' => count($this->pendingManagedRecycles),
            ]);
            return;
        }

        // 健康检测统一以 registry 持久化状态为准。
        // rolling / recycle / self-heal 期间有多条协程在推进 instanceManager 内存态，
        // 这里先 reload 一次，避免使用到尚未收敛的旧代 in-memory 视图。
        $reloadStartedAt = microtime(true);
        $this->instanceManager->reload();
        $this->traceGatewayHealthStep('maintain.health.instance_reload', $reloadStartedAt);
        $snapshotStartedAt = microtime(true);
        $snapshot = $this->instanceManager->snapshot();
        $this->traceGatewayHealthStep('maintain.health.instance_snapshot', $snapshotStartedAt);
        $activeVersion = (string)($snapshot['active_version'] ?? '');
        if ($activeVersion === '') {
            $this->lastManagedHealthActiveVersion = '';
            $this->managedUpstreamHealthState = [];
            $this->managedUpstreamHealthStableRounds = 0;
            $this->managedUpstreamHealthLastFailureAt = 0;
            $this->traceGatewayHealthStep('maintain.health.active_version_empty', $maintainStartedAt, [
                'return' => 'active_version_empty',
            ]);
            return;
        }
        if ($activeVersion !== $this->lastManagedHealthActiveVersion) {
            // active generation 一变，历史失败计数就没有参考意义了，需要对新代重新观察。
            $this->notifyManagedUpstreamGenerationIterated($activeVersion);
            $this->traceGatewayHealthStep('maintain.health.generation_iterated', $maintainStartedAt, [
                'return' => 'generation_iterated',
                'active_version' => $activeVersion,
            ]);
            return;
        }
        $plansStartedAt = microtime(true);
        $activePlans = $this->managedPlansFromSnapshot();
        $this->traceGatewayHealthStep('maintain.health.collect_active_plans', $plansStartedAt, [
            'plan_count' => count($activePlans),
        ]);
        if (!$activePlans) {
            $this->managedUpstreamHealthState = [];
            $this->managedUpstreamHealthStableRounds = 0;
            $this->managedUpstreamHealthLastFailureAt = 0;
            $this->traceGatewayHealthStep('maintain.health.active_plans_empty', $maintainStartedAt, [
                'return' => 'active_plans_empty',
            ]);
            return;
        }

        $unhealthyPlans = [];
        $activeKeys = [];
        $hasFailure = false;
        foreach ($activePlans as $plan) {
            $key = $this->managedPlanKey($plan);
            $activeKeys[$key] = true;

            if ($this->shouldSkipManagedUpstreamHealthCheck($plan, $now)) {
                unset($this->managedUpstreamHealthState[$key]);
                $this->traceGatewayHealthStep('maintain.health.plan_skip', $maintainStartedAt, [
                    'plan' => $this->managedPlanDescriptor($plan),
                    'reason' => 'startup_grace_or_pending_recycle',
                ]);
                continue;
            }

            // 健康检查采用“端口 + internal health”双重标准，避免只看 listen 就过早判定 ready。
            $probeStartedAt = microtime(true);
            $probe = $this->probeManagedUpstreamHealth($plan);
            $this->traceGatewayHealthStep('maintain.health.plan_probe', $probeStartedAt, [
                'plan' => $this->managedPlanDescriptor($plan),
                'healthy' => !empty($probe['healthy']) ? 1 : 0,
                'reason' => (string)($probe['reason'] ?? 'unknown'),
            ]);
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
            $hasFailure = true;
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
        if ($hasFailure) {
            $this->managedUpstreamHealthStableRounds = 0;
            $this->managedUpstreamHealthLastFailureAt = $now;
        } else {
            $this->managedUpstreamHealthStableRounds = min(3600, $this->managedUpstreamHealthStableRounds + 1);
        }

        if (!$unhealthyPlans) {
            $this->traceGatewayHealthStep('maintain.health.finish_without_unhealthy', $maintainStartedAt, [
                'active_plan_count' => count($activePlans),
                'stable_rounds' => $this->managedUpstreamHealthStableRounds,
            ]);
            return;
        }
        if ($this->lastManagedUpstreamSelfHealAt > 0 && ($now - $this->lastManagedUpstreamSelfHealAt) < self::ACTIVE_UPSTREAM_HEALTH_COOLDOWN_SECONDS) {
            $this->traceGatewayHealthStep('maintain.health.guard_selfheal_cooldown', $maintainStartedAt, [
                'return' => 'selfheal_cooldown',
                'unhealthy_count' => count($unhealthyPlans),
            ]);
            return;
        }

        $this->managedUpstreamSelfHealing = true;
        $descriptors = array_map(fn(array $plan) => $this->managedPlanDescriptor($plan), $unhealthyPlans);
        $recoveryPlans = $activePlans;
        Console::warning("【Gateway】检测到active业务实例连续异常，开始自动自愈: " . implode(' | ', $descriptors));
        $this->traceGatewayHealthStep('maintain.health.trigger_selfheal', $maintainStartedAt, [
            'unhealthy_count' => count($unhealthyPlans),
            'recovery_plan_count' => count($recoveryPlans),
        ]);
        Coroutine::create(function () use ($unhealthyPlans, $recoveryPlans) {
            try {
                // 自愈直接复用 rolling restart，确保切流、回滚、回收语义完全一致。
                $summary = $this->restartManagedUpstreams($recoveryPlans);
                if ($summary['failed_nodes']) {
                    Console::warning(
                        "【Gateway】自动自愈存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                        . ', ' . $this->managedRestartFailureDetails($summary, $recoveryPlans)
                    );
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

    /**
     * 探测托管 upstream 的健康状态。
     *
     * 以 HTTP 探针命中 worker 的结果作为唯一健康判定标准，确保判定依据直接对应
     * 实例实际请求吞吐能力；RPC 监听状态仅用于诊断观测，不参与本轮判死逻辑。
     *
     * @param array<string, mixed> $plan 托管实例计划。
     * @return array{healthy: bool, reason: string}
     */
    protected function probeManagedUpstreamHealth(array $plan): array {
        $probeStartedAt = microtime(true);
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        $planLabel = $host . ':' . $port;
        if ($port <= 0) {
            $this->traceGatewayHealthStep('probe.health.invalid_port', $probeStartedAt, [
                'plan' => $planLabel,
            ]);
            return ['healthy' => false, 'reason' => 'invalid_port'];
        }

        // 不同类型探测并发执行并在 Barrier 收口；健康判定以 HTTP 探活为准。
        // 用 Barrier 栅栏收口，避免串行探测累积拉长心跳周期。
        $rpcListening = $rpcPort <= 0;
        $httpProbeOk = false;
        $barrier = Barrier::make();

        if ($rpcPort > 0) {
            Coroutine::create(function () use ($barrier, &$rpcListening, $host, $rpcPort, $planLabel): void {
                $rpcListeningStartedAt = microtime(true);
                $rpcListening = $this->launcher
                    && ($this->launcher->isListening($host, $rpcPort, 0.2) || $this->launcher->isListening('0.0.0.0', $rpcPort, 0.2));
                $this->traceGatewayHealthStep('probe.health.rpc_listening', $rpcListeningStartedAt, [
                    'plan' => $planLabel,
                    'rpc_port' => $rpcPort,
                    'listening' => $rpcListening ? 1 : 0,
                ]);
            });
        }

        Coroutine::create(function () use ($barrier, &$httpProbeOk, $host, $port, $planLabel): void {
            $httpProbeStartedAt = microtime(true);
            $httpProbeOk = $this->launcher
                && $this->launcher->probeHttpConnectivity($host, $port, self::INTERNAL_UPSTREAM_HTTP_PROBE_PATH, 0.5);
            $this->traceGatewayHealthStep('probe.health.http_probe', $httpProbeStartedAt, [
                'plan' => $planLabel,
                'ok' => $httpProbeOk ? 1 : 0,
            ]);
        });

        try {
            Barrier::wait($barrier);
        } catch (Throwable $throwable) {
            $this->traceGatewayHealthStep('probe.health.barrier_wait_exception', $probeStartedAt, [
                'plan' => $planLabel,
                'error' => $throwable->getMessage(),
            ]);
            return ['healthy' => false, 'reason' => 'probe_barrier_exception'];
        }

        if ($rpcPort > 0 && !$rpcListening) {
            $this->traceGatewayHealthStep('probe.health.rpc_listening', $probeStartedAt, [
                'plan' => $planLabel,
                'rpc_port' => $rpcPort,
                'soft_fail' => 1,
            ]);
        }
        if (!$httpProbeOk) {
            return ['healthy' => false, 'reason' => 'http_probe_failed'];
        }

        $this->traceGatewayHealthStep('probe.health.total', $probeStartedAt, [
            'plan' => $planLabel,
            'healthy' => 1,
            'by' => 'http_probe',
            'rpc_listening' => ($rpcPort <= 0 || $rpcListening) ? 1 : 0,
        ]);
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
        $this->managedUpstreamHealthStableRounds = 0;
        $this->managedUpstreamHealthLastFailureAt = 0;
    }

    /**
     * 计算当前周期应使用的健康探测间隔。
     *
     * 当前按固定间隔探测，便于统一观测窗口与排障节奏。
     *
     * @param int $now 当前时间戳。
     * @return int
     */
    protected function resolveManagedUpstreamHealthCheckIntervalSeconds(int $now): int {
        unset($now);
        $serverConfig = Config::server();
        return max(
            5,
            (int)($serverConfig['gateway_upstream_health_check_interval'] ?? self::ACTIVE_UPSTREAM_HEALTH_CHECK_INTERVAL_SECONDS)
        );
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
        $subprocesses = $this->subProcessManager?->managedProcessDashboardSnapshot() ?: [
            'signature' => '--',
            'updated_at' => time(),
            'summary' => ['total' => 0, 'running' => 0, 'stale' => 0, 'offline' => 0, 'stopped' => 0],
            'items' => [],
        ];
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
            'framework_update_ready' => function_exists('scf_framework_update_ready') && scf_framework_update_ready(),
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
            'mysql_inflight' => 0,
            'redis_inflight' => 0,
            'outbound_http_inflight' => 0,
            'mysql_execute_count' => 0,
            'http_request_reject' => 0,
            'cpu_num' => function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0,
            'memory_usage' => $this->buildEmptyMemoryUsage(),
            'threads' => (int)($stats['worker_num'] ?? 0),
            'tables' => [],
            'tasks' => [],
            'subprocesses' => $subprocesses,
            'subprocess_signature' => (string)($subprocesses['signature'] ?? '--'),
            'fingerprint' => APP_FINGERPRINT . ':gateway',
        ];
        $heartbeatStatus = ServerNodeStatusTable::instance()->get('localhost');
        $useHeartbeatStatus = is_array($heartbeatStatus)
            && $heartbeatStatus
            && $this->isLocalGatewayHeartbeatStatusFresh($heartbeatStatus);
        if ($useHeartbeatStatus) {
            $node = array_replace_recursive($node, $heartbeatStatus);
        }
        $upstreams ??= $this->dashboardUpstreams();
        if ($useHeartbeatStatus) {
            // 心跳快照存在时仍叠加一次“轻量实时业务指标”，让 dashboard 1s 推送
            // 的连接数/并发类数据不再停留在 heartbeat 的 5s 粒度。
            $node = $this->composeGatewayNodeRealtimeBusinessMetrics($node, $upstreams);
        } else {
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

    /**
     * 在 heartbeat 快照之上叠加实时业务指标（不补采内存明细）。
     *
     * dashboard 每秒都会触发一次状态推送；如果 localhost 节点只信任 heartbeat
     * 缓存，连接数/并发类字段会表现为约 5 秒更新一次。这里复用已获取的 upstream
     * runtime 快照，仅刷新业务计数类字段，避免每秒重复执行 memory rows 补采。
     *
     * @param array<string, mixed> $status 当前节点状态。
     * @param array<int, array<string, mixed>> $upstreams 当前轮 dashboard 已抓取的 upstream 列表。
     * @return array<string, mixed>
     */
    protected function composeGatewayNodeRealtimeBusinessMetrics(array $status, array $upstreams): array {
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
            $status['started'] = (int)($primary['started'] ?? ($status['started'] ?? 0));
            $status['threads'] = (int)($primary['threads'] ?? ($status['threads'] ?? 0));
            $status['tables'] = (array)($primary['tables'] ?? ($status['tables'] ?? []));
        }
        return $status;
    }

    /**
     * 判断 localhost 心跳状态是否仍可用于 dashboard 展示。
     *
     * 当 cluster 协调子进程异常退出或 pipe 推送链路短暂失效时，状态表可能残留旧值。
     * 这里把“心跳时间 + 协调进程 heartbeat”都纳入新鲜度判断，避免面板长期展示旧数据。
     *
     * @param array<string, mixed> $heartbeatStatus
     * @return bool
     */
    protected function isLocalGatewayHeartbeatStatusFresh(array $heartbeatStatus): bool {
        $now = time();
        $nodeHeartbeatAt = (int)($heartbeatStatus['heart_beat'] ?? 0);
        if ($nodeHeartbeatAt <= 0 || ($now - $nodeHeartbeatAt) > 8) {
            return false;
        }
        $clusterHeartbeatAt = (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT) ?? 0);
        if ($clusterHeartbeatAt <= 0 || ($now - $clusterHeartbeatAt) > 8) {
            return false;
        }
        return true;
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

    /**
     * 组装单个 upstream 节点状态。
     *
     * @param array<string, mixed> $generation
     * @param array<string, mixed> $instance
     * @param array<string, mixed> $runtimeStatus
     * @param bool $skipLivePortProbe 是否跳过实时端口探测
     * @return array<string, mixed>
     */
    protected function buildUpstreamNode(array $generation, array $instance, array $runtimeStatus = [], bool $skipLivePortProbe = false): array {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $metadata = (array)($instance['metadata'] ?? []);
        $status = (string)($generation['status'] ?? ($instance['status'] ?? 'prepared'));
        $displayVersion = (string)($metadata['display_version'] ?? $generation['version'] ?? $instance['version'] ?? 'unknown');
        $runtimeOnline = array_key_exists('server_is_alive', $runtimeStatus)
            ? ((bool)$runtimeStatus['server_is_alive'])
            : null;
        $online = $runtimeOnline;
        if (is_null($online)) {
            if (!$skipLivePortProbe && $this->launcher) {
                $online = $this->launcher->isListening($host, $port, 0.2);
            } else {
                $online = true;
            }
        }

        $node = [
            'name' => '业务实例',
            'script' => $displayVersion,
            'ip' => $host,
            'port' => $port,
            'online' => $online,
            'role' => (string)($metadata['role'] ?? SERVER_ROLE),
            'started' => (int)($metadata['started_at'] ?? time()),
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_upstream/' . $status,
            'app_version' => $appVersion,
            'public_version' => $publicVersion,
            'framework_build_version' => FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => function_exists('scf_framework_update_ready') && scf_framework_update_ready(),
            'swoole_version' => swoole_version(),
            'scf_version' => SCF_COMPOSER_VERSION,
            'manager_pid' => (int)($metadata['manager_pid'] ?? 0) ?: '--',
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
            'online' => $online,
            'role' => (string)($metadata['role'] ?? ($runtimeStatus['role'] ?? SERVER_ROLE)),
            'server_run_mode' => (string)($runtimeStatus['server_run_mode'] ?? APP_SRC_TYPE),
            'proxy_mode_label' => 'gateway_upstream/' . $status,
            'manager_pid' => ($runtimeStatus['manager_pid'] ?? ((int)($metadata['manager_pid'] ?? 0) ?: '--')),
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
        $fetchStartedAt = microtime(true);
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $timeoutSeconds = $path === self::INTERNAL_UPSTREAM_STATUS_PATH ? 5.0 : 1.0;
        $planLabel = $host . ':' . $port;
        if ($host === '' || $port <= 0) {
            $this->traceGatewayHealthStep('fetch.internal.invalid_target', $fetchStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
            ]);
            return [];
        }
        $listenCheckStartedAt = microtime(true);
        if ($this->launcher && !$this->launcher->isListening($host, $port, 0.2)) {
            $this->traceGatewayHealthStep('fetch.internal.listen_check', $listenCheckStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
                'listening' => 0,
            ]);
            return [];
        }
        $this->traceGatewayHealthStep('fetch.internal.listen_check', $listenCheckStartedAt, [
            'path' => $path,
            'plan' => $planLabel,
            'listening' => 1,
        ]);

        try {
            $ipcAction = $this->mapUpstreamStatusPathToIpcAction($path);
            if ($ipcAction !== '') {
                $ipcRequestStartedAt = microtime(true);
                $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $timeoutSeconds);
                $this->traceGatewayHealthStep('fetch.internal.ipc_request', $ipcRequestStartedAt, [
                    'path' => $path,
                    'plan' => $planLabel,
                    'action' => $ipcAction,
                    'timeout' => $timeoutSeconds,
                    'status' => (int)($ipcResponse['status'] ?? 0),
                ]);
                if (is_array($ipcResponse) && (int)($ipcResponse['status'] ?? 0) === 200) {
                    $data = $ipcResponse['data'] ?? null;
                    $this->traceGatewayHealthStep('fetch.internal.success', $fetchStartedAt, [
                        'path' => $path,
                        'plan' => $planLabel,
                    ]);
                    return is_array($data) ? $data : [];
                }
                $this->traceGatewayHealthStep('fetch.internal.non_200', $fetchStartedAt, [
                    'path' => $path,
                    'plan' => $planLabel,
                ]);
                return [];
            }
            $this->traceGatewayHealthStep('fetch.internal.no_ipc_action', $fetchStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
            ]);
            return [];
        } catch (Throwable) {
            $this->traceGatewayHealthStep('fetch.internal.exception', $fetchStartedAt, [
                'path' => $path,
                'plan' => $planLabel,
            ]);
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
        return $this->fetchUpstreamInternalStatusSync($host, $port, self::INTERNAL_UPSTREAM_STATUS_PATH, 5.0);
    }

    protected function fetchUpstreamHealthStatusSync(string $host, int $port): array {
        return $this->fetchUpstreamInternalStatusSync($host, $port, self::INTERNAL_UPSTREAM_HEALTH_PATH, 1.5);
    }

    protected function fetchUpstreamInternalStatusSync(string $host, int $port, string $path, float $timeoutSeconds = 0.0): array {
        if ($host === '' || $port <= 0) {
            return [];
        }

        $ipcAction = $this->mapUpstreamStatusPathToIpcAction($path);
        if ($ipcAction === '') {
            return [];
        }

        if ($timeoutSeconds <= 0) {
            $timeoutSeconds = $path === self::INTERNAL_UPSTREAM_STATUS_PATH ? 5.0 : 1.5;
        }
        try {
            $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $timeoutSeconds);
            if (!is_array($ipcResponse) || (int)($ipcResponse['status'] ?? 0) !== 200) {
                return [];
            }
            $data = $ipcResponse['data'] ?? null;
            return is_array($data) ? $data : [];
        } catch (Throwable) {
            return [];
        }
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
            'mysql_inflight' => array_sum(array_map(static fn(array $item) => (int)($item['mysql_inflight'] ?? 0), $selected)),
            'redis_inflight' => array_sum(array_map(static fn(array $item) => (int)($item['redis_inflight'] ?? 0), $selected)),
            'outbound_http_inflight' => array_sum(array_map(static fn(array $item) => (int)($item['outbound_http_inflight'] ?? 0), $selected)),
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

        // 反向代理有时会把 `Host` 里的非默认端口剥掉，但浏览器 Referer 仍保留了
        // 真正对外访问的 `ip:port`。遇到“同主机但 Host 无端口”的情况，要优先把
        // Referer 里的端口补回来，否则 dashboard socket 会错误回退到 80/443。
        $normalizedHost = $this->preferDashboardRefererAuthorityPort($normalizedHost, $normalizedReferer);

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

    /**
     * 在 Host 缺失端口但 Referer 明确带端口时，优先回填 Referer 里的端口。
     *
     * 只在 Host/Referer 的主机名完全一致且当前 Host 自己没有端口时才回填，
     * 避免 dev server、反向代理或跨域入口把不相关的端口误拼到 websocket 地址。
     *
     * @param string $authority 当前待使用的 authority
     * @param string $referer 当前页面 Referer
     * @return string
     */
    protected function preferDashboardRefererAuthorityPort(string $authority, string $referer): string {
        $authority = trim($authority);
        if ($authority === '' || $referer === '') {
            return $authority;
        }

        $authorityParts = parse_url(str_contains($authority, '://') ? $authority : ('tcp://' . $authority));
        $refererParts = parse_url($referer);
        if (!is_array($authorityParts) || !is_array($refererParts)) {
            return $authority;
        }

        $authorityHost = strtolower(trim((string)($authorityParts['host'] ?? $authorityParts['path'] ?? '')));
        $refererHost = strtolower(trim((string)($refererParts['host'] ?? '')));
        if ($authorityHost === '' || $refererHost === '' || $authorityHost !== $refererHost) {
            return $authority;
        }

        $authorityPort = (int)($authorityParts['port'] ?? 0);
        $refererPort = (int)($refererParts['port'] ?? 0);
        if ($authorityPort > 0 || $refererPort <= 0) {
            return $authority;
        }

        return $authorityHost . ':' . $refererPort;
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

    protected function buildDashboardVersionStatus(bool $forceRefresh = false): array {
        $cached = $this->dashboardVersionStatusCache['value'] ?? null;
        if (!$forceRefresh && is_array($cached) && (int)($this->dashboardVersionStatusCache['expires_at'] ?? 0) > time()) {
            return $cached;
        }
        $dashboardDir = SCF_ROOT . '/build/public/dashboard';
        $versionJson = $dashboardDir . '/version.json';
        $currentDashboardVersion = ['version' => '0.0.0'];
        if (file_exists($versionJson)) {
            $currentDashboardVersion = JsonHelper::recover(File::read($versionJson)) ?: $currentDashboardVersion;
        }

        $dashboardVersion = ['version' => '--'];
        $client = \Scf\Client\Http::create($this->appendRemoteVersionCacheBust(str_replace('version.json', 'dashboard-version.json', (string)ENV_VARIABLES['scf_update_server'])));
        try {
            // dashboard 首页的版本状态只是展示信息，不能因为外部版本源抖动
            // 把 gateway 控制面 `/server` 卡成“像挂了一样”。
            $response = $client->get(self::REMOTE_VERSION_REQUEST_TIMEOUT_SECONDS);
            if (!$response->hasError()) {
                $dashboardVersion = $response->getData();
            }
        } finally {
            $client->close();
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

    protected function buildFrameworkVersionStatus(?array $upstreams = null, bool $forceRefresh = false): array {
        $remoteVersion = $this->cachedFrameworkRemoteVersion($forceRefresh);
        $upstreams ??= $this->dashboardUpstreams();
        $selected = $this->selectGatewayBusinessUpstreams($upstreams);
        $activeRecord = function_exists('scf_read_framework_active_record') ? scf_read_framework_active_record() : null;
        $activeFrameworkVersion = (string)($activeRecord['version'] ?? $selected[0]['framework_build_version'] ?? FRAMEWORK_BUILD_VERSION);
        $activeFrameworkBuild = (string)($activeRecord['build'] ?? FRAMEWORK_BUILD_TIME);
        $activeFrameworkReady = function_exists('scf_framework_update_ready') && scf_framework_update_ready();

        return [
            'is_phar' => FRAMEWORK_IS_PHAR,
            'version' => $activeFrameworkVersion,
            'latest_version' => $remoteVersion['version'] ?? FRAMEWORK_BUILD_VERSION,
            'latest_build' => $remoteVersion['build'] ?? $activeFrameworkBuild,
            'build' => FRAMEWORK_BUILD_TIME,
            'gateway_version' => FRAMEWORK_BUILD_VERSION,
            'gateway_build' => FRAMEWORK_BUILD_TIME,
            'gateway_pending_restart' => $activeFrameworkReady,
            'update_ready' => $activeFrameworkReady,
        ];
    }

    protected function cachedFrameworkRemoteVersion(bool $forceRefresh = false): array {
        $cached = $this->frameworkRemoteVersionCache['value'] ?? null;
        if (!$forceRefresh && is_array($cached) && (int)($this->frameworkRemoteVersionCache['expires_at'] ?? 0) > time()) {
            return $cached;
        }

        $remoteVersion = [
            'version' => FRAMEWORK_BUILD_VERSION,
            'build' => FRAMEWORK_BUILD_TIME,
        ];

        $client = \Scf\Client\Http::create($this->appendRemoteVersionCacheBust((string)ENV_VARIABLES['scf_update_server']));
        try {
            $response = $client->get(self::REMOTE_VERSION_REQUEST_TIMEOUT_SECONDS);
            if (!$response->hasError()) {
                $remoteVersion = $response->getData();
            }
        } finally {
            $client->close();
        }

        $this->frameworkRemoteVersionCache = [
            'expires_at' => time() + self::FRAMEWORK_VERSION_CACHE_TTL_SECONDS,
            'value' => $remoteVersion,
        ];

        return $remoteVersion;
    }

    /**
     * 为远端版本文件补充时间戳，避免 gateway 控制面命中旧版缓存。
     *
     * proxy/gateway 模式下 dashboard 的最新版本信息完全依赖这里的远端元数据。
     * 如果继续请求固定 URL，发布成功后控制面会长时间停留在旧版本提示上。
     *
     * @param string $url 远端版本文件地址。
     * @return string 带 cache-busting 参数的地址。
     */
    protected function appendRemoteVersionCacheBust(string $url): string {
        if ($url === '') {
            return $url;
        }
        $separator = str_contains($url, '?') ? '&' : '?';
        return $url . $separator . 'time=' . time();
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

    protected function restartUpstreamsByHost(string $host): Result {
        $plans = $this->matchedPlansByHost($host);
        if (!$plans) {
            return Result::error('未找到可重启的业务实例:' . $host);
        }

        Console::info("【Gateway】开始重启业务实例: host={$host}, count=" . count($plans));
        $summary = $this->restartManagedUpstreams($plans);
        $this->pushDashboardStatus();
        if ($summary['failed_nodes']) {
            Console::warning(
                "【Gateway】业务实例重启部分失败: host={$host}, success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                . ', ' . $this->managedRestartFailureDetails($summary, $plans)
            );
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
                Console::info('【Gateway】应用尚未安装，业务入口切换到 gateway 控制面板承接安装流程');
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
                Console::warning('【Gateway】检测到控制面板文件变动，准备重启 Gateway');
                $this->scheduleGatewayShutdown(false);
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
            [],
            25,
            $this->gatewayLeaseEpoch(),
            $this->businessPort(),
            $this->gatewayLeaseGraceSeconds
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

        foreach ($this->managedUpstreamPlans as $index => $plan) {
            $originalVersion = (string)($plan['version'] ?? '');
            $originalHost = (string)($plan['host'] ?? '127.0.0.1');
            $originalPort = (int)($plan['port'] ?? 0);
            $originalKey = ($originalVersion . '@' . $originalHost . ':' . $originalPort);
            // 已经成功拉起过的 plan 先按原始 key 去重，避免先做端口改写后把“自家已启动实例”
            // 误当成新 plan 再启动一次。
            if ($originalPort > 0 && isset($this->bootstrappedManagedInstances[$originalKey])) {
                continue;
            }

            $plan = $this->resolveManagedBootstrapPlan($plan);
            $plan = $this->withManagedPlanLeaseBinding($plan);
            $this->managedUpstreamPlans[$index] = $plan;
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
                if (!$this->startupSummaryPending()) {
                    Console::info("【Gateway】启动业务实例: " . $this->describePlan($plan));
                }
                $this->upstreamSupervisor->sendCommand([
                    'action' => 'spawn',
                    'owner_epoch' => $this->gatewayLeaseEpoch(),
                    'plan' => $plan,
                ]);
                if (!$this->launcher->waitUntilServicesReady(
                    $host,
                    $port,
                    $rpcPort,
                    (int)($plan['start_timeout'] ?? 25),
                    200,
                    !$this->startupSummaryPending()
                )) {
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

            if ($this->startupSummaryPending()) {
                $this->recordStartupReadyInstance($plan);
            }
            $this->registerManagedPlan($plan, true);
            $this->instanceManager->removeOtherInstances($version, $host, $port);
            $rpcInfo = (int)($plan['rpc_port'] ?? 0) > 0 ? ', RPC:' . (int)$plan['rpc_port'] : '';
            $cutoverReady = $this->syncNginxProxyTargets('register_managed_plan_activate');
            if (!$cutoverReady) {
                Console::warning("【Gateway】业务实例已启动，但入口切换未完成: {$version} {$host}:{$port}{$rpcInfo}");
            }
        }
    }

    /**
     * 输出 Gateway 启动完成摘要。
     *
     * 上半部分保留基础运行信息，下半部分以结果表的方式收口启动阶段异步完成的
     * 子进程与清理结果，减少“一行一个 PID”带来的刷屏感。
     *
     * @param array<int, array{section:string, label:string, value:string}> $resultRows 启动阶段结果态行
     * @return void
     */
    protected function renderStartupInfo(array $resultRows = []): void {
        $appInfo = App::info()?->toArray() ?: [];
        $appVersion = $this->resolveAppVersion($appInfo);
        $publicVersion = $this->resolvePublicVersion($appInfo);
        $stateFile = $this->instanceManager->stateFile();
        $frameworkRoot = Root::dir();
        $frameworkBuildTime = FRAMEWORK_BUILD_TIME;
        $packageVersion = $frameworkBuildTime === 'development' ? 'development' : FRAMEWORK_BUILD_VERSION;
        $packagistVersion = SCF_COMPOSER_VERSION === 'development' ? '非composer引用' : SCF_COMPOSER_VERSION;
        $loadedFiles = count(get_included_files());
        $taskWorkers = (int)(Config::server()['task_worker_num'] ?? 0);
        $listenLabel = $this->nginxProxyModeEnabled() ? '业务入口' : '业务TCP入口';
        $listenValue = "{$this->host}:{$this->port}";
        $controlValue = $this->internalControlHost() . ':' . $this->controlPort();
        $rpcValue = $this->rpcPort > 0 ? ($this->host . ':' . $this->rpcPort) : '--';
        $masterPid = $this->serverMasterPid > 0 ? $this->serverMasterPid : (int)($this->server->master_pid ?? 0);
        $managerPid = $this->serverManagerPid > 0 ? $this->serverManagerPid : (int)($this->server->manager_pid ?? 0);
        $infoLines = [
            '------------------Gateway启动完成------------------',
            '应用指纹：' . APP_FINGERPRINT,
            '运行系统：' . OS_ENV,
            '运行环境：' . SERVER_RUN_ENV,
            '应用目录：' . APP_DIR_NAME,
            '源码类型：' . APP_SRC_TYPE,
            '节点角色：' . SERVER_ROLE,
            '文件加载：' . $loadedFiles,
            '环境版本：' . swoole_version(),
            '框架源码：' . $frameworkRoot,
            'Packagist：' . $packagistVersion,
            '打包版本：' . $packageVersion,
            '打包时间：' . $frameworkBuildTime,
            '工作进程：' . $this->workerNum,
            '任务进程：' . $taskWorkers,
            '主机地址：' . SERVER_HOST,
            '应用版本：' . $appVersion,
            '资源版本：' . $publicVersion,
            '流量模式：' . $this->trafficMode(),
            $listenLabel . '：' . $listenValue,
            '控制面板：' . $controlValue,
            'RPC监听：' . $rpcValue,
            '进程信息：Master:' . $masterPid . ',Manager:' . $managerPid,
            '状态文件：' . $stateFile,
        ];
        $formattedResultRows = $this->formatStartupResultRows($resultRows);
        if ($formattedResultRows) {
            $infoLines = array_merge($infoLines, $formattedResultRows);
        }
        $infoLines[] = '--------------------------------------------------';
        $info = implode("\n", $infoLines);
        Console::write(Color::cyan($info));
    }

    protected function createBusinessTrafficListener(): void {
        if (!$this->tcpRelayModeEnabled()) {
            return;
        }
        if ($this->controlPort() === $this->port) {
            throw new RuntimeException('tcp流量模式下控制面板端口不能与业务端口相同');
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
