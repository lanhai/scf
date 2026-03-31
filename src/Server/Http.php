<?php

namespace Scf\Server;

use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\InflightCounter;
use Scf\Core\Key;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeTable;
use Scf\Core\Table\SocketConnectionTable;
use Scf\Server\Listener\Listener;
use Scf\Server\Proxy\ConsoleRelay;
use Scf\Server\Proxy\GatewayLease;
use Scf\Server\Proxy\LocalIpc;
use Scf\Server\Proxy\LocalIpcServer;
use Scf\Server\Task\CrontabManager;
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\MemoryMonitor;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Throwable;
use const SIGKILL;


class Http extends \Scf\Core\Server {
    protected const RELOAD_DIAGNOSTIC_GRACE_SECONDS = 10;
    protected const RELOAD_DIAGNOSTIC_INTERVAL_MS = 5000;

    /**
     * @var Server
     */
    protected Server $server;

    /**
     * @var string 当前服务器角色
     */
    protected string $role;
    /**
     * @var string id号
     */
    protected string $id = '';
    /**
     * @var string 绑定host
     */
    protected string $bindHost = '0.0.0.0';
    /**
     * @var int 绑定端口
     */
    protected int $bindPort = 0;
    /**
     * @var string 本机host地址
     */
    protected string $host;
    /**
     * @var string 节点名称
     */
    protected string $name;

    /**
     * @var int 启动时间
     */
    protected int $started = 0;

    protected SubProcessManager $subProcessManager;
    protected ?int $reloadDiagnosticTimerId = null;
    protected int $reloadDiagnosticStartedAt = 0;
    protected ?LocalIpcServer $localIpcServer = null;
    protected int $upstreamGatewayPort = 0;
    protected int $upstreamOwnerEpoch = 0;
    protected int $upstreamGatewayLeaseGraceSeconds = 20;
    protected int $upstreamGatewayRestartGraceSeconds = 120;
    protected int $upstreamGatewayLeaseLastHealthyAt = 0;
    protected int $upstreamGatewayLeaseLastWarnAt = 0;
    protected string $upstreamGatewayLeaseLastReason = '';
    protected bool $upstreamGatewayLeaseFenceTriggered = false;
    protected ?int $upstreamGatewayLeaseWatcherTimerId = null;
    protected bool $upstreamGatewayLeaseWatcherRunning = false;
    protected int $upstreamLeasePeerScanSkippedWarnAt = 0;

    protected function isProxyUpstreamMode(): bool {
        return defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true;
    }

    /**
     * 初始化 upstream 对 gateway lease 的 owner 绑定参数。
     *
     * 这些参数由 gateway 在 spawn 命令里注入到 CLI flags：
     * - `gateway_port`：租约文件定位键；
     * - `gateway_epoch`：owner 代际；
     * - `gateway_lease_grace`：租约失效后的宽限窗口。
     *
     * @param array<string, mixed> $serverConfig server 配置。
     * @return void
     */
    protected function initializeUpstreamGatewayLeaseBinding(array $serverConfig): void {
        $this->upstreamGatewayPort = (int)(Manager::instance()->getOpt('gateway_port') ?: 0);
        $this->upstreamOwnerEpoch = (int)(Manager::instance()->getOpt('gateway_epoch') ?: 0);
        $configuredGrace = (int)(Manager::instance()->getOpt('gateway_lease_grace') ?: ($serverConfig['gateway_lease_grace'] ?? 20));
        $this->upstreamGatewayLeaseGraceSeconds = max(3, $configuredGrace);
        $configuredRestartGrace = (int)(Manager::instance()->getOpt('gateway_restart_grace') ?: ($serverConfig['gateway_lease_restart_grace'] ?? max(120, $this->upstreamGatewayLeaseGraceSeconds * 3)));
        $this->upstreamGatewayRestartGraceSeconds = max($this->upstreamGatewayLeaseGraceSeconds, $configuredRestartGrace);

        if ($this->upstreamGatewayPort <= 0 || $this->upstreamOwnerEpoch <= 0) {
            Console::warning(
                '【Server】当前 upstream 未绑定有效 gateway lease 参数，孤儿自回收保护未启用: '
                . "gateway_port={$this->upstreamGatewayPort}, owner_epoch={$this->upstreamOwnerEpoch}",
                false
            );
            return;
        }

        Console::info(
            '【Server】upstream 绑定 gateway lease: '
            . "port={$this->upstreamGatewayPort}, owner_epoch={$this->upstreamOwnerEpoch}, grace={$this->upstreamGatewayLeaseGraceSeconds}s"
            . ", restart_grace={$this->upstreamGatewayRestartGraceSeconds}s",
            false
        );
    }

    /**
     * 启动 upstream 的 gateway lease 监控器。
     *
     * 监控器运行在 upstream master/onStart 侧，不进入 worker 生命周期，
     * 满足“控制链路不污染 worker”的约束。
     *
     * @return void
     */
    protected function startUpstreamGatewayLeaseWatcher(): void {
        if ($this->upstreamGatewayLeaseWatcherRunning || $this->upstreamGatewayLeaseWatcherTimerId !== null) {
            return;
        }
        if (!$this->isProxyUpstreamMode() || $this->upstreamGatewayPort <= 0 || $this->upstreamOwnerEpoch <= 0) {
            return;
        }
        $this->upstreamGatewayLeaseLastHealthyAt = time();
        $this->upstreamGatewayLeaseLastWarnAt = 0;
        $this->upstreamGatewayLeaseLastReason = '';
        $this->upstreamGatewayLeaseFenceTriggered = false;
        $this->upstreamGatewayLeaseWatcherRunning = true;
        Console::info(
            "【Server】启动 gateway lease watcher: port={$this->upstreamGatewayPort}, owner_epoch={$this->upstreamOwnerEpoch}",
            false
        );
        // lease watcher 需要在 upstream master 的完整生命周期内都可用：
        // 即使业务实例已经进入 draining/shutdown，也要继续观察 lease，
        // 防止 shutdown 卡住时出现“serverIsAlive=false 但仍占端口”的孤儿进程。
        // 这里统一用 Timer 驱动，避免依赖 onStart 协程调度时机。
        if ($this->upstreamGatewayLeaseWatcherTimerId !== null) {
            Timer::clear($this->upstreamGatewayLeaseWatcherTimerId);
            $this->upstreamGatewayLeaseWatcherTimerId = null;
        }
        $this->upstreamGatewayLeaseWatcherTimerId = Timer::tick(1000, function (int $timerId): void {
            if (!$this->upstreamGatewayLeaseWatcherRunning) {
                Timer::clear($timerId);
                $this->upstreamGatewayLeaseWatcherTimerId = null;
                return;
            }
            $this->checkUpstreamGatewayLeaseHealth();
        });
        $this->checkUpstreamGatewayLeaseHealth();
    }

    /**
     * 停止 upstream 的 gateway lease 监控器。
     *
     * @return void
     */
    protected function stopUpstreamGatewayLeaseWatcher(): void {
        $this->upstreamGatewayLeaseWatcherRunning = false;
        if ($this->upstreamGatewayLeaseWatcherTimerId === null) {
            return;
        }
        Timer::clear($this->upstreamGatewayLeaseWatcherTimerId);
        $this->upstreamGatewayLeaseWatcherTimerId = null;
    }

    /**
     * 评估当前 owner lease 是否仍然健康。
     *
     * @return void
     */
    protected function checkUpstreamGatewayLeaseHealth(): void {
        if ($this->upstreamGatewayLeaseFenceTriggered) {
            return;
        }
        $now = time();
        $lease = GatewayLease::readLease($this->upstreamGatewayPort, SERVER_ROLE);
        if (!is_array($lease)) {
            $this->handleUnhealthyGatewayLease('lease_missing', false);
            return;
        }

        $leaseEpoch = (int)($lease['epoch'] ?? 0);
        $leaseState = (string)($lease['state'] ?? '');
        $expiresAt = (int)($lease['expires_at'] ?? 0);

        // owner epoch 不一致属于“控制面所有权漂移”，不能再等待宽限，直接围栏退出。
        if ($leaseEpoch !== $this->upstreamOwnerEpoch) {
            $this->triggerUpstreamLeaseFence(
                "owner_epoch_changed(expected={$this->upstreamOwnerEpoch}, current={$leaseEpoch})"
            );
            return;
        }

        if ($leaseState === 'stopped') {
            $this->triggerUpstreamLeaseFence("gateway_stopped(epoch={$leaseEpoch})");
            return;
        }

        if (($leaseState === 'running' || $leaseState === 'restarting') && ($expiresAt === 0 || $expiresAt >= $now)) {
            $this->upstreamGatewayLeaseLastHealthyAt = $now;
            $this->upstreamGatewayLeaseLastReason = '';
            return;
        }

        if ($leaseState === 'restarting' && $this->shouldTolerateExpiredRestartLease($lease, $now)) {
            return;
        }

        $reason = $leaseState === ''
            ? 'lease_state_invalid'
            : "lease_{$leaseState}_expired";
        // restarting 一旦超过恢复窗口就应立即围栏，避免旧 owner 长时间悬挂。
        $forceImmediate = $leaseState === 'restarting';
        $this->handleUnhealthyGatewayLease($reason, $forceImmediate);
    }

    /**
     * 判断“restarting 已过期”是否仍处于可恢复窗口。
     *
     * gateway 计划重启时会先把 lease 写成 restarting。若控制面恢复稍慢，租约 TTL
     * 可能先过期；此时 upstream 不应立刻自杀，而应在 restart grace 内继续等待
     * 同 epoch owner 恢复。
     *
     * @param array<string, mixed> $lease 当前 lease 快照
     * @param int $now 当前时间戳
     * @return bool true 表示仍在恢复窗口内，应继续等待
     */
    protected function shouldTolerateExpiredRestartLease(array $lease, int $now): bool {
        $meta = (array)($lease['meta'] ?? []);
        $restartGrace = max(
            $this->upstreamGatewayRestartGraceSeconds,
            (int)($meta['restart_grace_seconds'] ?? 0)
        );
        if ($restartGrace <= 0) {
            return false;
        }

        $restartStartedAt = (int)($meta['restart_started_at'] ?? 0);
        if ($restartStartedAt <= 0) {
            // 兼容历史租约没有 restart_started_at 的场景。
            $restartStartedAt = (int)($lease['heartbeat_at'] ?? ($lease['updated_at'] ?? 0));
        }
        if ($restartStartedAt <= 0) {
            return false;
        }

        $elapsed = max(0, $now - $restartStartedAt);
        if ($elapsed > $restartGrace) {
            return false;
        }

        $this->upstreamGatewayLeaseLastHealthyAt = $now;
        if (
            $this->upstreamGatewayLeaseLastReason !== 'lease_restarting_recovering'
            || ($now - $this->upstreamGatewayLeaseLastWarnAt) >= 5
        ) {
            $this->upstreamGatewayLeaseLastReason = 'lease_restarting_recovering';
            $this->upstreamGatewayLeaseLastWarnAt = $now;
            Console::warning(
                "【Server】gateway restarting租约已过期但仍在恢复窗口，继续等待: "
                . "elapsed={$elapsed}s, restart_grace={$restartGrace}s, owner_epoch={$this->upstreamOwnerEpoch}",
                false
            );
        }
        return true;
    }

    /**
     * 处理非致命 lease 异常（可走宽限窗口）。
     *
     * @param string $reason 异常原因。
     * @param bool $forceImmediate 是否跳过宽限直接围栏。
     * @return void
     */
    protected function handleUnhealthyGatewayLease(string $reason, bool $forceImmediate): void {
        $now = time();
        if ($this->upstreamGatewayLeaseLastHealthyAt <= 0) {
            $this->upstreamGatewayLeaseLastHealthyAt = $now;
        }
        $elapsed = $now - $this->upstreamGatewayLeaseLastHealthyAt;
        if ($forceImmediate || $elapsed >= $this->upstreamGatewayLeaseGraceSeconds) {
            $this->triggerUpstreamLeaseFence("{$reason}, elapsed={$elapsed}s");
            return;
        }
        if (
            $reason !== $this->upstreamGatewayLeaseLastReason
            || ($now - $this->upstreamGatewayLeaseLastWarnAt) >= 5
        ) {
            $this->upstreamGatewayLeaseLastReason = $reason;
            $this->upstreamGatewayLeaseLastWarnAt = $now;
            Console::warning(
                "【Server】检测到gateway lease异常，进入宽限等待: reason={$reason}, "
                . "elapsed={$elapsed}s, grace={$this->upstreamGatewayLeaseGraceSeconds}s",
                false
            );
        }
    }

    /**
     * 触发 upstream 的自围栏回收。
     *
     * @param string $reason 围栏原因。
     * @return void
     */
    protected function triggerUpstreamLeaseFence(string $reason): void {
        if ($this->upstreamGatewayLeaseFenceTriggered) {
            return;
        }
        $this->upstreamGatewayLeaseFenceTriggered = true;
        Console::warning("【Server】Gateway lease失效，开始自回收: {$reason}", false);
        $masterPid = (int)($this->server->master_pid ?? getmypid());
        $managerPid = (int)($this->server->manager_pid ?? 0);
        $this->quiesceBusinessPlane();
        $this->shutdown();

        // 这里是 orphan fence 的收口路径：如果 master 先退而 manager 仍存活，
        // manager/worker 会变成孤儿继续占端口。先尝试立刻回收 manager，避免
        // 只关掉 master 却留下服务平面残留。
        if ($managerPid > 0 && $managerPid !== getmypid() && @Process::kill($managerPid, 0)) {
            Console::warning("【Server】执行 lease 围栏即时回收 manager: pid={$managerPid}", false);
            @Process::kill($managerPid, SIGKILL);
        }
        // manager/master 被围栏回收后，worker 可能短暂脱离父子树并继续占端口。
        // 这里按 gateway_port + owner_epoch 精确清理同代 upstream peers，避免
        // 把新代 gateway 已接管的 upstream 误杀。
        $this->forceKillPeerUpstreamProcessesByEpoch('immediate');

        $forceExitAfter = max(10, (int)(Config::server()['proxy_upstream_force_exit_after'] ?? 30));
        Timer::after($forceExitAfter * 1000, function () use ($masterPid, $managerPid): void {
            $this->forceKillPeerUpstreamProcessesByEpoch('timeout');
            $targets = array_values(array_unique(array_filter([
                $managerPid,
                $masterPid,
                (int)getmypid(),
            ], static fn(int $pid): bool => $pid > 0)));
            foreach ($targets as $pid) {
                if (!@Process::kill($pid, 0)) {
                    continue;
                }
                Console::warning("【Server】自回收超时，执行强制退出: pid={$pid}", false);
                @Process::kill($pid, SIGKILL);
            }
        });
    }

    /**
     * 强制回收当前 owner_epoch 下的同代 upstream 进程。
     *
     * 仅匹配 `gateway_upstream start` 且 `gateway_port/gateway_epoch` 完全一致的命令行，
     * 用于 lease 围栏路径下清理失去父进程的残留 worker。
     *
     * @param string $phase 日志阶段标记（immediate/timeout）。
     * @return void
     */
    protected function forceKillPeerUpstreamProcessesByEpoch(string $phase): void {
        $pids = $this->collectPeerUpstreamPidsByEpoch();
        if (!$pids) {
            return;
        }
        foreach ($pids as $pid) {
            if ($pid <= 0 || $pid === getmypid() || !@Process::kill($pid, 0)) {
                continue;
            }
            Console::warning("【Server】lease 围栏回收同代upstream进程: phase={$phase}, pid={$pid}", false);
            @Process::kill($pid, SIGKILL);
        }
    }

    /**
     * 扫描并收集与当前 owner_epoch 同代的 upstream 进程 PID。
     *
     * @return array<int>
     */
    protected function collectPeerUpstreamPidsByEpoch(): array {
        if ($this->upstreamGatewayPort <= 0 || $this->upstreamOwnerEpoch <= 0) {
            return [];
        }
        // lease watcher 是 Timer 驱动，运行时可能处在协程上下文。
        // 这里若直接 shell_exec，会被 Swoole 接管为 Coroutine::exec；
        // 在“仅此一个协程 + 无可唤醒事件”场景会触发 all coroutines asleep deadlock。
        // 因此协程上下文下跳过 peer 扫描，只走本实例 manager/master 的围栏回收链，
        // peer orphan 由 gateway 启动期 orphan reclaim 继续兜底。
        if (Coroutine::getCid() > 0) {
            $now = time();
            if ($this->upstreamLeasePeerScanSkippedWarnAt <= 0 || ($now - $this->upstreamLeasePeerScanSkippedWarnAt) >= 15) {
                $this->upstreamLeasePeerScanSkippedWarnAt = $now;
                Console::warning(
                    "【Server】lease 围栏跳过协程上下文 peer 扫描，避免 Coroutine::exec 死锁: "
                    . "gateway_port={$this->upstreamGatewayPort}, owner_epoch={$this->upstreamOwnerEpoch}",
                    false
                );
            }
            return [];
        }
        $output = @shell_exec('ps -axo pid=,command=');
        if (!is_string($output) || trim($output) === '') {
            return [];
        }

        $gatewayPortFlag = '-gateway_port=' . $this->upstreamGatewayPort;
        $gatewayEpochFlag = '-gateway_epoch=' . $this->upstreamOwnerEpoch;
        $matches = [];
        foreach (preg_split('/\r?\n/', $output) ?: [] as $line) {
            $line = trim((string)$line);
            if ($line === '' || !preg_match('/^(\d+)\s+(.+)$/', $line, $parts)) {
                continue;
            }
            $pid = (int)$parts[1];
            $command = $parts[2];
            if ($pid <= 0 || $pid === getmypid()) {
                continue;
            }
            if (!str_contains($command, 'boot gateway_upstream start')) {
                continue;
            }
            if (!str_contains($command, $gatewayPortFlag) || !str_contains($command, $gatewayEpochFlag)) {
                continue;
            }
            $matches[$pid] = $pid;
        }
        return array_values($matches);
    }

    /**
     * @param string $role
     * @param string $host
     * @param int $port
     */
    public function __construct(string $role, string $host = '0.0.0.0', int $port = 0) {
        $this->bindHost = $host;
        $this->bindPort = $port;
        $this->role = $role;
        $this->started = time();
        $this->id = APP_NODE_ID;
        $this->host = SERVER_HOST;
    }

    /**
     * 创建一个服务器对象
     * @param string $role
     * @param string $host
     * @param int $port
     * @return Http
     */
    public static function create(string $role, string $host = '0.0.0.0', int $port = 0): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class($role, $host, $port);
        }
        return self::$_instances[$class];
    }

    public static function server(): ?Server {
        try {
            return self::instance()->server;
        } catch (Throwable) {
            return null;
        }
    }

    /**
     * 启动服务
     * @return void
     */
    public function start(): void {
        // 一键协程化:
        // 这里统一禁用 FILE hook，而不是只在 proxy 模式禁用。
        //
        // 原因：
        // 1) gateway/upstream 启动链上存在大量文件 IO（lease、registry、runtime 标记等）；
        // 2) FILE/STDIO/STREAM_FUNCTION hook 与
        //    Swoole “create server 前不可发生异步文件操作”的约束组合后，
        //    会把普通 fopen/file_get_contents 语义升级成运行时致命错误；
        // 3) 控制面稳定性优先，文件 IO 保持同步语义，网络 hook 仍保留协程收益。
        $hookFlags = SWOOLE_HOOK_ALL;
        foreach (['SWOOLE_HOOK_FILE', 'SWOOLE_HOOK_STDIO', 'SWOOLE_HOOK_STREAM_FUNCTION'] as $hookConst) {
            if (defined($hookConst)) {
                $hookFlags &= ~constant($hookConst);
            }
        }
        Coroutine::set(['hook_flags' => $hookFlags]);
        //初始化内存表,放在最前面保证所有进程间能共享
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
            'Scf\Core\Table\ServerNodeStatusTable'
        ]);
//        Atomic::register([
//            'Scf\Core\Table\RequestAtomic',
//        ]);
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(false);
        Runtime::instance()->serverIsAlive(true);
        Runtime::instance()->set(Key::RUNTIME_SERVER_STARTED_AT, $this->started);
        //启动master节点管理面板服务器
        if (!$this->isProxyUpstreamMode()) {
            Dashboard::start();
        }
        //加载服务器配置
        $serverConfig = Config::server();
        if ($this->isProxyUpstreamMode()) {
            $this->initializeUpstreamGatewayLeaseBinding($serverConfig);
        }
        if (defined('APP_MODULE_STYLE') === false) {
            define('APP_MODULE_STYLE', $serverConfig['module_style'] ?? APP_MODULE_STYLE_MULTI);
        }
        //启动masterDB(redis协议)服务器
        //MasterDB::start(MDB_PORT);
        $this->bindPort = $this->bindPort ?: ($serverConfig['port'] ?? 9580);// \Scf\Core\Server::getUseablePort($this->bindPort ?: ($serverConfig['port'] ?? 9580));
        !defined('MAX_REQUEST_LIMIT') and define('MAX_REQUEST_LIMIT', $serverConfig['max_request_limit'] ?? 1280);
        !defined('SLOW_LOG_TIME') and define('SLOW_LOG_TIME', $serverConfig['slow_log_time'] ?? 10000);
        !defined('MAX_MYSQL_EXECUTE_LIMIT') and define('MAX_MYSQL_EXECUTE_LIMIT', $serverConfig['max_mysql_execute_limit'] ?? 1000);
        !defined('ENABLE_ADAPTIVE_OVERLOAD_SHEDDING') and define(
            'ENABLE_ADAPTIVE_OVERLOAD_SHEDDING',
            (bool)($serverConfig['adaptive_overload_shedding'] ?? true)
        );
        !defined('OVERLOAD_SOFT_RATIO') and define(
            'OVERLOAD_SOFT_RATIO',
            max(0.5, min(0.99, (float)($serverConfig['overload_soft_ratio'] ?? 0.85)))
        );
        !defined('OVERLOAD_HARD_RATIO') and define(
            'OVERLOAD_HARD_RATIO',
            max((float)OVERLOAD_SOFT_RATIO + 0.01, min(1.5, (float)($serverConfig['overload_hard_ratio'] ?? 1.0)))
        );
        !defined('OVERLOAD_MAX_SHED_PROBABILITY') and define(
            'OVERLOAD_MAX_SHED_PROBABILITY',
            max(0.05, min(0.95, (float)($serverConfig['overload_max_shed_probability'] ?? 0.8)))
        );
        !defined('REQUEST_TODAY_COUNTER_FLUSH_SIZE') and define(
            'REQUEST_TODAY_COUNTER_FLUSH_SIZE',
            max(1, (int)($serverConfig['request_today_counter_flush_size'] ?? 64))
        );
        !defined('REQUEST_TODAY_COUNTER_FLUSH_INTERVAL') and define(
            'REQUEST_TODAY_COUNTER_FLUSH_INTERVAL',
            max(1, (int)($serverConfig['request_today_counter_flush_interval'] ?? 1))
        );
        //是否允许跨域请求
        define('SERVER_ALLOW_CROSS_ORIGIN', (bool)($serverConfig['allow_cross_origin'] ?? false));
        //开启日志推送
        Console::enablePush($serverConfig['enable_log_push'] ?? STATUS_ON);
        if ($this->isProxyUpstreamMode()) {
            ConsoleRelay::setGatewayPort((int)(Manager::instance()->getOpt('gateway_port') ?: 0));
            Console::setPushHandler(static function (string $time, string $message): void {
                $gatewayPort = ConsoleRelay::gatewayPort();
                ConsoleRelay::reportToLocalGateway(
                    $time,
                    $message,
                    'upstream',
                    SERVER_HOST . ':' . ($gatewayPort > 0 ? $gatewayPort : Runtime::instance()->httpPort())
                );
            });
        }
        //实例化服务器
        $this->server = new Server($this->bindHost, mode: SWOOLE_PROCESS);
        $setting = [
            'worker_num' => $serverConfig['worker_num'] ?? 8,
            'max_wait_time' => $serverConfig['max_wait_time'] ?? 120,
            'reload_async' => true,
            'enable_reuse_port' => true,
            'daemonize' => false,//Manager::instance()->issetOpt('d'),
            'log_file' => APP_PATH . '/log/server.log',
            'pid_file' => SERVER_MASTER_PID_FILE,
            'task_worker_num' => $serverConfig['task_worker_num'] ?? 4,
            'task_enable_coroutine' => true,
            'max_connection' => $serverConfig['max_connection'] ?? 4096,//最大连接数
            'max_coroutine' => $serverConfig['max_coroutine'] ?? 1024 * 3,//最多启动多少个携程
            'max_concurrency' => $serverConfig['max_concurrency'] ?? 1024,//最高并发
            'package_max_length' => $serverConfig['package_max_length'] ?? 10 * 1024 * 1024
        ];
        if (!empty($serverConfig['static_handler_locations']) || Env::isDev()) {
            $setting['document_root'] = APP_PATH . '/public';
            $setting['enable_static_handler'] = true;
            $setting['http_autoindex'] = true;
            $setting['http_index_files'] = ['index.html'];
            $setting['static_handler_locations'] = $serverConfig['static_handler_locations'] ?? ['/cp', '/asset'];
        }
        $this->server->set($setting);
        //监听HTTP&socket连接
        try {
            // gateway 分配 upstream 端口时使用的是“真实监听态”判定；这里必须保持同一语义，
            // 否则会出现“gateway 认为端口可用，但 upstream 启动前用 bind 判定又误报占用”的分裂。
            $httpPortOccupied = $this->isProxyUpstreamMode()
                ? self::isListeningPortInUse($this->bindPort)
                : self::isPortInUse($this->bindPort, $this->bindHost);
            if ($httpPortOccupied) {
                if ($this->isProxyUpstreamMode()) {
                    throw new RuntimeException('upstream HTTP端口已被占用，等待gateway重新分配:' . $this->bindHost . ':' . $this->bindPort);
                }
                if (!self::killProcessByPort($this->bindPort)) {
                    $this->log(Color::red('HTTP服务端口[' . $this->bindPort . ']被占用,尝试结束进程失败'));
                    // 稍等片刻再退出/或由外层管理器重试
                    usleep(1000 * 1000);
                    exit(1);
                }
            }
            $httpServer = $this->server->listen($this->bindHost, $this->bindPort, SWOOLE_SOCK_TCP);
            if ($httpServer === false) {
                throw new RuntimeException('listen returned false, 端口可能仍未释放:' . $this->bindHost . ':' . $this->bindPort);
            }
            $httpServer->set([
                'package_max_length' => $serverConfig['package_max_length'] ?? 10 * 1024 * 1024,
                'open_http_protocol' => true,
                'open_http2_protocol' => true,
                'open_websocket_protocol' => true,
                // 'heartbeat_check_interval' => 60,
                // 'heartbeat_idle_time' => 180
            ]);
            Runtime::instance()->httpPort($this->bindPort);
        } catch (Throwable $exception) {
            $this->log(Color::yellow('HTTP服务端口[' . $this->bindPort . ']监听启动失败:' . $exception->getMessage()));
            // 稍等片刻再退出/或由外层管理器重试
            usleep(1000 * 1000);
            exit(1);
        }
        //监听RPC服务(tcp)请求
        $rport = $this->isProxyUpstreamMode() ? (RPC_PORT ?: 0) : (RPC_PORT ?: ($serverConfig['rpc_port'] ?? 0));
        if ($rport) {
            try {
                $rpcPort = $rport;
                $rpcBindHost = $this->isProxyUpstreamMode() ? '127.0.0.1' : '0.0.0.0';
                // 尝试杀掉占用端口的进程
                // upstream RPC 端口同样采用监听态判定，避免 bind 误判导致不必要的重试。
                $rpcPortOccupied = $this->isProxyUpstreamMode()
                    ? self::isListeningPortInUse($rpcPort)
                    : self::isPortInUse($rpcPort, $rpcBindHost);
                if ($rpcPortOccupied) {
                    if ($this->isProxyUpstreamMode()) {
                        throw new RuntimeException('upstream RPC端口已被占用，等待gateway重新分配:' . '127.0.0.1:' . $rpcPort);
                    }
                    $this->log(Color::yellow('RPC服务端口[' . $rpcPort . ']被占用,尝试结束进程'));
                    if (!self::killProcessByPort($rpcPort)) {
                        $this->log(Color::red('RPC服务端口[' . $rpcPort . ']被占用,尝试结束进程失败'));
                        usleep(1000 * 1000);
                        exit(1);
                    }
                }
                /** @var Server $rpcServer */
                $rpcServer = $this->server->listen($rpcBindHost, $rpcPort, SWOOLE_SOCK_TCP);
                if ($rpcServer === false) {
                    throw new RuntimeException('listen returned false, RPC端口可能仍未释放:' . $rpcBindHost . ':' . $rpcPort);
                }
                $rpcServer->set([
                    'package_max_length' => $serverConfig['package_max_length'] ?? 20 * 1024 * 1024,
                    'open_http_protocol' => false,
                    'open_http2_protocol' => false,
                    'open_websocket_protocol' => false,
                    'open_length_check' => true,
                    'package_length_type' => 'N',             // 无符号长整型，网络字节序（4字节）
                    'package_length_offset' => 0,               // 包头长度字段在第0字节开始
                    'package_body_offset' => 4,               // 从第4字节开始是包体
                ]);
            } catch (Throwable $exception) {
                $this->log(Color::red('RPC服务端口[' . $rpcPort . ']监听启动失败:' . $exception->getMessage()));
                // 稍等片刻再退出/或由外层管理器重试
                usleep(1000 * 1000);
                exit(1);
            }
            Runtime::instance()->rpcPort($rpcPort);
        }
        Listener::register([
            'SocketListener',
            'CgiListener',
            'WorkerListener',
            'RpcListener',
            'TaskListener'
        ]);
        $this->server->on("BeforeReload", function (Server $server) {
            $this->log(Color::yellow('服务器正在重启'));
            Runtime::instance()->serverIsReady(false);
            Runtime::instance()->serverIsDraining(true);
            //增加服务器重启次数计数
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
            $disconnected = $this->disconnectAllClients($server);
            $disconnected > 0 and $this->log(Color::yellow("已断开 {$disconnected} 个客户端连接"));
            $this->startReloadDiagnostics($server);
        });
        $this->server->on("AfterReload", function () {
            //重置执行中的请求数统计
            Counter::instance()->set(Key::COUNTER_REQUEST_PROCESSING, 0);
            $this->stopReloadDiagnostics('服务器重启诊断结束：worker 已完成重载');
            $this->log('第' . Counter::instance()->get(Key::COUNTER_SERVER_RESTART) . '次重启完成');
        });
//        $this->server->on('pipeMessage', function ($server, $src_worker_id, $data) {
//            echo "#{$server->worker_id} message from #$src_worker_id: $data\n";
//        });
        //服务器销毁前
        $this->server->on("BeforeShutdown", function (Server $server) {
            $this->log(Color::yellow('服务器正在关闭'));
            $this->stopReloadDiagnostics();
            // upstream 进入 shutdown 只是“开始平滑退出”，并不代表进程立刻结束。
            // 这里不能提前关闭 lease watcher / local IPC：
            // 1) lease watcher 需要持续工作，才能在 gateway lease 失效后围栏回收；
            // 2) local IPC 需要保留到最终 Shutdown，确保 gateway 还能持续下发
            //    quiesce/shutdown/health 查询，不会出现“进程还活着但控制链路先断”的窗口。
            $disconnected = $this->disconnectAllClients($server);
            $disconnected > 0 and $this->log(Color::yellow("已断开 {$disconnected} 个客户端连接"));
        });
        $this->server->on("Shutdown", function (Server $server) {
            if ($this->isProxyUpstreamMode()) {
                // upstream 的本地 IPC 挂在 master/onStart；Shutdown 再补一次收口，确保 accept 协程跟随 master 退出。
                $this->stopUpstreamGatewayLeaseWatcher();
                $this->stopLocalIpcServer();
            }
        });
        $this->server->on("ManagerStart", function (Server $server) {
            //Console::info('ManagerStart');
            //MemoryMonitor::start('Server:Manager');
            if ($this->isProxyUpstreamMode()) {
                // lease watcher 需要覆盖 manager 生命周期：
                // 当 upstream master 异常退出但 manager 仍残留时，仍可继续执行围栏回收。
                $this->startUpstreamGatewayLeaseWatcher();
            }
        });
        $this->server->on("ManagerStop", function (Server $server) {
            //Console::info('onManagerStop');
            //MemoryMonitor::stop();
            if ($this->isProxyUpstreamMode()) {
                $this->stopUpstreamGatewayLeaseWatcher();
            }
        });
        //服务器完成启动
        $this->server->on('start', function (Server $server) use ($serverConfig) {
            MemoryMonitor::start('Server:Master');
            if ($this->isProxyUpstreamMode()) {
                // 新 upstream 的 master/onStart 先挂载应用上下文，这样本地 IPC 读到的
                // appid/version/modules 与业务实例本身保持同一代代码语义。
                App::mount();
                // upstream 控制 IPC 属于 server 自己的控制能力，直接挂在 onStart，不进入任何 worker 生命周期。
                $this->startLocalIpcServer();
                // gateway lease watcher 统一在 ManagerStart 启动，避免 master/manager 双进程各启一份。
            }
            //每三秒将服务器运行状态写入内存表
            Timer::tick(1000 * 3, function ($tid) use ($server) {
                if (!Runtime::instance()->serverIsAlive()) {
                    Timer::clear($tid);
                    return;
                }
                Runtime::instance()->set('SERVER_STATS', $server->stats());
            });
            if ($this->isProxyUpstreamMode()) {
                ConsoleRelay::refreshSubscriptionFromGateway();
                Timer::tick(1000 * 5, function ($tid) {
                    if (!Runtime::instance()->serverIsAlive()) {
                        Timer::clear($tid);
                        return;
                    }
                    ConsoleRelay::refreshSubscriptionFromGateway();
                });
            }

            File::write(SERVER_DASHBOARD_PORT_FILE, Runtime::instance()->dashboardPort());
            //自动更新
            !$this->isProxyUpstreamMode() && APP_AUTO_UPDATE == STATUS_ON and App::checkVersion();
        });

        try {
            if (!$this->isProxyUpstreamMode()) {
                $this->subProcessManager = new SubProcessManager($this->server, $serverConfig, []);
                $this->subProcessManager->start();
            }
            $this->server->start();
        } catch (Throwable $exception) {
            Console::error($exception->getMessage());
        }
    }


    /**
     * 推送控制台日志
     * @param $time
     * @param $message
     * @return bool|int
     */
    public function pushConsoleLog($time, $message): bool|int {
        if (!isset($this->subProcessManager)) {
            return false;
        }
        return $this->subProcessManager->pushConsoleLog($time, $message);
    }

    public function proxyUpstreamRuntimeStatus(): array {
        $stats = $this->server ? $this->server->stats() : (Runtime::instance()->get('SERVER_STATS') ?: []);
        $profile = \Scf\Core\App::profile();
        $leaseBound = $this->isProxyUpstreamMode() && $this->upstreamGatewayPort > 0 && $this->upstreamOwnerEpoch > 0;

        return [
            'id' => APP_NODE_ID,
            'appid' => APP_ID,
            'name' => APP_DIR_NAME,
            'ip' => SERVER_HOST,
            'port' => Runtime::instance()->httpPort(),
            'socketPort' => Runtime::instance()->httpPort(),
            'started' => file_exists(SERVER_MASTER_PID_FILE) ? filemtime(SERVER_MASTER_PID_FILE) : time(),
            'restart_times' => Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0,
            'heart_beat' => time(),
            'framework_build_version' => FRAMEWORK_BUILD_VERSION,
            'framework_update_ready' => function_exists('scf_framework_update_ready') && scf_framework_update_ready(),
            'role' => SERVER_ROLE,
            'master_pid' => (int)($this->server->master_pid ?? 0),
            'manager_pid' => (int)($this->server->manager_pid ?? 0),
            'fingerprint' => APP_FINGERPRINT,
            'app_version' => \Scf\Core\App::version() ?: $profile->version,
            'public_version' => \Scf\Core\App::publicVersion() ?: ($profile->public_version ?: '--'),
            'scf_version' => SCF_COMPOSER_VERSION,
            'swoole_version' => swoole_version(),
            'cpu_num' => function_exists('swoole_cpu_num') ? swoole_cpu_num() : 0,
            'stack_useage' => memory_get_usage(true),
            'tables' => ATable::list(),
            'tasks' => \Scf\Server\Task\CrontabManager::allStatus(),
            'threads' => class_exists(\Swoole\Coroutine::class) ? count(\Swoole\Coroutine::list()) : 0,
            'thread_status' => class_exists(\Swoole\Coroutine::class) ? \Swoole\Coroutine::stats() : [],
            'server_run_mode' => APP_SRC_TYPE,
            'server_is_ready' => Runtime::instance()->serverIsReady(),
            'server_is_draining' => Runtime::instance()->serverIsDraining(),
            'server_is_alive' => Runtime::instance()->serverIsAlive(),
            // 对 gateway 来说，端口复用必须绑定 owner epoch。
            // 这里把 upstream 的 lease 绑定信息显式暴露给 status 调用方，
            // 让控制面可以按代际精确识别“可复用实例”和“外部占用者”。
            'upstream_gateway_port' => $leaseBound ? $this->upstreamGatewayPort : 0,
            'upstream_owner_epoch' => $leaseBound ? $this->upstreamOwnerEpoch : 0,
            'http_request_count_current' => Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0,
            'http_request_count_today' => Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0,
            'http_request_reject' => Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0,
            'http_request_count' => Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0,
            'http_request_processing' => Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0,
            'rpc_request_processing' => Counter::instance()->get(Key::COUNTER_RPC_REQUEST_PROCESSING) ?: 0,
            'redis_queue_processing' => Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0,
            'crontab_busy' => \Scf\Server\Task\CrontabManager::busyCount(),
            'mysql_inflight' => InflightCounter::mysqlInflight(),
            'redis_inflight' => InflightCounter::redisInflight(),
            'outbound_http_inflight' => InflightCounter::outboundHttpInflight(),
            'mysql_execute_count' => Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0,
            'server_stats' => array_merge((array)$stats, [
                'long_connection_num' => SocketConnectionTable::instance()->count(),
            ]),
            'memory_usage' => MemoryMonitor::sum(),
        ];
    }

    public function proxyUpstreamHealthStatus(): array {
        $leaseBound = $this->isProxyUpstreamMode() && $this->upstreamGatewayPort > 0 && $this->upstreamOwnerEpoch > 0;
        return [
            'id' => APP_NODE_ID,
            'ip' => SERVER_HOST,
            'port' => Runtime::instance()->httpPort(),
            'rpc_port' => Runtime::instance()->rpcPort(),
            'started' => file_exists(SERVER_MASTER_PID_FILE) ? filemtime(SERVER_MASTER_PID_FILE) : time(),
            'master_pid' => (int)($this->server->master_pid ?? 0),
            'manager_pid' => (int)($this->server->manager_pid ?? 0),
            'server_is_ready' => Runtime::instance()->serverIsReady(),
            'server_is_draining' => Runtime::instance()->serverIsDraining(),
            'server_is_alive' => Runtime::instance()->serverIsAlive(),
            'upstream_gateway_port' => $leaseBound ? $this->upstreamGatewayPort : 0,
            'upstream_owner_epoch' => $leaseBound ? $this->upstreamOwnerEpoch : 0,
        ];
    }

    /**
     * 在 upstream manager 进程中启动本地 IPC server。
     *
     * 这条控制 socket 继续复用现有协议与 handler，但承载位置改为 server 的 manager 侧，
     * 避免再把控制入口绑进任何 worker 生命周期。
     *
     * @return void
     */
    public function startLocalIpcServer(): void {
        if (!$this->isProxyUpstreamMode() || $this->localIpcServer instanceof LocalIpcServer) {
            return;
        }
        $port = (int)Runtime::instance()->httpPort();
        if ($port <= 0) {
            return;
        }
        // LocalIpcServer 与 LocalIpc 定义在同一文件中，这里显式载入，避免 onStart 时因自动加载找不到类。
        require_once __DIR__ . '/Proxy/LocalIpc.php';
        $this->localIpcServer = new LocalIpcServer(
            LocalIpc::upstreamSocketPath($port),
            function (array $request): array {
                return $this->handleLocalIpcRequest($request);
            },
            'upstream_local_ipc'
        );
        $this->localIpcServer->start();
    }

    /**
     * 停止 upstream manager 进程上的本地 IPC server。
     *
     * @return void
     */
    public function stopLocalIpcServer(): void {
        if ($this->localIpcServer instanceof LocalIpcServer) {
            $this->localIpcServer->stop();
            $this->localIpcServer = null;
        }
    }

    protected function handleLocalIpcRequest(array $request): array {
        $action = (string)($request['action'] ?? '');
        $payload = is_array($request['payload'] ?? null) ? $request['payload'] : [];
        return match ($action) {
            'upstream.status' => [
                'ok' => true,
                'status' => 200,
                'data' => $this->proxyUpstreamRuntimeStatus(),
            ],
            'upstream.health' => [
                'ok' => true,
                'status' => 200,
                'data' => $this->proxyUpstreamHealthStatus(),
            ],
            'upstream.memory_rows' => [
                'ok' => true,
                'status' => 200,
                'data' => $this->buildProxyUpstreamMemoryRows(),
                '__data_file_action' => 'upstream.memory_rows',
            ],
            'upstream.restart_worker' => $this->buildUpstreamWorkerRestartResponse($payload),
            'upstream.quiesce' => $this->queueLocalIpcAction(function (): void {
                $this->quiesceBusinessPlane();
            }, 'quiesce', 5),
            'upstream.shutdown' => $this->queueLocalIpcAction(function (): void {
                $this->shutdown();
            }, 'shutdown', 50),
            default => ['ok' => false, 'status' => 404, 'message' => 'unknown action'],
        };
    }

    /**
     * 受理 gateway 下发的单 worker 平滑轮换请求。
     *
     * 请求会在 upstream 本地 IPC 控制链里直接调用 `server->stop($workerId, true)`，
     * 由 upstream 自己的 server 生命周期去平滑轮换指定 worker。
     *
     * @param array $payload gateway 透传的 worker 定位信息。
     * @return array<string, mixed>
     */
    protected function buildUpstreamWorkerRestartResponse(array $payload): array {
        if (!(bool)(Config::server()['worker_memory_auto_restart'] ?? false)) {
            return ['ok' => false, 'status' => 403, 'message' => 'worker memory auto restart disabled'];
        }
        $processName = (string)($payload['process'] ?? '');
        $pid = (int)($payload['pid'] ?? 0);
        if (!preg_match('/^worker:(\d+)$/', $processName, $matches)) {
            return ['ok' => false, 'status' => 400, 'message' => 'invalid worker process'];
        }
        $workerNumber = (int)$matches[1];
        $workerId = $workerNumber - 1;
        if ($workerId < 0) {
            return ['ok' => false, 'status' => 400, 'message' => 'invalid worker id'];
        }
        $current = MemoryMonitorTable::instance()->get($processName);
        if (!$current) {
            return ['ok' => false, 'status' => 404, 'message' => 'worker not found'];
        }
        $currentPid = (int)($current['pid'] ?? 0);
        if ($pid > 0 && $currentPid > 0 && $currentPid !== $pid) {
            return ['ok' => false, 'status' => 409, 'message' => 'worker pid changed'];
        }

        return $this->queueLocalIpcAction(function () use ($workerId, $workerNumber, $currentPid): void {
            Console::warning("【Server】收到内存治理请求, server侧平滑轮换 Worker#{$workerNumber}, PID:{$currentPid}", false);
            $this->server->stop($workerId, true);
        }, 'restart_worker', 1);
    }

    /**
     * 返回 upstream 当前内存表的原始快照，供 gateway 侧按 pid 补采 OS 物理内存。
     *
     * 这里刻意只返回 worker 活跃时写入的表数据，不在 upstream 再做二次 /proc 采样，
     * 这样就能移除独立的 MemoryUsageCount 子进程，同时保留现有 usage/real/peak 记录口径。
     *
     * @return array<int, array<string, mixed>>
     */
    protected function buildProxyUpstreamMemoryRows(): array {
        $rows = [];
        foreach (MemoryMonitorTable::instance()->rows() as $row) {
            if (!is_array($row)) {
                continue;
            }
            $rows[] = [
                'process' => (string)($row['process'] ?? ''),
                'pid' => (int)($row['pid'] ?? 0),
                'usage_mb' => (float)($row['usage_mb'] ?? 0),
                'real_mb' => (float)($row['real_mb'] ?? 0),
                'peak_mb' => (float)($row['peak_mb'] ?? 0),
                'updated' => (int)($row['updated'] ?? 0),
                'usage_updated' => (int)($row['usage_updated'] ?? 0),
                'restart_ts' => (int)($row['restart_ts'] ?? 0),
                'restart_count' => (int)($row['restart_count'] ?? 0),
                'limit_memory_mb' => (int)($row['limit_memory_mb'] ?? 0),
                'auto_restart' => (int)($row['auto_restart'] ?? 0),
            ];
        }
        return $rows;
    }

    protected function queueLocalIpcAction(callable $handler, string $label, int $delayMs = 1): array {
        return [
            'ok' => true,
            'status' => 200,
            'data' => $label,
            '__after_write' => static function () use ($handler, $delayMs): void {
                Timer::after(max(1, $delayMs), static function () use ($handler): void {
                    $handler();
                });
            },
        ];
    }

    /**
     * 将业务平面切换到 draining，并立即断开 socket 长连接。
     *
     * 设计目标：
     * 1. 回收开始时停止接收新任务；
     * 2. 主动切断仍挂在旧实例上的 websocket/socket 长连接，避免旧代被长连“拖住”；
     * 3. 业务子进程改为 quiesce 状态，等待后续 shutdown 收口。
     *
     * @return void
     */
    public function quiesceBusinessPlane(): void {
        Runtime::instance()->serverIsDraining(true);
        Console::warning("【Server】业务实例进入平滑回收,停止接收新任务", false);
        $disconnected = $this->disconnectSocketLongConnections($this->server);
        if ($disconnected > 0) {
            Console::warning("【Server】业务实例回收已主动断开Socket长连接: {$disconnected}", false);
        }
        isset($this->subProcessManager) && $this->subProcessManager->quiesceBusinessProcesses();
    }


    /**
     * 停止服务器
     * @return void
     */
    public function shutdown(): void {
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        Runtime::instance()->serverIsAlive(false);
        $this->stopReloadDiagnostics();
        isset($this->subProcessManager) && $this->subProcessManager->shutdown();
        $this->disconnectAllClients($this->server);
        if (!$this->isProxyUpstreamMode()) {
            Timer::after(1000, function () {
                $this->server->shutdown();
            });
            return;
        }
        $drainTimeoutSeconds = max(
            \Scf\Server\Proxy\AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS,
            (int)(Config::server()['proxy_upstream_shutdown_timeout'] ?? \Scf\Server\Proxy\AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS)
        );
        // 当计数器发生泄漏时（例如某次异常路径未能正确 decr），旧实例会在“无连接”状态
        // 下长期卡在 draining。这里增加“计数稳定 + 无活动连接”的兜底收敛窗口，避免
        // 回收链路被假忙状态拖住。
        $staleCounterGraceSeconds = max(3, (int)(Config::server()['proxy_upstream_shutdown_stale_counter_grace'] ?? 8));
        $deadline = microtime(true) + $drainTimeoutSeconds;
        $lastDrainSignature = null;
        $staleSignatureSince = 0.0;
        Timer::tick(200, function (int $timerId) use ($deadline, $staleCounterGraceSeconds, &$lastDrainSignature, &$staleSignatureSince) {
            $drainStatus = $this->proxyUpstreamBusinessPlaneDrainStatus();
            $drained = (bool)($drainStatus['drained'] ?? false);
            $expired = microtime(true) >= $deadline;
            $connectionNum = max(0, (int)($drainStatus['connection_num'] ?? 0));
            $signature = implode(':', [
                (int)($drainStatus['http_processing'] ?? 0),
                (int)($drainStatus['rpc_processing'] ?? 0),
                (int)($drainStatus['queue_processing'] ?? 0),
                (int)($drainStatus['crontab_busy'] ?? 0),
            ]);

            if ($connectionNum > 0) {
                $lastDrainSignature = null;
                $staleSignatureSince = 0.0;
            } elseif ($signature !== $lastDrainSignature) {
                $lastDrainSignature = $signature;
                $staleSignatureSince = microtime(true);
            } elseif ($staleSignatureSince <= 0) {
                $staleSignatureSince = microtime(true);
            }

            $staleCounterExceeded = false;
            if (!$drained && $connectionNum <= 0 && $staleSignatureSince > 0) {
                $staleCounterExceeded = (microtime(true) - $staleSignatureSince) >= $staleCounterGraceSeconds;
            }

            if ($drained || $expired || $staleCounterExceeded) {
                Timer::clear($timerId);
                if (!$drained && $expired) {
                    Console::warning(
                        "【Server】业务实例平滑关闭超时，强制结束剩余任务: "
                        . "http=" . (int)($drainStatus['http_processing'] ?? 0)
                        . ", rpc=" . (int)($drainStatus['rpc_processing'] ?? 0)
                        . ", queue=" . (int)($drainStatus['queue_processing'] ?? 0)
                        . ", crontab=" . (int)($drainStatus['crontab_busy'] ?? 0)
                        . ", mysql=" . (int)($drainStatus['mysql_inflight'] ?? 0)
                        . ", redis=" . (int)($drainStatus['redis_inflight'] ?? 0)
                        . ", outbound_http=" . (int)($drainStatus['outbound_http_inflight'] ?? 0),
                        false
                    );
                } elseif (!$drained && $staleCounterExceeded) {
                    Console::warning(
                        "【Server】业务实例检测到计数长期不变化且无活动连接，执行兜底关停: "
                        . "http=" . (int)($drainStatus['http_processing'] ?? 0)
                        . ", rpc=" . (int)($drainStatus['rpc_processing'] ?? 0)
                        . ", queue=" . (int)($drainStatus['queue_processing'] ?? 0)
                        . ", crontab=" . (int)($drainStatus['crontab_busy'] ?? 0)
                        . ", mysql=" . (int)($drainStatus['mysql_inflight'] ?? 0)
                        . ", redis=" . (int)($drainStatus['redis_inflight'] ?? 0)
                        . ", outbound_http=" . (int)($drainStatus['outbound_http_inflight'] ?? 0)
                        . ", stale_grace={$staleCounterGraceSeconds}s",
                        false
                    );
                }
                $this->server->shutdown();
            }
        });
    }

    /**
     * 汇总 upstream 业务面的排空状态。
     *
     * 该状态用于 proxy_upstream 关闭阶段的循环判定：正常路径在计数归零时结束，
     * 异常路径则结合连接数与“计数是否长时间不变化”执行兜底收敛。
     *
     * @return array{
     *     drained: bool,
     *     http_processing: int,
     *     rpc_processing: int,
     *     queue_processing: int,
     *     crontab_busy: int,
     *     mysql_inflight: int,
     *     redis_inflight: int,
     *     outbound_http_inflight: int,
     *     connection_num: int
     * }
     */
    protected function proxyUpstreamBusinessPlaneDrainStatus(): array {
        $httpProcessing = (int)(Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0);
        $rpcProcessing = (int)(Counter::instance()->get(Key::COUNTER_RPC_REQUEST_PROCESSING) ?: 0);
        $queueProcessing = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0);
        $crontabBusy = CrontabManager::busyCount();
        $mysqlInflight = InflightCounter::mysqlInflight();
        $redisInflight = InflightCounter::redisInflight();
        $outboundHttpInflight = InflightCounter::outboundHttpInflight();
        $stats = $this->server->stats();
        $connectionNum = max(0, (int)($stats['connection_num'] ?? 0));

        return [
            'drained' => $httpProcessing <= 0
                && $rpcProcessing <= 0
                && $queueProcessing <= 0
                && $crontabBusy <= 0
                && $mysqlInflight <= 0
                && $redisInflight <= 0
                && $outboundHttpInflight <= 0,
            'http_processing' => $httpProcessing,
            'rpc_processing' => $rpcProcessing,
            'queue_processing' => $queueProcessing,
            'crontab_busy' => $crontabBusy,
            'mysql_inflight' => $mysqlInflight,
            'redis_inflight' => $redisInflight,
            'outbound_http_inflight' => $outboundHttpInflight,
            'connection_num' => $connectionNum,
        ];
    }

    protected function proxyUpstreamBusinessPlaneDrained(): bool {
        return (bool)($this->proxyUpstreamBusinessPlaneDrainStatus()['drained'] ?? false);
    }

    /**
     * 重启服务器
     * @return void
     */
    public function reload(): void {
        Runtime::instance()->serverIsReady(false);
        Runtime::instance()->serverIsDraining(true);
        if (!Env::isDev()) {
            $countdown = 3;
            Console::info('【Server】' . Color::yellow($countdown) . '秒后重启服务器');
            $channel = new Coroutine\Channel(1);
            Timer::tick(1000, function ($id) use (&$countdown, $channel) {
                $countdown--;
                if ($countdown == 0) {
                    Timer::clear($id);
                    $channel->push(true);
                } else {
                    Console::info('【Server】' . Color::yellow($countdown) . '秒后重启服务器');
                }
            });
            $channel->pop($countdown + 1);
        } else {
            Console::info('【Server】' . Color::yellow('正在重启服务器'));
        }
        isset($this->subProcessManager) && $this->subProcessManager->sendCommand('upgrade');
        $this->server->reload();
        //重启控制台
        if (!$this->isProxyUpstreamMode() && App::isMaster()) {
            $dashboardHost = 'http://localhost:' . Runtime::instance()->dashboardPort() . '/reload';
            $client = \Scf\Client\Http::create($dashboardHost);
            $client->get();
        }
    }

    /**
     * 分页断开所有连接，避免 reload 窗口内遗留长连接阻塞 worker 退出。
     */
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
                \Scf\Server\Manager::instance()->removeNodeClient($fd);
                \Scf\Server\Manager::instance()->removeDashboardClient($fd);
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

    /**
     * 仅断开 socket 长连接（来自 SocketConnectionTable 跟踪的 fd）。
     *
     * 这个动作专门用于“开始回收”阶段：优先释放会长期占用旧实例的长连接，
     * 同时避免把尚在收尾的普通短请求一并打断。
     *
     * @param Server $server
     * @return int 实际断开数量
     */
    protected function disconnectSocketLongConnections(Server $server): int {
        $disconnected = 0;
        foreach (SocketConnectionTable::instance()->rows() as $fd => $row) {
            $fd = (int)$fd;
            if ($fd <= 0) {
                continue;
            }
            // 管理面节点/控制台 fd 也同步做一次解绑，确保回收窗口内不再残留旧路由。
            \Scf\Server\Manager::instance()->removeNodeClient($fd);
            \Scf\Server\Manager::instance()->removeDashboardClient($fd);
            if (!$server->exist($fd)) {
                SocketConnectionTable::instance()->delete($fd);
                continue;
            }
            if ($server->isEstablished($fd)) {
                $server->disconnect($fd);
            } else {
                $server->close($fd);
            }
            $disconnected++;
        }
        return $disconnected;
    }

    protected function startReloadDiagnostics(Server $server): void {
        $this->stopReloadDiagnostics();
        $this->reloadDiagnosticStartedAt = time();
        $graceSeconds = static::RELOAD_DIAGNOSTIC_GRACE_SECONDS;
        $this->scheduleNextReloadDiagnostic($server, $graceSeconds);
    }

    protected function stopReloadDiagnostics(?string $message = null): void {
        if (!is_null($this->reloadDiagnosticTimerId)) {
            Timer::clear($this->reloadDiagnosticTimerId);
            $this->reloadDiagnosticTimerId = null;
        }
        $this->reloadDiagnosticStartedAt = 0;
        if ($message) {
            $this->log(Color::green($message));
        }
    }

    protected function scheduleNextReloadDiagnostic(Server $server, int $graceSeconds): void {
        if ($this->reloadDiagnosticStartedAt <= 0) {
            return;
        }
        $elapsed = time() - $this->reloadDiagnosticStartedAt;
        $delayMs = $elapsed < $graceSeconds
            ? max(1000, ($graceSeconds - $elapsed) * 1000)
            : static::RELOAD_DIAGNOSTIC_INTERVAL_MS;
        $intervalSeconds = (int)(static::RELOAD_DIAGNOSTIC_INTERVAL_MS / 1000);
        $this->reloadDiagnosticTimerId = Timer::after($delayMs, function () use ($server, $graceSeconds, $intervalSeconds) {
            $this->reloadDiagnosticTimerId = null;
            if ($this->reloadDiagnosticStartedAt <= 0) {
                return;
            }
            $elapsed = time() - $this->reloadDiagnosticStartedAt;
            if ($elapsed >= $graceSeconds) {
                $snapshot = $this->buildReloadDiagnosticSnapshot($server, $elapsed);
                $this->log(Color::yellow("重启超过 {$graceSeconds}s 未完成，以下每 {$intervalSeconds}s 输出一次诊断:\n{$snapshot}"));
            }
            if ($this->reloadDiagnosticStartedAt > 0) {
                $this->scheduleNextReloadDiagnostic($server, $graceSeconds);
            }
        });
    }

    protected function buildReloadDiagnosticSnapshot(Server $server, int $elapsed): string {
        $connectionStats = $this->countCurrentConnections($server);
        $serverStats = $server->stats();
        $requestProcessing = Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0;
        $requestReject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
        $mysqlProcessing = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
        $nodeClients = ServerNodeTable::instance()->count();
        $dashboardClients = count(Runtime::instance()->get('DASHBOARD_CLIENTS') ?: []);
        $socketRoutes = SocketConnectionTable::instance()->count();
        $socketWorkers = SocketConnectionTable::instance()->workerConnectionStats();
        ksort($socketWorkers);
        $crontabs = CrontabManager::allStatus();
        $busyCrontabs = array_values(array_filter($crontabs, static function ($task) {
            return (int)($task['is_busy'] ?? 0) === 1;
        }));
        $deadCrontabs = array_values(array_filter($crontabs, static function ($task) {
            return isset($task['process_is_alive']) && (int)$task['process_is_alive'] === STATUS_OFF;
        }));
        $busyCrontabLabels = array_map(static function ($task) {
            return ($task['name'] ?? $task['namespace'] ?? 'unknown') . '#' . ($task['pid'] ?? '--');
        }, array_slice($busyCrontabs, 0, 5));
        $deadCrontabLabels = array_map(static function ($task) {
            return ($task['name'] ?? $task['namespace'] ?? 'unknown') . '#' . ($task['pid'] ?? '--');
        }, array_slice($deadCrontabs, 0, 5));

        $lines = [
            "已等待: {$elapsed}s",
            "HTTP处理中: {$requestProcessing}, 最近拒绝: {$requestReject}, 最近MySQL处理中: {$mysqlProcessing}",
            "当前连接: total={$connectionStats['total']}, established={$connectionStats['established']}, websocket_routes={$socketRoutes}, node_clients={$nodeClients}, dashboard_clients={$dashboardClients}",
            "ServerStats: connection_num=" . ($serverStats['connection_num'] ?? 0) . ", tasking_num=" . ($serverStats['tasking_num'] ?? 0) . ", idle_worker_num=" . ($serverStats['idle_worker_num'] ?? 0) . ", task_idle_worker_num=" . ($serverStats['task_idle_worker_num'] ?? 0),
            "Socket worker分布: " . ($socketWorkers ? json_encode($socketWorkers, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : '[]'),
            "Crontab: total=" . count($crontabs) . ", busy=" . count($busyCrontabs) . ", dead=" . count($deadCrontabs),
        ];
        if ($busyCrontabLabels) {
            $lines[] = "Busy crontab(最多5个): " . implode(', ', $busyCrontabLabels);
        }
        if ($deadCrontabLabels) {
            $lines[] = "Dead crontab(最多5个): " . implode(', ', $deadCrontabLabels);
        }
        return implode("\n", $lines);
    }

    protected function countCurrentConnections(Server $server): array {
        $startFd = 0;
        $total = 0;
        $established = 0;
        while (true) {
            $clients = $server->getClientList($startFd, 100);
            if (!$clients) {
                break;
            }
            foreach ($clients as $fd) {
                $fd = (int)$fd;
                $startFd = max($startFd, $fd);
                $total++;
                if ($server->isEstablished($fd)) {
                    $established++;
                }
            }
            if (count($clients) < 100) {
                break;
            }
        }
        return [
            'total' => $total,
            'established' => $established,
        ];
    }

    /**
     * 向socket客户端推送内容
     * @param $fd
     * @param $str
     * @return bool
     */
    public function push($fd, $str): bool {
        if (!$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
            return false;
        }
        try {
            return $this->server->push($fd, $str);
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * @return string|null
     */
    public function host(): ?string {
        return $this->host;
    }

    public function getPort(): int {
        return Runtime::instance()->httpPort();
    }

    public function stats(): array {
        return $this->server->stats();
    }

}
