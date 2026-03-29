<?php

namespace Scf\Server;

use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Key;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeTable;
use Scf\Core\Table\SocketConnectionTable;
use Scf\Server\Listener\Listener;
use Scf\Server\Proxy\ConsoleRelay;
use Scf\Server\Proxy\LocalIpc;
use Scf\Server\Proxy\LocalIpcServer;
use Scf\Server\Task\CrontabManager;
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\MemoryMonitor;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Throwable;


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

    protected function isProxyUpstreamMode(): bool {
        return defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true;
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
        //一键协程化
        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);
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
        if (defined('APP_MODULE_STYLE') === false) {
            define('APP_MODULE_STYLE', $serverConfig['module_style'] ?? APP_MODULE_STYLE_MULTI);
        }
        //启动masterDB(redis协议)服务器
        //MasterDB::start(MDB_PORT);
        $this->bindPort = $this->bindPort ?: ($serverConfig['port'] ?? 9580);// \Scf\Core\Server::getUseablePort($this->bindPort ?: ($serverConfig['port'] ?? 9580));
        !defined('MAX_REQUEST_LIMIT') and define('MAX_REQUEST_LIMIT', $serverConfig['max_request_limit'] ?? 1280);
        !defined('SLOW_LOG_TIME') and define('SLOW_LOG_TIME', $serverConfig['slow_log_time'] ?? 10000);
        !defined('MAX_MYSQL_EXECUTE_LIMIT') and define('MAX_MYSQL_EXECUTE_LIMIT', $serverConfig['max_mysql_execute_limit'] ?? 1000);
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
            'max_connection' => $serverConfig['max_connection'] ?? 1024,//最大连接数
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
            if (self::isPortInUse($this->bindPort)) {
                if (!self::killProcessByPort($this->bindPort)) {
                    $this->log(Color::red('HTTP服务端口[' . $this->bindPort . ']被占用,尝试结束进程失败'));
                    // 稍等片刻再退出/或由外层管理器重试
                    usleep(1000 * 1000);
                    exit(1);
                }
            }
            $httpServer = $this->server->listen($this->bindHost, $this->bindPort, SWOOLE_SOCK_TCP);
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
                // 尝试杀掉占用端口的进程
                if (self::isPortInUse($rpcPort)) {
                    $this->log(Color::yellow('RPC服务端口[' . $rpcPort . ']被占用,尝试结束进程'));
                    if (!self::killProcessByPort($rpcPort)) {
                        $this->log(Color::red('RPC服务端口[' . $rpcPort . ']被占用,尝试结束进程失败'));
                        usleep(1000 * 1000);
                        exit(1);
                    }
                }
                $rpcBindHost = $this->isProxyUpstreamMode() ? '127.0.0.1' : '0.0.0.0';
                /** @var Server $rpcServer */
                $rpcServer = $this->server->listen($rpcBindHost, $rpcPort, SWOOLE_SOCK_TCP);
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
            if ($this->isProxyUpstreamMode()) {
                $this->stopLocalIpcServer();
            }
            $disconnected = $this->disconnectAllClients($server);
            $disconnected > 0 and $this->log(Color::yellow("已断开 {$disconnected} 个客户端连接"));
        });
        $this->server->on("Shutdown", function (Server $server) {
            if ($this->isProxyUpstreamMode()) {
                // upstream 的本地 IPC 挂在 master/onStart；Shutdown 再补一次收口，确保 accept 协程跟随 master 退出。
                $this->stopLocalIpcServer();
            }
        });
        $this->server->on("ManagerStart", function (Server $server) {
            //Console::info('ManagerStart');
            //MemoryMonitor::start('Server:Manager');
        });
        $this->server->on("ManagerStop", function (Server $server) {
            //Console::info('onManagerStop');
            //MemoryMonitor::stop();
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
            'http_request_count_current' => Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0,
            'http_request_count_today' => Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0,
            'http_request_reject' => Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0,
            'http_request_count' => Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0,
            'http_request_processing' => Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0,
            'rpc_request_processing' => Counter::instance()->get(Key::COUNTER_RPC_REQUEST_PROCESSING) ?: 0,
            'redis_queue_processing' => Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0,
            'crontab_busy' => \Scf\Server\Task\CrontabManager::busyCount(),
            'mysql_execute_count' => Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0,
            'server_stats' => array_merge((array)$stats, [
                'long_connection_num' => SocketConnectionTable::instance()->count(),
            ]),
            'memory_usage' => MemoryMonitor::sum(),
        ];
    }

    public function proxyUpstreamHealthStatus(): array {
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

    public function quiesceBusinessPlane(): void {
        Runtime::instance()->serverIsDraining(true);
        Console::warning("【Server】业务实例进入平滑回收,停止接收新任务", false);
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
        $deadline = microtime(true) + max(\Scf\Server\Proxy\AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS, (int)(Config::server()['proxy_upstream_shutdown_timeout'] ?? \Scf\Server\Proxy\AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS));
        Timer::tick(200, function (int $timerId) use ($deadline) {
            $drained = $this->proxyUpstreamBusinessPlaneDrained();
            $expired = microtime(true) >= $deadline;
            if ($drained || $expired) {
                Timer::clear($timerId);
                if (!$drained && $expired) {
                    $httpProcessing = (int)(Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0);
                    $rpcProcessing = (int)(Counter::instance()->get(Key::COUNTER_RPC_REQUEST_PROCESSING) ?: 0);
                    $queueProcessing = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0);
                    $crontabBusy = CrontabManager::busyCount();
                    Console::warning(
                        "【Server】业务实例平滑关闭超时，强制结束剩余任务: "
                        . "http={$httpProcessing}, rpc={$rpcProcessing}, queue={$queueProcessing}, crontab={$crontabBusy}",
                        false
                    );
                }
                $this->server->shutdown();
            }
        });
    }

    protected function proxyUpstreamBusinessPlaneDrained(): bool {
        $httpProcessing = (int)(Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0);
        $rpcProcessing = (int)(Counter::instance()->get(Key::COUNTER_RPC_REQUEST_PROCESSING) ?: 0);
        $queueProcessing = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0);
        $crontabBusy = CrontabManager::busyCount();
        return $httpProcessing <= 0
            && $rpcProcessing <= 0
            && $queueProcessing <= 0
            && $crontabBusy <= 0;
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
