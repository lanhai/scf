<?php

namespace Scf\Server;

use Scf\App\Updater;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Log;
use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Root;
use Scf\Server\Listener\Listener;
use Scf\Server\Runtime\Table;
use Scf\Server\Struct\Node;
use Scf\Server\Table\Counter;
use Scf\Server\Table\Runtime;
use Scf\Server\Task\Crontab;
use Scf\Server\Task\RQueue;
use Scf\Util\File;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Symfony\Component\Console\Output\ConsoleOutput;
use Throwable;


class Http extends \Scf\Core\Server {

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
     * @var string 绑定ip
     */
    protected string $bindHost = '0.0.0.0';
    /**
     * @var int 绑定端口
     */
    protected int $bindPort = 0;
    /**
     * @var string 本机ip地址
     */
    protected string $ip;
    /**
     * @var string 节点名称
     */
    protected string $name;

    /**
     * @var int 启动时间
     */
    protected int $started = 0;

    /**
     * @param $role
     * @param string $host
     * @param int $port
     */
    public function __construct($role, string $host = '0.0.0.0', int $port = 0) {
        $this->bindHost = $host;
        $this->bindPort = $port;
        $this->role = $role;
        $this->started = time();
        $this->name = SERVER_NAME;
        $this->id = SERVER_NODE_ID;
        $this->ip = SERVER_HOST;
    }


    /**
     * 创建一个服务器对象
     * @param $role
     * @param string $host
     * @param int $port
     * @return Http
     */
    public static function create($role, string $host = '0.0.0.0', int $port = 0): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class($role, $host, $port);
        }
        return self::$_instances[$class];
    }


    public static function bgs(): void {
        MasterDB::start();
    }

    /**
     * 启动定时任务和队列的守护进程,跟随server的重启而重新加载文件创建新进程迭代老进程,注意相关应用内部需要有table记录mannger id自增后自动终止销毁timer/while ture等循环机制避免出现僵尸进程
     * @param $config
     */
    protected function addCrontabProcess($config): void {
        $crontabProcess = new Process(function () use ($config) {
            //等待服务器启动完成
            while (true) {
                if (Runtime::instance()->serverStatus()) {
                    break;
                }
                sleep(1);
            }
            $managerId = Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
            define('IS_CRONTAB_PROCESS', true);
            while (true) {
                if (!Runtime::instance()->crontabProcessStatus()) {
                    $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
                    Crontab::startProcess();
                    Runtime::instance()->crontabProcessStatus(true);
                }
                //使用管理进程id判断是否迭代,迭代则重新启动进程
                if ($managerId !== Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS)) {
                    Runtime::instance()->crontabProcessStatus(false);
                    Console::warning("【Server】Crontab#{$managerId}管理进程已迭代,重启所有任务进程");
                    sleep(1);
                } else {
                    sleep(5);
                }
            }
        });
        $pid = $this->server->addProcess($crontabProcess);
        $this->log("Crontab 管理进程ID编号:{$pid}");
        //redis队列管理进程
        $runQueueInMaster = $config['redis_queue_in_master'] ?? true;
        $runQueueInSlave = $config['redis_queue_in_slave'] ?? false;
        if ((App::isMaster() && $runQueueInMaster) || (!App::isMaster() && $runQueueInSlave)) {
            $redisQueueProcess = new Process(function () use ($config) {
                //等待服务器启动完成
                while (true) {
                    if (Runtime::instance()->serverStatus()) {
                        break;
                    }
                    sleep(1);
                }
                $managerId = Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                while (true) {
                    if (!Runtime::instance()->redisQueueProcessStatus()) {
                        $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
                        RQueue::startProcess();
                        Runtime::instance()->redisQueueProcessStatus(true);
                    }
                    if ($managerId !== Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS)) {
                        Runtime::instance()->redisQueueProcessStatus(false);
                        Console::warning("【Server】RedisQueue#{$managerId}管理进程已迭代,重启队列进程");
                        sleep(1);
                    } else {
                        sleep(5);
                    }
                }
            });
            $pid = $this->server->addProcess($redisQueueProcess);
            $this->log("RedisQueue 管理进程ID编号:{$pid}");
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
        Table::register([
            'Scf\Server\Table\PdoPoolTable',
            'Scf\Server\Table\LogTable',
            'Scf\Server\Table\Counter',
            'Scf\Server\Table\Runtime',
            'Scf\Server\Table\RouteTable',
        ]);
        Runtime::instance()->serverStatus(false);
        //启动master节点管理面板服务器
        Dashboard::start();
        //usleep(1000 * 500);
        //启动masterDB(redis协议)服务器
        MasterDB::start(MDB_PORT);
        //加载服务器配置
        $serverConfig = Config::server();
        $this->bindPort = $this->bindPort ?: ($serverConfig['port'] ?? 9580);// \Scf\Core\Server::getUseablePort($this->bindPort ?: ($serverConfig['port'] ?? 9580));
        !defined('APP_MODULE_STYLE') and define('APP_MODULE_STYLE', $serverConfig['module_style'] ?? APP_MODULE_STYLE_LARGE);
        !defined('MAX_REQUEST_LIMIT') and define('MAX_REQUEST_LIMIT', $serverConfig['max_request_limit'] ?? 1280);
        !defined('SLOW_LOG_TIME') and define('SLOW_LOG_TIME', $serverConfig['slow_log_time'] ?? 10000);
        !defined('MAX_MYSQL_EXECUTE_LIMIT') and define('MAX_MYSQL_EXECUTE_LIMIT', $serverConfig['max_mysql_execute_limit'] ?? 1000);
        //检查是否存在异常进程
//        $serverPid = 0;
//        Coroutine::create(function () use (&$serverPid) {
//            $serverPid = File::read(SERVER_MASTER_PID_FILE);
//        });
//        \Swoole\Event::wait();
//        if ($serverPid) {
//            if (Process::kill($serverPid, 0)) {
//                Console::log(Color::yellow("Server Manager PID:{$serverPid} killed"));
//                Process::kill($serverPid, SIGKILL);
//                unlink(SERVER_MASTER_PID_FILE);
//            }
//        }
        //实例化服务器
        $this->server = new Server($this->bindHost, mode: SWOOLE_PROCESS);
        //添加一个后台任务管理进程
        $this->addCrontabProcess($serverConfig);
        $setting = [
            'worker_num' => $serverConfig['worker_num'] ?? 128,
            'max_wait_time' => $serverConfig['max_wait_time'] ?? 60,
            'reload_async' => true,
            'daemonize' => Manager::instance()->issetOpt('d'),
            'log_file' => APP_PATH . '/log/server.log',
            'pid_file' => SERVER_MASTER_PID_FILE,
            'task_worker_num' => $serverConfig['task_worker_num'] ?? 128,
            'task_enable_coroutine' => true,
            'max_connection' => $serverConfig['max_connection'] ?? 4096,//最大连接数
            'max_coroutine' => $serverConfig['max_coroutine'] ?? 10240,//最多启动多少个携程
            'max_concurrency' => $serverConfig['max_concurrency'] ?? 2048,//最高并发
        ];
        if (SERVER_ENABLE_STATIC_HANDER || Env::isDev()) {
            $setting['document_root'] = APP_PATH . '/public';
            $setting['enable_static_handler'] = true;
            $setting['http_autoindex'] = true;
            $setting['http_index_files'] = ['index.html'];
            $setting['static_handler_locations'] = $serverConfig['static_handler_locations'] ?? ['/cp', '/asset'];
        }
        //是否允许跨域请求
        define('ALLOW_CROSS_ORIGIN', $serverConfig['allow_cross_origin'] ?? false);
        $this->server->set($setting);
        //监听HTTP请求
        try {
            $httpServer = $this->server->listen($this->bindHost, $this->bindPort, SWOOLE_SOCK_TCP);
            $httpServer->set([
                'package_max_length' => $serverConfig['package_max_length'] ?? 8 * 1024 * 1024,
                'open_http_protocol' => true,
                'open_http2_protocol' => true,
                'open_websocket_protocol' => true
            ]);
            Runtime::instance()->httpPort($this->bindPort);
        } catch (Throwable $exception) {
            Console::log(Color::red('http服务端口监听启动失败:' . $exception->getMessage()));
            exit(1);
        }
        //监听SOCKET请求
        $socketPort = self::getUseablePort($this->bindPort + 1);
        try {
            $socketServer = $this->server->listen($this->bindHost, $socketPort, SWOOLE_SOCK_TCP);
            $socketServer->set([
                'open_http_protocol' => false,
                'open_http2_protocol' => false,
                'open_websocket_protocol' => true
            ]);
            Runtime::instance()->socketPort($socketPort);
        } catch (Throwable $exception) {
            Console::log(Color::red('socket服务端口监听失败:' . $exception->getMessage()));
            exit(1);
        }
        //监听RPC服务(tcp)请求
        if (isset($serverConfig['rpc_port'])) {
            try {
                $rpcPort = self::getUseablePort($serverConfig['rpc_port']);
                /** @var Server $rpcServer */
                $rpcServer = $this->server->listen('0.0.0.0', $rpcPort, SWOOLE_SOCK_TCP);
                $rpcServer->set([
                    'open_http_protocol' => false,
                    'open_http2_protocol' => false,
                    'open_websocket_protocol' => false
                ]);
                Runtime::instance()->rpcPort($rpcPort);
            } catch (Throwable $exception) {
                Console::log(Color::red('RPC服务端口监听失败:' . $exception->getMessage()));
                exit(1);
            }
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
            //Runtime::instance()->serverStatus(false);
            //增加服务器重启次数计数
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
            //定时任务迭代
            Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
            Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
            //断开所有客户端连接
            $clients = $this->server->getClientList();
            if ($clients) {
                foreach ($clients as $fd) {
                    if ($server->isEstablished($fd)) {
                        $server->disconnect($fd);
                    }
                }
                Console::clearAllSubscribe();
            }
        });
        $this->server->on("AfterReload", function () {
            $this->log('第' . Counter::instance()->get(Key::COUNTER_SERVER_RESTART) . '次重启完成');
            //重置执行中的请求数统计
            Counter::instance()->set(Key::COUNTER_REQUEST_PROCESSING, 0);
            Runtime::instance()->serverStatus(true);
        });
        //服务器销毁前
        $this->server->on("BeforeShutdown", function (Server $server) {
            $this->log(Color::red('服务器即将关闭'));
        });
        //服务器完成启动
        $this->server->on('start', function (Server $server) use ($serverConfig) {
            $masterPid = $server->master_pid;
            $managerPid = $server->manager_pid;
            define("SERVER_MASTER_PID", $masterPid);
            define("SERVER_MANAGER_PID", $managerPid);
            File::write(SERVER_MANAGER_PID_FILE, $managerPid);
            $scfVersion = SCF_VERSION;
            $role = SERVER_ROLE;
            $env = APP_RUN_ENV;
            $mode = APP_RUN_MODE;
            $alias = SERVER_ALIAS;
            $files = count(get_included_files());
            $os = SERVER_ENV;
            $host = SERVER_HOST;
            $version = swoole_version();
            $fingerprint = APP_FINGERPRINT;
            $frameworkRoot = Root::dir();
            $serverBuildTime = FRAMEWORK_BUILD_TIME;
            $frameworkVersion = $serverBuildTime == 'development' ? 'development' : FRAMEWORK_BUILD_VERSION;
            $info = <<<INFO
------------------Server启动完成------------------
应用指纹：{$fingerprint}
运行系统：{$os}
运行环境：{$env}
运行模式：{$mode}
节点名称：{$alias}
节点角色：{$role}
文件加载：{$files}
环境版本：{$version}
框架源码：{$frameworkRoot}
框架版本：{$scfVersion}
打包版本：{$frameworkVersion}
打包时间：{$serverBuildTime}
工作进程：{$serverConfig['worker_num']}
任务进程：{$serverConfig['task_worker_num']}
主机地址：{$host}
--------------------------------------------------
INFO;
            Console::write(Color::cyan($info));
            $renderData = [
                ['SERVER', Color::cyan("Master:{$masterPid},Manager:{$managerPid}"), Color::green($this->bindPort)],
                ['SOCKET', "--", Color::green(Runtime::instance()->socketPort())],
                ['DASHBOARD', \Scf\Core\App::isMaster() ? Color::cyan(Runtime::instance()->get('DASHBOARD_PID')) : '--', \Scf\Core\App::isMaster() ? Color::green(Runtime::instance()->dashboardPort()) : '--'],
                ['MasterDB', \Scf\Core\App::isMaster() ? Color::cyan(Runtime::instance()->get('MASTERDB_PID')) : '--', \Scf\Core\App::isMaster() ? Color::green(Runtime::instance()->masterDbPort()) : '--'],
            ];
            if ($rpcPort = Runtime::instance()->rpcPort()) {
                $renderData[] = ['RPC', "--", Color::green($rpcPort)];
            }
            $output = new ConsoleOutput();
            $table = new \Symfony\Component\Console\Helper\Table($output);
            $table
                ->setHeaders([Color::cyan('服务'), Color::cyan('进程ID'), Color::cyan('端口号')])
                ->setRows($renderData);
            $table->render();
            //自动更新
            APP_AUTO_UPDATE == STATUS_ON and $this->checkVersion();
        });
//        Timer::tick(3000, function () {
//            \Scf\Core\App::countMemory();
//        });
        try {
            //日志备份进程
            $this->server->addProcess(SubProcess::createLogBackupProcess($this->server));
            //心跳进程
            $this->addHeartbeatProcess();
            //启动文件监听进程
            if ((Env::isDev() && APP_RUN_MODE == 'src') || Manager::instance()->issetOpt('watch')) {
                $this->server->addProcess(SubProcess::createFileWatchProcess($this->server, $this->bindPort));
            }
            $this->server->start();
        } catch (Throwable $exception) {
            Console::error($exception->getMessage());
        }
    }


    /**
     * 创建心跳进程
     * @return void
     */
    protected function addHeartbeatProcess(): void {
        //心跳进程
        $node = Node::factory();
        $node->id = $this->id;
        $node->name = $this->name;
        $node->ip = $this->ip;
        $node->fingerprint = APP_FINGERPRINT;
        $node->port = $this->bindPort;
        $node->socketPort = Runtime::instance()->socketPort();
        $node->role = \Scf\Core\App::isMaster() ? 'master' : 'slave';
        $node->started = $this->started;
        $node->restart_times = 0;
        $node->master_pid = $this->server->master_pid;
        $node->manager_pid = $this->server->manager_pid;
        $node->swoole_version = swoole_version();
        $node->cpu_num = swoole_cpu_num();
        $node->stack_useage = Coroutine::getStackUsage();
        $node->heart_beat = time();
        $node->scf_version = SCF_VERSION;
        $node->tables = Table::list();
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->server_run_mode = APP_RUN_MODE;
        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
        $node->framework_update_ready = file_exists(SCF_ROOT . '/build/latest.core');
        $this->server->addProcess(SubProcess::heartbeat($this->server, $node));
    }

    /**
     * 检查版本
     * @return void
     */
    protected function checkVersion(): void {
        if (APP_RUN_MODE != 'phar') {
            return;
        }
        $versionInfo = "";
        $updater = Updater::instance();
        $version = $updater->getVersion();
        Console::line();
        $versionInfo .= "APP版本:" . $version['local']['version'] . ",更新时间:" . $version['local']['updated'] . "\n";
        $versionInfo .= "SCF版本:" . SCF_VERSION . ",更新时间:" . date('Y-m-d H:i:s') . "\n";
        if (is_null($version['remote'])) {
            $versionInfo .= "最新版本:获取失败\n";
        } else {
            $versionInfo .= "APP最新版本:" . (!$updater->hasNewAppVersion() ? Color::green("已是最新") : $version['remote']['app']['version'] . ",发布时间:" . $version['remote']['app']['release_date']) . "\n";
            $versionInfo .= "SCF最新版本:" . (!$updater->hasNewScfVersion() ? Color::green("已是最新") : $version['remote']['scf']['version'] . ",发布时间:" . $version['remote']['scf']['release_date']) . "\n";
        }
        $versionInfo .= "自动更新:" . ($updater->isEnableAutoUpdate() ? Color::green("开启") : Color::yellow("关闭"));
        Console::write($versionInfo);
        Console::line();
        if ($updater->hasNewAppVersion() && ($updater->isEnableAutoUpdate() || $version['remote']['app']['forced'])) {
            $this->log('开始执行更新');
            if ($updater->updateApp()) {
                $this->reload();
            }
        }
        $pid = Coroutine::create(function () use ($version) {
            $this->autoUpdate();
        });
        $this->log('自动更新服务启动成功!协程ID:' . $pid);
    }

    /**
     * 自动更新
     * @return mixed
     */
    protected function autoUpdate(): mixed {
        Coroutine::sleep(10);
        $updater = Updater::instance();
        $version = $updater->getVersion();
        if ($updater->hasNewAppVersion() && ($updater->isEnableAutoUpdate() || $version['remote']['app']['forced'])) {
            //强制更新
            Log::instance()->info('开始执行更新:' . $version['remote']['app']['version']);
            if ($updater->updateApp()) {
                $this->reload();
            } else {
                Log::instance()->info('更新失败:' . $version['remote']['app']['version']);
            }

        }
        return $this->autoUpdate();
    }

    /**
     * 重启服务器
     * @return void
     */
    protected function reload(): void {
        $countdown = 3;
        Console::info('【Server】' . Color::yellow($countdown) . '秒后重启服务器');
        Timer::tick(1000, function ($id) use (&$countdown) {
            $countdown--;
            if ($countdown == 0) {
                Timer::clear($id);
                $this->server->reload();
                //重启控制台
                if (App::isMaster()) {
                    $dashboardHost = 'http://localhost:' . Runtime::instance()->dashboardPort() . '/reload';
                    $client = \Scf\Client\Http::create($dashboardHost);
                    $client->get();
                }
            } else {
                Console::info('【Server】' . Color::yellow($countdown) . '秒后重启服务器');
            }
        });
    }

    /**
     * 自定义更新src/public到指定版本
     * @param $type
     * @param $version
     * @return bool
     */
    public function appointUpdateTo($type, $version): bool {
        if (Updater::instance()->appointUpdateTo($type, $version)) {
            $type == 'app' and $this->reload();
            return true;
        }
        return false;
    }

    /**
     * 强制更新
     * @param string|null $version 版本号
     * @return bool
     */
    public function forceUpdate(string $version = null): bool {
        $updater = Updater::instance();
        if (!$updater->hasNewAppVersion()) {
            return false;
        }
        if (!is_null($version)) {
            Log::instance()->info('开始执行更新:' . $version);
            if ($updater->changeAppVersion($version)) {
                $this->reload();
                return true;
            } else {
                Log::instance()->info('更新失败:' . $version);
            }
        } else {
            $version = $updater->getVersion();
            Log::instance()->info('开始执行更新:' . $version['remote']['app']['version']);
            if ($updater->updateApp()) {
                $this->reload();
                return true;
            } else {
                Log::instance()->info('更新失败:' . $version['remote']['app']['version']);
            }
        }
        return false;
    }

    /**
     * 向socket客户端推送内容
     * @param $fd
     * @param $str
     * @return bool
     */
    public function push($fd, $str): bool {
        if (!$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
            Console::unsubscribe($fd);
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
    public function ip(): ?string {
        return $this->ip;
    }

    public function getPort(): int {
        return $this->bindPort;
    }

    public function stats(): array {
        return $this->server->stats();
    }

    public function inServerProcess(): bool {
        return isset($this->server);
    }

    protected function _master(): Server {
        return $this->server;
    }


    public static function allowCrossOrigin(): bool {
        return defined('ALLOW_CROSS_ORIGIN') && ALLOW_CROSS_ORIGIN;
    }

    public static function server(): ?Server {
        try {
            return self::instance()->_master();
        } catch (Throwable) {
            return null;
        }
    }

    /**
     * @return ?Server
     */
    public static function master(): ?Server {
        return self::server();
    }
}