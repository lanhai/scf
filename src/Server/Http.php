<?php

namespace Scf\Server;

use Scf\App\Updater;
use Scf\Core\Config;
use Scf\Core\Console;
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
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\File;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
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
    protected int $bindPort = 9580;
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
     * @var int 重启次数
     */
    protected int $restartTimes = 0;

    /**
     * @var bool 服务是否可用
     */
    protected bool $serviceEnable = false;

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
//    public static function startMasterProcess($config) {
//        $process = new Process(function () use ($config) {
//            $runQueueInMaster = $config['redis_queue_in_master'] ?? true;
//            $runQueueInSlave = $config['redis_queue_in_slave'] ?? false;
//            Counter::instance()->incr('_background_process_id_');
//            define('IS_CRONTAB_PROCESS', true);
//            while (true) {
//                if (Runtime::instance()->get('_background_process_status_') == STATUS_OFF) {
//                    Runtime::instance()->set('_background_process_status_', STATUS_ON);
//                    Crontab::startProcess();
//                    if ((App::isMaster() && $runQueueInMaster) || (!App::isMaster() && $runQueueInSlave)) {
//                        $runQueueInMaster and RQueue::startProcess();
//                    }
//                }
//                usleep(1000 * 1000 * 30);
//            }
//        });
//        $pid = $process->start();
//        Console::success('Background Process PID:' . Color::info($pid));
//    }
    protected function addCrontabProcess($config): void {
        $process = new Process(function () use ($config) {
            $runQueueInMaster = $config['redis_queue_in_master'] ?? true;
            $runQueueInSlave = $config['redis_queue_in_slave'] ?? false;
            Counter::instance()->incr('_background_process_id_');
            define('IS_CRONTAB_PROCESS', true);
            while (true) {
                if (Runtime::instance()->get('_background_process_status_') == STATUS_OFF) {
                    Runtime::instance()->set('_background_process_status_', STATUS_ON);
                    Crontab::startProcess();
                    if ((App::isMaster() && $runQueueInMaster) || (!App::isMaster() && $runQueueInSlave)) {
                        $runQueueInMaster and RQueue::startProcess();
                    }
                }
                usleep(1000 * 1000 * 30);
            }
        });
        $this->server->addProcess($process);
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
        $this->bindPort = $this->bindPort ?: 9580;
        //启动master节点管理面板服务器
        Dashboard::start($this->bindPort + 2);
        //等待APP安装完成
        App::await();
        //加载服务器配置
        $serverConfig = Config::server();
        !defined('MAX_REQUEST_LIMIT') and define('MAX_REQUEST_LIMIT', $serverConfig['max_request_limit']);
        !defined('SLOW_LOG_TIME') and define('SLOW_LOG_TIME', $serverConfig['slow_log_time'] ?? 10000);
        !defined('MAX_MYSQL_EXECUTE_LIMIT') and define('MAX_MYSQL_EXECUTE_LIMIT', $serverConfig['max_mysql_execute_limit'] ?? (128 * 10));
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
        //启动masterDB(redis)服务器
        MasterDB::start(MDB_PORT);
        //启动后台任务管理进程
        //self::startMasterProcess($serverConfig);
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
            $setting['static_handler_locations'] = $serverConfig['static_handler_locations'] ?? ['/document', '/cp'];
        }
        $this->server->set($setting);
        //监听HTTP请求
        try {
            $httpPort = $this->server->listen($this->bindHost, $this->bindPort, SWOOLE_SOCK_TCP);
            $httpPort->set([
                'package_max_length' => $serverConfig['package_max_length'] ?? 8 * 1024 * 1024,
                'open_http_protocol' => true,
                'open_http2_protocol' => true,
                'open_websocket_protocol' => true
            ]);
        } catch (Throwable $exception) {
            Console::log(Color::red('http服务端口监听启动失败:' . $exception->getMessage()));
            exit(1);
        }
        //监听SOCKET请求
        try {
            $socketPort = $this->server->listen($this->bindHost, ($this->bindPort + 1), SWOOLE_SOCK_TCP);
            $socketPort->set([
                'open_http_protocol' => false,
                'open_http2_protocol' => false,
                'open_websocket_protocol' => true
            ]);
        } catch (Throwable $exception) {
            Console::log(Color::red('socket服务端口监听失败:' . $exception->getMessage()));
            exit(1);
        }
        //监听RPC服务(tcp)请求
        try {
            /** @var Server $rpcPort */
            $rpcPort = $this->server->listen($this->bindHost, ($this->bindPort + 5), SWOOLE_SOCK_TCP);
            $rpcPort->set([
                'open_http_protocol' => false,
                'open_http2_protocol' => false,
                'open_websocket_protocol' => false
            ]);
        } catch (Throwable $exception) {
            Console::log(Color::red('RPC服务端口监听失败:' . $exception->getMessage()));
            exit(1);
        }
        Listener::register([
            'SocketListener',
            'CgiListener',
            'WorkerListener',
            'RpcListener',
            'TaskListener'
        ]);
        $this->server->on("BeforeReload", function (Server $server) {
            //增加服务器重启次数计数
            $this->restartTimes += 1;
            Counter::instance()->incr('_HTTP_SERVER_RESTART_COUNT_');
            //后台定时任务进程ID迭代
            Counter::instance()->incr('_background_process_id_');
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
            $this->log(Color::notice('第' . $this->restartTimes . '次重启完成'));
            //重置执行中的请求数统计
            Counter::instance()->set('_REQUEST_PROCESSING_', 0);
            //Counter::instance()->incr('_background_process_id_');
            //Runtime::instance()->set('_background_process_status_', STATUS_OFF);
        });
        //服务器完成启动
        $this->server->on('start', function (Server $server) use ($serverConfig) {
            $this->onStart($server, $serverConfig);
        });
        try {
            $this->server->start();
        } catch (Throwable $exception) {
            Console::error($exception->getMessage());
        }
    }

    /**
     * 服务器完成启动
     * @param Server $server
     * @param $serverConfig
     * @return void
     */
    protected function onStart(Server $server, $serverConfig): void {
        if (!App::isReady()) {
            Console::info("等待安装配置文件就绪...");
            $this->waittingInstall();
        }
        App::mount();
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
        $port = $this->bindPort;
        $files = count(get_included_files());
        $os = SERVER_ENV;
        $host = SERVER_HOST;
        $version = swoole_version();
        $fingerprint = APP_FINGERPRINT;
        $info = <<<INFO
---------Server启动完成---------
主机地址：{$host}
运行系统：{$os}
节点名称：{$alias}
运行环境：{$env}
运行模式：{$mode}
节点角色：{$role}
监听地址：{$this->bindHost}:{$port}
管理进程：{$managerPid}
文件加载：{$files}
SW版本号：{$version}
框架版本：{$scfVersion}
应用指纹：{$fingerprint}
Worker：{$serverConfig['worker_num']}
Task Worker：{$serverConfig['task_worker_num']}
Master:$masterPid
--------------------------------
INFO;
        Console::write(Color::info($info));
        $this->enable();
        //延迟一秒执行避免和 WorkerStart 事件后的节点报到事件抢redis锁
        Coroutine::sleep(1);
        $this->report();
        $this->backupLog();
        (Env::isDev() && APP_RUN_MODE == 'src') || Manager::instance()->issetOpt('watch') and $this->watchFileChange();
        APP_AUTO_UPDATE == STATUS_ON and $this->checkVersion();
    }

    /**
     * 等待安装
     * @return void
     */
    protected function waittingInstall(): void {
        while (true) {
            $app = App::installer();
            if ($app->readyToInstall()) {
                $updater = Updater::instance();
                $version = $updater->getVersion();
                $this->disable();
                Log::instance()->info('开始执行更新:' . $version['remote']['app']['version']);
                if ($updater->updateApp()) {
                    App::mount();
                    $this->reload();
                    break;
                } else {
                    Log::instance()->info('更新失败:' . $version['remote']['app']['version']);
                }
            }
            usleep(5000 * 1000);
        }
    }

    /**
     * 检查版本
     * @return void
     */
    protected function checkVersion(): void {
        if (APP_RUN_MODE != 'phar') {
            $this->enable();
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
        } else {
            $this->enable();
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
        if ($updater->hasNewAppVersion()) {
            //强制更新
            if ($updater->isEnableAutoUpdate() || $version['remote']['app']['forced']) {
                $this->disable();
                Log::instance()->info('开始执行更新:' . $version['remote']['app']['version']);
                if ($updater->updateApp()) {
                    $this->reload();
                } else {
                    Log::instance()->info('更新失败:' . $version['remote']['app']['version']);
                }
            } else {
                //Log::instance()->info('检测到新版本:' . $version['remote']['app']['version']);
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
        Console::log(Color::yellow($countdown) . '秒后重启服务器');
        Timer::tick(1000, function ($id) use (&$countdown) {
            $countdown--;
            if ($countdown == 0) {
                $version = Updater::instance()->getVersion();
                Timer::clear($id);
                $this->server->reload();
                if (App::isMaster()) {
                    $dashboardHost = PROTOCOL_HTTP . '127.0.0.1:' . ($this->bindPort + 2) . '/reload';
                    $client = \Scf\Client\Http::create($dashboardHost);
                    $client->get();
                }
                Log::instance()->info('已更新至版本:' . $version['remote']['app']['version']);
                $this->enable();
            } else {
                Console::log(Color::yellow($countdown) . '秒后重启服务器');
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
        $this->disable();
        if (Updater::instance()->appointUpdateTo($type, $version)) {
            $type == 'app' and $this->reload();
            usleep(5000);
            $this->enable();
            return true;
        } else {
            $this->enable();
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
        $this->disable();
        if (!is_null($version)) {
            Log::instance()->info('开始执行更新:' . $version);
            if ($updater->changeAppVersion($version)) {
                $this->reload();
//                $this->server->reload();
//                Log::instance()->info('已更新至版本:' . $version);
//                $this->enable();
                return true;
            } else {
                $this->enable();
                Log::instance()->info('更新失败:' . $version);
            }
        } else {
            $version = $updater->getVersion();
            Log::instance()->info('开始执行更新:' . $version['remote']['app']['version']);
            if ($updater->updateApp()) {
                $this->reload();
//                $this->server->reload();
//                Log::instance()->info('已更新至版本:' . $version['remote']['app']['version']);
//                $this->enable();
                return true;
            } else {
                $this->enable();
                Log::instance()->info('更新失败:' . $version['remote']['app']['version']);
            }
        }
        return false;
    }

    /**
     * 转存内存table的日志到redis和日志文件
     * @return void
     */
    protected function backupLog(): void {
        $pid = Coroutine::create(function () {
            $logger = Log::instance();
            Timer::tick(3000, function () use ($logger) {
                $logger->backup();
            });
        });
        $this->log('日志备份启动完成!协程ID:' . $pid);
    }


    /**
     * 监听文件改动自动重启服务
     * @return void
     */
    protected function watchFileChange(): void {
        $pid = Coroutine::create(function () {
            $scanDirectories = function () {
                if (APP_RUN_MODE == 'src') {
                    $appFiles = Dir::scan(APP_PATH . '/src');
                } else {
                    $appFiles = [];
                }
                return [...$appFiles, ...Dir::scan(Root::dir())];
            };

            $files = $scanDirectories();
            $fileList = [];
            foreach ($files as $path) {
                $fileList[] = [
                    'path' => $path,
                    'md5' => md5_file($path)
                ];
            }

            while (true) {
                $changed = false;
                $changedFiles = [];

                // Rescan directories to find new files
                $currentFiles = $scanDirectories();
                $currentFilePaths = array_map(fn($file) => $file, $currentFiles);

                // Check for new files
                foreach ($currentFilePaths as $path) {
                    if (!in_array($path, array_column($fileList, 'path'))) {
                        $fileList[] = [
                            'path' => $path,
                            'md5' => md5_file($path)
                        ];
                        $changed = true;
                        $changedFiles[] = $path;
                    }
                }

                foreach ($fileList as $key => &$file) {
                    if (!file_exists($file['path'])) {
                        $changed = true;
                        $changedFiles[] = $file['path'];
                        unset($fileList[$key]);
                        continue;
                    }
                    $getMd5 = md5_file($file['path']);
                    if (strcmp($file['md5'], $getMd5) !== 0) {
                        $file['md5'] = $getMd5;
                        $changed = true;
                        $changedFiles[] = $file['path'];
                    }
                }

                if ($changed) {
                    Console::warning('---------以下文件发生变动,即将重启---------');
                    foreach ($changedFiles as $f) {
                        Console::write($f);
                    }
                    Console::warning('-------------------------------------------');
                    $this->server->reload();
                    // Console::info("重启状态:" . $this->server->reload());
                    if (App::isMaster()) {
                        $dashboardHost = PROTOCOL_HTTP . '127.0.0.1:' . ($this->bindPort + 2) . '/reload';
                        $client = \Scf\Client\Http::create($dashboardHost);
                        $client->get();
                    }
                }
                Coroutine::sleep(3);
            }
        });
        $this->log('文件改动监听服务启动完成!ID:' . $pid);
    }

    /**
     * 向节点管理员报到
     * @return void
     */
    protected function report(): void {
        $manager = \Scf\Server\Manager::instance();
        $node = Node::factory();
        $node->appid = App::id();
        $node->id = $this->id;
        $node->name = $this->name;
        $node->ip = $this->ip;
        $node->fingerprint = APP_FINGERPRINT;
        $node->port = $this->bindPort;
        $node->socketPort = $this->bindPort + 1;
        $node->role = App::isMaster() ? 'master' : 'slave';
        $node->started = $this->started;
        $node->restart_times = 0;
        $node->master_pid = SERVER_MASTER_PID;
        $node->manager_pid = SERVER_MANAGER_PID;
        $node->swoole_version = swoole_version();
        $node->cpu_num = swoole_cpu_num();
        $node->stack_useage = Coroutine::getStackUsage();
        $node->heart_beat = time();
        $node->app_version = App::version();
        $node->public_version = App::publicVersion();
        $node->scf_version = SCF_VERSION;
        $node->tables = Table::list();
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->server_run_mode = APP_RUN_MODE;
        try {
            if ($manager->report($node) === false) {
                $this->log('节点报道失败:' . Color::red("MasterDB不可用"));
                Timer::after(5000, function () {
                    $this->report();
                });
            }
            Timer::tick(1000, function () use ($manager, $node) {
                if (Counter::instance()->exist('_REQUEST_COUNT_' . Date::leftday(2))) {
                    Counter::instance()->delete('_REQUEST_COUNT_' . Date::leftday(2));
                }
                Counter::instance()->delete('_MYSQL_EXECUTE_COUNT_' . (time() - 5));
                Counter::instance()->delete('_REQUEST_COUNT_' . (time() - 5));
                $manager->heartbeat($this->server, $node);
            });
        } catch (\Exception $exception) {
            $this->log('节点报道失败:' . Color::red($exception->getMessage()));
        }
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
     * 设置服务为可用状态
     * @return void
     */
    protected function enable(): void {
        $this->serviceEnable = true;
        Runtime::instance()->set('_SERVER_STATUS_', 1);

    }

    /**
     * 设置服务为不可用状态
     * @return void
     */
    protected function disable(): void {
        $this->serviceEnable = false;
        Runtime::instance()->set('_SERVER_STATUS_', 0);
    }

    /**
     * 服务是否可用
     * @return bool
     */
    public function isEnable(): bool {
        $status = Runtime::instance()->get('_SERVER_STATUS_');
        $this->serviceEnable = $status == 1;
        return $this->serviceEnable;
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