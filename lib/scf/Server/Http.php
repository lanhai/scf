<?php

namespace Scf\Server;

use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Updater;
use Scf\Runtime\Table;
use Scf\Server\Listener\CgiListener;
use Scf\Server\Listener\SocketListener;
use Scf\Server\Struct\NodeStruct;
use Scf\Service\QueueManager;
use Scf\Util\Dir;
use Scf\Util\File;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Process;
use Swoole\Runtime;
use Swoole\Server\Task;
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
    protected int $bindPort = 9502;
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
    public static Server $master;


    /**
     * @param $role
     * @param string $host
     * @param int $port
     */
    public function __construct($role, string $host = '0.0.0.0', int $port = 9502) {
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
    public static function create($role, string $host = '0.0.0.0', int $port = 9502): static {
        $class = get_called_class();
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class($role, $host, $port);
        }
        return self::$_instances[$class];
    }

    /**
     * 启动masterDB服务
     * @return void
     */
    protected static function startRuntimeDB() {
        if (!SERVER_IS_MASTER) {
            return;
        }
        $masterDbPid = 0;
        Coroutine::create(function () use (&$masterDbPid) {
            $masterDbPid = File::read(SERVER_MASTER_DB_PID_FILE);
        });
        Event::wait();
        if ($masterDbPid) {
            if (!Process::kill($masterDbPid, 0)) {
                //Console::log(Color::yellow("MasterDB PID:{$masterDbPid} not exist"));
            } else {
                Console::log(Color::yellow("MasterDB PID:{$masterDbPid} killed"));
                Process::kill($masterDbPid, SIGKILL);
                unlink(SERVER_MASTER_DB_PID_FILE);
            }
        }
        $process = new Process(function () {
            \Scf\Core\App::setPath();
            try {
                Redis::instance()->create();
            } catch (\Throwable $exception) {
                Console::log('[' . $exception->getCode() . ']' . Color::red($exception->getMessage()));
            }
        });
        $redisServerPid = $process->start();
        File::write(SERVER_MASTER_DB_PID_FILE, $redisServerPid);
        Console::log('MasterDB Manager PID:' . Color::green($redisServerPid));
    }

    /**
     * 启动后台任务管理
     * @return void
     */
    protected static function startQueueManager() {
//        $managerPid = File::read(SERVER_QUEUE_MANAGER_PID_FILE);
//        if ($managerPid) {
//            if (!Process::kill($managerPid, 0)) {
//                //Console::log(Color::yellow("Queue Manager PID:{$managerPid} not exist"));
//            } else {
//                Console::log(Color::yellow("Queue Manager PID:{$managerPid} killed"));
//                Process::kill($managerPid, SIGKILL);
//                unlink(SERVER_QUEUE_MANAGER_PID_FILE);
//            }
//        }
        $process = new Process(function () {
            \Scf\Core\App::setPath();
            Config::init();
            App::loadModules();
            $managerId = QueueManager::instance()->watch();
            Event::wait();
            Console::error('[' . $managerId . ']startQueueManager 终止运行');
        });
        $pid = $process->start();
        File::write(SERVER_QUEUE_MANAGER_PID_FILE, $pid);
        Console::log('Queue Manager PID:' . Color::green($pid));
    }

    /**
     * 启动队列任务管理
     * @return void
     */
    protected static function startCrontabManager() {
//        $managerPid = File::read(SERVER_CRONTAB_MANAGER_PID_FILE);
//        if ($managerPid) {
//            if (!Process::kill($managerPid, 0)) {
//                //Console::log(Color::yellow("Queue Manager PID:{$managerPid} not exist"));
//            } else {
//                Console::log(Color::yellow("Crontab Manager PID:{$managerPid} killed"));
//                Process::kill($managerPid, SIGKILL);
//                unlink(SERVER_CRONTAB_MANAGER_PID_FILE);
//            }
//        }
        $process = new Process(function () {
            \Scf\Core\App::setPath();

            Config::init();
            App::loadModules();
            if (SERVER_CRONTAB_ENABLE == SWITCH_ON && Crontab::load()) {
                $managerId = Crontab::instance()->start();
                Event::wait();
                Console::error('[' . $managerId . ']startCrontabManager 终止运行');
            }
        });
        $pid = $process->start();
        File::write(SERVER_CRONTAB_MANAGER_PID_FILE, $pid);
        Console::log('Crontab Manager PID:' . Color::green($pid));
    }

    /**
     * 启动mater节点子进程,跟随manager的重启而重新加载文件重启,注意相关应用内部需要有table记录mannger id自增后自动终止销毁timer/while ture等循环机制避免出现僵尸进程
     * @return void
     */
    protected static function startMasterProcess() {
        if (SERVER_IS_MASTER) {
            self::startQueueManager();
            self::startCrontabManager();
        }
    }

    /**
     * 启动服务
     * @return void
     */
    public function start() {
        Runtime::enableCoroutine();
        //初始化内存表格,放在最前面保证所有进程间能共享
        Table::register([
            'Scf\Server\Table\LogTable',
            'Scf\Server\Table\Counter',
            'Scf\Server\Table\Runtime',
        ]);
        self::startRuntimeDB();
        self::startMasterProcess();
        //实例化服务器
        $this->server = new Server($this->bindHost);
        self::$master = $this->server;
        $configFile = \Scf\Core\App::srcPath() . 'config/server.php';
        $serverConfig = file_exists($configFile) ? require_once $configFile : [
            'port' => 9502,
            'worker_num' => 10,
            'max_wait_time' => 60,
            'task_worker_num' => 100,
        ];
        $setting = [
            'worker_num' => $serverConfig['worker_num'] ?? 128,
            'max_wait_time' => $serverConfig['max_wait_time'] ?? 60,
            'reload_async' => true,
            'daemonize' => Manager::instance()->issetOpt('d'),
            'log_file' => APP_PATH . 'log/server.log',
            'pid_file' => SERVER_MASTER_PID_FILE,
            'task_worker_num' => $serverConfig['task_worker_num'] ?? 128,
            'task_enable_coroutine' => true
        ];
        if (SERVER_ENABLE_STATIC_HANDER) {
            $setting['document_root'] = APP_PATH . 'public';
            $setting['enable_static_handler'] = true;
            $setting['http_autoindex'] = true;
            $setting['http_index_files'] = ['index.html'];
            $setting['static_handler_locations'] = ['/document', '/cp'];
        }
        $this->server->set($setting);
        //监听HTTP请求
        try {
            $httpPort = $this->server->listen($this->bindHost, $serverConfig['port'] ?? $this->bindPort, SWOOLE_SOCK_TCP);
            $httpPort->set([
                'open_http_protocol' => true,
                'open_http2_protocol' => true,
                'open_websocket_protocol' => false
            ]);
        } catch (\Throwable $exception) {
            Console::log(Color::red('http服务器启动失败:' . $exception->getMessage()));
            exit(1);
        }
        //监听SOCKET请求
        try {
            $socketPort = $this->server->listen($this->bindHost, isset($serverConfig['port']) ? $serverConfig['port'] + 1 : $this->bindPort + 1, SWOOLE_SOCK_TCP);
            $socketPort->set([
                'open_http_protocol' => false,
                'open_http2_protocol' => false,
                'open_websocket_protocol' => true
            ]);
        } catch (\Throwable $exception) {
            Console::log(Color::red('socket服务器启动失败:' . $exception->getMessage()));
            exit(1);
        }
        SocketListener::bind('handshake', 'message', 'close');
        CgiListener::bind('request');
        $this->server->on('WorkerError', function ($server, int $worker_id, int $worker_pid, int $exit_code, int $signal) {
            $this->log(Color::red('worker #' . $worker_id . ' 发生致命错误!signal:' . $signal . ',exit_code:' . $exit_code));
        });
//        //WebSocket连接打开事件,只有未设置handshake回调时此回调才生效
//        $this->server->on('open', function (Server $server, Request $request) {
//            print_r($request);
//            $server->push($request->fd, "未授权的请求");
//            $server->close($request->fd);
//        });
//        $this->server->on('connect', function (Server $server, $clientId) {
//            $client = $this->server->getClientInfo($clientId);
//            $this->log('新连接:' . $client['remote_ip']);
//        });
        $this->server->on("BeforeReload", function (Server $server) {
            //即将重启
            //Log::instance()->info('开始重启');
            $clients = $this->server->getClientList(0);
            if ($clients) {
                foreach ($clients as $fd) {
                    if ($server->isEstablished($fd)) {
                        Log::instance()->info('结束会话:' . $fd);
                        Console::unsubscribe($fd);
                    }
                }
            }
        });
        $this->server->on("AfterReload", function (Server $server) {
            $this->restartTimes += 1;
            \Scf\Server\Table\Runtime::instance()->set('restart_times', $this->restartTimes);
            Log::instance()->info('第' . $this->restartTimes . '次重启完成');
            self::startMasterProcess();
        });
        //worker启动完成
        $this->server->on('WorkerStart', function (Server $server, $workerId) {
            \Scf\Core\App::setPath();
            Config::init();
            Console::enablePush();
            //Log::instance()->enableTable();
            //要使用app命名空间必须先加载模块
            App::loadModules();
            if ($workerId == 0) {
                //队列任务放在这里执行的原因是业务代码变动后自动重新加载
                //QueueManager::instance()->watch();
//                if (SERVER_IS_MASTER && SERVER_CRONTAB_ENABLE == SWITCH_ON && Crontab::load()) {
//                    Crontab::instance()->start();
//                }
                $srcPath = \Scf\Core\App::srcPath();
                $version = \Scf\Core\App::version();
                $info = <<<INFO
---------Workers启动完成---------
应用版本：{$version}
源码目录：{$srcPath}
---------------------------------
INFO;
                Console::write(Color::green($info));
            }
        });
        //处理异步任务(此回调函数在task进程中执行)
        //携程模式 task_enable_coroutine=true
        $this->server->on('Task', function (Server $server, Task $task) {
            //执行任务
            $task->data['handler']::instance()->execute($task);
        });
        //非携程模式
//        $this->server->on('Task', function (Server $server, $taskId, $reactor_id, $data) {
//            //执行任务
//            $data['handler']::instance()->execute($taskId, ...$data);
//        });
        //处理异步任务的结果(此回调函数在worker进程中执行)
        $this->server->on('Finish', function (Server $server, $taskId, $data) {
            Console::log(Color::green($taskId . ' 执行完成:' . JsonHelper::toJson($data)));
        });
        //服务器完成启动
        $this->server->on('start', function (Server $server) use ($serverConfig) {
            \Scf\Core\App::setPath();
            $this->onStart($server, $serverConfig);
        });
        try {
            $this->server->start();
        } catch (\Throwable $exception) {
            Console::error($exception->getMessage());
        }

    }

    /**
     * 服务器完成启动
     * @return void
     */
    protected function onStart(Server $server, $serverConfig) {
        $masterPid = $server->master_pid;
        $managerPid = $server->manager_pid;
        define("SERVER_MASTER_PID", $masterPid);
        define("SERVER_MANAGER_PID", $managerPid);
        Config::init();
        Log::instance()->enableTable();
        $role = SERVER_ROLE;
        $env = APP_RUN_ENV;
        $mode = APP_RUN_MODE;
        $alias = SERVER_ALIAS;
        $port = $serverConfig['port'] ?? $this->bindPort;
        $files = count(get_included_files());
        $os = SERVER_ENV;
        $host = SERVER_HOST;
        $version = swoole_version();
        $info = <<<INFO
---------Server启动完成---------
内网地址：{$host}
运行系统：{$os}
节点名称：{$alias}
运行环境：{$env}
运行模式：{$mode}
节点角色：{$role}
监听地址：{$this->bindHost}:{$port}
管理进程：{$masterPid}
文件加载：{$files}
SW版本号：{$version}
Worker：{$serverConfig['worker_num']}
Task Worker：{$serverConfig['task_worker_num']}
--------------------------------
INFO;
        Console::write(Color::yellow($info));
        $this->enable();
        $this->report();
        //延迟一秒执行避免和 WorkerStart 事件后的节点报到事件抢redis锁
        Coroutine::sleep(1);
        $this->heartbeat();
        $this->backupLog();
        Env::isDev() || Manager::instance()->issetOpt('watch') and $this->watchFileChange();
        Env::isDev() and $this->checkVersion();
    }

    /**
     * 检查版本
     * @return void
     */
    protected function checkVersion() {
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
        if ($updater->hasNewAppVersion() && ($updater->isEnableAutoUpdate() || $version['remote']['app']['forced'] == true)) {
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
            if ($updater->isEnableAutoUpdate() || $version['remote']['app']['forced'] == true) {
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

    protected function reload() {
        $countdown = 5;
        Console::log(Color::yellow($countdown) . '秒后重启服务器');
        $this->server->tick(1000, function ($id) use (&$countdown) {
            $countdown--;
            if ($countdown == 0) {
                $version = Updater::instance()->getVersion();
                $this->server->clearTimer($id);
                $this->server->reload();
                Log::instance()->info('已更新至版本:' . $version['remote']['app']['version']);
                $this->enable();
            } else {
                Console::log(Color::yellow($countdown) . '秒后重启服务器');
            }
        });
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
                Log::instance()->info('更新失败:' . $version['remote']['app']['version']);
            }
        }
        return false;
    }

    /**
     * 转存内存table的日志到redis和日志文件
     * @return void
     */
    protected function backupLog() {
        $pid = Coroutine::create(function () {
            $logger = Log::instance();
            Timer::tick(3000, function () use ($logger) {
                $count = $logger->backup();
                //$count > 0 and $this->log('本次同步转存日志:' . $count);
            });
        });
        $this->log('日志备份启动完成!协程ID:' . $pid);
    }

    /**
     * 发送心跳包
     * @return void
     */
    protected function heartbeat() {
        $manager = \Scf\Server\Manager::instance();
        $pid = Coroutine::create(function () use ($manager) {
            Timer::tick(1000, function () use ($manager) {
                $manager->heartbeat();
                //Coroutine::sleep(1);
            });
        });
        $this->log('节点心跳报到服务启动完成!协程ID:' . $pid);
    }

    /**
     * 监听文件改动自动重启服务
     * @return void
     */
    protected function watchFileChange() {
        $pid = Coroutine::create(function () {
            if (APP_RUN_MODE == 'src') {
                $appFiles = Dir::getDirFiles(APP_PATH . 'src');
            } else {
                $appFiles = [];
                // $appFiles = [SCF_ROOT . '/' . APP_ID . '.app'];
            }
            $files = [...$appFiles, ...Dir::getDirFiles(SCF_ROOT . '/lib/scf')];
            $fileList = [];
            foreach ($files as &$path) {
                $fileList[] = [
                    'path' => $path,
                    'md5' => md5_file($path)
                ];
            }
            while (true) {
                $changed = false;
                $changedFiles = [];
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
                    Console::write('---------以下文件发生变动,即将重启---------');
                    foreach ($changedFiles as $f) {
                        Console::write($f);
                    }
                    Console::write('-------------------------------------------');
                    $this->server->reload();
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
    protected function report() {
        $manager = \Scf\Server\Manager::instance();
        $node = NodeStruct::factory();
        $node->appid = APP_ID;
        $node->id = $this->id;
        $node->name = $this->name;
        $node->ip = $this->ip;
        $node->port = SERVER_PORT;
        $node->socketPort = SERVER_PORT + 1;
        $node->role = $this->role;
        $node->started = $this->started;
        $node->restart_times = $this->restartTimes;
        $node->master_pid = SERVER_MASTER_PID;
        $node->manager_pid = SERVER_MANAGER_PID;
        $node->swoole_version = swoole_version();
        $node->cpu_num = swoole_cpu_num();
        $node->stack_useage = Coroutine::getStackUsage();
        $node->heart_beat = time();
        try {
            $manager->report($node);
        } catch (\Exception $exception) {
            $this->log('节点报道失败:' . Color::red($exception->getMessage()));
        }
    }

    /**
     * 向socket客户端推送内容
     * @param $fd
     * @param $str
     * @return false|mixed
     */
    public function push($fd, $str): mixed {
        try {
            return $this->server->push($fd, $str);
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * @return int
     */
    public function getReloadTimes(): int {
        return \Scf\Server\Table\Runtime::instance()->get('restart_times');
    }


    /**
     * 设置服务为可用状态
     * @return void
     */
    protected function enable() {
        $this->serviceEnable = true;
        \Scf\Server\Table\Runtime::instance()->set('_SERVER_STATUS_', 1);

    }

    /**
     * 设置服务为不可用状态
     * @return void
     */
    protected function disable() {
        $this->serviceEnable = false;
        \Scf\Server\Table\Runtime::instance()->set('_SERVER_STATUS_', 0);
    }

    /**
     * 服务是否可用
     * @return bool
     */
    public function isEnable(): bool {
        $status = \Scf\Server\Table\Runtime::instance()->get('_SERVER_STATUS_');
        $this->serviceEnable = $status == 1;
        return $this->serviceEnable;
    }

    /**
     * @return mixed|string
     */
    public function ip(): mixed {
        return $this->ip;
    }

    /**
     * @return Server
     */
    public static function master(): Server {
        return self::$master;
    }
}