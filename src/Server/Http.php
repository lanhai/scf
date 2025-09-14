<?php

namespace Scf\Server;

use Scf\Cache\Redis;
use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ATable;
use Scf\Database\Exception\NullPool;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Root;
use Scf\Server\Listener\Listener;
use Scf\Server\Struct\Node;
use Scf\Server\Task\CrontabManager;
use Scf\Server\Task\RQueue;
use Scf\Util\File;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;
use Throwable;
use function Co\run;


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
            'Scf\Core\Table\CrontabTable',
            'Scf\Core\Table\MemoryMonitorTable'
        ]);
        Runtime::instance()->serverStatus(false);
        //启动master节点管理面板服务器
        Dashboard::start();
        //启动masterDB(redis协议)服务器
        //MasterDB::start(MDB_PORT);
        //检查Redis是否配置
        $process = new Process(function () {
            App::mount();
            $pool = Redis::pool(\Scf\Server\Manager::instance()->getConfig('service_center_server') ?: 'main');
            if ($pool instanceof NullPool) {
                Runtime::instance()->set('REDIS_ENABLE', false);
                Runtime::instance()->set('REDIS_UNAVAILABLE_REMARK', $pool->getError());
            } else {
                Runtime::instance()->set('REDIS_ENABLE', true);
            }
        });
        $process->start();
        Process::wait();
        if (!Runtime::instance()->get('REDIS_ENABLE')) {
            Console::error("【Server】服务注册不可用:" . Runtime::instance()->get('REDIS_UNAVAILABLE_REMARK'));
            exit(0);
        }
        //加载服务器配置
        $serverConfig = Config::server();
        $this->bindPort = $this->bindPort ?: ($serverConfig['port'] ?? 9580);// \Scf\Core\Server::getUseablePort($this->bindPort ?: ($serverConfig['port'] ?? 9580));
        !defined('APP_MODULE_STYLE') and define('APP_MODULE_STYLE', $serverConfig['module_style'] ?? APP_MODULE_STYLE_LARGE);
        !defined('MAX_REQUEST_LIMIT') and define('MAX_REQUEST_LIMIT', $serverConfig['max_request_limit'] ?? 1280);
        !defined('SLOW_LOG_TIME') and define('SLOW_LOG_TIME', $serverConfig['slow_log_time'] ?? 10000);
        !defined('MAX_MYSQL_EXECUTE_LIMIT') and define('MAX_MYSQL_EXECUTE_LIMIT', $serverConfig['max_mysql_execute_limit'] ?? 1000);
        //开启日志推送
        Console::enablePush($serverConfig['enable_log_push'] ?? 1);
        //实例化服务器
        $this->server = new Server($this->bindHost, mode: SWOOLE_PROCESS);
        //添加后台任务管理子进程
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
                'package_max_length' => $serverConfig['package_max_length'] ?? 10 * 1024 * 1024,
                'open_http_protocol' => true,
                'open_http2_protocol' => true,
                'open_websocket_protocol' => true,
//                'heartbeat_check_interval' => 60,
//                'heartbeat_idle_time' => 180
            ]);
            Runtime::instance()->httpPort($this->bindPort);
        } catch (Throwable $exception) {
            Console::log(Color::red('http服务端口监听启动失败:' . $exception->getMessage()));
            exit(1);
        }
        //监听SOCKET请求
//        $socketPort = self::getUseablePort($this->bindPort + 1);
//        try {
//            $socketServer = $this->server->listen($this->bindHost, $socketPort, SWOOLE_SOCK_TCP);
//            $socketServer->set([
//                'open_http_protocol' => false,
//                'open_http2_protocol' => false,
//                'open_websocket_protocol' => true
//            ]);
//            Runtime::instance()->socketPort($socketPort);
//        } catch (Throwable $exception) {
//            Console::log(Color::red('socket服务端口监听失败:' . $exception->getMessage()));
//            exit(1);
//        }
        //监听RPC服务(tcp)请求
        $rport = RPC_PORT ?: ($serverConfig['rpc_port'] ?? 0);
        if ($rport) {
            try {
                $rpcPort = self::getUseablePort($rport);
                /** @var Server $rpcServer */
                $rpcServer = $this->server->listen('0.0.0.0', $rpcPort, SWOOLE_SOCK_TCP);
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
            Runtime::instance()->serverStatus(false);
            //增加服务器重启次数计数
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
            //断开所有客户端连接
            $clients = $this->server->getClientList();
            if ($clients) {
                foreach ($clients as $fd) {
                    if ($server->isEstablished($fd)) {
                        $server->disconnect($fd);
                    }
                }
                \Scf\Server\Manager::clearAllSocketClients();
            }
        });
        $this->server->on("AfterReload", function () {
            //重置执行中的请求数统计
            Counter::instance()->set(Key::COUNTER_REQUEST_PROCESSING, 0);
            $this->log('第' . Counter::instance()->get(Key::COUNTER_SERVER_RESTART) . '次重启完成');
        });
        $this->server->on('pipeMessage', function ($server, $src_worker_id, $data) {
            echo "#{$server->worker_id} message from #$src_worker_id: $data\n";
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
            File::write(SERVER_PORT_FILE, Runtime::instance()->dashboardPort());
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
                ['DASHBOARD', App::isMaster() ? Color::cyan(Runtime::instance()->get('DASHBOARD_PID')) : '--', App::isMaster() ? Color::green(Runtime::instance()->dashboardPort()) : '--'],
            ];
            if ($rpcPort = Runtime::instance()->rpcPort()) {
                $renderData[] = ['RPC', "--", Color::green($rpcPort)];
            }
            $output = new ConsoleOutput();
            $table = new Table($output);
            $table
                ->setHeaders([Color::cyan('服务'), Color::cyan('进程ID'), Color::cyan('端口号')])
                ->setRows($renderData);
            $table->render();
            //自动更新
            APP_AUTO_UPDATE == STATUS_ON and App::checkVersion();
            Timer::tick(1000, function () {

            });
        });
        try {
            //日志备份进程
            $this->server->addProcess(SubProcess::createLogBackupProcess($this->server));
            //连接主节点
            $this->server->addProcess(SubProcess::connectMaster($this->server));
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

    public function clearWorkerTimer(): void {
        $server = $this->server;
        // 向所有 worker 广播清理定时器（通过 SIGUSR1）
        $workerNum = (int)($server->setting['worker_num'] ?? 0);
        $taskNum = (int)($server->setting['task_worker_num'] ?? 0);
        // 普通 worker
        for ($i = 0; $i < $workerNum; $i++) {
            //$server->sendMessage('cleanTimer', $i);
            $pid = $server->getWorkerPid($i);
            if ($pid > 0) Process::kill($pid, SIGUSR2);
        }
        // task worker
        for ($i = 0; $i < $taskNum; $i++) {
            //$server->sendMessage('cleanTimer', $workerNum + $i);
            $pid = $server->getWorkerPid($workerNum + $i);
            if ($pid > 0) Process::kill($pid, SIGUSR2);
        }

        Console::info('【Server】已向所有 worker 发送清理定时器指令');
    }

    /**
     * 给CrontabManager 发送消息
     * @param $cmd
     * @param array $params
     * @return bool|int
     */
    public function sendCommandToCrontabManager($cmd, array $params = []): bool|int {
        $socket = $this->crontabManagerProcess->exportSocket();
        return $socket->send(JsonHelper::toJson([
            'command' => $cmd,
            'params' => $params,
        ]));
    }

    protected Process $crontabManagerProcess;

    /**
     * 启动定时任务和队列的守护进程,跟随server的重启而重新加载文件创建新进程迭代老进程,注意相关应用内部需要有table记录mannger id自增后自动终止销毁timer/while ture等循环机制避免出现僵尸进程
     * @param $config
     */
    protected function addCrontabProcess($config): void {
        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
        $crontabProcess = new Process(function (Process $process) use ($config) {
            //等待服务器启动完成
            while (true) {
                if (Runtime::instance()->serverStatus()) {
                    break;
                }
                sleep(1);
            }
            define('IS_CRONTAB_PROCESS', true);
            while (true) {
                $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
                if (!Runtime::instance()->crontabProcessStatus()) {
                    Runtime::instance()->crontabProcessStatus(true);
                    $taskList = CrontabManager::start();
                    if ($taskList) {
                        Console::info("【Server】Crontab#{$managerId} 排程任务已创建");
                        while ($ret = Process::wait(false)) {
                            //Console::warning("process exit:" . $ret['pid']);
                            if ($t = CrontabManager::getTaskTableByPid($ret['pid'])) {
                                CrontabManager::removeTaskTable($t['id']);
                            }
                        }
                    }
                }
                $tasks = CrontabManager::getTaskTable();
                if (!$tasks) {
                    Runtime::instance()->crontabProcessStatus(false);
                } else {
                    foreach ($tasks as $processTask) {
                        if (Counter::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR')) {
                            CrontabManager::errorReport($processTask);
                        }
                        $taskInstance = CrontabManager::getTaskTableById($processTask['id']);
                        if ($taskInstance['is_running'] == STATUS_OFF) {
                            sleep($processTask['retry_timeout'] ?? 60);
                            if ($managerId == $processTask['manager_id']) {
                                CrontabManager::createTaskProcess($processTask);
                            } else {
                                CrontabManager::removeTaskTable($processTask['id']);
                            }
                        } elseif ($taskInstance['manager_id'] !== $managerId) {
//                        if (Process::kill((int)$taskInstance['pid'], 0)) {
//                            Process::kill((int)$taskInstance['pid'], SIGKILL);
//                        }
                            CrontabManager::removeTaskTable($processTask['id']);
                        }
                    }
                }

                run(function () use ($process) {
                    $socket = $process->exportSocket();
                    $msg = $socket->recv(timeout: 5);
                    if ($msg && StringHelper::isJson($msg)) {
                        $payload = JsonHelper::recover($msg);
                        $command = $payload['command'] ?? 'unknow';
                        $params = $payload['params'] ?? [];
                        switch ($command) {
                            case 'upgrade':
                                Console::warning("【Server】Crontab 收到升级指令，管理器已迭代");
                                //Runtime::instance()->crontabProcessStatus(false);
                                Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
                                Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                                break;
                            default:
                                Console::info($command);
                        }
                    }
                    unset($socket);
                });
            }
        });
        $this->crontabManagerProcess = $crontabProcess;
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
                $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
                define('IS_REDIS_QUEUE_PROCESS', true);
                while (true) {
                    if (!Runtime::instance()->redisQueueProcessStatus()) {
                        $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
                        Runtime::instance()->redisQueueProcessStatus(true);
                        RQueue::startProcess();
                    }
                    Runtime::instance()->redisQueueProcessStatus(false);
                    Console::warning("【Server】RedisQueue#{$managerId}管理进程已迭代,重启队列进程");
                    sleep(1);
                }
            });
            $pid = $this->server->addProcess($redisQueueProcess);
            $this->log("RedisQueue 管理进程ID编号:{$pid}");
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
        $node->port = Runtime::instance()->httpPort();
        $node->socketPort = Runtime::instance()->httpPort();
        $node->role = App::isMaster() ? 'master' : 'slave';
        $node->started = $this->started;
        $node->restart_times = 0;
        $node->master_pid = $this->server->master_pid;
        $node->manager_pid = $this->server->manager_pid;
        $node->swoole_version = swoole_version();
        $node->cpu_num = swoole_cpu_num();
        $node->stack_useage = Coroutine::getStackUsage();
        $node->heart_beat = time();
        $node->scf_version = SCF_VERSION;
        $node->tables = ATable::list();
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->server_run_mode = APP_RUN_MODE;
        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
        $node->framework_update_ready = file_exists(SCF_ROOT . '/build/update.pack');
        $this->server->addProcess(SubProcess::heartbeat($this->server, $node));
    }

    /**
     * 重启服务器
     * @return void
     */
    public function reload(): void {
        $countdown = 3;
        Console::info('【Server】' . Color::yellow($countdown) . '秒后重启服务器');
        Timer::tick(1000, function ($id) use (&$countdown) {
            $countdown--;
            if ($countdown == 0) {
                Timer::clear($id);
                //定时任务迭代
//                Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
//                Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                $this->sendCommandToCrontabManager('upgrade', [
                    'scene' => 'reload'
                ]);
                $this->clearWorkerTimer();
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
    public function ip(): ?string {
        return $this->ip;
    }

    public function getPort(): int {
        return $this->bindPort;
    }

    public function stats(): array {
        return $this->server->stats();
    }

    protected function _master(): Server {
        return $this->server;
    }

}