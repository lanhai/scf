<?php

namespace Scf\Server;

use Scf\Command\Color;
use Scf\Command\Manager as CommandManager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Root;
use Scf\Server\Struct\Node;
use Scf\Server\Task\CrontabManager;
use Scf\Server\Task\RQueue;
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use Throwable;
use function Co\run;

class SubProcess {

    protected array $process = [];
    protected array $pidList = [];
    protected array $serverConfig;
    protected Server $server;
    protected Process $consolePushProcess;

    public function __construct(Server $server, $serverConfig) {
        $this->server = $server;
        $this->serverConfig = $serverConfig;
        $runQueueInMaster = $serverConfig['redis_queue_in_master'] ?? true;
        $runQueueInSlave = $serverConfig['redis_queue_in_slave'] ?? false;
        //内存使用情况统计
        $this->process['MemoryUsageCount'] = $this->createMemoryUsageCountProcess();
        //心跳检测
        $this->process['Heartbeat'] = $this->createHeartbeatProcess();
        //日志备份
        $this->process['LogBackup'] = $this->createLogBackupProcess();
        //定时任务
        $this->process['CrontabManager'] = $this->createCrontabManagerProcess();
        //控制台日志推送
        $this->consolePushProcess = $this->createConsolePushProcess();
        //redis队列
        if ((App::isMaster() && $runQueueInMaster) || (!App::isMaster() && $runQueueInSlave)) {
            $this->process['RedisQueue'] = $this->createRedisQueueProcess();
        }
        //文件变更监听
        if ((Env::isDev() && APP_SRC_TYPE == 'dir') || CommandManager::instance()->issetOpt('watch')) {
            $this->process['FileWatch'] = $this->createFileWatchProcess();
        }
    }

    public function start(): void {
        $this->consolePushProcess->start();
        foreach ($this->process as $name => $process) {
            /** @var Process $process */
            $process->start();
            $this->pidList[$process->pid] = $name;
        }
        while (true) {
            if (!Runtime::instance()->serverIsAlive()) {
                break;
            }
            if ($ret = Process::wait()) {
                $pid = $ret['pid'];
                if (isset($this->pidList[$pid])) {
                    $oldProcessName = $this->pidList[$pid];
                    unset($this->pidList[$pid]);
                    Console::warning("【{$oldProcessName}】子进程#{$pid}退出，准备重启");
                    switch ($oldProcessName) {
                        case 'MemoryUsageCount':
                            $newProcess = $this->createMemoryUsageCountProcess();
                            $this->process['MemoryUsageCount'] = $newProcess;
                            break;
                        case 'Heartbeat':
                            $newProcess = $this->createHeartbeatProcess();
                            $this->process['Heartbeat'] = $newProcess;
                            break;
                        case 'LogBackup':
                            $newProcess = $this->createLogBackupProcess();
                            $this->process['LogBackup'] = $newProcess;
                            break;
                        case 'CrontabManager':
                            $newProcess = $this->createCrontabManagerProcess();
                            $this->process['CrontabManager'] = $newProcess;
                            break;
                        case 'FileWatch':
                            $newProcess = $this->createFileWatchProcess();
                            $this->process['FileWatch'] = $newProcess;
                            break;
                        case 'RedisQueue':
                            $newProcess = $this->createRedisQueueProcess();
                            $this->process['RedisQueue'] = $newProcess;
                            break;
                        default:
                            Console::warning("子进程 {$pid} 退出，未知进程");
                    }
                    if (!empty($newProcess)) {
                        $newProcess->start();
                        $this->pidList[$newProcess->pid] = $oldProcessName;
                    }
                }
            }
            sleep(1);
        }
    }

    public function shutdown(): void {
        //Process::kill($this->consolePushProcess->pid, SIGTERM);
        $this->consolePushProcess->write('shutdown');
        foreach ($this->process as $process) {
            /** @var Process $process */
            $process->write('shutdown');
            //$process->exit();
            //Process::kill($process->pid, SIGTERM);
        }
    }

    public function sendCommand($cmd, array $params = []): void {
        foreach ($this->process as $process) {
            /** @var Process $process */
            $socket = $process->exportSocket();
            $socket->send(JsonHelper::toJson([
                'command' => $cmd,
                'params' => $params,
            ]));
        }
    }

    /**
     * 推送控制台日志
     * @param $time
     * @param $message
     * @return bool|int
     */
    public function pushConsoleLog($time, $message): bool|int {
        if (isset($this->consolePushProcess)) {
            $socket = $this->consolePushProcess->exportSocket();
            return $socket->send(JsonHelper::toJson([
                'time' => $time,
                'message' => $message,
            ]));
        }
        return false;
    }

    /**
     * 控制台消息推送socket
     * @return Process
     */
    private function createConsolePushProcess(): Process {
        return new Process(function (Process $process) {
            App::mount();
            Console::info("【ConsolePush】控制台消息推送PID:" . $process->pid, false);
            MemoryMonitor::start('ConsolePush');
            run(function () use ($process) {
                while (true) {
                    $masterSocket = Manager::instance()->getMasterSocketConnection();
                    while (true) {
                        $masterSocket->push('::ping');
                        $reply = $masterSocket->recv(5);
                        if ($reply === false || empty($reply->data)) {
                            $masterSocket->close();
                            Console::warning('【ConsolePush】与master节点连接已断开', false);
                            break;
                        }
                        $processSocket = $process->exportSocket();
                        $msg = $processSocket->recv(timeout: 30);
                        if ($msg) {
                            if (StringHelper::isJson($msg)) {
                                $payload = JsonHelper::recover($msg);
                                $masterSocket->push(JsonHelper::toJson(['event' => 'console_log', 'data' => [
                                    'host' => SERVER_ROLE == NODE_ROLE_MASTER ? 'master' : SERVER_HOST,
                                    ...$payload
                                ]]));
                            } elseif ($msg == 'shutdown') {
                                $masterSocket->close();
                                Console::error('【ConsolePush】管理进程退出,结束推送', false);
                                MemoryMonitor::stop();
                                Process::kill($process->pid, SIGTERM);
                                //break 2;
                            }
                        }
                        MemoryMonitor::updateUsage('ConsolePush');
                    }
                }
            });
        });
    }

    /**
     * redis队列
     * @return Process
     */
    private function createRedisQueueProcess(): Process {
        return new Process(function (Process $process) {
            Console::info("【RedisQueue】Redis队列管理PID:" . $process->pid, false);
            define('IS_REDIS_QUEUE_PROCESS', true);
            while (true) {
                if (!Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }
                $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
                if (!Runtime::instance()->redisQueueProcessStatus() && Runtime::instance()->serverIsAlive()) {
                    $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
                    Runtime::instance()->redisQueueProcessStatus(true);
                    RQueue::startProcess();
                }
                Runtime::instance()->redisQueueProcessStatus(false);
                $cmd = $process->read();
                if ($cmd == 'shutdown') {
                    Console::error("【RedisQueue】#{$managerId} 服务器已关闭,结束运行");
                    break;
                } else {
                    Console::warning("【RedisQueue】#{$managerId}管理进程已迭代,重新创建进程");
                    sleep(1);
                }
            }
        });
    }

    /**
     * 排程任务
     * @return Process
     */
    private function createCrontabManagerProcess(): Process {
        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
        return new Process(function (Process $process) {
            Console::info("【Crontab】排程任务管理PID:" . $process->pid, false);
            define('IS_CRONTAB_PROCESS', true);
            while (true) {
                if (!Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }
                $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
                if (!Runtime::instance()->crontabProcessStatus() && Runtime::instance()->serverIsAlive()) {
                    $taskList = CrontabManager::start();
                    Runtime::instance()->crontabProcessStatus(true);
                    if ($taskList) {
                        while ($ret = Process::wait(false)) {
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
                        if (!isset($processTask['id'])) {
                            Console::warning("【Crontab】任务ID为空:" . JsonHelper::toJson($processTask));
                            continue;
                        }
                        if (Counter::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR')) {
                            CrontabManager::errorReport($processTask);
                        }
                        $taskInstance = CrontabManager::getTaskTableById($processTask['id']);
                        if ($taskInstance['process_is_alive'] == STATUS_OFF) {
                            sleep($processTask['retry_timeout'] ?? 60);
                            if ($managerId == $processTask['manager_id']) {//重新创建发生致命错误的任务进程
                                CrontabManager::createTaskProcess($processTask, $processTask['restart_num'] + 1);
                            } else {
                                CrontabManager::removeTaskTable($processTask['id']);
                            }
                        } elseif ($taskInstance['manager_id'] !== $managerId) {
                            CrontabManager::removeTaskTable($processTask['id']);
                        }
                    }
                }
                $shouldExit = false;
                run(function () use ($process, $managerId, &$shouldExit) {
                    $socket = $process->exportSocket();
                    $msg = $socket->recv(timeout: 5);
                    if ($msg) {
                        if (StringHelper::isJson($msg)) {
                            $payload = JsonHelper::recover($msg);
                            $command = $payload['command'] ?? 'unknow';
                            Console::log("【Crontab】#{$managerId} 收到命令:" . Color::cyan($command));
                            switch ($command) {
                                case 'upgrade':
                                case 'shutdown':
                                    Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
                                    Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                                    break;
                                default:
                                    Console::info($command);
                            }
                        } elseif ($msg == 'shutdown') {
                            $shouldExit = true;
                            Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
                            Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                        }
                    }
                    unset($socket);
                });
                \Swoole\Event::wait();

                if ($shouldExit) {
                    Console::error("【Crontab】服务器已关闭,结束运行");
                    break;
                }
            }
        });
    }


    /**
     * 内存占用统计
     * @return Process
     */
    private function createMemoryUsageCountProcess(): Process {
        return new Process(function (Process $process) {
            register_shutdown_function(function () use ($process) {
                $err = error_get_last();
                if ($err && in_array($err['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
                    Console::error(
                        "【MemoryMonitor】致命错误退出 [{$err['type']}]: {$err['message']} @ {$err['file']}:{$err['line']}"
                    );
                    // 必须显式退出，让 master 的 wait() 感知
                    Process::kill($process->pid, SIGTERM);
                }
            });
            Console::info("【MemoryMonitor】内存监控PID:" . $process->pid, false);
            MemoryMonitor::start('MemoryMonitor');
            run(function () use ($process) {
                $schedule = function () use (&$schedule, $process) {
                    try {
                        // 1) 退出条件：仅当上一轮完全完成后才安排下一轮
                        $socket = $process->exportSocket();
                        $msg = $socket->recv(timeout: 0.1);
                        if ($msg === 'shutdown') {
                            MemoryMonitor::stop();
                            Console::error("【MemoryMonitor】收到 shutdown，安全退出");
                            Process::kill($process->pid, SIGTERM);
                        }
                        // 2) 执行一次完整统计
                        $processList = MemoryMonitorTable::instance()->rows();
                        if ($processList) {
                            $isDarwin = (PHP_OS_FAMILY === 'Darwin');
                            $barrier = Coroutine\Barrier::make();
                            foreach ($processList as $processInfo) {
                                Coroutine::create(function () use ($barrier, $processInfo, $process, $isDarwin) {
                                    try {
                                        $processName = $processInfo['process'];
                                        $limitMb = $processInfo['limit_memory_mb'] ?? 300;
                                        $pid = (int)$processInfo['pid'];

                                        if (PHP_OS_FAMILY !== 'Darwin' && !Process::kill($pid, 0)) {
                                            return;
                                        }

                                        $mem = MemoryMonitor::getPssRssByPid($pid);
                                        $rss = isset($mem['rss_kb']) ? round($mem['rss_kb'] / 1024, 1) : null;
                                        $pss = isset($mem['pss_kb']) ? round($mem['pss_kb'] / 1024, 1) : null;
                                        $osActualMb = $pss ?? $rss;

                                        $processInfo['rss_mb'] = $rss;
                                        $processInfo['pss_mb'] = $pss;
                                        $processInfo['os_actual'] = $osActualMb;
                                        $processInfo['updated'] = time();

                                        $autoRestart = $processInfo['auto_restart'] ?? STATUS_OFF;
                                        if (
                                            $autoRestart == STATUS_ON
                                            && PHP_OS_FAMILY !== 'Darwin'
                                            && str_starts_with($processName, 'worker:')
                                            && $osActualMb !== null
                                            && $osActualMb > $limitMb
                                        ) {
                                            if (preg_match('/^worker:(\d+)/', $processName, $m)) {
                                                if (time() - ($processInfo['restart_ts'] ?? 0) >= 120) {
                                                    Log::instance()->setModule('system')
                                                        ->error("{$processName}[PID:$pid] 内存 {$osActualMb}MB ≥ {$limitMb}MB，强制重启");
                                                    Process::kill($pid, SIGTERM);
                                                    $processInfo['restart_ts'] = time();
                                                    $processInfo['restart_count'] = ($processInfo['restart_count'] ?? 0) + 1;
                                                }
                                            }
                                        }
                                        $curr = MemoryMonitorTable::instance()->get($processName);
                                        if ($curr && $curr['pid'] !== $processInfo['pid']) {
                                            $processInfo['pid'] = $curr['pid'];
                                        }
                                        MemoryMonitorTable::instance()->set($processName, $processInfo);
                                    } catch (Throwable $e) {
                                        Log::instance()->error("【MemoryMonitor】统计发生错误：" . $e->getMessage());
                                        Process::kill($process->pid, SIGTERM);
                                    }
                                });
                            }
                            try {
                                Coroutine\Barrier::wait($barrier, 60);
                            } catch (Throwable $throwable) {
                                Log::instance()->error("内存统计错误:" . $throwable->getMessage());
                            }
                        } else {
                            Console::warning("【MemoryMonitor】暂无待统计进程", false);
                        }
                        MemoryMonitor::updateUsage('MemoryMonitor');
                        // 3) 统计完成后再安排下一轮，避免 tick 重叠
                        Timer::after(5000, $schedule);
                    } catch (Throwable $e) {
                        Log::instance()->error("【MemoryMonitor】调度异常:" . $e->getMessage());
                        Process::kill($process->pid, SIGTERM);
                    }
                };
                // 启动即执行一次，避免首轮 5 秒空窗
                $schedule();
            });
        });
    }

    /**
     * 心跳和状态推送
     * @return Process
     */
    private function createHeartbeatProcess(): Process {
        return new Process(function (Process $process) {
            App::mount();
            //Config::init();
            //var_dump(Config::get('app'));
            Console::info("【Heatbeat】心跳进程PID:" . $process->pid, false);
            run(function () use ($process) {
                MemoryMonitor::start('Heatbeat');
                $node = Node::factory();
                $node->appid = APP_ID;
                $node->id = APP_NODE_ID;
                $node->name = APP_DIR_NAME;
                $node->ip = SERVER_HOST;
                $node->fingerprint = APP_FINGERPRINT;
                $node->port = Runtime::instance()->httpPort();
                $node->socketPort = Runtime::instance()->httpPort();
                $node->started = time();
                $node->restart_times = 0;
                $node->master_pid = $this->server->master_pid;
                $node->manager_pid = $this->server->manager_pid;
                $node->swoole_version = swoole_version();
                $node->cpu_num = swoole_cpu_num();
                $node->stack_useage = Coroutine::getStackUsage();
                $node->scf_version = SCF_VERSION;
                $node->server_run_mode = APP_SRC_TYPE;
                while (true) {
                    $socket = Manager::instance()->getMasterSocketConnection();
                    $socket->push(JsonHelper::toJson(['event' => 'slave_node_report', 'data' => [
                        'host' => SERVER_HOST,
                        'role' => SERVER_ROLE
                    ]]));
                    // 定时发送 WS 心跳，避免中间层(nginx/LB/frp)与服务端心跳超时导致断开
                    $pingTimerId = Timer::tick(1000 * 5, function () use ($socket, &$node) {
                        $profile = App::profile();
                        $node->role = SERVER_ROLE;
                        $node->app_version = $profile->version;
                        $node->public_version = $profile->public_version ?: '--';
                        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
                        $node->heart_beat = time();
                        $node->framework_update_ready = file_exists(SCF_ROOT . '/build/update.pack');
                        $node->tables = ATable::list();
                        $node->restart_times = Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0;
                        $node->stack_useage = memory_get_usage(true);
                        $node->threads = count(Coroutine::list());
                        $node->thread_status = Coroutine::stats();
                        $node->server_stats = Runtime::instance()->get('SERVER_STATS') ?: []; //$server->stats();
                        $node->mysql_execute_count = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
                        $node->http_request_reject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
                        $node->http_request_count = Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0;
                        $node->http_request_count_current = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
                        $node->http_request_count_today = Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0;
                        $node->http_request_processing = Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0;
                        $node->memory_usage = MemoryMonitor::sum();
                        $node->tasks = CrontabManager::allStatus();
                        if ($node->role == NODE_ROLE_MASTER) {
                            ServerNodeStatusTable::instance()->set('localhost', $node->asArray());
                            $socket->push('::ping');
                        } else {
                            $socket->push(JsonHelper::toJson(['event' => 'node_heart_beat', 'data' => [
                                'host' => SERVER_HOST,
                                'status' => $node->asArray()
                            ]]));
                        }
                        MemoryMonitor::updateUsage('Heatbeat');
                    });
                    // 读循环：直到断开
                    while (true) {
                        // 若是非阻塞 recv，则需要小睡避免空转
                        $reply = $socket->recv();
                        if ($reply === false) {
                            Timer::clear($pingTimerId);
                            unset($pingTimerId);
                            // 读错误：断开
                            Console::warning('【Heatbeat】已断开master节点连接', false);
                            $socket->close();
                            $processSocket = $process->exportSocket();
                            $cmd = $processSocket->recv(timeout: 0.1);
                            if ($cmd == 'shutdown') {
                                Console::error('【Heatbeat】服务器已关闭,终止心跳');
                                $socket->close();
                                MemoryMonitor::stop();
                                Process::kill($process->pid, SIGTERM);
                                //break 2; // 跳出两层循环
                            }
                            break;
                        }
                        if ($reply && !empty($reply->data) && $reply->data !== "::pong") {
                            // 如果服务端发来 ping 帧，立刻回 pong，保持长连接
                            if (isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                                $socket->push('', WEBSOCKET_OPCODE_PONG);
                            }
                            if (JsonHelper::is($reply->data)) {
                                $data = JsonHelper::recover($reply->data);
                                $event = $data['event'] ?? 'unknow';
                                if ($event == 'command') {
                                    $command = $data['data']['command'];
                                    $params = $data['data']['params'];
                                    switch ($command) {
                                        case 'shutdown':
                                            $socket->push("【" . SERVER_HOST . "】start shutdown");
                                            Http::instance()->shutdown();
                                            break;
                                        case 'restart':
                                            $socket->push("【" . SERVER_HOST . "】start reload");
                                            Http::instance()->reload();
                                            break;
                                        case 'appoint_update':
                                            if (App::appointUpdateTo($params['type'], $params['version'])) {
                                                $socket->push("【" . SERVER_HOST . "】版本更新成功:{$params['type']} => {$params['version']}");
                                            } else {
                                                $socket->push("【" . SERVER_HOST . "】版本更新失败:{$params['type']} => {$params['version']}");
                                            }
                                            break;
                                        default:
                                            Console::warning("【Heatbeat】Command '$command' is not supported", false);
                                    }
                                } elseif ($event == 'slave_node_report_response') {
                                    $masterHost = Manager::instance()->getMasterHost();
                                    Console::success('【Heatbeat】已与master[' . $masterHost . ']建立连接,客户端ID:' . $data['data'], false);
                                } else {
                                    Console::info("【Heatbeat】收到master消息:" . $reply->data, false);
                                }
                            } else {
                                Console::info("【Heatbeat】收到master消息:" . $reply->data, false);
                            }
                        }
                        // 周期性检测主进程是否还在
//                        static $tick = 0;
//                        if ((++$tick % 10) === 0 && !Process::kill($this->server->manager_pid, 0)) {
//                            Console::error('【Heatbeat】主进程退出，断开并退出');
//                            $socket->close();
//                            break 2; // 跳出两层循环
//                        }
                    }
                }
            });

        });
    }

    /**
     * 日志备份
     * @return Process
     */
    private function createLogBackupProcess(): Process {
        return new Process(function (Process $process) {
            Console::info("【LogBackup】日志备份PID:" . $process->pid, false);
            App::mount();
            MemoryMonitor::start('LogBackup');
            $serverConfig = Config::server();
            $logger = Log::instance();
            $logExpireDays = $serverConfig['log_expire_days'] ?? 15;
            //清理过期日志
            $clearCount = $logger->clear($logExpireDays);
            if ($clearCount) {
                Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
            }
            run(function () use ($logger, $process, $logExpireDays) {
                $sock = $process->exportSocket();
                while (true) {
                    $cmd = $sock->recv(timeout: 0.1);
                    if ($cmd == 'shutdown') {
                        MemoryMonitor::stop();
                        Console::error("【LogBackup】管理进程退出,结束备份");
                        Process::kill($process->pid, SIGTERM);
                        break;
                    }
                    if ((int)Runtime::instance()->get('_LOG_CLEAR_DAY_') !== (int)Date::today()) {
                        $clearCount = $logger->clear($logExpireDays);
                        if ($clearCount) {
                            Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
                        }
                        //清理过期请求统计
                        $countKeyDay = Key::COUNTER_REQUEST . Date::leftday(2);
                        if (Counter::instance()->get($countKeyDay)) {
                            Counter::instance()->delete($countKeyDay);
                        }
                    }
                    $logger->backup();
                    MemoryMonitor::updateUsage('LogBackup');
                    Coroutine::sleep(5);
                }
            });
        });
    }

    /**
     * 文件变更监听
     * @return Process
     */
    private function createFileWatchProcess(): Process {
        return new Process(function (Process $process) {
            register_shutdown_function(function () use ($process) {
                $err = error_get_last();
                if ($err && in_array($err['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
                    Console::error("【FileWatcher】子进程致命错误，退出: " . $err['message']);
                    Process::kill($process->pid, SIGTERM);
                }
            });
            Console::info("【FileWatcher】文件改动监听服务PID:" . $process->pid, false);
            sleep(1);
            App::mount();
            MemoryMonitor::start('FileWatcher');
            $scanDirectories = function () {
                if (APP_SRC_TYPE == 'dir') {
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
                    'mtime' => filemtime($path),
                    'size' => filesize($path),
                ];
            }
            run(function () use ($fileList, $process, $scanDirectories) {
                while (true) {
                    $socket = $process->exportSocket();
                    $msg = $socket->recv(timeout: 0.1);
                    if ($msg == 'shutdown') {
                        MemoryMonitor::stop();
                        Console::error("【FileWatcher】管理进程退出,结束监听");
                        Process::kill($process->pid, SIGTERM);
                        break;
                    }
                    $changed = false;
                    $changedFiles = [];
                    $currentFiles = $scanDirectories();
                    $currentFilePaths = array_map(fn($file) => $file, $currentFiles);
                    foreach ($currentFilePaths as $path) {
                        if (!in_array($path, array_column($fileList, 'path'))) {
                            $fileList[] = [
                                'path' => $path,
                                'mtime' => filemtime($path),
                                'size' => filesize($path),
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
                        $mtime = filemtime($file['path']);
                        $size = filesize($file['path']);
                        if ($file['mtime'] !== $mtime || $file['size'] !== $size) {
                            $file['mtime'] = $mtime;
                            $file['size'] = $size;
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
                        Http::instance()->reload();
                    }
                    MemoryMonitor::updateUsage('FileWatcher');
                    Coroutine::sleep(2);
                }
            });
            MemoryMonitor::stop();
        });
    }

}