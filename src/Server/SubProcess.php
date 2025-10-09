<?php

namespace Scf\Server;

use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
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
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use function Co\run;

class SubProcess {
    /**
     * 心跳和状态推送
     * @param Server $server
     * @return Process
     */
    public static function createConsolePushProcess(Server $server): Process {
        return new Process(function (Process $process) use ($server) {
            sleep(3);
            Console::info("【ConsolePush】控制台消息推送PID:" . $process->pid, false);
            Runtime::instance()->serverIsAlive() and MemoryMonitor::start('ConsolePush');
            run(function () use ($server, $process) {
                $masterSocket = Manager::instance()->getMasterSocketConnection();
                while (true) {
                    if (!Process::kill($server->manager_pid, 0) || !Runtime::instance()->serverIsAlive()) {
                        Console::warning('【ConsolePush】管理进程退出,结束心跳', false);
                        break;
                    }
                    $masterSocket->push('::ping');
                    $reply = $masterSocket->recv(3);
                    if ($reply === false || empty($reply->data)) {
                        Console::warning('【ConsolePush】与master节点连接读错误，准备重连', false);
                        $masterSocket->close();
                        break;
                    }
                    $socket = $process->exportSocket();
                    $msg = $socket->recv(timeout: 30);
                    if ($msg && StringHelper::isJson($msg)) {
                        $payload = JsonHelper::recover($msg);
                        $masterSocket->push(JsonHelper::toJson(['event' => 'console_log', 'data' => [
                            'host' => SERVER_ROLE == NODE_ROLE_MASTER ? 'master' : SERVER_HOST,
                            ...$payload
                        ]]));
                    }
                }
                Coroutine::defer(function () {
                    unset($masterSocket);
                });
            });
            MemoryMonitor::stop();
        });
    }

    /**
     * 内存占用统计
     * @param Server $server
     * @return Process
     */
    public static function createMemoryUsageCountProcess(Server $server): Process {
        return new Process(function (Process $process) use ($server) {
            Console::info("【MemoryMonitor】内存监控PID:" . $process->pid, false);
            Runtime::instance()->serverIsAlive() and MemoryMonitor::start('MemoryMonitor');
            Coroutine\run(function () use ($server, $process) {
                $schedule = null; // self-rescheduling closure
                $schedule = function () use (&$schedule, $process, $server) {
                    // 1) 退出条件：仅当上一轮完全完成后才安排下一轮
                    if (!Process::kill($server->manager_pid, 0) || !Runtime::instance()->serverIsAlive()) {
                        MemoryMonitor::stop();
                        Console::warning("【MemoryMonitor】管理进程退出,结束统计");
//                        Process::kill($server->manager_pid, SIGTERM);
//                        Process::kill($server->master_pid, SIGTERM);
                        return; // 不再重排
                    }
                    // 2) 执行一次完整统计
                    $processList = MemoryMonitorTable::instance()->rows();
                    if ($processList) {
                        $barrier = Coroutine\Barrier::make();
                        foreach ($processList as $processInfo) {
                            Coroutine::create(function () use ($barrier, $processInfo, $process, $server) {
                                $processName = $processInfo['process'];
                                $autoRestart = $processInfo['auto_restart'] ?? STATUS_OFF;
                                $limitMb = $processInfo['limit_memory_mb'] ?? 1024;
                                $pid = $processInfo['pid'];
                                if (!Process::kill($pid, 0)) {
                                    Console::warning("【MemoryMonitor】{$processName} PID:{$pid}进程不存在,结束统计", false);
                                    return;
                                }
                                // 根据PID查询 PSS/RSS（KB）
                                $mem = self::getPssRssByPid((int)$pid);
                                $rss = isset($mem['rss_kb']) ? round($mem['rss_kb'] / 1024, 1) : null;    // MB
                                $pss = isset($mem['pss_kb']) ? round($mem['pss_kb'] / 1024, 1) : null;    // MB
                                // 实际用于阈值判断的 OS 占用：优先 PSS，没有则回落到 RSS
                                $osActualMb = $pss ?? $rss;
                                $processInfo['rss_mb'] = $rss;
                                $processInfo['pss_mb'] = $pss;
                                $processInfo['os_actual'] = $osActualMb;
                                $processInfo['time'] = date('Y-m-d H:i:s');
                                //更新占用
                                if ($autoRestart == STATUS_ON && str_starts_with($processName, 'worker:') && $osActualMb > $limitMb) {
                                    // 解析 workerId
                                    $workerId = null;
                                    if (preg_match('/^worker:(\d+)/', $processName, $mWid)) {
                                        $workerId = (int)$mWid[1];
                                    }
                                    if ($workerId !== null) {
                                        // 节流：120 秒内只触发一次，避免抖动
                                        if (time() - $processInfo['restart_ts'] >= 120) {
                                            Log::instance()->setModule('system')->info("{$processName}[PID:{$processInfo['pid']}]内存过高 {$osActualMb}MB ≥ {$limitMb}MB，触发重启", true);
                                            $server->stop($workerId);
                                            $processInfo['restart_ts'] = time();
                                            $processInfo['restart_count']++;
                                            if ($processInfo['restart_count'] >= 2) {
                                                Process::kill($processInfo['pid'], SIGUSR2);
                                            }
                                        }
                                    }
                                }
                                $curr = MemoryMonitorTable::instance()->get($processName);
                                if ($curr['pid'] !== $processInfo['pid']) {
                                    $processInfo['pid'] = $curr['pid'];
                                }
                                MemoryMonitorTable::instance()->set($processName, $processInfo);
                            });
                        }
                        try {
                            Coroutine\Barrier::wait($barrier);
                        } catch (\Throwable $throwable) {
                            Log::instance()->error("内存统计错误:" . $throwable->getMessage());
                        }
                    } else {
                        Console::warning("【MemoryMonitor】无进程", false);
                    }
                    // 3) 统计完成后再安排下一轮，避免 tick 重叠
                    Timer::after(5000, $schedule);
                };
                // 立即安排首轮；若希望立刻执行一次可改为 $schedule();
                Timer::after(2000, $schedule);
            });
        });
    }

    /**
     * 心跳和状态推送
     * @param Server $server
     * @return Process
     */
    public static function createHeartbeatProcess(Server $server): Process {
        return new Process(function (Process $process) use ($server) {
            Console::info("【Heatbeat】主节点连接PID:" . $process->pid, false);
            App::mount();
            run(function () use ($server) {
                Runtime::instance()->serverIsAlive() and MemoryMonitor::start('Heatbeat');
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
                $node->master_pid = $server->master_pid;
                $node->manager_pid = $server->manager_pid;
                $node->swoole_version = swoole_version();
                $node->cpu_num = swoole_cpu_num();
                $node->stack_useage = Coroutine::getStackUsage();
                $node->scf_version = SCF_VERSION;
                $node->server_run_mode = APP_SRC_TYPE;
                while (true) {
                    // 主进程存活检测
                    if (!Process::kill($server->manager_pid, 0) || !Runtime::instance()->serverIsAlive()) {
                        MemoryMonitor::stop();
                        Console::warning('【Heatbeat】管理进程退出,结束心跳');
                        break;
                    }
                    $socket = Manager::instance()->getMasterSocketConnection();
                    $socket->push(JsonHelper::toJson(['event' => 'slave_node_report', 'data' => [
                        'host' => SERVER_HOST,
                        'role' => SERVER_ROLE
                    ]]));
                    // 定时发送 WS 心跳，避免中间层(nginx/LB/frp)与服务端心跳超时导致断开
                    $pingTimerId = Timer::tick(1000 * 5, function () use ($socket, $server, &$node) {
                        $profile = App::profile();
                        $node->role = $profile->role;
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
                        $node->server_stats = $server->stats();
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
                    });
                    // 读循环：直到断开
                    while (true) {
                        // 若是非阻塞 recv，则需要小睡避免空转
                        $reply = $socket->recv();
                        if ($reply === false) {
                            Timer::clear($pingTimerId);
                            unset($pingTimerId);
                            // 读错误：断开
                            Console::warning('【Heatbeat】与master节点连接读错误，准备重连', false);
                            $socket->close();
                            break;
                        }
                        if ($reply && empty($reply->data)) {
                            Timer::clear($pingTimerId);
                            unset($pingTimerId);
                            Console::warning("【Heatbeat】已断开master节点连接", false);
                            $socket->close();
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
                        } else {
                            // 无数据可读（非阻塞场景）
                            usleep(100 * 1000); // 100ms，避免 Coroutine::sleep 在非协程环境的问题
                        }
                        // 周期性检测主进程是否还在
                        static $tick = 0;
                        if ((++$tick % 10) === 0 && !Process::kill($server->manager_pid, 0)) {
                            Console::warning('【Heatbeat】主进程退出，断开并退出');
                            $socket->close();
                            break 2; // 跳出两层循环
                        }
                    }
                }
            });
        });
    }

    /**
     * 日志备份
     * @param Server $server
     * @return Process
     */
    public static function createLogBackupProcess(Server $server): Process {
        return new Process(function (Process $process) use ($server) {
            Console::info("【LogBackup】日志备份PID:" . $process->pid, false);
            App::mount();
            Runtime::instance()->serverIsAlive() and MemoryMonitor::start('LogBackup');
            $serverConfig = Config::server();
            $logger = Log::instance();
            $logExpireDays = $serverConfig['log_expire_days'] ?? 15;
            //清理过期日志
            $clearCount = $logger->clear($logExpireDays);
            if ($clearCount) {
                Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
            }
            Timer::tick(5000, function ($tid) use ($logger, $server, $process, $logExpireDays) {
                if (!Process::kill($server->manager_pid, 0) || !Runtime::instance()->serverIsAlive()) {
                    MemoryMonitor::stop();
                    Console::warning("【LogBackup】管理进程退出,结束备份");
                    Timer::clear($tid);
                    return;
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
            });
            \Swoole\Event::wait();
        });
    }

    /**
     * 文件变更监听
     * @param Server|\Swoole\Server $server
     * @param int $port
     * @return Process
     */
    public static function createFileWatchProcess(Server|\Swoole\Server $server, int $port = 0): Process {
        return new Process(function (Process $process) use ($server, $port) {
            Console::info("【FileWatcher】文件改动监听服务PID:" . $process->pid, false);
            sleep(1);
            App::mount();
            Runtime::instance()->serverIsAlive() and MemoryMonitor::start('FileWatcher');
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
                    'md5' => md5_file($path)
                ];
            }
            while (true) {
                if (!Process::kill($server->manager_pid, 0) || !Runtime::instance()->serverIsAlive()) {
                    MemoryMonitor::stop();
                    Console::warning("【FileWatcher】管理进程退出,结束监听");
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
                    Http::instance()->reload();
                }
                sleep(3);
            }
        }, false, 2, 1);
    }

    /**
     * 获取进程内存占用（PSS/RSS）
     * 返回单位：KB；若不可得则为 null
     */
    public static function getPssRssByPid(int $pid): array {
        $rssKb = null;
        $pssKb = null;
        // 优先 Linux /proc 读取（容器&宿主机通用）
        $smapsRollup = "/proc/{$pid}/smaps_rollup";
        $smaps = "/proc/{$pid}/smaps";
        $status = "/proc/{$pid}/status";
        if (is_readable($smapsRollup)) {
            [$pssKb, $rssKb] = self::parseSmapsLike($smapsRollup);
            return ['pss_kb' => $pssKb, 'rss_kb' => $rssKb];
        }
        if (is_readable($smaps)) {
            [$pssKb, $rssKb] = self::parseSmapsLike($smaps);
            return ['pss_kb' => $pssKb, 'rss_kb' => $rssKb];
        }
        // 尝试从 /proc/<pid>/status 读取 RSS（VmRSS）
        if (is_readable($status)) {
            $content = @file($status) ?: [];
            foreach ($content as $line) {
                if (str_starts_with($line, 'VmRSS:')) {
                    if (preg_match('/(\d+)/', $line, $m)) {
                        $rssKb = (int)$m[1];
                    }
                    break;
                }
            }
        }
        // 非 Linux（如 macOS）或受限环境：使用命令行兜底
        if ($rssKb === null) {
            $psOut = @shell_exec("ps -o rss= -p " . (int)$pid);
            if ($psOut !== null) {
                $rssKb = (int)trim($psOut) ?: null;
            }
        }
        // macOS 近似 PSS：使用 vmmap -summary 的 Physical footprint
        if ($pssKb === null && PHP_OS_FAMILY === 'Darwin') {
            $vmmap = @shell_exec("vmmap " . (int)$pid . " -summary 2>/dev/null");
            if ($vmmap) {
                // 兼容不同本地化：匹配 Physical footprint / PhysFootprint
                if (preg_match('/(Physical footprint|PhysFootprint):\s*([0-9\.]+)\s*(KB|MB|GB)/i', $vmmap, $m)) {
                    $val = (float)$m[2];
                    $unit = strtoupper($m[3]);
                    $kb = $val * ($unit === 'GB' ? 1048576 : ($unit === 'MB' ? 1024 : 1));
                    $pssKb = (int)round($kb);
                }
            }
        }
        return ['pss_kb' => $pssKb, 'rss_kb' => $rssKb];
    }

    /**
     * 解析 smaps/smaps_rollup：汇总 Pss/Rss
     * 返回 [pssKb, rssKb]
     */
    private static function parseSmapsLike(string $file): array {
        $pss = 0;
        $rss = 0;
        $hasPss = false;
        $hasRss = false;
        // 使用 @file 读取，避免在进程退出时 fgets 报错
        $lines = @file($file);
        if ($lines === false) {
            return [null, null];
        }
        foreach ($lines as $line) {
            if (strncmp($line, 'Pss:', 4) === 0) {
                if (preg_match('/(\d+)/', $line, $m)) {
                    $pss += (int)$m[1];
                    $hasPss = true;
                }
            } elseif (strncmp($line, 'Rss:', 4) === 0) {
                if (preg_match('/(\d+)/', $line, $m)) {
                    $rss += (int)$m[1];
                    $hasRss = true;
                }
            }
        }
        return [$hasPss ? $pss : null, $hasRss ? $rss : null];
    }
}