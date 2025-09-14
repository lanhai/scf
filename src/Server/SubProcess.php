<?php

namespace Scf\Server;

use Exception;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Root;
use Scf\Server\Manager as ServerManager;
use Scf\Server\Struct\Node;
use Scf\Util\Dir;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;
use function Co\run;

class SubProcess {

    public static function connectMaster(Server $server): Process {
        return new Process(function ($process) use ($server) {
            if (!Process::kill($server->manager_pid, 0)) {
                return;
            }
            Console::info("【Server】主节点连接PID:" . $process->pid, false);
            App::mount();
            run(function () use ($server) {
                while (true) {
                    // 主进程存活检测
                    if (!Process::kill($server->manager_pid, 0)) {
                        Console::warning('【Server】主进程退出，connectMaster 随之退出');
                        break;
                    }
                    $socket = Manager::instance()->getMasterSocketConnection();
                    $socket->push('slave-node-report');
                    // 定时发送 WS 心跳，避免中间层(nginx/LB/frp)与服务端心跳超时导致 60s 断开
                    $pingTimerId = Timer::tick(1000 * 30, function () use ($socket) {
                        try {
                            // 改为应用层心跳，避免某些代理/FRP 对 PING 控制帧处理不一致导致的断开
                            // 发送轻量文本帧（例如 '::ping'），服务端可选择忽略或打印
                            $ok = $socket->push('::ping');
                            if ($ok === false) {
                                // 发送失败让读循环去处理断开
                            }
                        } catch (\Throwable $e) {
                            // 忽略发送失败，读循环会感知断开
                        }
                    });
                    // 读循环：直到断开
                    while (true) {
                        // 若是非阻塞 recv，则需要小睡避免空转
                        $reply = $socket->recv();  // 你这边的 recv() 看起来返回对象
                        if ($reply === false) {
                            Timer::clear($pingTimerId);
                            unset($pingTimerId);
                            // 读错误：断开
                            Console::warning('【Server】与master节点连接读错误，准备重连', false);
                            $socket->close(); // 若有
                            break;
                        }
                        if ($reply && empty($reply->data)) {
                            Timer::clear($pingTimerId);
                            unset($pingTimerId);
                            Console::warning("【Server】已断开master节点连接", false);
                            $socket->close(); // 若有
                            break;
                        }
                        if ($reply && !empty($reply->data)) {
                            // 如果服务端发来 ping 帧，立刻回 pong，保持长连接
                            if (isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                                $socket->push('', WEBSOCKET_OPCODE_PONG);
                            }
                            if ($reply->data !== "::pong") {
                                Console::info("【Server】收到master消息:" . $reply->data, false);
                                if (JsonHelper::is($reply->data)) {
                                    $data = JsonHelper::recover($reply->data);
                                    $event = $data['event'] ?? 'message';
                                    if ($event == 'command') {
                                        $command = $data['data']['command'];
                                        $params = $data['data']['params'];
                                        switch ($command) {
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
                                                Console::warning("【Server】Command '$command' is not supported", false);
                                        }
                                    } elseif ($event == 'welcome') {
                                        $masterHost = Manager::instance()->getMasterHost();
                                        Console::success('【Server】已连接到master节点:' . $masterHost, false);
                                    }
                                }
                            }
                        } else {
                            // 无数据可读（非阻塞场景）
                            usleep(100 * 1000); // 100ms，避免 Coroutine::sleep 在非协程环境的问题
                        }
                        // 周期性检测主进程是否还在
                        static $tick = 0;
                        if ((++$tick % 10) === 0 && !Process::kill($server->manager_pid, 0)) {
                            Console::warning('【Server】主进程退出，断开并退出');
                            // $socket->close(); // 若有
                            break 2; // 跳出两层循环
                        }
                    }
                    // 下一轮外层 while 会尝试重连（带退避）
                }
            });
            \Swoole\Event::wait();

        });
    }

    /**
     * 心跳进程
     * @param Server $server
     * @param Node $node
     * @return Process
     */
    public static function heartbeat(Server $server, Node $node): Process {
        return new Process(function ($process) use ($server, $node) {
            sleep(1);
            if (Process::kill($server->manager_pid, 0)) {
                Console::info("【Server】心跳服务PID:" . $process->pid, false);
                App::mount();
                $node->appid = App::id();
                $node->app_version = App::version();
                $node->public_version = App::publicVersion();
                function register($process, $server, $node): void {
                    $manager = ServerManager::instance();
                    try {
                        if ($manager->register($node) === false) {
                            Console::log('【Server】节点报道失败:' . Color::red("MasterDB不可用"));
                            Timer::after(5000, function () use ($process, $server, $node) {
                                register($process, $server, $node);
                            });
                        }
                        Timer::tick(5000, function () use ($manager, $node, $server, $process) {
                            $manager->heartbeat($server, $node);
                        });
                        \Swoole\Event::wait();
                    } catch (Exception $exception) {
                        Console::log('【Server】节点报道失败:' . Color::red($exception->getMessage()));
                    }
                }

                register($process, $server, $node);
            }
        });
    }

    /**
     * 日志备份
     * @param Server $server
     * @return Process
     */
    public static function createLogBackupProcess(Server $server): Process {
        return new Process(function ($process) use ($server) {
            sleep(1);
            if (Process::kill($server->manager_pid, 0)) {
                Console::info("【Server】日志备份PID:" . $process->pid, false);
                App::mount();
                $logger = Log::instance();
                Timer::tick(5000, function () use ($logger, $server, $process) {
                    $logger->backup();
                });
                \Swoole\Event::wait();
            }
        });
    }

    /**
     * 文件变更监听
     * @param Server|\Swoole\Server $server
     * @param int $port
     * @return Process
     */
    public static function createFileWatchProcess(Server|\Swoole\Server $server, int $port = 0): Process {
        return new Process(function ($process) use ($server, $port) {
            sleep(1);
            if (Process::kill($server->manager_pid, 0)) {
                Console::info("【Server】文件改动监听服务PID:" . $process->pid, false);
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
                        $httpServer = Http::instance();
                        $httpServer->clearWorkerTimer();
                        $httpServer->sendCommandToCrontabManager('upgrade');
                        $server->reload();
                        // Console::info("重启状态:" . $this->server->reload());
                        //定时任务迭代
//                        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);
//                        Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESS);
                        if ($port && App::isMaster()) {
                            $dashboardHost = PROTOCOL_HTTP . 'localhost:' . Runtime::instance()->dashboardPort() . '/reload';
                            $client = \Scf\Client\Http::create($dashboardHost);
                            $client->get();
                        }
                    }
                    sleep(3);
                }
            }
        }, false, 2, 1);
    }
}