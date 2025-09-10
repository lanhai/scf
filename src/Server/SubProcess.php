<?php

namespace Scf\Server;

use Exception;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Core\Table\Runtime;
use Scf\Root;
use Scf\Server\Manager as ServerManager;
use Scf\Server\Struct\Node;
use Scf\Util\Dir;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;

class SubProcess {

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