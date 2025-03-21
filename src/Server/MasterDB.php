<?php

namespace Scf\Server;

use Scf\Core\Console;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\App;
use Scf\Command\Color;
use Scf\Server\Table\Runtime;
use Scf\Util\File;
use Swoole\Coroutine\System;
use Swoole\Event;
use Swoole\Process;
use Swoole\Redis\Server;
use Swoole\Timer;
use Throwable;

/**
 * master节点的redis服务器,用于存储所有节点的运行状态数据和日志归一化
 */
class MasterDB {

    protected array $data = [];


    public static function start(int $port = 16379): void {
        if (!App::isMaster()) {
            return;
        }
        $port = \Scf\Core\Server::getUseablePort($port);
        if (\Scf\Core\Server::isPortInUse($port)) {
            $masterDbPid = File::read(SERVER_MASTER_DB_PID_FILE);
            if ($masterDbPid && Process::kill($masterDbPid, 0)) {
                Console::warning("【MasterDB】端口被[{$masterDbPid}]占用,尝试结束进程");
                if (!self::kill($masterDbPid)) {
                    Console::error("【MasterDB】端口被[{$masterDbPid}]占用,尝试结束进程失败");
                    exit();
                }
            } else {
                Console::warning("【MasterDB】端口被占用,尝试重启结束所有PHP进程");
                if (!self::killall($port)) {
                    Console::error("【MasterDB】端口被占用,尝试结束进程失败");
                    exit();
                }
            }
        }
        $process = new Process(function () use ($port) {
            while (true) {
                $masterDbPid = Runtime::instance()->get('masterDbPid');
                if (!$masterDbPid || !Process::kill($masterDbPid, 0)) {
                    // 启动服务器的独立进程
                    $serverProcess = new Process(function () use ($port) {
                        // 这里是启动服务器的逻辑
                        $class = new static();
                        $class->create(true, $port);
                    });
                    // 启动服务器进程
                    $serverProcessPid = $serverProcess->start();
                    Runtime::instance()->set('masterDbManagerPid', $serverProcessPid);
                    // 监听子进程状态
                    $status = Process::wait(false);
                    if ($status) {
                        Console::warning("【MasterDB】子进程退出, PID: " . $status['pid'] . ", 退出状态: " . $status['code']);
                    }
                }
                sleep(5);
            }
        });
        $process->start();
        Timer::after(100, function () use (&$masterDbPid) {
            $masterDbPid = File::read(SERVER_MASTER_DB_PID_FILE);
        });
        Event::wait();
        // 检查主进程是否启动
        if (!Process::kill($masterDbPid, 0)) {
            Console::error("【MasterDB】服务启动失败");
            exit();
        } else {
            $serverProcessPid = Runtime::instance()->get('masterDbManagerPid');
            Runtime::instance()->set('MASTERDB_PID', "Master:{$process->pid},Manager:{$serverProcessPid},Server:{$masterDbPid}");
            Console::info("【MasterDB】" . Color::green("服务启动完成"));
        }

    }

    /**
     * @param bool $daemonize
     * @param int $port
     * @return void
     */
    public function create(bool $daemonize = false, int $port = 16379): void {
        Runtime::instance()->masterDbPort($port);
        try {
            ini_set('memory_limit', '256M');
            $server = new Server('0.0.0.0', $port, SWOOLE_BASE);
            if (!is_dir(APP_PATH . '/db/logs')) {
                mkdir(APP_PATH . '/db/logs', 0777, true);
            }
            $setting = [
                'worker_num' => 1,
                'daemonize' => $daemonize,
                'pid_file' => SERVER_MASTER_DB_PID_FILE,
                'log_file' => APP_PATH . '/db/logs/log',
                'log_date_format' => '%Y-%m-%d %H:%M:%S',
                'log_rotation' => SWOOLE_LOG_ROTATION_DAILY
            ];
            $server->set($setting);
            $this->data = is_file(APP_RUNTIME_DB) ? (unserialize(file_get_contents(APP_RUNTIME_DB)) ?: []) : [];
            $server->setHandler('lLen', function ($fd, $data) use ($server) {
                $key = $data[0];
                if (empty($this->data[$key])) {
                    return $server->send($fd, Server::format(Server::INT, 0));
                }
                return $server->send($fd, Server::format(Server::INT, count($this->data[$key])));
            });
            $server->setHandler('lPush', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'lPush' command"));
                }
                $key = $data[0];
                if (!isset($this->data[$key])) {
                    $this->data[$key] = [];
                }
                array_unshift($this->data[$key], $data[1]);
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            $server->setHandler('lRange', function ($fd, $data) use ($server) {
                $key = $data[0];
                if (empty($this->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                $start = $data[1];
                $end = $data[2];
                if ($end == -1) {
                    return $server->send($fd, Server::format(Server::STRING, JsonHelper::toJson($this->data[$key])));
                }
                $list = [];
                for ($i = $start; $i <= $end; $i++) {
                    if (!isset($this->data[$key][$i])) {
                        break;
                    }
                    $list[] = $this->data[$key][$i];
                }
                return $server->send($fd, Server::format(Server::STRING, JsonHelper::toJson($list)));
            });
            $server->setHandler('GET', function ($fd, $data) use ($server) {
                if (count($data) == 0) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'GET' command"));
                }
                $key = $data[0];
                if (empty($this->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                } else {
                    return $server->send($fd, Server::format(Server::STRING, $this->data[$key]));
                }
            });
            $server->setHandler('SET', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'SET' command"));
                }
                $key = $data[0];
                $this->data[$key] = $data[1];
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            $server->setHandler('DELETE', function ($fd, $data) use ($server) {
                if (count($data) < 1) {
                    return $server->send($fd, Server::format(Server::ERROR, "DELETE需要至少一个参数"));
                }
                $key = $data[0];
                if (isset($this->data[$key])) {
                    unset($this->data[$key]);
                }
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            $server->setHandler('hSet', function ($fd, $data) use ($server) {
                if (count($data) < 3) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'hSet' command"));
                }
                $key = $data[0];
                if (!isset($this->data[$key])) {
                    $this->data[$key] = [];
                }
                $field = $data[1];
                $value = $data[2];
                $count = !isset($this->data[$key][$field]) ? 1 : 0;
                $this->data[$key][$field] = $value;
                return $server->send($fd, Server::format(Server::INT, $count));
            });
            $server->setHandler('hDel', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'hDel' command"));
                }
                $key = $data[0];
                $field = $data[1];
                if (!empty($this->data[$key][$field])) {
                    unset($this->data[$key][$field]);
                    return $server->send($fd, Server::format(Server::NIL));
                } else {
                    return $server->send($fd, Server::format(Server::INT, 0));
                }
            });
            $server->setHandler('hGet', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'hGet' command"));
                }
                $key = $data[0];
                if (!isset($this->data[$key])) {
                    $this->data[$key] = [];
                }
                $field = $data[1];
                if (empty($this->data[$key][$field])) {
                    return $server->send($fd, Server::format(Server::NIL));
                } else {
                    return $server->send($fd, Server::format(Server::STRING, $this->data[$key][$field]));
                }
            });
            $server->setHandler('hGetAll', function ($fd, $data) use ($server) {
                if (count($data) < 1) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'hGetAll' command"));
                }
                $key = $data[0];
                if (!isset($this->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                return $server->send($fd, Server::format(Server::MAP, $this->data[$key]));
            });
            $server->setHandler('sIsMember', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "sIsMember 至少需要2个参数"));
                }
                $key = $data[0];
                $member = $data[1];
                $arr = $this->data[$key] ?? [];
                return $server->send($fd, Server::format(Server::NIL, isset($arr[$member])));
            });
            $server->setHandler('sAdd', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'sAdd' command"));
                }
                $key = $data[0];
                $count = 0;
                for ($i = 1; $i < count($data); $i++) {
                    $value = $data[$i];
                    if (!isset($this->data[$key][$value])) {
                        $this->data[$key][$value] = 1;
                        $count++;
                    }
                }
                return $server->send($fd, Server::format(Server::INT, $count));
            });
            $server->setHandler('sRemove', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "sRemove 至少需要2个参数"));
                }
                $key = $data[0];
                $count = 0;
                for ($i = 1; $i < count($data); $i++) {
                    $value = $data[$i];
                    if (isset($this->data[$key][$value])) {
                        unset($this->data[$key][$value]);
                        $count++;
                    }
                }
                return $server->send($fd, Server::format(Server::INT, $count));
            });
            $server->setHandler('sClear', function ($fd, $data) use ($server) {
                if (count($data) < 1) {
                    return $server->send($fd, Server::format(Server::ERROR, "sClear 至少需要1个参数"));
                }
                if (isset($this->data[$data[0]])) {
                    unset($this->data[$data[0]]);
                }
                return $server->send($fd, Server::format(Server::INT, 1));
            });
            $server->setHandler('sMembers', function ($fd, $data) use ($server) {
                if (count($data) < 1) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'sMembers' command"));
                }
                $key = $data[0];
                if (!isset($this->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                return $server->send($fd, Server::format(Server::SET, array_keys($this->data[$key])));
            });
            $server->setHandler('incr', function ($fd, $data) use ($server) {
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            $server->setHandler('decrement', function ($fd, $data) use ($server) {
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            //写入日志
            $server->setHandler('addLog', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'addLog' command"));
                }
                //本地化
                $dir = APP_LOG_PATH . '/' . $data[0] . '/';
                if (!is_dir($dir)) {
                    mkdir($dir, 0775, true);
                }
                $fileName = $dir . date('Y-m-d') . '.log';
                if (!isset($this->data[md5($fileName)])) {
                    $this->data[md5($fileName)] = 1;
                } else {
                    $this->data[md5($fileName)] += 1;
                }
                return $server->send($fd, Server::format(Server::STATUS, File::write($fileName, $data[1], true) ? "OK" : "FAIL"));
            });
            $server->setHandler('countLog', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'countLog' command"));
                }
                $day = $data[1];
                $dir = APP_LOG_PATH . '/' . $data[0] . '/';
                $fileName = $dir . $day . '.log';
                $line = $this->countFileLines($fileName);
//                if (file_exists($fileName) && $fp = fopen($fileName, 'r')) {
//                    while (stream_get_line($fp, 102400, "\n")) {
//                        $line++;
//                    }
//                    fclose($fp);//关闭文件
//                }
                return $server->send($fd, Server::format(Server::INT, $line));
            });
            $server->setHandler('getLog', function ($fd, $data) use ($server) {
                $day = $data[1];
                $dir = APP_LOG_PATH . '/' . $data[0] . '/';
                $start = $data[2];
                $size = $data[3];
                $fileName = $dir . $day . '.log';
                if (!file_exists($fileName)) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                if ($start < 0) {
                    $size = abs($start);
                    $start = 0;
                }
                clearstatcache();
                $logs = [];
                // 使用 tac 命令倒序读取文件，然后用 sed 命令读取指定行数
                $command = sprintf(
                    'tac %s | sed -n %d,%dp',
                    escapeshellarg($fileName),
                    $start + 1,
                    $start + $size
                );
                $result = System::exec($command);
                if ($result === false) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                $lines = explode("\n", $result['output']);
                foreach ($lines as $line) {
                    if (trim($line) && ($log = JsonHelper::recover($line))) {
                        $logs[] = $log;
                    }
                }
                return $server->send($fd, Server::format(Server::STRING, JsonHelper::toJson($logs)));
            });
//            $server->setHandler('getLog', function ($fd, $data) use ($server) {
//                $day = $data[1];
//                $dir = APP_LOG_PATH . '/' . $data[0] . '/';
//                $start = $data[2];
//                $size = $data[3];
//                $fileName = $dir . $day . '.log';
//                if (!file_exists($fileName)) {
//                    return $server->send($fd, Server::format(Server::NIL));
//                }
//                if ($start < 0) {
//                    $size = abs($start);
//                    $start = 0;
//                }
//                clearstatcache();
//                $content = System::readFile($fileName);
//                $logs = [];
//                if ($content) {
//                    $list = array_reverse(explode("\n", $content));
//                    if ($start < 0) {
//                        //$list = array_reverse($list);
//                        $size = abs($start);
//                        $start = 0;
//                    }
//                    foreach ($list as $index => $c) {
//                        if (!trim($c)) {
//                            continue;
//                        }
//                        if ($size != -1) {
//                            if ($index < $start) {
//                                continue;
//                            }
//                            if (count($logs) >= $size) {
//                                break;
//                            }
//                        }
//                        if (!$log = JsonHelper::recover($c)) {
//                            continue;
//                        }
//                        $logs[] = $log;
//                    }
//                }
//                return $server->send($fd, Server::format(Server::STRING, JsonHelper::toJson($logs)));
//            });
//            $server->on('Connect', function ($server, int $fd) {
//                Console::log("【MasterDB】#" . $fd . " Connectted");
//            });
//            $server->on('Close', function ($server, int $fd) {
//                Console::log("【MasterDB】#" . $fd . " Closed");
//            });
//            $server->on('WorkerStart', function (Server $server) {
//
//            });
            $server->on('WorkerStart', function (Server $server) {
                Runtime::instance()->set('masterDbPid', $server->master_pid);
//                Timer::after(1000 * 5, function () use ($server) {
//                    // 模拟内存耗尽
//                    $largeArray = [];
//                    for ($i = 0; $i < 1000000; $i++) {
//                        $largeArray[] = str_repeat('a', 1024); // 每个元素占用 1KB 内存
//                    }
//                });
                $managerPid = Runtime::instance()->get('masterDbManagerPid');
                Timer::tick(1000 * 2, function () use ($server, $managerPid) {
                    if ($managerPid && !Process::kill($managerPid, 0)) {
                        //Console::log('【MasterDB】主进程已结束,关闭服务器,ManagerPID:' . $managerPid);
                        //$server->shutdown();
                        //$server->stop();
                        exec("kill -9 " . $server->master_pid);
                    }
                });
                Timer::tick(1000 * 30, function () use ($server) {
//                    Console::log("【MasterDB】数据大小:" . round((strlen(serialize($this->data)) / 1024 / 1024), 2) . "MB");
                    System::writeFile(APP_RUNTIME_DB, serialize($this->data));
                    //File::write(APP_RUNTIME_DB, serialize($this->data));
                });
            });
//            $server->on('start', function (Server $server) use ($port) {
//                Runtime::instance()->masterDbPort($port);
//                //Console::info('【MasterDB】服务启动成功:' . $server->master_pid);
//            });
            $server->start();
        } catch (Throwable $exception) {
            Console::log('【MasterDB】服务启动失败:' . Color::red($exception->getMessage()));
        }
    }

    protected static function kill($pid, $try = 0): bool {
        if ($try >= 10) {
            return false;
        }
        if (Process::kill($pid, 0)) {
            $try++;
            exec("kill -9 " . $pid);
            sleep(1);
            return self::kill($pid, $try);
        }
        return true;
    }

    protected static function killall($port, $try = 0): bool {
        if ($try >= 3) {
            if (self::killProcessByPort($port)) {
                return true;
            }
            return false;
        }
        if (\Scf\Core\Server::isPortInUse($port)) {
            $try++;
            //exec("killall php");
            self::killProcessByPort($port);
            sleep(1);
            return self::killall($port, $try);
        }
        return true;
    }

    protected static function killProcessByPort(int $port): bool {
        // 查找占用端口的进程
        $output = shell_exec("lsof -ti :$port");
        if (empty($output)) {
            Console::info("【MasterDB】没有进程占用端口:$port");
            return true;
        }

        $pids = explode("\n", trim($output));
        foreach ($pids as $pid) {
            if (!empty($pid)) {
                Console::info("【MasterDB】结束进程 $pid 占用端口:$port");
                exec("kill -9 $pid");
            }
        }
        return true;
    }

    /**
     * 统计日志文件行数
     * @param $file
     * @return int
     */
    protected function countFileLines($file): int {
        $line = 0; //初始化行数
        if (file_exists($file)) {
            $output = trim(System::exec("wc -l " . escapeshellarg($file))['output']);
            $arr = explode(' ', $output);
            $line = (int)$arr[0];
        }
        return $line;
    }
}