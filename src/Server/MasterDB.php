<?php

namespace Scf\Server;

use Monolog\Handler\RotatingFileHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Scf\Core\Console;
use Scf\Core\Traits\Singleton;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\App;
use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Util\File;
use Swoole\Coroutine\System;
use Swoole\Event;
use Swoole\Process;
use Swoole\Redis\Server;
use Swoole\Timer;
use Throwable;

class MasterDB {
    use Singleton;

    protected array $_config = [
        'enable_save_to_table' => false,
        'enable_sql' => true,
        'enable_error' => true,
        'enable_message' => true,
        'enable_access' => true,
        'check_dir' => true,
        'error_level' => Logger::NOTICE, // 错误日志记录等级
    ];

    protected array $data = [];
    /**
     * 日志对象集合
     * @var array(Logger)
     */
    protected array $_loggers = [];

    public static function start(int $port = 16379): void {
        if (!App::isMaster()) {
            return;
        }
        $process = new Process(function () use ($port) {
            try {
                MasterDB::instance()->create(Manager::instance()->issetOpt('d'), $port);
            } catch (Throwable $exception) {
                Console::log('[' . $exception->getCode() . ']' . Color::red($exception->getMessage()));
            }
        });
        $process->start();
        Timer::after(100, function () use (&$masterDbPid) {
            $masterDbPid = File::read(SERVER_MASTER_DB_PID_FILE);
        });
        Event::wait();
        if (!Process::kill($masterDbPid, 0)) {
            Console::error('【MasterDB】服务启动失败');
            exit();
        } else {
            Console::info("【MasterDB】服务启动完成!PID:" . $masterDbPid . ",PORT:" . $port);
        }
    }


    /**
     * @param bool $daemonize
     * @param int $port
     * @return void
     */
    public function create(bool $daemonize = false, int $port = 16379): void {
        try {
            ini_set('memory_limit', '256M');
            $server = new Server('0.0.0.0', $port, SWOOLE_BASE);
            $setting = [
                'worker_num' => 1,
                'daemonize' => $daemonize,
                'pid_file' => SERVER_MASTER_DB_PID_FILE
            ];
            $server->set($setting);
            if (is_file(APP_RUNTIME_DB)) {
                $this->data = unserialize(file_get_contents(APP_RUNTIME_DB)) ?: [];
            } else {
                $this->data = [];
            }
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
                Timer::tick(1000*10, function () use ($server) {
                    //Console::log("【MasterDB】数据持久化大小:" . strlen(serialize($this->data)));
                    file_put_contents(APP_RUNTIME_DB, serialize($this->data));
                });
            });
//            $server->on('start', function (Server $server) {
//                Timer::tick(5000, function () use ($server) {
//                    file_put_contents(APP_RUNTIME_DB, serialize($this->data));
//                });
//            });
            $server->start();
        } catch (Throwable $exception) {
            Console::log('【MasterDB】服务启动失败:' . Color::red($exception->getMessage()));
            exit();
        }
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

    /**
     * 设置日志记录器
     * @param $name
     * @param Logger $logger
     * @return void
     */
    public function setLogger($name, Logger $logger): void {
        $this->_loggers[$name] = $logger;
    }

    /**
     * @param $name
     * @return ?Logger
     */
    public function getLogger($name): ?Logger {
        if (!isset($this->_loggers[$name])) {
            switch ($name) {
                case 'error':
                    $this->setLogger('error', $this->createLogger('error', $this->_config['error_level']));
                    break;
                case 'access':
                    if ($this->_config['enable_access']) {
                        $this->setLogger('access', $this->createLogger('access'));
                    }
                    break;
                case 'message':
                    if ($this->_config['enable_message']) {
                        $logger = $this->createLogger('message');
                        $this->setLogger('message', $logger);
                    }
                    break;
                case 'sql';
                    if ($this->_config['enable_sql']) {
                        $this->setLogger('sql', $this->createLogger('sql'));
                    }
                    break;
                default:
                    $this->setLogger($name, $this->createLogger($name));
                    break;
            }
        }
        return $this->_loggers[$name] ?: null;
    }


    /**
     * 创建日志记录器
     * @param $name
     * @param int $level
     * @param bool $rotating
     * @return Logger
     */
    public function createLogger($name, int $level = Logger::INFO, bool $rotating = true): Logger {
        $dir = APP_LOG_PATH;
        if ($this->_config['check_dir'] and !is_dir($dir)) {
            mkdir($dir, 0775, true);
        }
        $handler = $rotating ?
            new RotatingFileHandler($dir . '/' . $name . '.log', 100, $level) :
            new StreamHandler($dir . '/' . $name . '.log', $level);
        $logger = new Logger($name);
        $logger->pushHandler($handler);

        return $logger;
    }
}