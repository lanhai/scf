<?php

namespace Scf\Server;

use Monolog\Handler\RotatingFileHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Scf\Command\Color;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Instance;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Util\Date;
use Scf\Util\File;
use Swoole\Coroutine\System;
use Swoole\Redis\Server;
use function Swoole\Coroutine\run;

class Redis {
    use Instance;

    protected array $_config = [
        'enable_save_to_table' => false,
        'enable_sql' => true,
        'enable_error' => true,
        'enable_message' => true,
        'enable_access' => true,
        'check_dir' => true,
        'error_level' => Logger::NOTICE, // 错误日志记录等级
    ];
    /**
     * 日志对象集合
     * @var array(Logger)
     */
    protected array $_loggers = [];

    public function create() {
        Config::init();
        $appConfig = Config::get('app');
        try {
            $server = new Server($appConfig['master_host'], $appConfig['master_db_port'], SWOOLE_BASE);
            $setting = [
                'daemonize' => \Scf\Command\Manager::instance()->issetOpt('d'),
                'pid_file' => SERVER_MASTER_DB_PID_FILE
            ];
            $server->set($setting);
            if (is_file(APP_RUNTIME_DB)) {
                $server->data = unserialize(file_get_contents(APP_RUNTIME_DB));
            } else {
                $server->data = [];
            }
            $server->setHandler('lLen', function ($fd, $data) use ($server) {
                $key = $data[0];
                if (empty($server->data[$key])) {
                    return $server->send($fd, Server::format(Server::INT, 0));
                }
                return $server->send($fd, Server::format(Server::INT, count($server->data[$key])));
            });
            $server->setHandler('lPush', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'lPush' command"));
                }
                $key = $data[0];
                if (!isset($server->data[$key])) {
                    $server->data[$key] = [];
                }
                array_unshift($server->data[$key], $data[1]);
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            $server->setHandler('listRange', function ($fd, $data) use ($server) {
                $key = $data[0];
                if (empty($server->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                $start = $data[1];
                $end = $data[2];
                if ($end == -1) {
                    return $server->send($fd, Server::format(Server::STRING, JsonHelper::toJson($server->data[$key])));
                }
                $list = [];
                for ($i = $start; $i <= $end; $i++) {
                    if (!isset($server->data[$key][$i])) {
                        break;
                    }
                    $list[] = $server->data[$key][$i];
                }
                return $server->send($fd, Server::format(Server::STRING, JsonHelper::toJson($list)));
            });
            $server->setHandler('GET', function ($fd, $data) use ($server) {
                if (count($data) == 0) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'GET' command"));
                }
                $key = $data[0];
                if (empty($server->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                } else {
                    return $server->send($fd, Server::format(Server::STRING, $server->data[$key]));
                }
            });
            $server->setHandler('SET', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'SET' command"));
                }
                $key = $data[0];
                $server->data[$key] = $data[1];
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            //incr
            $server->setHandler('incr', function ($fd, $data) use ($server) {
                var_dump($data);
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            //incr
            $server->setHandler('decrement', function ($fd, $data) use ($server) {
                var_dump($data);
                return $server->send($fd, Server::format(Server::STATUS, "OK"));
            });
            //写入日志
            $server->setHandler('addLog', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'addLog' command"));
                }
                //本地化
                $dir = APP_LOG_PATH . $data[0] . '/';
                if (!is_dir($dir)) {
                    mkdir($dir, 0775, true);
                }
                $fileName = $dir . date('Y-m-d') . '.log';
                if (!isset($server->data[md5($fileName)])) {
                    $server->data[md5($fileName)] = 1;
                } else {
                    $server->data[md5($fileName)] += 1;
                }
//                $logger = $this->getLogger($data[0]);
//                $logger?->info($data[1]);
//                if ($data[0] == 'error') {
//                    $logger = $this->getLogger('error');
//                    $logger?->error($data[1]);
//                } else {
//                    $logger = $this->getLogger($data[0]);
//                    $logger?->info($data[1]);
//                }
                return $server->send($fd, Server::format(Server::STATUS, File::write($fileName, $data[1], true) ? "OK" : "FAIL"));
            });
            $server->setHandler('countLog', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'countLog' command"));
                }
                $day = $data[1];
                $dir = APP_LOG_PATH . $data[0] . '/';
                $fileName = $dir . $day . '.log';
                if (!isset($server->data[md5($fileName)])) {
                    return $server->send($fd, Server::format(Server::INT, 0));
                }
                return $server->send($fd, Server::format(Server::INT, $server->data[md5($fileName)]));
//                if (!file_exists($fileName)) {
//                    return $server->send($fd, Server::format(Server::INT, 0));
//                }
//                return $server->send($fd, Server::format(Server::INT, filesize($fileName)));
            });
            $server->setHandler('getLog', function ($fd, $data) use ($server) {
                $day = $data[1];
                $dir = APP_LOG_PATH . $data[0] . '/';
                $start = $data[2];
                $size = $data[3];
                $fileName = $dir . $day . '.log';
                if (!file_exists($fileName)) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                $content = System::readFile($fileName);
                $logs = [];

                if ($content) {
                    $list = explode("\n", $content);
                    if ($start < 0) {
                        $list = array_reverse($list);
                        $size = abs($start);
                        $start = 0;
                    }
                    foreach ($list as $index => $c) {
                        if (!trim($c)) {
                            continue;
                        }
                        if ($size != -1) {
                            if ($index < $start) {
                                continue;
                            }
                            if (count($logs) >= $size) {
                                break;
                            }
                        }
                        if (!$log = JsonHelper::recover($c)) {
                            continue;
                        }
                        $logs[] = $log;
                    }
                }
                return $server->send($fd, Server::format(Server::STRING, JsonHelper::toJson($logs)));
            });
            $server->setHandler('sAdd', function ($fd, $data) use ($server) {
                if (count($data) < 2) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'sAdd' command"));
                }
                $key = $data[0];
                if (!isset($server->data[$key])) {
                    $array[$key] = array();
                }
                $count = 0;
                for ($i = 1; $i < count($data); $i++) {
                    $value = $data[$i];
                    if (!isset($server->data[$key][$value])) {
                        $server->data[$key][$value] = 1;
                        $count++;
                    }
                }

                return $server->send($fd, Server::format(Server::INT, $count));
            });
            $server->setHandler('sMembers', function ($fd, $data) use ($server) {
                if (count($data) < 1) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'sMembers' command"));
                }
                $key = $data[0];
                if (!isset($server->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                return $server->send($fd, Server::format(Server::SET, array_keys($server->data[$key])));
            });
            $server->setHandler('hSet', function ($fd, $data) use ($server) {
                if (count($data) < 3) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'hSet' command"));
                }

                $key = $data[0];
                if (!isset($server->data[$key])) {
                    $array[$key] = array();
                }
                $field = $data[1];
                $value = $data[2];
                $count = !isset($server->data[$key][$field]) ? 1 : 0;
                $server->data[$key][$field] = $value;
                return $server->send($fd, Server::format(Server::INT, $count));
            });
            $server->setHandler('hGetAll', function ($fd, $data) use ($server) {
                if (count($data) < 1) {
                    return $server->send($fd, Server::format(Server::ERROR, "ERR wrong number of arguments for 'hGetAll' command"));
                }
                $key = $data[0];
                if (!isset($server->data[$key])) {
                    return $server->send($fd, Server::format(Server::NIL));
                }
                return $server->send($fd, Server::format(Server::MAP, $server->data[$key]));
            });
            $server->on('WorkerStart', function ($server) {
                //Console::log(Color::green('MasterDB启动成功'));
                $server->tick(10000, function () use ($server) {
                    file_put_contents(APP_RUNTIME_DB, serialize($server->data));
                });
            });
            $server->start();
        } catch (\Throwable $exception) {
            Console::log('MasterDB:' . Color::red($exception->getMessage()));
        }
    }

    /**
     * 设置日志记录器
     * @param $name
     * @param Logger $logger
     * @return void
     */
    public function setLogger($name, Logger $logger) {
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
            new RotatingFileHandler($dir . $name . '.log', 100, $level) :
            new StreamHandler($dir . $name . '.log', $level);
        $logger = new Logger($name);
        $logger->pushHandler($handler);

        return $logger;
    }
}