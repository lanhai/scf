<?php

namespace Scf\Server;

use Scf\App\Updater;
use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Core\Table\Runtime;
use Scf\Core\Traits\Singleton;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Mode\Web\Router;
use Scf\Server\Controller\DashboardController;
use Scf\Server\Listener\CgiListener;
use Scf\Util\File;
use Scf\Util\MemoryMonitor;
use Swoole\Event;
use Swoole\ExitException;
use Swoole\Http\Server;
use Swoole\Process;
use Swoole\Server\Task;
use Swoole\Timer;
use Throwable;
use function Co\run;

class Dashboard {
    use Singleton;

    protected ?Server $_SERVER = null;

    public static function host(): string {
        return 'http://127.0.0.1:' . File::read(SERVER_PORT_FILE);
    }

    public static function start(): void {
        if (!App::isMaster() && App::isReady()) {
            return;
        }
        if (!App::isReady()) {
            $port = (SERVER_PORT ?: 9580) + 1;
        } else {
            $serverConfig = Config::server();
            $port = (SERVER_PORT ?: ($serverConfig['port'] ?? 9580)) + 1;
        }
        $port = Http::getUseablePort($port);
        $process = new Process(function () use ($port) {
            try {
                self::instance()->create($port, Manager::instance()->issetOpt('d'));
            } catch (Throwable $exception) {
                Console::log('[' . $exception->getCode() . ']' . Color::red($exception->getMessage()));
            }
        });
        $masterPid = $process->start();
        $pid = "";
        Timer::after(500, function () use (&$pid) {
            $pid = File::read(SERVER_DASHBOARD_PID_FILE);
        });
        Event::wait();
        if (!$pid || !Process::kill((int)$pid, 0)) {
            Console::info("【Dashboard】" . Color::red("服务启动失败"));
            exit();
        }
        usleep(1000 * 100);
        Runtime::instance()->dashboardPort($port);
        Runtime::instance()->set('DASHBOARD_PID', "Master:{$masterPid},Server:" . Runtime::instance()->get('DASHBOARD_SERVER_PID'));
        //应用未安装启动一个安装http服务器
        if (!App::isReady()) {
            $serverPort = SERVER_PORT ?: 9580;
            // 尝试杀掉占用端口的进程
            if (\Scf\Core\Server::isPortInUse($serverPort)) {
                Console::log(Color::yellow('HTTP服务端口[' . $serverPort . ']被占用,尝试结束进程'));
                if (!\Scf\Core\Server::killProcessByPort($serverPort)) {
                    Console::log(Color::red('HTTP服务端口[' . $serverPort . ']被占用,尝试结束进程失败'));
                    exit();
                }
                sleep(1);
            }
            try {
                $installServer = new Server('0.0.0.0', $serverPort);
                $installServer->on('request', function (\Swoole\Http\Request $request, \Swoole\Http\Response $response) use ($installServer) {
                    if ($request->server['path_info'] == '/favicon.ico' || $request->server['request_uri'] == '/favicon.ico') {
                        $response->end();
                        return;
                    }
                    Response::instance()->register($response);
                    Request::instance()->register($request);
                    $listener = new CgiListener($installServer);
                    if (!$listener->dashboradTakeover($request, $response)) {
                        $response->status(503);
                        $response->header("Content-Type", "text/html; charset=utf-8");
                        $response->end("应用安装中...");
                    }
                });
                $installServer->on('start', function (Server $server) {
                    Console::info("【Dashboard】安装服务器已启动,等待安装完成");
                    App::await();
                    $timeout = 3;
                    Timer::tick(1000, function ($id) use ($server, &$timeout) {
                        if ($timeout == 0) {
                            Timer::clear($id);
                            $server->shutdown();
                            return;
                        }
                        Console::log("【Dashboard】应用安装完成," . Color::red($timeout) . "秒后关闭安装服务器");
                        $timeout--;
                    });
                });
                $installServer->start();
            } catch (Throwable) {
                Console::error("【Dashboard】启动安装服务失败");
            }
        }
    }

    /**
     * @param int $port
     * @param bool $daemonize
     * @return void
     */
    public function create(int $port, bool $daemonize = false): void {
        if (!is_null($this->_SERVER)) {
            $this->_SERVER->reload();
        } else {
            try {
                $this->_SERVER = new Server('127.0.0.1');
                $setting = [
                    'worker_num' => 2,
                    'max_wait_time' => 60,
                    'reload_async' => true,
                    'daemonize' => $daemonize,
                    'log_file' => APP_PATH . '/log/server.log',
                    'pid_file' => SERVER_DASHBOARD_PID_FILE,
                    'enable_reuse_port' => true,
                    'open_http_protocol' => true,
                    'open_http2_protocol' => true,
                    'open_websocket_protocol' => false,
                    'document_root' => SCF_ROOT . '/build/public',
                    'enable_static_handler' => true,
                    'http_autoindex' => true,
                    'static_handler_locations' => ['/dashboard'],
                    'http_index_files' => ['index.html']
                ];
                $this->_SERVER->set($setting);
                //监听HTTP请求
                try {
                    $this->_SERVER->listen('127.0.0.1', $port, SWOOLE_BASE);
                } catch (Throwable $exception) {
                    Console::log(Color::red('【Dashboard】服务[' . $port . ']启动失败:' . $exception->getMessage()));
                    exit(1);
                }
                $this->_SERVER->on('WorkerStart', function (Server $server, $workerId) {
                    if (App::isReady()) {
                        App::mount();
//                        if ($workerId == 0) {
//                            Router::loadRoutes();
//                        }
                    }
                });
                $this->_SERVER->on("AfterReload", function (Server $server) {
                    Console::info("【Dashboard】重启完成");
                });
                $this->_SERVER->on("BeforeShutdown", function (Server $server) {

                });
                $this->_SERVER->on('Task', function (Server $server, Task $task) {
                    //执行任务
                    /** @var \Scf\Core\Task $hander */
                    $hander = $task->data['handler'];
                    $hander::instance()->execute($task);
                });
                $this->_SERVER->on("Request", function (\Swoole\Http\Request $request, \Swoole\Http\Response $response) use ($port) {
                    $request->server['path_info'] = str_replace("/~", "", $request->server['path_info']);
                    if ($request->server['path_info'] == '/favicon.ico' || $request->server['request_uri'] == '/favicon.ico') {
                        $response->end();
                        return;
                    }
                    Response::instance()->register($response);
                    Request::instance()->register($request);
                    try {
                        $controller = new DashboardController();
                        $method = 'action' . StringHelper::lower2camel(str_replace("/", "_", substr($request->server['path_info'], 1)));
                        if (!method_exists($controller, $method)) {
                            if ($method == 'actionReload') {
                                $this->_SERVER->reload();
                                return;
                            }
                            //转发回cgi 用于api接口文档调试场景
                            $httpPort = Runtime::instance()->httpPort();
                            $path = $request->server['path_info'];
                            if (isset($request->server['query_string'])) {
                                $path .= '?' . $request->server['query_string'];
                            }
                            $dashboardHost = PROTOCOL_HTTP . 'localhost:' . $httpPort;
                            $client = \Scf\Client\Http::create($dashboardHost . $path);
                            foreach ($request->header as $key => $value) {
                                $client->setHeader($key, $value);
                            }
                            if ($request->server['request_method'] == 'GET') {
                                $client->get();
                            } else {
                                $client->post(Request::post()->pack());
                            }
                            $headers = $client->responseHeaders();
                            $response->header('Content-Type', $headers['content-type']);
                            $response->status((int)$client->statusCode());
                            $response->end($client->body());
                        } else {
                            \Scf\Mode\Web\App::instance()->start();
                            $controller->init($request->server['path_info']);
                            $result = $controller->$method();
                            if ($result instanceof Result) {
                                if ($result->hasError()) {
                                    Response::interrupt($result->getMessage(), $result->getErrCode(), $result->getData(), status: 200);
                                } else {
                                    Response::success($result->getData());
                                }
                            } else {
                                $response->status(200);
                                $response->end($result);
                            }
                        }
                    } catch (ExitException $exception) {
                        if (Response::instance()->isEnd()) {
                            return;
                        }
                        Response::error($exception->getStatus());
                    } catch (Throwable $exception) {
                        Response::error("STSTEM ERROR:" . $exception->getMessage());
                    }
                });
                //服务器完成启动
                $this->_SERVER->on('start', function (Server $server) {
                    MemoryMonitor::start('Server:Dashboard');
                    Runtime::instance()->set('DASHBOARD_SERVER_PID', $server->master_pid);
                    if (!App::isReady()) {
                        Console::info("【Dashboard】等待安装配置文件就绪...");
                        $this->waittingInstall();
                        Console::info("【Dashboard】应用安装成功!配置文件加载完成!开始重启");
                        $server->reload();
                    }
                    Timer::tick(1000 * 3, function ($tid) use ($server) {
                        if (!Runtime::instance()->serverIsAlive()) {
                            Timer::clear($tid);
                            Console::info("【Dashboard】" . Color::red('服务器即将关闭'));
                            $server->shutdown();
                        }
                    });
                });
                $this->_SERVER->start();
            } catch (Throwable $exception) {
                Console::log('【Dashboard】' . Color::red($exception->getMessage()));
            }
        }
    }

    public function getServer(): ?Server {
        return $this->_SERVER;
    }

    public static function server(): ?Server {
        try {
            return self::instance()->getServer();
        } catch (Throwable) {
            return null;
        }
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
                Console::info('【Dashboard】开始执行更新:' . $version['remote']['app']['version']);
                if ($updater->updateApp()) {
                    break;
                } else {
                    Console::info('【Dashboard】更新失败:' . $version['remote']['app']['version']);
                }
            }
            sleep(3);
        }
    }
}