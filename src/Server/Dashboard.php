<?php

namespace Scf\Server;

use Scf\App\Updater;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Core\Traits\Singleton;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Mode\Web\Route\AnnotationRouteRegister;
use Scf\Root;
use Scf\Server\Controller\DashboardController;
use Scf\Server\Listener\CgiListener;
use Scf\Server\Table\Runtime;
use Scf\Util\File;
use Swoole\Event;
use Swoole\ExitException;
use Swoole\Http\Server;
use Swoole\Process;
use Swoole\Server\Task;
use Swoole\Timer;
use Throwable;

class Dashboard {
    use Singleton;

    protected ?Server $_SERVER = null;

    public static function start(): void {
        if (!App::isMaster() && App::isReady()) {
            return;
        }
        $process = new Process(function () {
            try {
                $port = Http::getUseablePort(8580);
                Runtime::instance()->dashboardPort($port);
                self::instance()->create($port, Manager::instance()->issetOpt('d'));
            } catch (Throwable $exception) {
                Console::log('[' . $exception->getCode() . ']' . Color::red($exception->getMessage()));
            }
        });
        $process->start();
        $pid = "";
        Timer::after(500, function () use (&$pid) {
            $pid = File::read(SERVER_DASHBOARD_PID_FILE);
        });
        Event::wait();
        if (!$pid || !Process::kill($pid, 0)) {
            Console::info("【Dashboard】" . Color::red("服务启动失败"));
            exit();
        }
        Runtime::instance()->set('DASHBOARD_PID', $pid);
        Console::info("【Dashboard】" . Color::green("服务启动完成"));
        //应用未安装启动一个安装http服务器
        if (!App::isReady()) {
            try {
                $installServer = new Server('0.0.0.0', SERVER_PORT ?: 9580);
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
                $this->_SERVER = new Server('0.0.0.0');
                $setting = [
                    'worker_num' => 2,
                    'max_wait_time' => 60,
                    'reload_async' => true,
                    'daemonize' => $daemonize,
                    'log_file' => APP_PATH . '/log/server.log',
                    'pid_file' => SERVER_DASHBOARD_PID_FILE,
                ];
                $setting['document_root'] = Root::root() . '/public';
                $setting['enable_static_handler'] = true;
                $setting['http_autoindex'] = true;
                $setting['static_handler_locations'] = ['/dashboard'];
                $setting['http_index_files'] = ['index.html'];
                $this->_SERVER->set($setting);
                //监听HTTP请求
                try {
                    $httpPort = $this->_SERVER->listen('0.0.0.0', $port, SWOOLE_BASE);
                    $httpPort->set([
                        'open_http_protocol' => true,
                        'open_http2_protocol' => true,
                        'open_websocket_protocol' => false
                    ]);
                } catch (Throwable $exception) {
                    Console::log(Color::red('【Dashboard】服务[' . $port . ']启动失败:' . $exception->getMessage()));
                    exit(1);
                }
                $this->_SERVER->on('WorkerStart', function (Server $server, $workerId) {
                    if (App::isReady()) {
                        App::mount();
                        if ($workerId == 0) {
                            AnnotationRouteRegister::instance()->load();
                        }
                    }
                    //Console::info("【Dashboard】worker#" . $workerId . " 启动完成");
                });
                $this->_SERVER->on("AfterReload", function (Server $server) {
                    Console::info("【Dashboard】重启完成");
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
                            //转发回cgi
                            $httpPort = Runtime::instance()->httpPort() ?: 9580;
                            $path = $request->server['path_info'];
                            if (isset($request->server['query_string'])) {
                                $path .= '?' . $request->server['query_string'];
                            }
                            if (SERVER_HOST_IS_IP) {
                                $dashboardHost = PROTOCOL_HTTP . 'localhost:' . $httpPort;
                            } else {
                                $dashboardHost = PROTOCOL_HTTP . $httpPort . '.' . SERVER_HOST;
                            }
                            $client = \Scf\Client\Http::create($dashboardHost . $path);
                            foreach ($request->header as $key => $value) {
                                $client->setHeader($key, $value);
                            }
                            if ($request->server['request_method'] == 'GET') {
                                $client->get();
                            } else {
                                $client->post(Request::post()->pack());
                            }
                            $response->status($client->statusCode());
                            $response->end($client->body());
                        } else {
                            App::instance()->start();
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
                    if (!App::isReady()) {
                        Console::info("【Dashboard】等待安装配置文件就绪...");
                        $this->waittingInstall();
                        Console::info("【Dashboard】应用安装成功!配置文件加载完成!开始重启");
                        $server->reload();
                    }
                });
                //启动文件监听进程
//                if ((Env::isDev() && APP_RUN_MODE == 'src') || Manager::instance()->issetOpt('watch')) {
//                    $this->_SERVER->addProcess(SubProcess::createFileWatchProcess($this->_SERVER, $this->bindPort));
//                }
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