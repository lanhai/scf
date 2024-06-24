<?php

namespace Scf\Server;

use Scf\App\Updater;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Core\Traits\Singleton;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Root;
use Scf\Server\Controller\DashboardController;
use Scf\Server\Listener\CgiListener;
use Scf\Util\File;
use Swoole\Event;
use Swoole\ExitException;
use Swoole\Http\Server;
use Swoole\Process;
use Swoole\Server\Task;
use Swoole\Timer;

class Dashboard {
    use Singleton;

    protected ?Server $_SERVER = null;

    public static function start($port): void {
        if (!App::isMaster() && App::isReady()) {
            return;
        }
        $process = new Process(function () use ($port) {
            try {
                self::instance()->create($port, Manager::instance()->issetOpt('d'));
            } catch (\Throwable $exception) {
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
            Console::error('Dashboard服务启动失败');
            exit();
        }
        Console::success("Dashboard服务启动完成!PID:" . $pid);
        //应用未安装启动一个安装http服务器
        if (!App::isReady()) {
            try {
                $installServer = new Server('0.0.0.0', Http::instance()->getPort());
                $installServer->on('request', function (\Swoole\Http\Request $request, \Swoole\Http\Response $response) {
                    if ($request->server['path_info'] == '/favicon.ico' || $request->server['request_uri'] == '/favicon.ico') {
                        $response->end();
                        return;
                    }
                    Response::instance()->register($response);
                    Request::instance()->register($request);
                    $listener = new CgiListener();
                    App::isReady() and App::mount();
                    if (!$listener->dashboradTakeover($request, $response)) {
                        $response->status(503);
                        $response->end("应用安装中...");
                    }
                });
                $installServer->on('start', function (Server $server) {
                    Console::info("安装服务器已启动,等待安装完成");
                    App::await();
                    $server->shutdown();
                    Console::log("应用安装完成," . Color::red(5) . "秒后关闭安装服务器");
                    $timeout = 4;
                    Timer::tick(1000, function ($id) use ($server, &$timeout) {
                        if ($timeout == 0) {
                            Timer::clear($id);
                            return;
                        }
                        Console::log("应用安装完成," . Color::red($timeout) . "秒后关闭安装服务器");
                        $timeout--;
                    });

                });
                $installServer->start();
            } catch (\Throwable $throwable) {
                Console::error("启动安装服务失败");
            }
        }
    }

    /**
     * @param int $port
     * @param bool $daemonize
     * @return void
     */
    public function create(int $port = 9582, bool $daemonize = false): void {
        if (!is_null($this->_SERVER)) {
            $this->_SERVER->reload();
        } else {
            //检查是否存在异常进程
//            $pid = 0;
//            Coroutine::create(function () use (&$pid) {
//                $pid = File::read(SERVER_DASHBOARD_PID_FILE);
//            });
//            Event::wait();
//            if ($pid && Process::kill($pid, 0)) {
//                Process::kill($pid, SIGKILL);
//                $time = time();
//                while (true) {
//                    usleep(1000);
//                    if (!Process::kill($pid, 0)) {
//                        if (is_file(SERVER_DASHBOARD_PID_FILE)) {
//                            unlink(SERVER_DASHBOARD_PID_FILE);
//                        }
//                        Console::log(Color::yellow("Dashboard PID:{$pid} killed"));
//                        break;
//                    } else {
//                        if (time() - $time > 15) {
//                            Console::info("结束Dashboard进程失败:{$pid} , 请重试");
//                            break;
//                        }
//                    }
//                }
//            }
            try {
                $this->_SERVER = new Server('0.0.0.0');
                $setting = [
                    'worker_num' => 4,
                    'max_wait_time' => 60,
                    'reload_async' => true,
                    'daemonize' => $daemonize,
                    'log_file' => APP_PATH . 'log/server.log',
                    'pid_file' => SERVER_DASHBOARD_PID_FILE,
                ];
                $setting['document_root'] = Root::dir() . '/public';
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
                } catch (\Throwable $exception) {
                    Console::log(Color::red('dashboard服务[' . $port . ']启动失败:' . $exception->getMessage()));
                    exit(1);
                }
                $this->_SERVER->on('WorkerStart', function (Server $server, $workerId) {
                    App::isReady() and App::mount();
                    //Console::info("[Dashboard]worker#" . $workerId . "启动完成");
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
//                            $cgi = new CgiListener();
//                            $cgi->onRequest($request, $response, true);
                            if ($method == 'actionReload') {
                                $this->_SERVER->reload();
                                return;
                            }
                            //转发回cgi
                            $path = $request->server['path_info'];
                            if (isset($request->server['query_string'])) {
                                $path .= '?' . $request->server['query_string'];
                            }
                            $client = \Scf\Client\Http::create(PROTOCOL_HTTP . 'localhost:' . ($port - 2) . $path);
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
                                    Response::interrupt($result->getMessage(), $result->getErrCode(), $result->getData());
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
                    } catch (\Throwable $exception) {
                        Response::error("STSTEM ERROR:" . $exception->getMessage());
                    }
                });
                //服务器完成启动
                $this->_SERVER->on('start', function (Server $server) {
                    if (!App::isReady()) {
                        //App::await();
                        Console::info("等待安装配置文件就绪...");
                        $this->waittingInstall();
                        Console::info("[Dashboard]应用安装成功!配置文件加载完成!开始重启");
                        $server->reload();
                    }

                });
                $this->_SERVER->start();
            } catch (\Throwable $exception) {
                Console::log('dashboard:' . Color::red($exception->getMessage()));
            }
        }
    }

    public function getServer(): ?Server {
        return $this->_SERVER;
    }

    public static function server(): ?Server {
        try {
            return self::instance()->getServer();
        } catch (\Throwable) {
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
                Log::instance()->info('开始执行更新:' . $version['remote']['app']['version']);
                if ($updater->updateApp()) {
                    break;
                } else {
                    Log::instance()->info('更新失败:' . $version['remote']['app']['version']);
                }
            }
            usleep(5000 * 1000);
        }
    }
}