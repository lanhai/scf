<?php

namespace Scf\Server\Listener;

use Scf\Client\Http;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Core\Result;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Exception\AppError;
use Scf\Mode\Web\Exception\NotFoundException;
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Server\Controller\DashboardController;
use Scf\Server\Env;
use Scf\Server\Http as Server;
use Scf\Server\Table\Counter;
use Scf\Server\Table\Runtime;
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\Sn;
use Swoole\Event;
use Swoole\ExitException;
use Swoole\Timer;
use Throwable;

class CgiListener extends Listener {
    private static string $subscribersTableKey = 'log_subscribers';

    /**
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     * @return void
     * @throws Exception
     */
    public function onRequest(\Swoole\Http\Request $request, \Swoole\Http\Response $response): void {
//        register_shutdown_function(function () use ($response) {
//            $error = error_get_last();
//            switch ($error['type'] ?? null) {
//                case E_ERROR :
//                case E_PARSE :
//                case E_CORE_ERROR :
//                case E_COMPILE_ERROR :
//                    // log or send:
//                    // error_log($message);
//                    // $server->send($fd, $error['message']);
//                    $response->status(500);
//                    $response->end($error['message']);
//                    break;
//            }
//        });
        // 设置CORS响应头
        if (Runtime::instance()->get('allow_cross_origin')) {
            $response->header('Access-Control-Allow-Origin', '*'); // 允许所有源
            $response->header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
            $response->header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
        }
        // 如果是预检请求，直接返回200 OK
        if ($request->server['request_method'] == 'OPTIONS') {
            $response->status(200);
            $response->end();
            return;
        }

        if ($request->server['path_info'] == '/favicon.ico' || $request->server['request_uri'] == '/favicon.ico') {
            $response->end();
            return;
        }
        $status = Runtime::instance()->get('_SERVER_STATUS_');
        if ((int)$status == STATUS_OFF) {
            $response->header("Content-Type", "text/html; charset=utf-8");
            $response->status(503);
            $response->end(JsonHelper::toJson([
                'errCode' => 'SERVICE_UNAVAILABLE',
                'message' => "服务暂不可用,请稍后重试",
                'data' => ""
            ]));
            return;
        }
        $mysqlExecuteCount = Counter::instance()->get('_MYSQL_EXECUTE_COUNT_' . (time() - 1)) ?: 0;
        $requestCount = Counter::instance()->get('_REQUEST_COUNT_' . (time() - 1)) ?: 0;
//        $serverStatus = $this->server->stats();
//        $requestCount = $serverStatus['connection_num'] ?? 0;
        if ($requestCount > MAX_REQUEST_LIMIT || $mysqlExecuteCount > MAX_MYSQL_EXECUTE_LIMIT) {
            Counter::instance()->incr('_REQUEST_REJECT_');
            $response->header("Content-Type", "text/html; charset=utf-8");
            $response->status(503);
            $response->end(JsonHelper::toJson([
                'errCode' => 'SERVER_BUSY',
                'message' => "服务器繁忙,请稍后重试",
                'data' => ""
            ]));
            return;
        }
        Response::instance()->register($response);
        Request::instance()->register($request);
        //使用原子子增值统计并发访问量
        Counter::instance()->incr('_REQUEST_COUNT_');
        $countKey = '_REQUEST_COUNT_' . time();
        $count = Counter::instance()->incr($countKey);
        if ($count == 1) {
            Timer::after(2000, function () use ($countKey, $count) {
                Counter::instance()->delete($countKey);
            });
        }
        $countKeyDay = '_REQUEST_COUNT_' . Date::today();
        $countToday = Counter::instance()->incr($countKeyDay);
        if ($countToday == 1) {
            Timer::after(86400 * 1000 * 2, function () use ($countKeyDay) {
                Counter::instance()->delete($countKeyDay);
            });
        }
        Counter::instance()->incr('_REQUEST_PROCESSING_');
        if (!$this->dashboradTakeover($request, $response) && !$this->isConsoleMessage($request, $response)) {
            $logger = Log::instance();
            $app = App::instance();
            Env::isDev() and $logger->enableDebug();
            try {
                $app->init();
                $result = $app->run();
                if ($result instanceof Result) {
                    if ($result->hasError()) {
                        Response::error($result->getMessage(), $result->getErrCode(), $result->getData(), status: 200);
                    } else {
                        Response::success($result->getData());
                    }
                } else {
                    Response::instance()->status(200);
                    Response::instance()->end($result);
                }
            } catch (NotFoundException $e) {
                $app->handleNotFound($e->getMessage());
            } catch (Throwable $exception) {
                $message = $exception->getMessage();
                $code = $exception->getCode();
                $file = $exception->getFile();
                $line = $exception->getLine();
                if ($exception instanceof ExitException) {
                    if (!Response::instance()->isEnd()) {
                        $app->handleError([
                            'code' => 501,
                            'error' => $exception->getStatus(),
                            'file' => $exception->getFile(),
                            'line' => $exception->getLine(),
                            'time' => date('Y-m-d H:i:s'),
                            'trace' => Env::isDev() ? $exception->getTrace() : null,
                            'ip' => Server::instance()->ip()
                        ]);
                    }
                    goto Done;
                } elseif ($exception instanceof AppError) {
                    $backTrace = $exception->getTrace();
                    $file = $backTrace[0]['file'] ?? $file;
                    $line = $backTrace[0]['line'] ?? $line;
                }
                $logger->error($exception);
                $app->handleError([
                    'code' => $code,
                    'error' => $message,
                    'file' => $file,
                    'line' => $line,
                    'time' => date('Y-m-d H:i:s'),
                    'trace' => Env::isDev() ? $exception->getTrace() : null,
                    'ip' => Server::instance()->ip()
                ]);
            }
        }
        Done:
        Event::defer(function () {
            Counter::instance()->decr('_REQUEST_PROCESSING_');
        });
    }

    protected function isConsoleMessage(\Swoole\Http\Request $request, \Swoole\Http\Response $response): bool {
        if (str_starts_with($request->server['path_info'], '/@console.message@/')) {
            $data = Request::instance()->post()->pack();
            $subscribers = Runtime::instance()->get(self::$subscribersTableKey) ?: [];
            if ($subscribers) {
                foreach ($subscribers as $subscriber) {
                    if (!$this->server->exist($subscriber) || !$this->server->isEstablished($subscriber)) {
                        Console::unsubscribe($subscriber);
                        continue;
                    }
                    $this->server->push($subscriber, $data['message']);
                }
            }
            $response->end('ok');
            return true;
        }
        return false;
    }

    /**
     * 代理控制面板访问
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     * @return bool
     */
    public function dashboradTakeover(\Swoole\Http\Request $request, \Swoole\Http\Response $response): bool {
        if (str_starts_with($request->server['path_info'], '/~',)) {
            $isIndex = $request->server['path_info'] == '/~/' || $request->server['path_info'] == '/~';
            $path = str_replace("/~", "", $request->server['path_info']);
            $controller = new DashboardController();
            $method = 'action' . StringHelper::lower2camel(str_replace("/", "_", substr($path, 1)));
            if (!$isIndex && !method_exists($controller, $method)) {
                Request::resetPath($path);
                return false;
            }
            $port = Runtime::instance()->get('DASHBOARD_PORT');
            if (App::isReady()) {
                $masterHost = App::isMaster() ? 'localhost' : (Config::get('app')['master_host'] ?? 'localhost');
                if (SERVER_HOST_IS_IP || App::isMaster()) {
                    $dashboardHost = PROTOCOL_HTTP . $masterHost . ':' . $port;
                } else {
                    $dashboardHost = PROTOCOL_HTTP . $port . '.' . $masterHost;
                }
            } else {
                $masterHost = 'localhost';
                $dashboardHost = PROTOCOL_HTTP . 'localhost:' . $port;
            }
            if ($isIndex) {
                $url = $dashboardHost . '/dashboard';
            } else {
                if (isset($request->server['query_string'])) {
                    $path .= '?' . $request->server['query_string'];
                }
                $url = $dashboardHost . $path;
            }
            $client = Http::create($url);
            foreach ($request->header as $key => $value) {
                $client->setHeader($key, $value);
            }
            $client->setHeader('host', $request->header['host'] ?? $masterHost);
            $client->setHeader('referer', $request->header['referer'] ?? $masterHost);
            $sessionId = Request::cookie('_SESSIONID_');
            if (!$sessionId) {
                $sessionId = Sn::create_uuid();
                Response::instance()->setCookie('_SESSIONID_', $sessionId);
            }
            $cookieFile = APP_PATH . '/tmp/dashboard_' . $sessionId . '.cookie';
            if (file_exists($cookieFile)) {
                $client->setHeader('Cookie', File::read($cookieFile));
            }
            if ($request->server['request_method'] == 'GET') {
                $result = $client->get();
            } else {
                $result = $client->post(Request::post()->pack());
            }
            $response->status(200);
            if ($result->hasError()) {
                $response->end(JsonHelper::toJson([
                    'errCode' => 'SERVICE_UNAVAILABLE',
                    'message' => "转发请求至控制面板失败:" . $result->getMessage(),
                    'data' => [
                        'master' => $masterHost,
                        'host' => $dashboardHost,
                        'url' => $url,
                        'method' => $request->server['request_method']
                    ]
                ]));
            } else {
                $headers = $client->responseHeaders();
                $cookie = $headers['set-cookie'] ?? null;
                $cookie && $sessionId and File::write($cookieFile, $cookie);
                if ($path == '/logout' && file_exists($cookieFile)) {
                    unlink($cookieFile);
                }
                $response->end($client->body());
            }
            return true;
        }
        return false;
    }
}