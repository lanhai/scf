<?php

namespace Scf\Server\Listener;

use Scf\Client\Http;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Exception;
use Scf\Core\Key;
use Scf\Core\Result;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Exception\AppError;
use Scf\Mode\Web\Exception\NotFoundException;
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Server\Controller\DashboardController;
use Scf\Server\Http as Server;
use Scf\Server\Manager;
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\Sn;
use Scf\Util\Time;
use Swoole\Event;
use Swoole\ExitException;
use Swoole\Timer;
use Throwable;

class CgiListener extends Listener {

    /**
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     * @return void
     * @throws Exception
     */
    public function onRequest(\Swoole\Http\Request $request, \Swoole\Http\Response $response): void {
        register_shutdown_function(function () use ($response) {
            $error = error_get_last();
            switch ($error['type'] ?? null) {
                case E_ERROR :
                case E_PARSE :
                case E_CORE_ERROR :
                case E_COMPILE_ERROR :
                    Log::instance()->error($error['message']);
                    $response->status(500);
                    $response->end($error['message']);
                    break;
            }
        });
        // 设置CORS响应头
        if (defined('SERVER_ALLOW_CROSS_ORIGIN') && SERVER_ALLOW_CROSS_ORIGIN) {
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

        $mysqlExecuteCount = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
        $requestCount = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
        if ($requestCount > MAX_REQUEST_LIMIT || $mysqlExecuteCount > MAX_MYSQL_EXECUTE_LIMIT) {
            Counter::instance()->incr(Key::COUNTER_REQUEST_REJECT_);
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
        //内存使用
        $workerId = Server::server()->worker_id + 1;
        $key = 'WORKER_MEMORY_USAGE:' . $workerId + 1;
        //$lastUsage = Runtime::instance()->get($key) ?: 0;
        Counter::instance()->incr("worker:" . $workerId . ":connection");
        //等待响应
        Counter::instance()->incr(Key::COUNTER_REQUEST_PROCESSING);
        //并发请求
        $currentRequestKey = Key::COUNTER_REQUEST . time();
        $currentRequest = Counter::instance()->incr($currentRequestKey);
        if ($currentRequest == 1) {
            Timer::after(3000, function () use ($currentRequestKey) {
                Counter::instance()->delete($currentRequestKey);
            });
        }

        if (!$this->dashboradTakeover($request, $response)) {
            //今日请求
            $requestTodayCountKey = Key::COUNTER_REQUEST . Date::today();
            Counter::instance()->incr($requestTodayCountKey);
            if (!Runtime::instance()->serverIsReady()) {
                $response->header("Content-Type", "application/json;charset=utf-8");
                $response->status(503);
                $response->end(JsonHelper::toJson([
                    'errCode' => 'SERVICE_UNAVAILABLE',
                    'message' => "服务暂不可用,请稍后重试",
                    'data' => ""
                ]));
                return;
            }
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
        Event::defer(function () use ($workerId, $request) {
            Counter::instance()->decr("worker:" . $workerId . ":connection");
            Counter::instance()->decr(Key::COUNTER_REQUEST_PROCESSING);
//            $usage = memory_get_usage();
//            $usageMb = round($usage / 1048576, 2);
//            if ($usageMb - $lastUsage >= 2) {
//                \Scf\Core\Log::instance()->setModule("server")->info("{$key} [http:{$request->server['path_info']}]当前内存使用：{$usageMb}MB,较上次增长：" . round($usageMb - $lastUsage, 2) . "MB", false);
//            }
//            Runtime::instance()->set($key, $usageMb);
        });
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
            $response->status(200);
            if (in_array($path, DashboardController::$protectedActions)) {
                $response->header('Content-Type', 'application/json');
                $response->end(JsonHelper::toJson([
                    'errCode' => 'UNAUTHORIZED',
                    'message' => "未授权的访问",
                    'data' => null
                ]));
            }
            $controller = new DashboardController();
            $method = 'action' . StringHelper::lower2camel(str_replace("/", "_", substr($path, 1)));
            if (!$isIndex && !method_exists($controller, $method)) {
                Request::resetPath($path);
                return false;
            }
            $port = Runtime::instance()->dashboardPort();
            $dashboardHost = PROTOCOL_HTTP . '127.0.0.1:' . $port;
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
            $client->setHeader('host', $request->header['host'] ?? 'localhost');
            $client->setHeader('referer', $request->header['referer'] ?? 'localhost');
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
            $response->header('Content-Type', 'application/json');
            if ($result->hasError()) {
                $response->end(JsonHelper::toJson([
                    'errCode' => $result->getErrCode(),
                    'message' => $result->getData('message') ?? "转发请求至控制面板失败:" . $result->getMessage(),
                    'data' => [
                        'host' => $dashboardHost,
                        'url' => $url,
                        'method' => $request->server['request_method'],
                        'body' => $result->getData()
                    ]
                ]));
            } else {
                $headers = $client->responseHeaders();
                $response->header('Content-Type', $headers['content-type']);
                if (isset($headers['error-info'])) {
                    $response->header('Error-Info', $headers['error-info']);
                }
                if (isset($headers['server'])) {
                    $response->header('Server', $headers['server']);
                }
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