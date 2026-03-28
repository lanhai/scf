<?php

namespace Scf\Server\Listener;

use Scf\Client\Http;
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
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\MemoryMonitor;
use Scf\Util\Sn;
use Swoole\Event;
use Swoole\ExitException;
use Swoole\Timer;
use Throwable;

class CgiListener extends Listener {

    protected const UPSTREAM_CONTROL_SHUTDOWN_PATH = '/_gateway/internal/upstream/shutdown';
    protected const UPSTREAM_CONTROL_STATUS_PATH = '/_gateway/internal/upstream/status';
    protected const UPSTREAM_CONTROL_HEALTH_PATH = '/_gateway/internal/upstream/health';
    protected const UPSTREAM_CONTROL_HTTP_PROBE_PATH = '/_gateway/internal/upstream/http_probe';
    protected const UPSTREAM_CONTROL_QUIESCE_PATH = '/_gateway/internal/upstream/quiesce';

    /**
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     * @return void
     * @throws Exception
     */
    public function onRequest(\Swoole\Http\Request $request, \Swoole\Http\Response $response): void {
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
        Response::instance()->register($response);
        Request::instance()->register($request);
        if ($this->handleProxyUpstreamControl($request, $response)) {
            return;
        }
        if ($request->server['path_info'] == '/favicon.ico' || $request->server['request_uri'] == '/favicon.ico') {
            $response->end();
            return;
        } elseif ($request->server['path_info'] == '/@log:dingtalk:auth:callback@/') {
            $response->status(200);
            //$response->header("Content-Type", "text/html; charset=utf-8");
            $response->header("Content-Type", "application/json;charset=utf-8");
            Request::get()->pack($data);
            $result = \Scf\Core\Log::instance()->dingtalkAuthCallback($data);
            if ($result->hasError()) {
                $response->end(JsonHelper::toJson([
                    'errCode' => $result->getErrCode(),
                    'message' => $result->getMessage(),
                    'data' => $result->getData()
                ]));
            } else {
                $response->end(JsonHelper::toJson([
                    'errCode' => 0,
                    'message' => "SUCCESS",
                    'data' => "授权完成"
                ]));
            }
            return;
        }
        $mysqlExecuteCount = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
        $requestCount = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
        if ($requestCount > MAX_REQUEST_LIMIT || $mysqlExecuteCount > MAX_MYSQL_EXECUTE_LIMIT) {
            Counter::instance()->incr(Key::COUNTER_REQUEST_REJECT_);
            $response->header("Content-Type", "application/json;charset=utf-8");
            $response->status(503);
            $response->end(JsonHelper::toJson([
                'errCode' => 'SERVER_BUSY',
                'message' => "服务器繁忙,请稍后重试",
                'data' => ""
            ]));
            return;
        }

        $workerId = Server::server()->worker_id + 1;
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
                Counter::instance()->incr(Key::COUNTER_REQUEST_REJECT_);
                $response->header("Content-Type", "application/json;charset=utf-8");
                $response->status(503);
                $response->end(JsonHelper::toJson([
                    'errCode' => 'SERVICE_UNAVAILABLE',
                    'message' => "服务暂不可用,请稍后重试",
                    'data' => ""
                ]));
                goto Done;
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
                            'ip' => Server::instance()->host()
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
                    'ip' => Server::instance()->host()
                ]);
            }
        }
        Done:
        Event::defer(function () use ($workerId, $request) {
            Counter::instance()->decr(Key::COUNTER_REQUEST_PROCESSING);
            if (defined('PROXY_GATEWAY_MODE') && PROXY_GATEWAY_MODE === true) {
                return;
            }
            $latestUsageUpdated = Runtime::instance()->get("worker.memory.usage.updated:{$workerId}") ?: 0;
            if (time() - $latestUsageUpdated >= 5) {
                $processName = "worker:" . ($workerId + 1);
                MemoryMonitor::updateUsage($processName);
                Runtime::instance()->set("worker.memory.usage.updated:{$workerId}", time());
            }
        });
    }

    protected function handleProxyUpstreamControl(\Swoole\Http\Request $request, \Swoole\Http\Response $response): bool {
        if (!(defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true)) {
            return false;
        }

        $path = (string)($request->server['path_info'] ?? $request->server['request_uri'] ?? '/');
        if (!in_array($path, [
            self::UPSTREAM_CONTROL_SHUTDOWN_PATH,
            self::UPSTREAM_CONTROL_STATUS_PATH,
            self::UPSTREAM_CONTROL_HEALTH_PATH,
            self::UPSTREAM_CONTROL_HTTP_PROBE_PATH,
            self::UPSTREAM_CONTROL_QUIESCE_PATH,
        ], true)) {
            return false;
        }

        $remoteAddr = (string)($request->server['remote_addr'] ?? '');
        if (!in_array($remoteAddr, ['127.0.0.1', '::1', 'localhost'], true)) {
            $response->status(403);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(JsonHelper::toJson([
                'errCode' => 'FORBIDDEN',
                'message' => 'forbidden',
                'data' => ''
            ]));
            return true;
        }

        if ($path === self::UPSTREAM_CONTROL_STATUS_PATH) {
            $status = $this->buildProxyUpstreamRuntimeStatus();
            $response->status(200);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(JsonHelper::toJson([
                'errCode' => 0,
                'message' => 'SUCCESS',
                'data' => $status
            ]));
            return true;
        }

        if ($path === self::UPSTREAM_CONTROL_HEALTH_PATH) {
            $response->status(200);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(JsonHelper::toJson([
                'errCode' => 0,
                'message' => 'SUCCESS',
                'data' => Server::instance()->proxyUpstreamHealthStatus(),
            ]));
            return true;
        }

        if ($path === self::UPSTREAM_CONTROL_HTTP_PROBE_PATH) {
            // 这条探针必须走真实的 onRequest/worker 响应链，专门用来证明 HTTP worker 还能接收并返回请求。
            $response->status(200);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(JsonHelper::toJson([
                'errCode' => 0,
                'message' => 'SUCCESS',
                'data' => [
                    'probe' => 'http',
                    'worker_id' => (int)(Server::server()->worker_id ?? -1),
                    'ts' => microtime(true),
                ],
            ]));
            return true;
        }

        if ($path === self::UPSTREAM_CONTROL_QUIESCE_PATH) {
            $response->status(200);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(JsonHelper::toJson([
                'errCode' => 0,
                'message' => 'SUCCESS',
                'data' => 'quiesce'
            ]));
            Timer::after(1, static function () {
                Server::instance()->quiesceBusinessPlane();
            });
            return true;
        }

        $response->status(200);
        $response->header('Content-Type', 'application/json;charset=utf-8');
        $response->end(JsonHelper::toJson([
            'errCode' => 0,
            'message' => 'SUCCESS',
            'data' => 'shutdown'
        ]));
        Timer::after(1, static function () {
            Server::instance()->shutdown();
        });
        return true;
    }

    protected function buildProxyUpstreamRuntimeStatus(): array {
        return Server::instance()->proxyUpstreamRuntimeStatus();
    }

    /**
     * 代理控制面板访问
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     * @return bool
     */
    public function dashboradTakeover(\Swoole\Http\Request $request, \Swoole\Http\Response $response): bool {
        if (str_starts_with($request->server['path_info'], '/~')) {
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
                $result = $client->get(600);
            } else {
                $result = $client->post(Request::post()->pack(), 600);
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
