<?php

namespace Scf\Server\Listener;

use Scf\Core\Env;
use Scf\Core\Exception;
use Scf\Core\Key;
use Scf\Core\Result;
use Scf\Core\SecondWindowCounter;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Exception\AppError;
use Scf\Mode\Web\Exception\NotFoundException;
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Server\Http as Server;
use Scf\Util\Date;
use Scf\Util\MemoryMonitor;
use Swoole\Event;
use Swoole\ExitException;
use Throwable;

class CgiListener extends Listener {

    protected const UPSTREAM_CONTROL_HTTP_PROBE_PATH = '/_gateway/internal/upstream/http_probe';
    protected const UPSTREAM_CONTROL_CUTOVER_PROBE_PATH = '/_scf_internal/upstream/cutover_probe';
    protected static string $requestTodayBucket = '';
    protected static int $requestTodayBuffered = 0;
    protected static int $requestTodayLastFlushAt = 0;

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
        $requestMethod = strtoupper((string)($request->server['request_method'] ?? 'GET'));
        $mysqlExecuteCount = SecondWindowCounter::mysqlCountOfSecond(time() - 1);
        $requestCount = SecondWindowCounter::requestCountOfSecond(time() - 1);
        if ($this->shouldRejectForOverload($requestMethod, $requestCount, $mysqlExecuteCount, $request)) {
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
        // 记录当前 worker 的秒级请求量，供上一秒 QPS 读侧汇总使用。
        $this->incrCurrentSecondRequestCounter();
        // dashboard 入口已统一收敛到 gateway 控制面，这里只保留业务请求主链。
        $this->bufferTodayRequestCounterIncrement();
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
            }
            if ($exception instanceof AppError) {
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
        Done:
        Event::defer(function () use ($workerId) {
            Counter::instance()->decr(Key::COUNTER_REQUEST_PROCESSING);
            $this->flushBufferedRequestTodayCounter(false);
            $latestUsageUpdated = Runtime::instance()->get("worker.memory.usage.updated:{$workerId}") ?: 0;
            if (time() - $latestUsageUpdated >= 5) {
                $processName = "worker:" . ($workerId + 1);
                MemoryMonitor::updateUsage($processName);
                Runtime::instance()->set("worker.memory.usage.updated:{$workerId}", time());
            }
        });
    }

    /**
     * 按当前负载状态判断是否需要拒绝请求。
     *
     * 该策略分两级：
     * 1. 超过硬阈值直接拒绝；
     * 2. 进入软阈值区间后按线性概率降载，避免硬切 503。
     *
     * @param string $requestMethod 请求方法。
     * @param int $requestCount 最近一秒请求量。
     * @param int $mysqlExecuteCount 最近一秒 mysql 执行量。
     * @param \Swoole\Http\Request $request 原始请求对象。
     * @return bool
     */
    protected function shouldRejectForOverload(
        string $requestMethod,
        int $requestCount,
        int $mysqlExecuteCount,
        \Swoole\Http\Request $request
    ): bool {
        $requestRatio = MAX_REQUEST_LIMIT > 0 ? ($requestCount / MAX_REQUEST_LIMIT) : 0.0;
        $mysqlRatio = MAX_MYSQL_EXECUTE_LIMIT > 0 ? ($mysqlExecuteCount / MAX_MYSQL_EXECUTE_LIMIT) : 0.0;
        $peakRatio = max($requestRatio, $mysqlRatio);

        if ($peakRatio >= (float)OVERLOAD_HARD_RATIO) {
            return true;
        }
        if (!ENABLE_ADAPTIVE_OVERLOAD_SHEDDING || $peakRatio < (float)OVERLOAD_SOFT_RATIO) {
            return false;
        }

        $window = max(0.01, (float)OVERLOAD_HARD_RATIO - (float)OVERLOAD_SOFT_RATIO);
        $shedSlope = ($peakRatio - (float)OVERLOAD_SOFT_RATIO) / $window;
        $shedProbability = min((float)OVERLOAD_MAX_SHED_PROBABILITY, max(0.0, $shedSlope * (float)OVERLOAD_MAX_SHED_PROBABILITY));
        // 写请求优先保留，减轻“业务写接口在软限流区被大量拒绝”的副作用。
        if (in_array($requestMethod, ['POST', 'PUT', 'PATCH', 'DELETE'], true)) {
            $shedProbability *= 0.5;
        }
        $seed = (string)($request->server['request_uri'] ?? '/')
            . '|' . microtime(true)
            . '|' . mt_rand(0, PHP_INT_MAX);
        $sample = ((float)sprintf('%u', crc32($seed))) / 4294967295.0;
        return $sample < $shedProbability;
    }

    /**
     * 记录当前 worker 的秒级请求量。
     *
     * @return int
     */
    protected function incrCurrentSecondRequestCounter(): int {
        return SecondWindowCounter::incrRequestSecondForWorker(Server::server()->worker_id + 1);
    }

    /**
     * 在 worker 本地缓冲“今日请求量”计数。
     *
     * @return void
     */
    protected function bufferTodayRequestCounterIncrement(): void {
        $today = Date::today();
        if (self::$requestTodayBucket !== '' && self::$requestTodayBucket !== $today) {
            $this->flushBufferedRequestTodayCounter(true);
        }
        if (self::$requestTodayBucket === '') {
            self::$requestTodayBucket = $today;
        }
        self::$requestTodayBuffered++;
        if (self::$requestTodayLastFlushAt <= 0) {
            self::$requestTodayLastFlushAt = time();
        }
        $this->flushBufferedRequestTodayCounter(false);
    }

    /**
     * 将 worker 本地缓冲的“今日请求量/总请求量”批量回写到共享计数表。
     *
     * @param bool $force 是否强制立即 flush。
     * @return void
     */
    protected function flushBufferedRequestTodayCounter(bool $force): void {
        if (self::$requestTodayBuffered <= 0) {
            return;
        }
        $now = time();
        $needFlush = $force
            || self::$requestTodayBuffered >= REQUEST_TODAY_COUNTER_FLUSH_SIZE
            || ($now - self::$requestTodayLastFlushAt) >= REQUEST_TODAY_COUNTER_FLUSH_INTERVAL;
        if (!$needFlush) {
            return;
        }

        $today = self::$requestTodayBucket !== '' ? self::$requestTodayBucket : Date::today();
        $delta = self::$requestTodayBuffered;
        Counter::instance()->incr(Key::COUNTER_REQUEST . $today, '_value', $delta);
        Counter::instance()->incr(Key::COUNTER_REQUEST, '_value', $delta);
        self::$requestTodayBuffered = 0;
        self::$requestTodayLastFlushAt = $now;
        self::$requestTodayBucket = $today;
    }

    protected function handleProxyUpstreamControl(\Swoole\Http\Request $request, \Swoole\Http\Response $response): bool {
        if (!(defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true)) {
            return false;
        }

        $path = (string)($request->server['path_info'] ?? $request->server['request_uri'] ?? '/');
        // upstream 与 gateway 的控制链路（status/health/quiesce/shutdown）统一走本地 IPC，
        // 不再占用 upstream worker 的 onRequest 通道。这里仅保留：
        // 1) HTTP worker 探针；
        // 2) 切流校验阶段的入口命中探针（gateway business port -> nginx -> upstream）。
        if (!in_array($path, [self::UPSTREAM_CONTROL_HTTP_PROBE_PATH, self::UPSTREAM_CONTROL_CUTOVER_PROBE_PATH], true)) {
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

        if ($path === self::UPSTREAM_CONTROL_CUTOVER_PROBE_PATH) {
            // 切流探针要求显式 header，避免业务路径偶然命中该 internal path。
            if (((string)($request->header['x-scf-cutover-probe'] ?? '')) !== '1') {
                return false;
            }
            $response->status(200);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(JsonHelper::toJson([
                'errCode' => 0,
                'message' => 'SUCCESS',
                'data' => [
                    'probe' => 'cutover',
                    'upstream_port' => (int)(Runtime::instance()->httpPort() ?: 0),
                    'worker_id' => (int)(Server::server()->worker_id ?? -1),
                    'ts' => microtime(true),
                ],
            ]));
            return true;
        }

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

}
