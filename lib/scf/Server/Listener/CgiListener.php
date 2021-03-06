<?php

namespace Scf\Server\Listener;

use Scf\Core\Env;
use Scf\Core\Exception;
use Scf\Core\Result;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Exception\AppError;
use Scf\Mode\Web\Exception\AppException;
use Scf\Mode\Web\Exception\NotFoundException;
use Scf\Mode\Web\Log;
use Scf\Server\Http;
use Scf\Server\Table\Counter;
use Swoole\ExitException;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Throwable;

class CgiListener extends Listener {


    /**
     * @param Request $request
     * @param Response $response
     * @return void
     * @throws NotFoundException
     * @throws AppException
     * @throws Exception
     */
    protected function onRequest(Request $request, Response $response) {
        if ($request->server['path_info'] == '/favicon.ico' || $request->server['request_uri'] == '/favicon.ico') {
            $response->end();
            return;
        }
        if (!Http::instance()->isEnable()) {
            $response->status(503);
            $response->end("Service Unavailable!Try later");
            return;
        }
        Counter::instance()->incr('_REQUEST_COUNT_');
        $logger = Log::create();
        \Scf\Mode\Web\Response::create()->register($response);
        \Scf\Mode\Web\Request::create()->register($request);
        $app = App::instance();
        Env::isDev() and $logger->enableDebug();
        try {
            $app->init();
            $result = $app->run();
            if ($result instanceof Result) {
                if ($result->hasError()) {
                    \Scf\Mode\Web\Response::interrupt($result->getMessage(), $result->getErrCode(), $result->getData());
                } else {
                    \Scf\Mode\Web\Response::success($result->getData());
                }
            } else {
                $response->status(200);
                $response->end($result);
            }
        } catch (Throwable $exception) {
            if ($exception instanceof NotFoundException) {
                $app->handleNotFound();
            } elseif ($exception instanceof ExitException) {
                if (\Scf\Mode\Web\Response::instance()->isEnd()) {
                    return;
                }
                //$exception->getStatus() != 200 and Console::log(Color::red('#' . Coroutine::getCid() . ' ????????????:' . $exception->getStatus()));
                $app->handleError([
                    'code' => 503,
                    'error' => $exception->getStatus(),
                    'file' => $exception->getFile(),
                    'line' => $exception->getLine(),
                    'time' => date('Y-m-d H:i:s'),
                    'trace' => $exception->getTrace(),
                    'ip' => Http::instance()->ip()
                ]);
            } else {
                $logger->error($exception);
                $message = $exception->getMessage();
                $code = $exception->getCode();
                $file = $exception->getFile();
                $line = $exception->getLine();
                if ($exception instanceof AppError) {
                    $backTrace = $exception->getTrace();
                    $file = $backTrace[0]['file'] ?? $file;
                    $line = $backTrace[0]['line'] ?? $line;
                }
                $app->handleError([
                    'code' => $code,
                    'error' => $message,
                    'file' => $file,
                    'line' => $line,
                    'time' => date('Y-m-d H:i:s'),
                    'trace' => $exception->getTrace(),
                    'ip' => Http::instance()->ip()
                ]);
            }
        }
    }
}