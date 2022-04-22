<?php

namespace Scf\Mode\Web;


use JetBrains\PhpStorm\NoReturn;
use Scf\Core\Env;
use Scf\Core\RequestInstance;
use Scf\Core\Result;
use Scf\Helper\StringHelper;

class Response {
    use RequestInstance;

    /**
     * @var \Swoole\Http\Response
     */
    protected \Swoole\Http\Response $response;
    protected bool $isEnd = false;

    public function register(\Swoole\Http\Response $response) {
        $this->response = $response;
    }

    /**
     * @return bool
     */
    public function isEnd(): bool {
        return $this->isEnd;
    }

    /**
     * 输出成功结果
     * @param mixed $data
     * @return void
     */
    #[NoReturn] public static function success(mixed $data = '') {
        $output = [
            'errCode' => 0,
            'message' => 'SUCCESS',
            'data' => $data
        ];
        self::instance()->json($output);
        //self::instance()->stop(200);
    }

    /**
     * 向客户端响应输出错误
     * @param $error
     * @param string $code
     * @param mixed $data
     * @return void
     */
    #[NoReturn] public static function error($error, string $code = 'SERVICE_ERROR', mixed $data = '') {
        if ($error instanceof Result) {
            $output = [
                'errCode' => $error->getErrCode(),
                'message' => $error->getMessage(),
                'data' => $error->getData()
            ];
        } else {
            $output = [
                'errCode' => $code,
                'message' => $error,
                'data' => $data
            ];
        }
        self::instance()->json($output);
        //self::instance()->stop(503);
    }

    /**
     * 中断请求
     * @param $error
     * @param string $code
     * @param mixed $data
     * @return void
     */
    #[NoReturn] public static function interrupt($error, string $code = 'SERVICE_ERROR', mixed $data = '') {
        $output = [
            'errCode' => $code,
            'message' => $error,
            'data' => $data
        ];
        self::instance()->json($output);
        self::instance()->stop(503);
    }

    /**
     * @param $code
     * @return mixed
     */
    public function status($code): mixed {
        return $this->response->status($code);
    }

    public function setCookie($name, $val) {
        $cookie = session_get_cookie_params();
        $lifeTime = 0;
        if ($cookie['lifetime']) {
            $lifeTime = time() + $cookie['lifetime'];
        }
        $this->response->cookie($name, $val, $lifeTime, $cookie['path'], $cookie['domain'], $cookie['secure'], $cookie['httponly']);
    }

    public function setHeader($key, $val, $format = true) {
        $this->response->header(StringHelper::lower2camel($key), $val, $format);
    }

    public function json($data) {
        $logger = Log::instance();
        if ($logger->isDebugEnable()) {
            $data['debug'] = $logger->requestDebugInfo();
        }
//        if (Env::isDev()) {
//            $data['debug_backtrace'] = debug_backtrace();
//        }
        $this->response->header('content-type', 'application/json;charset=utf-8', true);
        $this->response->end(json_encode($data, JSON_UNESCAPED_UNICODE));
    }

    public function write($content) {
        $this->response->write($content);
    }

    public function end($content) {
        $this->response->end($content);
    }

    #[NoReturn] public function stop($code) {
        $this->isEnd = true;
        exit($code);
    }
}