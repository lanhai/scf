<?php

/**
 * 控制器
 */

namespace Scf\Mode\Web;

use JetBrains\PhpStorm\NoReturn;
use Scf\Core\Result;
use Scf\Mode\Web\Request\ControllerRequest;

abstract class Controller {

    protected array $_config = [];

    public function __construct($config = []) {
        $this->_config = $config;
        $this->_preInit();
        $this->_init();
    }

    /**
     * 预初始化
     */
    protected function _preInit() {

    }

    /**
     * 初始化
     */
    protected function _init() {

    }

    /**
     * 获取request对象
     * @return ControllerRequest
     */
    protected function request(): ControllerRequest {
        return ControllerRequest::instance();
    }

    /**
     * @return Response
     */
    private function response(): Response {
        return Response::instance();
    }

    /**
     * 获取配置文件
     * @param $key
     * @return mixed
     */
    protected function getConfig($key): mixed {
        return $this->_config[$key] ?? null;
    }

    /**
     * @return App
     */
    protected function app(): App {
        return App::instance();
    }

    /**
     * 获取日志记录器
     * @return Log
     */
    protected function log(): Log {
        return Log::instance();
    }


    /**
     * 输出成功结果
     * @param mixed $data
     * @return void
     */
    #[NoReturn] protected function success(mixed $data = ''): void {
        Response::success($data);
    }

    /**
     * 向客户端响应输出错误
     * @param $error
     * @param string $code
     * @param mixed $data
     * @return void
     */
    #[NoReturn] protected function error($error, string $code = 'SERVICE_ERROR', mixed $data = ''): void {
        Response::interrupt($error, $code, $data);
    }

    /**
     * 向客户端响应json数据
     * @param $data
     * @return void
     */
    protected function json($data): void {
        $this->response()->json($data);
    }

    /**
     * 获取结果
     * @param $result
     * @return Result
     */
    protected function getResult($result): Result {
        return new Result($result);
    }

}