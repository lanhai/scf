<?php

/**
 * 控制器
 */

namespace Scf\Mode\Web;

use JetBrains\PhpStorm\NoReturn;
use Scf\Core\App;
use Scf\Core\Result;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Request\ControllerRequest;
use Twig\Environment;
use Twig\Error\LoaderError;
use Twig\Error\RuntimeError;
use Twig\Error\SyntaxError;
use Twig\Loader\FilesystemLoader;

abstract class Controller {

    protected array $_config = [];
    protected string $theme = 'default';
    protected array $_tplValues = [];

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

    protected function assign(string $k, mixed $v): void {
        $this->_tplValues[$k] = $v;
    }

    protected function display($theme = 'default'): void {
        $tplPath = App::src() . 'template/' . (StringHelper::camel2lower($this->request()->getModuleName())) . '/' . $theme . '/' . (StringHelper::camel2lower($this->request()->getControllerName())) . '/';
        $tplFile = StringHelper::camel2lower($this->request()->getActionName()) . '.html';
        if (!file_exists($tplPath . $tplFile)) {
            Response::interrupt(App::isDevEnv() ? '模板文件:' . ($tplPath . $tplFile) . ' 不存在' : '系统繁忙,请稍后重试');
        } else {
            $loader = new FilesystemLoader($tplPath);
            $twig = new Environment($loader, [
                'cache' => APP_TMP_PATH . 'template',
                'auto_reload' => true,  // 当模板文件修改时自动重新编译
                'debug' => App::isDevEnv(),       // 开启调试模式
            ]);
            try {
                Response::exit($twig->render($tplFile, $this->_tplValues));
            } catch (LoaderError|RuntimeError|SyntaxError $e) {
                Response::interrupt($e->getMessage());
            }
        }
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