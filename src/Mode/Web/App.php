<?php

namespace Scf\Mode\Web;

use Scf\Core\Env;
use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Mode\Web\Exception\AppException;
use Scf\Mode\Web\Exception\NotFoundException;


class App extends RequestLifetime {
    /**
     * @var Router
     */
    protected Router $router;

    /**
     * @var Log 日志记录器
     */
    protected Log $_logger;

    /**
     * 初始化
     */
    public function init(): void {
        $this->start();
        $this->router = Router::instance();
        //配置初始化
        if (!$this->router->matchAnnotationRoute($this->request()->server())) {
            $this->router->dispatch();
        }
        //创建日志记录
        $this->_logger = Log::instance()->setModule($this->router->getModule());
    }

    /**
     * @return string|Result
     * @throws NotFoundException
     * @throws AppException
     */
    public function run(): Result|string {
        $router = $this->router;
        //自定义错误处理
        set_error_handler([$this, 'errorHandler']);
        //自定义异常处理
        set_exception_handler([$this, 'exceptionHandler']);
        $modules = \Scf\Core\App::getModules();
        $moduleConfig = ArrayHelper::findColumn($modules, 'name', $router->getModule()) ?: [];
        if ($router->isAnnotationRoute()) {
            $route = $router->getAnnotationRoute();
            $ctrlPath = $router->getController() . '/' . $router->getAction();
            $router->setCtrlPath($ctrlPath);
            $class = new $route['route']['space']($moduleConfig);
            $method = $route['route']['action'];
            return call_user_func_array([$class, $method], $route['params']);
        }
        $path = $router->getRoute();
        $matched = $router->matchNormalRoute();
        if (!$matched) {
            //微信公众号域名接入文件校验特殊处理
            if (str_contains($router->getPath(), 'MP_verify_')) {
                $arr = explode('.', $router->getPath());
                $str = $arr[0];
                return ltrim(str_replace('MP_verify_', '', $str), '/');
            } elseif (preg_match('/^(\d+)\.txt$/', $router->getModule()) && file_exists(APP_PUBLIC_PATH . '/' . $router->getModule())) {
                return file_get_contents(APP_PUBLIC_PATH . '/' . $router->getModule());
            } else {
                Response::instance()->setHeader('Error-Info', 'route not match:' . $path);
                throw new NotFoundException('访问的页面不存在');
            }
        }
        $ctrlPath = $router->getController() . '/' . $router->getAction();
        $router->setCtrlPath($ctrlPath);
        $class = new $matched['space']($moduleConfig);
        return call_user_func([$class, $matched['action']]);
    }

    /**
     * 异常处理
     * @param $exception
     * @throws AppException
     */
    public function exceptionHandler($exception) {
        throw new AppException($exception);
    }

    /**
     * worker运行错误处理
     * @param $level
     * @param $message
     * @param null $file
     * @param null $line
     */
    public function errorHandler($level, $message, $file = null, $line = null): void {
        \Scf\Core\Log::instance()->error($message, file: $file, line: $line);
        //判断是否是子携程触发的错误
        $response = Response::instance();
        if ($response->isAncestorThread() && !$response->isEnd()) {
            Response::error(Env::isDev() ? $message : '系统繁忙,请稍后重试', 'SERVER_ERROR', status: 500);
        }
    }


    /**
     * 处理404错误
     * @param string $msg
     * @return void
     */
    public function handleNotFound(string $msg = 'Page Not Found!'): void {
        Response::error(Env::isDev() ? $msg : '没有找到您要访问的页面', status: 404);
    }

    /**
     * server错误处理
     * @param $error
     * @return void
     */
    public function handleError($error): void {
        if ($this->response()->isEnd()) {
            return;
        }
        Response::error(Env::isDev() ? (!str_contains($error['error'], '@') ? $error['error'] . '@' . $error['file'] . ':' . $error['line'] : $error['error']) : '系统繁忙,请稍后重试', status: 500);
    }

    /**
     * 返回request对象
     * @return Request
     */
    public function request(): Request {
        return Request::instance();
    }

    /**
     * 返回request对象
     * @return Response
     */
    public function response(): Response {
        return Response::instance();
    }

    /**
     * 记录日志
     * @return Log
     */
    public function log(): Log {
        return $this->_logger;
    }

    public function getNamespace(): string {
        return substr(get_class($this), 0, -6);
    }

    public function getName(): ?string {
        $class = explode('\\', get_class($this));
        array_pop($class);
        return array_pop($class);
    }

} 
