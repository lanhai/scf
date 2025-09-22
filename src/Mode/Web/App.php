<?php

namespace Scf\Mode\Web;

use Scf\Core\Env;
use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Mode\Web\Exception\AppException;
use Scf\Mode\Web\Exception\NotFoundException;


class App extends Lifetime {
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
     * @throws NotFoundException
     */
    public function init(): void {
        $this->start();
        $modules = \Scf\Core\App::getModules();
        $this->router = Router::instance();
        //配置初始化
        try {
            if (!$this->router->matchAnnotationRoute($this->request()->server())) {
                $this->router->dispatch($this->request()->server(), $modules);
            }
            //创建日志记录
            $this->_logger = Log::instance()->setModule($this->router->getModule());
        } catch (NotFoundException) {
            $this->_logger = Log::instance()->setModule('server');
            if (str_contains($this->router->getPath(), 'MP_verify_')) {
                //微信公众号域名接入文件校验特殊处理
                $arr = explode('.', $this->router->getPath());
                $str = $arr[0];
                $str = ltrim(str_replace('MP_verify_', '', $str), '/');
                $this->response()->end($str);
            } elseif (preg_match('/^(\d+)\.txt$/', $this->router->getModule(), $matches) && file_exists(APP_PUBLIC_PATH . '/' . $this->router->getModule())) {
                $this->response()->end(file_get_contents(APP_PUBLIC_PATH . '/' . $this->router->getModule()));
            } else {
                Response::instance()->setHeader('Error-Info', 'module undefined:' . $this->router->getModule());
                throw new NotFoundException('未定义的模块:' . $this->router->getModule());
            }
        }
    }

    /**
     * @return string|Result
     * @throws NotFoundException
     * @throws AppException
     */
    public function run(): string|Result {
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
            Response::instance()->setHeader('Error-Info', 'controller not exist:' . $path);
            throw new NotFoundException('控制器不存在: ' . $path);
        }
        $ctrlPath = $router->getController() . '/' . $router->getAction();
        $router->setCtrlPath($ctrlPath);
        $class = new $matched['space']($moduleConfig);
        return call_user_func([$class, $matched['action']]);
//        $method = $router->getMethod();
//        if (APP_MODULE_STYLE == APP_MODULE_STYLE_MICRO) {
//            $ctrlClass = self::buildControllerPath('Controller', $router->getController());
//            $ctrlPath = $router->getController() . '/' . $router->getAction();
//            $isSubController = false;
//            //查找子目录控制器
//            if (!class_exists($ctrlClass)) {
//                $method = $router->getFragment(2) ?: 'index';
//                $router->fixPartition('module', StringHelper::lower2camel($router->getFragment(0) ?: 'Index'));
//                $router->fixPartition('controller', StringHelper::lower2camel($router->getAction()));
//                $router->fixPartition('action', StringHelper::lower2camel($method));
//                $ctrlClass = self::buildControllerPath('Controller', $router->getModule(), $router->getController());
//                $ctrlPath = $router->getModule() . '/' . $router->getController() . '/' . $router->getAction();
//                if (!class_exists($ctrlClass)) {
//                    $method = $router->getFragment(3) ?: 'index';
//                    $ctrlClass = self::buildControllerPath('Controller', $router->getModule(), $router->getController(), $router->getAction());
//                    $ctrlPath = $router->getController() . '/' . $router->getAction() . '/' . StringHelper::lower2camel($method);
//                    if (!class_exists($ctrlClass)) {
//                        Response::instance()->setHeader('Error-Info', 'controller not exist:' . $ctrlClass);
//                        throw new NotFoundException('控制器不存在:' . $ctrlClass);
//                    }
//                }
//                $isSubController = true;
//            }
//        } else {
//            $ctrlClass = self::buildControllerPath($router->getModule(), 'Controller', $router->getController());
//            $ctrlPath = $router->getController() . '/' . $router->getAction();
//            $isSubController = false;
//            //查找子目录控制器
//            if (!class_exists($ctrlClass)) {
//                $ctrlClass = self::buildControllerPath($router->getModule(), 'Controller', $router->getController(), $router->getAction());
//                if (!class_exists($ctrlClass)) {
//                    Response::instance()->setHeader('Error-Info', 'controller not exist:' . $ctrlClass);
//                    throw new NotFoundException('控制器不存在:' . $ctrlClass);
//                }
//                $method = $router->getFragment(3);
//                $ctrlPath = $router->getController() . '/' . $router->getAction() . '/' . $router->getFragment(3);
//                $isSubController = true;
//            }
//        }
//        $router->setCtrlPath($ctrlPath);
//        $ref = new ReflectionClass($ctrlClass);
//        if ($ref->isAbstract() or $ref->isInterface()) {
//            Response::instance()->setHeader('Error-Info', 'controller can not be abstract:' . $ctrlClass);
//            throw new NotFoundException('控制器为抽象类:' . $ctrlClass);
//        }
//        //实例化控制器
//        Config::load($moduleConfig);
//        $class = new $ctrlClass($moduleConfig);
//        $method = $isSubController ? $router->getMethod(StringHelper::lower2camel($method)) : $router->getMethod();
//        if (!method_exists($class, $method)) {
//            Response::instance()->setHeader('Error-Info', 'method not exist:' . $ctrlClass);
//            throw new NotFoundException('控制器方法不存在:' . $method);
//        }
//        //return call_user_func([$class, $method]);
//        return $class->$method();
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
//        if (Env::isDev()) {
//            Console::warning('******************发生错误(level:' . $level . ')******************');
//            Console::warning($message);
//            Console::warning($file . '@' . $line);
//            Console::warning('*******************************************');
//        }
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
