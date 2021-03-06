<?php

namespace Scf\Mode\Web;

use JetBrains\PhpStorm\NoReturn;
use Scf\Core\Config;
use Scf\Core\Env;
use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Exception\AppError;
use Scf\Mode\Web\Exception\AppException;
use Scf\Mode\Web\Exception\NotFoundException;
use Swoole\Event;
use function Swoole\Coroutine\run;


class App extends Thread {
    /**
     * @var Router
     */
    protected Router $router;
    /**
     * @var \Swoole\Http\Request 响应对象
     */

    /**
     * @var Log 日志记录器
     */
    protected Log $_logger;

    /**
     * 初始化
     * @throws NotFoundException
     */
    public function init() {
        $this->start();
        $modules = self::$_modules;
        $this->router = Router::instance();
        //配置初始化
        try {
            $this->router->dispatch($this->request()->server(), $modules);
            //创建日志记录
            $this->_logger = Log::instance()->setModule($this->router->getModule());
        } catch (NotFoundException $exception) {
            $this->_logger = Log::instance()->setModule('server');
            if (str_contains($this->router->getPath(), 'MP_verify_')) {
                //微信公众号域名接入文件校验特殊处理
                $arr = explode('.', $this->router->getPath());
                $str = $arr[0];
                $str = ltrim(str_replace('MP_verify_', '', $str), '/');
                $this->response()->end($str);
            } else {
                Response::instance()->setHeader('Error-Info', 'module undefined:' . $this->router->getModule());
                throw new NotFoundException('未定义的模块:' . $this->router->getModule());
            }
        }
    }


    /**
     * @return ?Result
     * @throws NotFoundException
     * @throws AppException
     */
    public function run(): string|Result {
        $router = $this->router;
        $ctrlClass = APP_TOP_NAMESPACE . '\\' . $router->getModule() . '\\Controller\\' . $router->getController();
        $ctrlPath = $router->getController() . '/' . $router->getAction();
        $action = $router->getAction();
        $isSubController = false;
        //查找子目录控制器
        if (!class_exists($ctrlClass)) {
            $ctrlClass = APP_TOP_NAMESPACE . '\\' . $router->getModule() . '\\Controller\\' . $router->getController() . '\\' . $action;
            if (!class_exists($ctrlClass)) {
                Response::instance()->setHeader('Error-Info', 'controller not exist:' . $ctrlClass);
                throw new NotFoundException('控制器不存在:' . $ctrlClass);
            }
            $ctrlPath = $router->getController() . '/' . $router->getAction() . '/' . $router->getFragment(3);
            $isSubController = true;
        }
        $router->setCtrlPath($ctrlPath);
        $ref = new \ReflectionClass($ctrlClass);
        if ($ref->isAbstract() or $ref->isInterface()) {
            Response::instance()->setHeader('Error-Info', 'controller can not be abstract:' . $ctrlClass);
            throw new NotFoundException('控制器为抽象类:' . $ctrlClass);
        }
        //实例化控制器
        $modules = self::$_modules;
        $moduleConfig = ArrayHelper::findColumn($modules, 'name', $router->getModule()) ?: [];
        Config::load($moduleConfig);
        $class = new $ctrlClass($moduleConfig);
        //自定义错误处理
        set_error_handler([$this, 'errorHandler']);
        //自定义异常处理
        set_exception_handler([$this, 'exceptionHandler']);
        $method = $isSubController ? $router->getMethod(StringHelper::lower2camel($router->getFragment(3))) : $router->getMethod();
        if (!method_exists($class, $method)) {
            Response::instance()->setHeader('Error-Info', 'method not exist:' . $ctrlClass);
            throw new NotFoundException('控制器方法不存在:' . $method);
        }
        //        if (!$reesult instanceof Result) {
//            throw new AppException('服务器开了点小差,请联系管理员[CONTROLLER_RETURN_ERROR]', 500);
//        }
        return $class->$method();
    }

    /**
     * 错误处理
     * @param $level
     * @param $message
     * @param null $file
     * @param null $line
     * @throws AppError
     */
    public function errorHandler($level, $message, $file = null, $line = null) {
//        Console::write('******************发生错误(level:' . $level . ')******************');
//        Console::log(Color::red($message));
//        Console::log(Color::notice($file . '@' . $line));
//        Console::write('*******************************************');
        //\Scf\Core\Log::instance()->error($message, $level, $file, $line);
        //Response::interrupt($message);
        throw new AppError($message);
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
     * 处理404错误
     * @return void
     */
    #[NoReturn]
    public function handleNotFound() {
        $file = APP_SRC_PATH . 'template/common/error_404.html';
        $this->response()->status(404);
        if (file_exists($file)) {
            $this->response()->end(file_get_contents($file));
        } else {
            $this->response()->end('Page Not Found!');
        }
    }

    /**
     * 处理运行错误
     * @param $error
     * @return void
     */
    public function handleError($error) {
        if ($this->response()->isEnd()) {
            return;
        }
        $this->response()->status(200);
        if (Env::isDev()) {
            $output = [
                'errCode' => $error['code'] ?: 'SERVER_ERROR',
                'message' => !str_contains($error['error'], '@') ? $error['error'] . '@' . $error['file'] . ':' . $error['line'] : $error['error'],
                'data' => $error
            ];
            $logger = Log::instance();
            if ($logger->isDebugEnable()) {
                $output['debug'] = $logger->requestDebugInfo();
            }
            $this->response()->json($output);
        } else {
            if ($this->request()->isAjax()) {
                $this->response()->json(
                    [
                        'errCode' => $error['code'] ?? 'SERVER_ERROR',
                        'message' => '系统繁忙,请稍后重试',
                        'data' => $error['error'] ?? '未知错误'
                    ]
                );
            } else {
                $file = APP_SRC_PATH . 'template/common/error.html';
                if (file_exists($file)) {
                    $this->response()->end(file_get_contents($file));
                } else {
                    $this->response()->end('SERVER ERROR!');
                }
            }
        }
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
