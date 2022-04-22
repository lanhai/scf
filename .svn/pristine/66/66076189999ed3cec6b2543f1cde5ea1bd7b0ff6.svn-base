<?php
/**
 * 连客云开发框架
 * User: linkcloud
 * Date: 14-9-24
 * Time: 21:58
 */

namespace HuiYun\Mode\Rpc;


use HuiYun\Component\Error;
use HuiYun\Component\Log;
use HuiYun\Exception\NotFoundException;
use HuiYun\Mode\Web\View;

class App extends \HuiYun\Core\App {

    public function errorHandler($level, $message, $file = null, $line = null) {
        Log::instance()->error($message, $level, $file, $line);
        send_http_status(500);
        exit('Server Error:' . $message . '@' . $file . ' line:' . $line);
    }

    /**
     * 异常处理
     * @param \Exception $exception
     */
    public function exceptionHandler(\Exception $exception) {
        Log::instance()->error($exception->getMessage(), $exception->getCode(), $exception->getFile(), $exception->getLine());
        send_http_status(500);
        exit('Server Error(exception):' . $exception->getMessage() . '@' . $exception->getFile() . ' line:' . $exception->getLine());
    }

    /**
     * 处理404错误
     * @param NotFoundException $exception
     */
    public function handleNotFound(NotFoundException $exception = null) {
        send_http_status(404);
        parent::handleNotFound($exception);
    }

    public function shutdownHandler() {
        parent::shutdownHandler();
    }
} 