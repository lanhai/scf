<?php
/**
 * 连客云集成框架
 * User: linkcloud
 * Date: 14-7-4
 * Time: 上午11:26
 */

namespace HuiYun\Mode\Rpc;

use HuiYun\Component\Debug;
use HuiYun\Component\Error;
use HuiYun\Exception\NotFoundException;

abstract class Module extends \HuiYun\Core\Module {

    public function run() {
        // 关闭错误输出
        Error::disable();
        // 关闭调试工具栏
        Debug::disable();
        $router = App::instance()->getRouter();
        $ctlClass = $this->getNamespace() . 'Service\\' . $router->getController();
         if (!class_exists($ctlClass)) {
            send_http_status(404);
            if (!empty($_SERVER['HTTP_USER_AGENT'])) {

                exit('Illegal Request:' . $ctlClass);
            }
            exit();
            //throw new NotFoundException($router->toString());
        }
        new $ctlClass;
    }

    protected function _init() {
        parent::_init();
        if (!IS_WEB) {
            echo "Bad Command.\n";
            exit;
        }
    }
}