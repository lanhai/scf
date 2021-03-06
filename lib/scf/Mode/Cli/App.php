<?php

namespace Scf\Mode\Cli;

use Scf\Core\Console;
use Scf\Helper\StringHelper;

class App extends \Scf\Core\App {
    /**
     * @var array 应用列表
     */
    protected array $_apps = [];

    public function __construct() {
        self::$_modules = self::loadModules('cli');
        if (!self::$_modules) {
            Console::write('未查找到任何可运行的模块');
            die();
        }
        foreach (self::$_modules as $module) {
            $this->_apps = $this->_apps ? [$this->_apps, ...$module['apps']] : $module['apps'];
        }
    }

    public function ready(): float|int|string {
        foreach ($this->_apps as $k => $app) {
            Console::write(($k + 1) . ':' . ($app['name'] ?? $app));
        }
        Console::write("----------------------------------------------------");
        $cmdNum = trim(fgets(STDIN));
        if (!is_numeric($cmdNum)) {
            $cmdNum = strtolower($cmdNum);
            if ($cmdNum == 'quit' || $cmdNum == 'q') {
                Console::write("----------------------------------------------------\n欢迎再次使用\n----------------------------------------------------");
                exit;
            }
            Console::write("----------------------------------------------------\n输入有误,请输入正确的指令编号\n----------------------------------------------------");
            return $this->ready();
        }
        $appNum = $cmdNum - 1;
        if (!isset($this->_apps[$appNum])) {
            Console::write("----------------------------------------------------\n对应的指令不存在,请重新输入\n----------------------------------------------------");
            return $this->ready();
        }
        return $this->run($appNum);
    }

    /**
     * 运行
     * @param $appNum
     * @return bool
     */
    protected function run($appNum): bool {
        spl_autoload_register([__CLASS__, 'autoload'], true);
        $app = $this->_apps[$appNum];
        $module = $app['module'];
        $controller = $app['controller'];
        $ctrlClass = APP_TOP_NAMESPACE . '\\' .StringHelper::lower2camel($module) . '\\Controller\\' . $controller;
        if (!class_exists($ctrlClass)) {
            Console::log('控制器不存在:' . $ctrlClass);
        }
        $app = new $ctrlClass();
        $app->run();
        return true;
    }

} 