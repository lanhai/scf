<?php

namespace Scf\Mode\Cli;

use Scf\Command\Color;
use Scf\Command\Manager;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Helper\StringHelper;

class App {
    /**
     * @var array 应用列表
     */
    protected array $_apps = [];

    public function __construct() {
        $modules = \Scf\Core\App::getModules(MODE_CLI);
        if (!$modules) {
            Console::error('未查找到任何可运行的模块');
            die();
        }
        foreach ($modules as $module) {
            if (!isset($module['apps'])) {
                continue;
            }
            $this->_apps = $this->_apps ? [...$this->_apps, ...$module['apps']] : $module['apps'];
        }
    }

    public function ready($cmdNum = 0): float|int|string {
        $options = [];
        foreach ($this->_apps as $app) {
            $options[] = ($app['name'] ?? $app);
        }
        $params = Manager::instance()->getOpts();
        $controller = $params['controller'] ?? '';
        if ($controller && in_array($controller, $options)) {
            //查询控制器在options数组里是第几个
            $cmdNum = array_search($controller, $options) + 1;
        }
        if (!$cmdNum) {
            $cmdNum = Console::select($options, 1, 1, "请选择要执行的操作,当前运行环境:" . (\Scf\Core\App::isDevEnv() ? Color::green('开发环境') : Color::yellow('生产环境')));
        }
        $appNum = $cmdNum - 1;
        if (!isset($this->_apps[$appNum])) {
            Console::line();
            Console::write("对应的指令不存在,请重新输入");
            Console::line();
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
        spl_autoload_register(['\Scf\Core\App', 'autoload'], true);
        $app = $this->_apps[$appNum];
        $module = $app['module'];
        $controller = $app['controller'];
        $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
        if ($moduleStyle == APP_MODULE_STYLE_LARGE) {
            $ctrlClass = \Scf\Core\App::buildControllerPath(StringHelper::lower2camel($module), 'Controller', $controller);
        } else {
            $ctrlClass = \Scf\Core\App::buildControllerPath('Cli', $controller);
        }
        if (!class_exists($ctrlClass)) {
            Console::log('控制器不存在:' . $ctrlClass);
        }
        $params = Manager::instance()->getOpts();
        $selectNum = $params['select'] ?? 0;
        $app = new $ctrlClass();
        if ($selectNum) {
            $app->run($selectNum);
        } else {
            $app->run();
        }
        return true;
    }

} 