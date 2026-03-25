<?php

namespace Scf\Mode\Cli;

use Scf\Command\Color;
use Scf\Command\Manager;
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
        if (!$controller && is_string($cmdNum) && !is_numeric($cmdNum)) {
            $controller = trim($cmdNum);
            $cmdNum = 0;
        }
        if ($controller && ($directController = $this->resolveDirectControllerClass($controller))) {
            return $this->runController($directController);
        }
        if ($controller) {
            $appNum = $this->matchAppNum($controller);
            if (!is_null($appNum)) {
                $cmdNum = $appNum + 1;
            }
        }
        if (is_string($cmdNum) && is_numeric($cmdNum)) {
            $cmdNum = (int)$cmdNum;
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

    protected function matchAppNum(string $controller): ?int {
        $controller = $this->normalizeControllerName($controller);
        foreach ($this->_apps as $index => $app) {
            $appController = $app['controller'] ?? '';
            if ($appController && $this->normalizeControllerName($appController) === $controller) {
                return $index;
            }
            $appName = $app['name'] ?? '';
            if ($appName && strtolower($appName) === strtolower(trim($controller))) {
                return $index;
            }
        }
        return null;
    }

    protected function normalizeControllerName(string $controller): string {
        $controller = trim($controller);
        if (str_contains($controller, '\\')) {
            $controller = substr($controller, strrpos($controller, '\\') + 1);
        }
        $controller = str_replace('-', '_', $controller);
        if (!preg_match('/[A-Z]/', $controller)) {
            $controller = StringHelper::lower2camel(strtolower($controller));
        }
        return strtolower($controller);
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
        $moduleStyle = APP_MODULE_STYLE;
        if ($moduleStyle == APP_MODULE_STYLE_MULTI) {
            $ctrlClass = \Scf\Core\App::buildControllerPath(StringHelper::lower2camel($module), 'Controller', $controller);
        } else {
            $ctrlClass = \Scf\Core\App::buildControllerPath('Cli', $controller);
        }
        return $this->runController($ctrlClass);
    }

    protected function runController(string $ctrlClass): bool {
        if (!class_exists($ctrlClass)) {
            Console::log('控制器不存在:' . $ctrlClass);
            return false;
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

    protected function resolveDirectControllerClass(string $controller): ?string {
        spl_autoload_register(['\Scf\Core\App', 'autoload'], true);
        $normalizedController = $this->normalizeControllerClassName($controller);
        $moduleStyle = APP_MODULE_STYLE;
        if ($moduleStyle == APP_MODULE_STYLE_MULTI) {
            $ctrlClass = \Scf\Core\App::buildControllerPath(StringHelper::lower2camel('Cli'), 'Controller', $normalizedController);
        } else {
            $ctrlClass = \Scf\Core\App::buildControllerPath('Cli', $normalizedController);
        }
        return class_exists($ctrlClass) ? $ctrlClass : null;
    }

    protected function normalizeControllerClassName(string $controller): string {
        $controller = trim($controller);
        if (str_contains($controller, '\\')) {
            $controller = substr($controller, strrpos($controller, '\\') + 1);
        }
        $controller = str_replace('-', '_', strtolower($controller));
        return StringHelper::lower2camel($controller);
    }

} 
