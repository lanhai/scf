<?php

namespace Scf\Mode\Rpc;

use Scf\Core\Config;
use Scf\Rpc\Manager;
use Scf\Util\Dir;

class App extends \Scf\Core\App {
    /**
     * @var string 绑定ip
     */
    protected string $bindHost = '0.0.0.0';
    /**
     * @var int 绑定端口
     */
    protected int $bindPort = 9585;
    /**
     * @var string 本机ip地址
     */
    protected string $ip;

    protected static self $instance;

    public static function addService(): void {
        self::loadModules(MODE_RPC);
        if (!self::$_modules[MODE_RPC]) {
            return;
        }
        $serviceManager = Manager::instance();
        foreach (self::$_modules[MODE_RPC] as $conf) {
            $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
            if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
                $cls = "\\" . APP_TOP_NAMESPACE . "\\Service\\{$conf['name']}\\service";
            } else {
                $cls = "\\" . APP_TOP_NAMESPACE . "\\{$conf['name']}\\service";
            }
            if (class_exists($cls)) {
                /** @var Service $service */
                $service = new $cls();
                //创建module
                $controllers = self::getControllers($conf['name']);
                if ($controllers) {
                    foreach ($controllers as $controller) {
                        $service->addModule(new $controller);
                    }
                }
                $serviceManager->addService($service);
            }
        }
    }

    private static function getControllers($service): array {
        //注册加载器
        $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
        if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
            $entryScripts = Dir::scan(self::src() . 'lib/Service/' . $service, 1);
        } else {
            $entryScripts = Dir::scan(self::src() . 'lib/' . $service . '/Service', 1);
        }
        $modules = [];
        if ($entryScripts) {
            foreach ($entryScripts as $file) {
                $arr = explode('/', $file);
                $name = str_replace(".php", "", array_pop($arr));
                $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
                if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
                    $cls = "\\" . APP_TOP_NAMESPACE . "\\Service\\{$service}\\" . $name;
                } else {
                    $cls = "\\" . APP_TOP_NAMESPACE . "\\{$service}\\Service\\" . $name;
                }
                if (class_exists($cls)) {
                    $modules[] = $cls;
                }
            }
        }
        return $modules;
    }
} 