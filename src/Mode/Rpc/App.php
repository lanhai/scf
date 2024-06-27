<?php

namespace Scf\Mode\Rpc;

use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Rpc\Manager;
use Scf\Rpc\NodeManager\RedisManager;
use Scf\Rpc\Server\ServiceNode;
use Scf\Util\Dir;
use Scf\Util\Random;

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

    public static function addService(int $port = 9585, int $workerId = 0): void {
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
                if ($workerId == 0) {
                    Console::success("RPC服务【{$service->serviceName()}】注册成功");
//                    //注册服务
//                    $nodeId = md5(SERVER_HOST . $port);
//                    $serviceNode = new ServiceNode();
//                    $serviceNode->setService($service->serviceName());
//                    $serviceNode->setNodeId($nodeId);
//                    $serviceNode->setIp(SERVER_HOST);
//                    $serviceNode->setPort($port);
//                    $serverName = Config::get('rpc')['server']['manager']['connection'] ?? 'main';
//                    $serviceCenter = new RedisManager($serverName);
//                    $serviceCenter->alive($serviceNode);
//                    var_dump($serviceNode->toArray());
                }
            }
        }
    }

    private static function getControllers($service): array {
        //注册加载器
        $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
        $entryScripts = [];
        if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
            is_dir(self::src() . 'lib/Controller/' . $service) and $entryScripts = Dir::scan(self::src() . 'lib/Controller/' . $service, 1);
            is_dir(self::src() . 'lib/Service/' . $service) and $entryScripts = Dir::scan(self::src() . 'lib/Service/' . $service, 1);
        } else {
            $entryScripts = Dir::scan(self::src() . 'lib/' . $service . '/Service', 1);
        }
        $modules = [];
        if ($entryScripts) {
            foreach ($entryScripts as $file) {
                $arr = explode('/', $file);
                $name = str_replace(".php", "", array_pop($arr));
                if ($name == 'service') {
                    continue;
                }
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