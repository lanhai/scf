<?php

namespace Scf\Mode\Rpc;

use Scf\Command\Color;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Rpc\Manager;
use Scf\Util\Dir;

class App {
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
        \Scf\Core\App::loadModules(MODE_RPC);
        $modules = \Scf\Core\App::getModules(MODE_RPC);
        if (!$modules) {
            return;
        }
        $serviceManager = Manager::instance();
        foreach ($modules as $conf) {
            $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
            if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
                $cls = \Scf\Core\App::buildControllerPath('Rpc', 'service');
            } else {
                $cls = \Scf\Core\App::buildControllerPath($conf['name'], 'service');
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
                    Console::log("【Server】RPC服务" . Color::notice($service->serviceName()) . Color::success('注册成功'));
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
            $serviceDir =  \Scf\Core\App::buildPath( \Scf\Core\App::src(), 'lib', 'Rpc');
            is_dir($serviceDir) and $entryScripts = Dir::scan($serviceDir, 1);
        } else {
            $entryScripts = Dir::scan( \Scf\Core\App::buildPath( \Scf\Core\App::src(), 'lib', $service, 'Service'), 1);
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
                    $cls =  \Scf\Core\App::buildControllerPath('Rpc', $name);
                } else {
                    $cls =  \Scf\Core\App::buildControllerPath($service, 'Service', $name);
                }
                if (class_exists($cls)) {
                    $modules[] = $cls;
                }
            }
        }
        return $modules;
    }
} 