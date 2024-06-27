<?php

namespace Scf\Rpc;

use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Mode\Rpc\Document;
use Scf\Command\Color;
use Swoole\Event;

class Updater {

    public function run(): void {
        $rpcConfig = Config::get('rpc');
        if (!$rpcConfig) {
            Console::error("未查询到RPC服务配置");
            exit();
        }
        $servers = $rpcConfig['client']['servers'] ?? [];
        $services = $rpcConfig['client']['services'] ?? [];
        if (!$services) {
            Console::error("未查询到RPC客户端服务配置");
            exit();
        }
        Console::write('----------------------------------------------------------------------------------------------------');
        Console::write('序号  命名空间                    服务器                远程服务        appid    远程版本     本地版本');
        Console::write('----------------------------------------------------------------------------------------------------');
        $i = 1;
        $list = [];
        foreach ($services as $name => $service) {
            $server = $servers[$service['server']];
            $remoteVersion = null;
            //查询远程版本
            go(function () use (&$remoteVersion, $server, $service) {
                $rpc = new \Scf\Mode\Rpc\Client($service, $server);
                $docment = $rpc->__document__();
                if ($docment->hasError()) {
                    $remoteVersion = $docment->getMessage();
                } else {
                    $remoteVersion = $docment->getData('version') ?: "unknow";
                }
            });
            Event::wait();
            if (class_exists($name)) {
                try {
                    $local = Document::instance()->local($name);
                    $versionLocal = $local['version'];
                } catch (\Throwable $exception) {
                    $versionLocal = $exception->getMessage();
                }
            } else {
                $versionLocal = "未安装";
            }
            Console::write("#{$i}    " . $name . "         {$service['server']}    {$service['service']}        {$service['appid']}    【" . $remoteVersion . "】      【" . ($remoteVersion == $versionLocal ? Color::green($versionLocal) : Color::red($versionLocal)) . "】");
            $i++;
            $service['namespace'] = $name;
            $list[] = [
                'service' => $service,
                'server' => $server
            ];
        }
        Console::write('----------------------------------------------------------------------------------------------------');
        $choice = Console::input("请输入要更新的服务序号:", false);
        if (!$choice || !is_numeric($choice)) {
            $this->run();
        }
        $selectedService = $list[$choice - 1];
        if (!$this->write($selectedService)) {
            $input = Console::input("【" . $selectedService['service']['namespace'] . "】 服务更新失败,请选择下一步操作\n1:更新其它服务\n2:退出");
            if ($input == 1) {
                $this->run();
            }
            exit();
        }
        Console::write('----------------------------------------------------------------------------------------------------');
        Console::success("【" . $selectedService['service']['namespace'] . "】 服务更新成功");
        $this->run();
    }

    protected function write($service) {
        $document = [];
        go(function () use ($service, &$document) {
            $rpc = new \Scf\Mode\Rpc\Client($service['service'], $service['server']);
            $result = $rpc->__document__();
            if ($result->hasError()) {
                Console::error($result->getMessage());
                exit();
            }
            $document = $result->getData();

        });
        Event::wait();
        if (!$document['methods']) {
            $input = Console::input("服务端未定义可用服务,请选择下一步操作\n1:更新其它服务\n2:退出");
            if ($input == 1) {
                $this->run();
            }
            exit();
        }
        $paths = explode("\\", $service['service']['namespace']);
        $appMode = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
        if ($appMode == APP_MODULE_STYLE_LARGE) {
            $fileDir = APP_LIB_PATH . $paths[1] . '/' . $paths[2];
            $clientFile = $fileDir . '/' . $paths[3] . '.php';
        } else {
            $fileDir = APP_LIB_PATH . '/' . $paths[1];
            $clientFile = $fileDir . '/' . $paths[2] . '.php';
        }
        if (!is_dir($fileDir) && !mkdir($fileDir, 0775, true)) {
            Console::error("创建文件夹失败!" . $fileDir);
            exit();
        }
        $bodyContent = "";
        $replaceMent = [
            'integer' => 'int',
            'date' => 'string'
        ];
        foreach ($document['methods'] as $method) {
            $requestType = $method['httpRequest'] ?? "get";
            $bodyContent .= '    /**';
            $bodyContent .= PHP_EOL . '     * ' . $method['desc'];
            $agvs = [];
            if ($method['params']) {
                foreach ($method['params'] as $param) {
                    $param['type'] = strtolower($param['type']);
                    foreach ($replaceMent as $key => $val) {
                        $param['type'] = str_replace($key, $val, $param['type']);
                    }
                    $agv = strtolower($param['type']) . ' $' . $param['name'];
                    if (!$param['require']) {
                        if (strtolower($param['type']) == 'string') {
                            $agv .= ' = "' . $param['default_value'] . '"';
                        } elseif (!empty($param['default_value'])) {
                            $agv .= ' = ' . $param['default_value'];
                        }
                    }
                    $agvs[] = $agv;
                    $bodyContent .= PHP_EOL . '     * @param $' . $param['name'] . ' ' . strtolower($param['type']) . ' ' . $param['desc'];
                }
            }
            if (!isset($method['return']) && isset($method['output'])) {
                $method['return'] = $method['output'];
            }
            $bodyContent .= PHP_EOL . '     * @return ' . $method['return']['type'];
            if ($method['return']['data']['type'] != 'array' && isset($method['return']['data']['fields'])) {
                foreach ($method['return']['data']['fields'] as $field) {
                    $bodyContent .= PHP_EOL . '     * @data ' . $field['key'] . ' ' . $field['type'] . ' ' . $field['desc'] ?? $field['key'];
                }
            } else {
                $bodyContent .= PHP_EOL . '     * @data ' . $method['return']['data']['type'] . ' ' . $method['return']['data']['desc'];
            }
            $bodyContent .= PHP_EOL . '     */';
            $bodyContent .= PHP_EOL . '    public function ' . $method['name'] . '(' . implode(', ', $agvs) . '): ' . $method['return']['type'] . ' {';
            $bodyContent .= PHP_EOL . '        return $this->exec(...func_get_args());';
            $bodyContent .= PHP_EOL . '    }' . PHP_EOL . PHP_EOL;
        }
        $now = date('Y-m-d H:i:s');
        $namespaceArr = $paths;
        unset($namespaceArr[count($namespaceArr) - 1]);
        $namespace = implode("\\", $namespaceArr);
        $className = array_pop($paths);
        $fileContent = <<<EOF
<?php
namespace {$namespace};

use Scf\Mode\Rpc\Client;
use Scf\Core\Result;

/**
 * {$document['desc']}
 * @version {$document['version']}
 * @updated {$document['updated']}
 * @created {$now}
 * @package Client\Client
 */
class {$className} extends Client {

    
{$bodyContent}
}
EOF;
        if (!$this->writeFile($clientFile, $fileContent)) {
            return false;
        }
        return true;
    }

    /**
     * 写入文件
     * @param $file
     * @param $content
     * @return bool
     */
    protected function writeFile($file, $content): bool {
        try {
            $fp = fopen($file, "w");
        } catch (\Exception $err) {
            return false;
        }
        if ($fp) {
            fwrite($fp, $content);
            fclose($fp);
            return true;
        }
        return false;
    }
}