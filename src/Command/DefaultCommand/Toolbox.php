<?php

namespace Scf\Command\DefaultCommand;

use ErrorException;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Cache\Redis;
use Scf\Mode\Cli\App;
use Scf\Rpc\Updater;
use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Handler\ArCreater;
use Scf\Command\Handler\NodeManager;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Server\Core;
use Scf\Util\Auth;
use Scf\Util\File;
use Swoole\ExitException;
use Symfony\Component\Yaml\Yaml;
use function Swoole\Coroutine\run;

class Toolbox implements CommandInterface {
    private Log $logger;

    public function commandName(): string {
        return 'toolbox';
    }

    public function desc(): string {
        return '控制台工具箱';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('cli', '控制台应用');
        $commandHelp->addAction('rpc', 'RPC服务更新');
        $commandHelp->addAction('ar', '活动记录对象创建');
        $commandHelp->addAction('info', '服务器环境信息');
        $commandHelp->addAction('trans', '字符串翻译&转换');
        $commandHelp->addAction('logs', '日志查看');
        $commandHelp->addAction('nodes', '服务器节点管理');
        $commandHelp->addAction('config', '配置文件转换');

        $commandHelp->addActionOpt('-app', '应用目录');
        return $commandHelp;
    }

    public function rpc(): void {
        if (APP_RUN_ENV !== 'dev') {
            Console::error('此功能只能在测试环境使用');
            exit();
        }
        $updater = new Updater();
        $updater->run();
    }

    public function config(): void {
        if (!App::isDevEnv()) {
            Console::error('此功能只能在测试环境使用');
            return;
        }
        $serverFile = App::src() . '/config/server';
        if (file_exists($serverFile . '.php')) {
            $content = require $serverFile . '.php';
            File::write($serverFile . '.yml', Yaml::dump($content, 10));
            unlink($serverFile . '.php');
            Console::success("server.php 已转换");
        }
        $configFile = App::src() . '/config/app';
        if (file_exists($configFile . '.php')) {
            $content = require $configFile . '.php';
            File::write($configFile . '.yml', Yaml::dump($content, 10));
            unlink($configFile . '.php');
            Console::success("app.php 已转换");
        }
        $envConfig = App::src() . '/config/app_' . strtolower(APP_RUN_ENV);
        if (file_exists($envConfig . '.php')) {
            $content = require $envConfig . '.php';
            File::write($envConfig . '.yml', Yaml::dump($content, 10));
            unlink($envConfig . '.php');
            Console::success("app_" . strtolower(APP_RUN_ENV) . ".php 已转换");
        }
    }

    public function cli(): void {
        run(function () {
            try {
                $app = new App();
//                Console::line();
//                Console::write("当前运行环境:" . (Lifetime::isDevEnv() ? '开发环境' : '生产环境'));
//                Console::line();
                $app->ready(Manager::instance()->getArg(1) ?: 0);
            } catch (ExitException) {
                \Swoole\Event::exit();
            }
            \Swoole\Event::exit();
        });
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            $action == 'ar' and define('APP_RUN_ENV', 'dev');
            Core::initialize();
            return $this->$action();
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    /**
     * @throws ErrorException
     */
    public function nodes(): void {
        $nodeManager = new NodeManager();
        $nodeManager->run();
    }

    public function ar(): void {
        run(function () {
            try {
                $arCreater = new ArCreater();
                $arCreater->run();
            } catch (ExitException) {
                \Swoole\Event::exit();
            }
            \Swoole\Event::exit();
//            Coroutine::defer(function () {
//                echo "Coroutine End\n";
//            });
        });
    }

    public function logs(): void {
        $this->logger = new Log();
        run(function () {
            $this->logger();
        });
    }

    public function trans(): void {
        Console::write("请输入内容\n时间戳转Datetime:时间戳|t2d\nDatetime转时间戳:Datetime|d2t\n获取当前外网IP:ip\n字符串加密转换:内容|encode\n字符串解密:内容|decode\n翻译:直接输入文本内容");
        $this->transformation();
    }


    public function info(): void {
        run(function () {
            $data = date('Y-m-d H:i:s');
            $info = [
                ['name' => 'APPID', 'value' => APP_ID],
                ['name' => 'PHP版本', 'value' => PHP_VERSION],
                ['name' => 'Swoole版本', 'value' => swoole_version()],
                ['name' => '当前时间', 'value' => $data],
                ['name' => 'Redis扩展', 'value' => (class_exists('Redis') ? '已安装' : '未安装')],
            ];
            $ips = swoole_get_local_ip();
            foreach ($ips as $k => $ip) {
                $info[] = ['name' => 'IP@' . $k, 'value' => $ip];
            }
            $cacheKey = '_CACHE_DEBUG_KEY';
            if (class_exists('Redis')) {
                $connection = Redis::pool();
                $info[] = ['name' => 'Redis服务器', 'value' => $connection->getConfig('servers')['main']['host']];
                $cacheData = $connection->get($cacheKey);
                $info[] = ['name' => 'Redis缓存时间', 'value' => $cacheData];
                if (!$cacheData) {
                    $connection->set($cacheKey, $data, 86400);
                }
            }
            Console::line();
            foreach ($info as $item) {
                Console::write($item['name'] . "：" . $item['value']);
            }
            Console::line();
        });
    }


    protected function logger() {
        $count = $this->logger->count();
        $cmd = Console::input("请选择要查看的日志类型,类型 日期(缺省为今天),示范:1 2022-01-13\r\n【1】错误日志(error)：" . ($count['error'] ?? 0) . " 条\r\n【2】信息日志(info)：" . ($count['info'] ?? 0) . " 条\r\n-------------------");
        if ($cmd) {
            if ($cmd == 'quit') {
                Console::exit();
            }
            $arr = explode(" ", $cmd);
            $type = $arr[0] == 1 ? 'error' : ($arr[0] == 2 ? 'info' : $arr[0]);

            $day = $arr[1] ?? date('Y-m-d');
            $logs = $this->logger->get($type, $day);
            if (!$logs) {
                Console::write("-------------------\r\n暂无日志\r\n-------------------");
            } else {
                Console::write("--------------------------------------累计" . count($logs) . "条日志--------------------------------------");
                foreach ($logs as $log) {
                    Console::write("【" . $log['date'] . "】" . ($type == 'error' ? Color::red($log['message']) : Color::blue($log['message'])) . ' @' . $log['file']);
                }
                Console::line();
            }
        }
        return $this->logger();
    }

    protected function transformation() {
        $input = fgets(STDIN);
        if (!$input) {
            Console::write('请输入内容');
            return $this->transformation();
        }
        $arr = explode('|', $input);
        if (count($arr) == 1) {
            $input = rtrim($input);
            if (is_numeric($input) && strlen($input) == 10) {
                Console::write("识别到时间戳\n" . $input . "=>" . date('Y-m-d H:i:s', $input));
            } elseif ($datetime = strtotime($input)) {
                Console::write("识别到日期\n" . $input . "=>" . $datetime);
            } elseif ($input == 'ip') {
                $ip = file_get_contents('https://api.dmool.com/ws/system/myip/');
                Console::write("当前外网IP:" . $ip);
            } else {
                $gateway = 'https://fanyi.youdao.com/translate?doctype=json&type=AUTO&i=' . $input;
                $result = file_get_contents($gateway, 10);
                $result = json_decode($result, true);
                //print_r($result);
                Console::write($input . " " . $result['type'] . " => 翻译结果:\n" . $result['translateResult'][0][0]['tgt']);
            }
            return $this->transformation();
        }
        $cmd = $arr[1];
        $content = $arr[0];
        switch (trim($cmd)) {
            case 't2d':
                Console::write($content . "=>" . date('Y-m-d H:i:s', $content));
                break;
            case 'd2t':
                Console::write($content . "=>" . strtotime($content));
                break;
            case 'encode':
                Console::write($content . "=>" . Auth::encode($content));
                break;
            case 'decode':
                Console::write($content . "=>" . Auth::decode($content));
                break;
            default:
                Console::write("未知指令:" . $cmd);
                break;
        }
        return $this->transformation();
    }
}