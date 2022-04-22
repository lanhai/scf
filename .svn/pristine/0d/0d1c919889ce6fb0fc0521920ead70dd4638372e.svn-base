<?php

namespace Scf\Command\DefaultCommand;

use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Handler\AppCreater;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Server\Core;
use Scf\Util\Auth;
use Swoole\Coroutine;
use Swoole\Runtime;
use function Co\run;


class Install implements CommandInterface {
    public function commandName(): string {
        return 'install';
    }

    public function desc(): string {
        return '应用安装&拉取';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('create', '创建新应用');
        $commandHelp->addAction('pull', '从云端拉取应用');
        $commandHelp->addAction('list', '查看本地应用');

        $commandHelp->addActionOpt('-appid', '应用id');
        $commandHelp->addActionOpt('-path', '创建路径');
        $commandHelp->addActionOpt('-key', '秘钥');
        $commandHelp->addActionOpt('-server', '版本更新引导文件');
        return $commandHelp;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            return $this->$action();
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    /**
     * 创建一个全新的应用
     * @return void
     */
    public function create() {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $creater = AppCreater::factory();
        $appid = $options['appid'] ?? Console::input('请输入appid,缺省值:scfapp-YmdHis');
        $creater->appid = $appid ?: 'scfapp-' . date('YmdHis');
        $path = $options['path'] ?? Console::input('请输入应用路径(位于apps下的相对路径),缺省值:main');
        $creater->app_path = $path ?: 'main';
        if (is_dir(SCF_APPS_ROOT . $creater->app_path)) {
            Console::write(Color::red('应用文件夹已存在:' . SCF_APPS_ROOT . $creater->app_path));
            exit();
        }
        $defaultKey = md5($creater->appid);
        $key = $options['key'] ?? Console::input('请输入应用秘钥,缺省值:' . $defaultKey);
        $creater->app_auth_key = $key ?: $defaultKey;
        $creater->public_path = 'public';
        $creater->version = '1.0.0';
        $creater->updated = date('Y-m-d H:i:s');
        $creater->execute();
    }

    /**
     * 本机应用列表
     * @return void
     */
    public function list() {
        $apps = App::all();
        Console::line();
        if (!$apps) {
            Console::write(Color::red('暂无应用'));
        } else {
            foreach ($apps as $app) {
                Console::write($app['appid'] . ' v' . $app['version']);
            }
        }
        Console::line();
    }

    /**
     * 拉取发行的版本
     * @return void
     */
    public function pull() {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $creater = AppCreater::factory();
        $creater->public_path = 'public';
        $path = $options['path'] ?? Console::input('请输入应用路径(位于apps下的相对路径),缺省值:main');
        $creater->app_path = $path ?: 'main';
        if (is_dir(SCF_APPS_ROOT . $creater->app_path)) {
            Console::write(Color::green('应用文件夹已存在:' . SCF_APPS_ROOT . $creater->app_path));
            //exit();
        }
        $key = $options['key'] ?? '';
        $server = $options['server'] ?? '';
        if ($key && $server) {
            goto start;
        }
        $key = Console::input('请输入发行秘钥');
        $decode = Auth::decode($key, 'scfapp');
        if (!$decode) {
            Console::line(Color::red('错误的发行秘钥'));
            exit();
        }
        $config = JsonHelper::recover($decode);
        if (empty($config['key']) || empty($config['url'])) {
            Console::line(Color::red('错误的发行秘钥'));
            exit();
        }
        $creater->app_auth_key = $config['key'];
        $server = $config['url'];
        start:
        $creater->update_server = $server;
        run(function () use ($server, $creater) {
            $client = Http::create($server . '?time=' . time());
            $result = $client->get();
            if ($result->hasError()) {
                Console::log('获取云端版本号失败:' . Color::red($result->getMessage()));
                exit();
            }
            $remote = $result->getData();
            $appVersion = $remote['app'] ?? '';
//        $updateServerVersion = file_get_contents($server . '?time=' . time());
//        if (!$updateServerVersion) {
//            Console::log('获取云端版本号失败');
//            exit();
//        }

//            $remote = JsonHelper::recover($updateServerVersion);
//            $appVersion = $remote['app'];
            if ($appVersion) {
                $creater->version = '0.0.0';
                $creater->appid = $appVersion[0]['appid'];
            } else {
                Console::log('获取云端版本号失败');
                exit();
            }
            $creater->updated = date('Y-m-d H:i:s');
            $creater->execute();
        });

    }
}