<?php

namespace Scf\Command\DefaultCommand;

use Scf\App\Installer;
use Scf\Client\Http;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Helper\JsonHelper;
use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Util\Auth;
use Scf\Util\Random;
use Scf\Util\Sn;
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
    public function create(): void {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $path = $options['path'] ?? Console::input('请输入应用路径(位于apps下的相对路径),缺省值:app');
        $installer = Installer::mount($path, create: true);
        $installer->app_path = $path ?: 'app';
        $defaultAppid = $installer->app_path . '-' . Random::character();
        $appid = $options['appid'] ?? Console::input('请输入appid,缺省值:' . $defaultAppid);
        $installer->appid = $appid ?: $defaultAppid;

        if (is_dir(SCF_APPS_ROOT. '/' . $installer->app_path)) {
            Console::log(Color::yellow('应用文件夹已存在,此操作将覆盖已存在文件,请谨慎操作:' . SCF_APPS_ROOT. '/' . $installer->app_path));
        }
        $defaultKey = Random::character(32);
        $key = $options['key'] ?? Console::input('请输入应用秘钥,缺省值:' . $defaultKey);
        $installer->app_auth_key = $key ?: $defaultKey;
        $installer->public_path = 'public';
        $installer->version = '0.0.0';
        $installer->updated = date('Y-m-d H:i:s');
        $installer->node_id = Sn::create_guid();
        if ($installer->add()) {
            Console::success("应用初创建成功");
        } else {
            Console::error("应用初创建失败");
        }
    }

    /**
     * 本机应用列表
     * @return void
     */
    public function list(): void {
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
    public function pull(): void {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $path = $options['path'] ?? Console::input('请输入应用路径(位于apps下的相对路径),缺省值:app');

        if (is_dir(SCF_APPS_ROOT. '/' . $path)) {
            Console::write(Color::green('应用文件夹已存在:' . SCF_APPS_ROOT. '/' . $path));
            //exit();
        }
        $creater = App::appoint($path);
        $creater->public_path = 'public';
        $creater->app_path = $path ?: 'app';

        $key = $options['key'] ?? '';
        $server = $options['server'] ?? '';
        if ($key && $server) {
            goto start;
        }
        $key = Console::input('请输入安装秘钥');
        $appsecret = substr($key, 0, 32);
        $installKey = substr($key, 32);
        $decode = Auth::decode($installKey, $appsecret);
        if (!$decode) {
            Console::line(Color::red('错误的安装秘钥'));
            exit();
        }
        $config = JsonHelper::recover($decode);
        if (empty($config['key']) || empty($config['server'])) {
            Console::line(Color::red('错误的安装秘钥'));
            exit();
        }
        if (time() > $config['expired']) {
            Console::line(Color::red('安装秘钥已过期'));
            exit();
        }
        $creater->app_auth_key = $config['key'];
        $creater->dashboard_password = $config['dashboard_password'];
        $server = $config['server'];
        start:
        $creater->update_server = $server;
        run(function () use ($server, &$creater) {
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
                $creater->version = $appVersion[0]['version'];;
                $creater->appid = $appVersion[0]['appid'];
            } else {
                Console::log('获取云端版本号失败');
                exit();
            }
            $creater->updated = date('Y-m-d H:i:s');
            $creater->add();
        });

    }
}