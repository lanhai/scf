<?php

namespace Scf\Command\DefaultCommand;

use Scf\Core\App;
use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Server\Core;
use Scf\Server\Native as Server;

class Native implements CommandInterface {

    public function commandName(): string {
        return 'native';
    }

    public function desc(): string {
        return '桌面应用';
    }

    public function help(Help $commandHelp): Help {
        $apps = App::all();
        $names = [];
        foreach ($apps as $app) {
            $names[] = $app['app_path'];
        }
        $commandHelp->addAction('start', '启动开发服务器');
        $commandHelp->addAction('build', '打包应用');
        $commandHelp->addActionOpt('-app', '启动的应用文件夹名【' . implode('|', $names) . '】,默认:app');
        return $commandHelp;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            Core::initialize(MODE_NATIVE);
            return $this->$action();
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    protected function start(): void {
        Server::instance()->start(SERVER_PORT);
    }
}