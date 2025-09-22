<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Env;


class Run implements CommandInterface {
    public function commandName(): string {
        return 'run';
    }

    public function desc(): string {
        return '运行脚本程序';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addActionOpt('{path}', '脚本路径');
        return $commandHelp;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        Env::initialize();
        if ($action && file_exists(APP_PATH . '/' . $action . ".php") && $action != 'help') {
            require APP_PATH . $action . ".php";
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }
}