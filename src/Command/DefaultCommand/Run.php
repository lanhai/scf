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
use Scf\Server\Core;
use Scf\Util\Auth;
use Scf\Util\Random;
use Scf\Util\Sn;


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
        Core::initialize();
        if ($action && file_exists(APP_PATH . $action . ".php") && $action != 'help') {
            require APP_PATH . $action . ".php";
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }
}