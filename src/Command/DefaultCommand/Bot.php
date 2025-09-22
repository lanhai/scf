<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Env;

class Bot implements CommandInterface {

    public function commandName(): string {
        return 'bot';
    }

    public function desc(): string {
        return '微信机器人管理';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('run', '微信机器人');
        $commandHelp->addActionOpt('-app', '应用目录');
        return $commandHelp;
    }

    public function run(): bool {

        echo 'bot';
        return true;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            Env::initialize();
            return $this->run();
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }
}