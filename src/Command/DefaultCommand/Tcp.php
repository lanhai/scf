<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Config;
use Scf\Core\Env;
use Scf\Server\Tcp as TcpServer;

/** 将选定的 SCF 应用挂载到独立的 TCP 服务。 */
final class Tcp implements CommandInterface {

    public function commandName(): string {
        return 'tcp';
    }

    public function desc(): string {
        return 'TCP 应用服务';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('start', '启动 TCP 监听');
        $commandHelp->addActionOpt('-app', '应用目录名');
        $commandHelp->addActionOpt('-port', '覆盖 TCP 监听端口');
        return $commandHelp;
    }

    public function exec(): ?string {
        $manager = Manager::instance();
        if ($manager->getArg(0) !== 'start') {
            return $manager->displayCommandHelp($this->commandName());
        }
        Env::initialize(MODE_TCP);
        $config = Config::server()['tcp'] ?? [];
        if ($manager->issetOpt('port')) {
            $config = array_replace(is_array($config) ? $config : [], ['port' => $manager->getOpt('port')]);
        }
        (new TcpServer())->start($config);
        return null;
    }
}
