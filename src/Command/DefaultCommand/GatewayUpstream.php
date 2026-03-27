<?php

namespace Scf\Command\DefaultCommand;

require_once dirname(__DIR__, 2) . '/Server/Proxy/CliBootstrap.php';

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Server\Proxy\CliBootstrap;

class GatewayUpstream implements CommandInterface {

    public function commandName(): string {
        return 'gateway_upstream';
    }

    public function desc(): string {
        return '代理业务实例';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('start', '启动代理托管的业务实例');
        $commandHelp->addActionOpt('-app', '应用目录名');
        $commandHelp->addActionOpt('-env', '运行环境, 例如 dev');
        $commandHelp->addActionOpt('-port', '业务实例端口');
        $commandHelp->addActionOpt('-rport', '业务实例内部 RPC 端口, 由 Gateway 分配并转发');
        $commandHelp->addActionOpt('-role', '节点角色');
        return $commandHelp;
    }

    public function exec(): ?string {
        $action = (string)(Manager::instance()->getArg(0) ?: 'start');
        if ($action !== 'start') {
            return Manager::instance()->displayCommandHelp($this->commandName());
        }
        CliBootstrap::bootedRunUpstream();
        return null;
    }
}
