<?php

namespace Scf\Command;


use Scf\Core\Traits\Singleton;
use Scf\Command\DefaultCommand\Bot;
use Scf\Command\DefaultCommand\Build;
use Scf\Command\DefaultCommand\Install;
use Scf\Command\DefaultCommand\Server;
use Scf\Command\DefaultCommand\Toolbox;
use Scf\Command\DefaultCommand\Run;
class Runner {
    use Singleton;

    public function __construct() {
        Manager::instance()->addCommand(new Server());
        Manager::instance()->addCommand(new Install());
        Manager::instance()->addCommand(new Build());
        Manager::instance()->addCommand(new Toolbox());
        Manager::instance()->addCommand(new Bot());
        Manager::instance()->addCommand(new Run());
    }

    private $beforeCommand;

    public function setBeforeCommand(callable $before): void {
        $this->beforeCommand = $before;
    }

    public function run(Caller $caller): Result {
        if (is_callable($this->beforeCommand)) {
            call_user_func($this->beforeCommand, $caller);
        }
        if (function_exists('apc_clear_cache')) {
            apc_clear_cache();
        }
        if (function_exists('opcache_reset')) {
            opcache_reset();
        }

        $msg = Manager::instance()->run($caller);

        $result = new Result();
        $result->setMsg($msg);
        return $result;
    }
}
