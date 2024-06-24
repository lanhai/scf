<?php

namespace Scf\Command;

use Scf\Core\Console;

class Caller {
    private string $script;
    private string $command;
    private array $params;

    public function getCommand(): string {
        return $this->command;
    }

    public function setCommand(string|bool $command): void {
        if(!$command){
            Console::error("请输入正确的指令");
        }
        $this->command = $command;
    }

    public function setParams($params): void {
        $this->params = $params;
    }

    public function getParams(): array {
        return $this->params;
    }

    public function setScript(string $script): void {
        $this->script = $script;
    }

    public function getScript(): string {
        return $this->script;
    }
}