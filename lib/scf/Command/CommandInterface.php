<?php


namespace Scf\Command;


interface CommandInterface {
    public function commandName(): string;

    public function exec(): ?string;

    public function help(Help $commandHelp): Help;

    public function desc(): string;
}