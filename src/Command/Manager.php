<?php

namespace Scf\Command;

use Scf\Core\Traits\Singleton;

class Manager {
    use Singleton;

    /**
     * desc
     * @var string
     */
    private string $desc = 'Welcome To Scf Command Console!';

    /**
     * a b framework=easyswoole
     * @var array
     */
    private array $args = [];

    /**
     * --config=dev.php -d
     * @var array
     */
    private array $opts = [];

    /**
     * 脚本
     * @var string
     */
    private string $script = '';

    /**
     * add commands
     * @var array
     */
    private array $commands = [];

    /**
     * @var int
     */
    private int $width = 1;

    /**
     * @var array
     */
    private array $originArgv = [];

    /**
     * @param Caller $caller
     * @return string|null
     */
    public function run(Caller $caller): ?string {
        $argv = $this->originArgv = $caller->getParams();
        // remove script command
        array_shift($argv);
        array_shift($argv);
        // script
        $this->script = $caller->getScript();
        // command
        $command = '';
        $command1 = $caller->getCommand();
        $this->parseArgv(array_values($argv));
        if (!($command = $command1)) {
            return $this->displayHelp();
        }
        if ($command == '--help' || $command == '-h') {
            return $this->displayHelp();
        }
        if ($this->issetOpt('h') || $this->issetOpt('help')) {
            return $this->displayCommandHelp($command);
        }
        if (!array_key_exists($command, $this->commands)) {
            return $this->displayAlternativesHelp($command);
        }
        /** @var CommandInterface $handler */
        $handler = $this->commands[$command];
        //定义应用路径
        $options = $this->getOpts();
        !defined('SCF_APPS_ROOT') and define("SCF_APPS_ROOT", ($options['apps'] ?? dirname(SCF_ROOT)) . '/apps');
        return $handler->exec();
    }

    /**
     * @return array
     */
    public function getOriginArgv(): array {
        return $this->originArgv;
    }

    /**
     * @param array $params
     */
    private function parseArgv(array $params): void {
        while (false !== ($param = current($params))) {
            next($params);
            if (str_starts_with($param, '-')) {
                $option = ltrim($param, '-');
                $value = null;
                if (str_contains($option, '=')) {
                    [$option, $value] = explode('=', $option, 2);
                }
                if ($option) $this->opts[$option] = $value;
            } else if (str_contains($param, '=')) {
                [$name, $value] = explode('=', $param, 2);
                if ($name) $this->args[$name] = $value;
            } else {
                $this->args[] = $param;
            }
        }
    }

    public function addCommand(CommandInterface $handler): void {
        $command = $handler->commandName();

        $this->commands[$command] = $handler;

        if (($len = strlen($command)) > $this->width) {
            $this->width = $len;
        }

    }

    public function displayAlternativesHelp($command): string {
        $text = "The command '{$command}' is not exists!\n";
        $commandNames = array_keys($this->commands);
        $alternatives = [];
        foreach ($commandNames as $commandName) {
            $lev = levenshtein($command, $commandName);
            if ($lev <= strlen($command) / 3 || str_contains($commandName, $command)) {
                $alternatives[$commandName] = $lev;
            }
        }
        $threshold = 1e3;
        $alternatives = array_filter($alternatives, function ($lev) use ($threshold) {
            return $lev < 2 * $threshold;
        });
        ksort($alternatives);

        if ($alternatives) {
            $text .= "Did you mean one of these?\n";
            foreach (array_keys($alternatives) as $alternative) {
                $text .= "$alternative\n";
            }
        } else {
            $text .= $this->displayHelp();
        }

        return Color::danger($text);
    }

    public function displayCommandHelp($command): string {
        /** @var CommandInterface $handler */
        $handler = $this->commands[$command] ?? '';
        if (!$handler) {
            $result = Color::danger("The command '{$command}' is not exists!\n");
            $result .= $this->displayHelp();
            return $result;
        }
        $fullCmd = $this->script . " " . $handler->commandName();
        $desc = $handler->desc() ? ucfirst($handler->desc()) : 'No description for the command';
        $desc = "<brown>$desc</brown>";
        $usage = "<cyan>$fullCmd ACTION</cyan> [--opts ...]";

        $nodes = [
            $desc,
            "<brown>Usage:</brown>" . "\n  $usage\n",
        ];
        $helpMsg = implode("\n", $nodes);
        /**-----------------CommandHelp--------------------------------*/
        /** @var Help $commandHelp */
        $commandHelp = $handler->help(new Help());
        $helpMsg .= "<brown>Actions:</brown>\n";
        $actions = $commandHelp->getActions();
        $actionWidth = $commandHelp->getActionWidth();
        if (empty($actions)) $helpMsg .= "\n";
        foreach ($actions as $name => $desc) {
            $name = str_pad($name, $actionWidth, ' ');
            $helpMsg .= "  <green>$name</green>  $desc\n";
        }
        $helpMsg .= "<brown>Options:</brown>\n";
        $opts = $commandHelp->getOpts();
        $optWidth = $commandHelp->getOptWidth();
        foreach ($opts as $name => $desc) {
            $name = str_pad($name, $optWidth, ' ');
            $helpMsg .= "  <green>$name</green>  $desc\n";
        }
        /**-----------------CommandHelp--------------------------------*/
        return Color::render($helpMsg);
    }

    public function displayHelp(): string {
        // help
        $desc = ucfirst($this->desc) . "\n";
        $usage = "<cyan>{$this->script} COMMAND -h</cyan>";
        $help = "<brown>{$desc}Usage:</brown>" . " $usage\n<brown>Commands:</brown>\n";
        $data = $this->commands;
        ksort($data);

        /**
         * @var string $command
         * @var CommandInterface $handler
         */
        foreach ($data as $command => $handler) {
            $command = str_pad($command, $this->width, ' ');
            $desc = $handler->desc() ? ucfirst($handler->desc()) : 'No description for the command';
            $help .= "  <green>$command</green>  $desc\n";
        }
        $help .= "\nFor command usage please run: $usage\n";
        return Color::render($help);
    }

    /**
     * @return array
     */
    public function getArgs(): array {
        return $this->args;
    }

    /**
     * @param array $args
     */
    public function setArgs(array $args): void {
        $this->args = $args;
    }

    /**
     * @return array
     */
    public function getOpts(): array {
        return $this->opts;
    }

    /**
     * @param array $opts
     */
    public function setOpts(array $opts): void {
        $this->opts = $opts;
    }

    /**
     * @param int|string $name
     * @param mixed|null $default
     *
     * @return mixed|null
     */
    public function getArg(int|string $name, mixed $default = null): mixed {
        return $this->args[$name] ?? $default;
    }

    /**
     * @param int|string $name
     * @param mixed|null $default
     * @return mixed|null
     */
    public function getOpt(int|string $name, mixed $default = null): mixed {
        return $this->opts[$name] ?? $default;
    }

    /**
     * @param int|string $name
     * @return bool
     */
    public function issetArg(int|string $name): bool {
        return isset($this->args[$name]);
    }

    /**
     * @param int|string $name
     * @return bool
     */
    public function issetOpt(int|string $name): bool {
        return array_key_exists($name, $this->opts);
    }

    /**
     * @return string
     */
    public function getDesc(): string {
        return $this->desc;
    }

    /**
     * @param string $desc
     */
    public function setDesc(string $desc): void {
        $this->desc = $desc;
    }
}
