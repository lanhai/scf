<?php


namespace Scf\Command;


class Help {
    /**
     * @var array
     */
    protected array $actions = [];

    /**
     * @var array
     */
    protected array $opts = [];

    /**
     * @var int
     */
    protected int $actionWidth = 1;

    /**
     * @var int
     */
    protected int $optWidth = 1;

    public function addAction(string $actionName, string $desc) {
        $this->actions[$actionName] = $desc;

        if (($len = strlen($actionName)) > $this->actionWidth) {
            $this->actionWidth = $len;
        }
    }

    public function addActionOpt(string $actionOptName, string $desc) {
        $this->opts[$actionOptName] = $desc;

        if (($len = strlen($actionOptName)) > $this->optWidth) {
            $this->optWidth = $len;
        }
    }

    /**
     * @return array
     */
    public function getActions(): array {
        return $this->actions;
    }

    /**
     * @return array
     */
    public function getOpts(): array {
        return $this->opts;
    }

    /**
     * @return int
     */
    public function getActionWidth(): int {
        return $this->actionWidth;
    }

    /**
     * @return int
     */
    public function getOptWidth(): int {
        return $this->optWidth;
    }
}