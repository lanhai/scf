<?php


namespace Scf\Server\Trigger;


class Location {
    private mixed $file;
    private mixed $line;

    /**
     * @return mixed
     */
    public function getFile(): mixed {
        return $this->file;
    }

    /**
     * @param mixed $file
     */
    public function setFile(mixed $file): void {
        $this->file = $file;
    }

    /**
     * @return mixed
     */
    public function getLine(): mixed {
        return $this->line;
    }

    /**
     * @param mixed $line
     */
    public function setLine(mixed $line): void {
        $this->line = $line;
    }
}