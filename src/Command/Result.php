<?php

namespace Scf\Command;

class Result {
    private mixed $result;
    private ?string $msg;

    function getMsg(): ?string {
        return $this->msg;
    }

    function setResult($result): void {
        $this->result = $result;
    }

    function getResult() {
        return $this->result;
    }

    function setMsg(?string $msg): void {
        $this->msg = $msg;
    }
}