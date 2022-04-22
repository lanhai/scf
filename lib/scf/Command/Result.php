<?php

namespace Scf\Command;

class Result {
    private $result;
    private $msg;

    function getMsg(): ?string {
        return $this->msg;
    }

    function setResult($result) {
        $this->result = $result;
    }

    function getResult() {
        return $this->result;
    }

    function setMsg(?string $msg) {
        $this->msg = $msg;
    }
}