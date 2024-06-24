<?php

namespace Scf\Database\Exception;

use Scf\Core\Console;
use Scf\Server\Http;

class NullMasterDb {
    protected string $msg = '未知错误';
    protected string $name;

    public function __construct($name, $msg) {
        $this->name = $name;
        $this->msg = $msg;
    }


    public function __get($name) {
        return null;
        //Console::error('MasterDB连接失败[' . $this->msg . ']');
    }

    public function __call($name, $args) {
        //Console::error('MasterDB连接失败[' . $this->msg . ']');
        return false;
    }
}