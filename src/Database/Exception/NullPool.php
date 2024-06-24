<?php

namespace Scf\Database\Exception;

use Scf\Mode\Web\Exception\AppError;
use Scf\Server\Env;

class NullPool {
    protected string $msg = '未知错误';
    protected string $name;

    public function __construct($name, $msg) {
        $this->name = $name;
        $this->msg = $msg;
    }

    /**
     * @throws AppError
     */
    public function __get($name) {
        if (Env::isDev()) {
            throw new AppError($this->msg);
        } else {
            throw new AppError('系统繁忙,请稍后重试[Redis service error]');
        }
    }

    /**
     * @throws AppError
     */
    public function __call($name, $args) {
        if (Env::isDev()) {
            throw new AppError($this->msg);
        } else {
            throw new AppError('系统繁忙,请稍后重试[Redis service error]');
        }
    }

    public function getError(): string {
        return $this->msg;
    }
}