<?php

namespace Scf\Database\Exception;

use Scf\Mode\Web\Exception\AppError;
use Scf\Server\Env;

class NullAR {
    protected mixed $sql;
    protected string $ar;

    public function __construct($ar, $sql) {
        $this->ar = $ar;
        $this->sql = $sql;
    }

    /**
     * @throws AppError
     */
    public function __get($name) {
        if (Env::isDev()) {
            throw new AppError('活动记录数据不存在:' . $this->ar . ';sql:' . $this->sql);
        } else {
            throw new AppError('未查询到相关数据');
        }
    }

    /**
     * @throws AppError
     */
    public function __call($name, $args) {
        if (Env::isDev()) {
            throw new AppError('活动记录数据不存在:' . $this->ar . ';sql:' . $this->sql);
        } else {
            throw new AppError('未查询到相关数据');
        }
    }

    /**
     * @return bool
     */
    public function notExist(): bool {
        return true;
    }

    /**
     * @return bool
     */
    public function exist(): bool {
        return false;
    }
}