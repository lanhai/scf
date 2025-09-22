<?php

namespace Scf\Database\Exception;

use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Mode\Web\Exception\AppError;

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
        $this->throwError();

    }

    /**
     * @throws AppError
     */
    public function __call($name, $args) {
        $this->throwError();
    }

    /**
     * @throws AppError
     */
    protected function throwError(): void {
        if (defined('IS_CRONTAB_PROCESS') || ENV_MODE == MODE_CLI) {
            Console::error('活动记录数据不存在:' . $this->ar . ';sql:' . $this->sql);
        } else {
            if (Env::isDev()) {
                throw new AppError('活动记录数据不存在:' . $this->ar . ';sql:' . $this->sql);
            } else {
                throw new AppError('未查询到相关数据');
            }
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