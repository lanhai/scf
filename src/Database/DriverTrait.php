<?php

namespace Scf\Database;

use Mix\ObjectPool\AbstractObjectPool;

trait DriverTrait {

    /**
     * @var ?AbstractObjectPool
     */
    public ?AbstractObjectPool $pool = null;

    public string $id;

    /**
     * @var int
     */
    public int $createTime = 0;

    public int $lastUseTime = 0;

    public int $lastPingTime = 0;

    public int $pingTimes = 0;


    /**
     * 丢弃连接
     * @return bool
     */
    public function __discard(): bool {
        if (isset($this->pool)) {
            return $this->pool->discard($this);
        }
        return false;
    }

    /**
     * 归还连接
     * @return bool
     */
    public function __return(): bool {
        if (isset($this->pool)) {
            return $this->pool->return($this);
        }
        return false;
    }
}