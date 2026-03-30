<?php

namespace Scf\Cache\Connection;

use Mix\Redis\Connection;
use Scf\Core\InflightCounter;

/**
 * 带 inflight 计数的 Redis 连接封装。
 *
 * 责任边界：
 * - 不改变底层 Mix\Redis 的命令执行与重连语义；
 * - 仅在命令执行前后维护 Redis inflight 计数，供 shutdown/recycle 判定使用。
 */
class InflightRedisConnection extends Connection {

    /**
     * 执行 Redis 命令并维护 inflight 计数。
     *
     * @param string $name 命令名。
     * @param array $arguments 命令参数。
     * @return mixed
     * @throws \Throwable
     */
    public function __call($name, $arguments = []) {
        InflightCounter::beginRedis();
        try {
            return parent::__call($name, $arguments);
        } finally {
            InflightCounter::endRedis();
        }
    }
}

