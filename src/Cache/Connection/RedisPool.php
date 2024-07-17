<?php

namespace Scf\Cache\Connection;

use Mix\Redis\Connection;
use Mix\Redis\Pool\Dialer;
use Mix\Redis\Redis;
use Scf\Database\Pool\ConnectionPool;

class RedisPool extends Redis {

    /**
     * @param int $maxOpen
     * @param int $maxIdle
     * @param int $maxLifetime
     * @param float $waitTimeout
     */
    public function start(int $maxOpen, int $maxIdle, int $maxLifetime = 0, float $waitTimeout = 0.0): void {
        $this->maxOpen = $maxOpen;
        $this->maxIdle = $maxIdle;
        $this->maxLifetime = $maxLifetime;
        $this->waitTimeout = $waitTimeout;
        $this->create();
    }

    protected function create(): void {
        if ($this->driver) {
            $this->driver->close();
            $this->driver = null;
        }

        $this->pool = new ConnectionPool(
            new Dialer(
                $this->host,
                $this->port,
                $this->password,
                $this->database,
                $this->timeout,
                $this->retryInterval,
                $this->readTimeout
            ),
            $this->maxOpen,
            $this->maxIdle,
            $this->maxLifetime,
            $this->waitTimeout
        );
    }

    protected function borrow(): Connection {
        if ($this->pool) {
            $driver = $this->pool->borrow();
            $conn = new Connection($driver, $this->logger);
        } else {
            $conn = new Connection($this->driver, $this->logger);
        }
        return $conn;
    }
}