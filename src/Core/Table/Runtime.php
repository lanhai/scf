<?php

namespace Scf\Core\Table;

use Scf\Core\Key;
use Swoole\Table;

class Runtime extends ATable {

    /**
     * 配置项
     * @var array
     */
    protected array $_config = [
        'size' => 256,
        'colums' => [
            '_value' => ['type' => Table::TYPE_STRING, 'size' => 1024 * 100]
        ]
    ];


    /**
     * 后台任务状态
     * @param bool|null $status
     * @return bool
     */
    public function crontabProcessStatus(?bool $status = null): bool {
        if (!is_null($status)) {
            return $this->set(Key::RUNTIME_CRONTAB_STATUS, $status);
        }
        return $this->get(Key::RUNTIME_CRONTAB_STATUS) ?: false;
    }

    /**
     * Redis队列任务状态
     * @param bool|null $status
     * @return bool
     */
    public function redisQueueProcessStatus(?bool $status = null): bool {
        if (!is_null($status)) {
            return $this->set(Key::RUNTIME_REDIS_QUEUE_STATUS, $status);
        }
        return $this->get(Key::RUNTIME_REDIS_QUEUE_STATUS) ?: false;
    }

    /**
     * 服务器启动状态
     * @param ?bool $status
     * @return bool
     */
    public function serverUseable(?bool $status = null): bool {
        if (is_bool($status)) {
            return $this->set(Key::RUNTIME_SERVER_STATUS, $status);
        }
        return $this->get(Key::RUNTIME_SERVER_STATUS) ?: false;
    }

    public function serverIsAlive(?bool $status = null) {
        if (is_bool($status)) {
            return $this->set(Key::COUNTER_SERVER_ISRUNNING, $status);
        }
        return $this->get(Key::COUNTER_SERVER_ISRUNNING) ?: false;
    }

    /**
     * Rpc服务端口
     * @param int|null $port
     * @return bool|mixed
     */
    public function rpcPort(?int $port = null): mixed {
        if ($port) {
            return $this->set(Key::RUNTIME_RPC_PORT, $port);
        }
        return $this->get(Key::RUNTIME_RPC_PORT);
    }

    /**
     * masterDB服务端口
     * @param int|null $port
     * @return bool|mixed
     */
    public function masterDbPort(?int $port = null): mixed {
        if ($port) {
            return $this->set(Key::RUNTIME_MASTERDB_PORT, $port);
        }
        return $this->get(Key::RUNTIME_MASTERDB_PORT);
    }

    /**
     * Socket服务端口
     * @param int|null $port
     * @return bool|mixed
     */
    public function socketPort(?int $port = null): mixed {
        if ($port) {
            return $this->set(Key::RUNTIME_SOCKET_PORT, $port);
        }
        return $this->get(Key::RUNTIME_SOCKET_PORT);
    }

    /**
     * Http服务端口
     * @param int|null $port
     * @return bool|mixed
     */
    public function httpPort(?int $port = null): mixed {
        if ($port) {
            return $this->set(Key::RUNTIME_HTTP_PORT, $port);
        }
        return $this->get(Key::RUNTIME_HTTP_PORT);
    }

    /**
     * 控制面板服务端口
     * @param int|null $port
     * @return bool|mixed
     */
    public function dashboardPort(?int $port = null): mixed {
        if ($port) {
            return $this->set(Key::RUNTIME_DASHBOARD_PORT, $port);
        }
        return $this->get(Key::RUNTIME_DASHBOARD_PORT);
    }
}