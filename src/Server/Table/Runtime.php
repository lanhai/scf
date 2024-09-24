<?php

namespace Scf\Server\Table;

use Scf\Core\Key;
use Scf\Server\Runtime\Table;

class Runtime extends Table {

    /**
     * 配置项
     * @var array
     */
    protected array $_config = [
        'size' => 128,
        'colums' => [
            '_value' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 102400]
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
     * 服务器启动状态
     * @param ?bool $status
     * @return bool
     */
    public function serverStatus(?bool $status = null): bool {
        if (!is_null($status)) {
            return $this->set(Key::RUNTIME_SERVER_STATUS, $status);
        }
        return $this->get(Key::RUNTIME_SERVER_STATUS) ?: false;
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
}