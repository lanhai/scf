<?php

namespace Scf\Mode\Socket;

use Scf\Helper\JsonHelper;
use Swoole\WebSocket\Server;
use Swoole\Http\Request;

abstract class Connection {
    const EVENT_CONNECT = 'connect';
    const EVENT_MESSAGE = 'message';
    const EVENT_DISCONNECT = 'disconnect';
    const EVENT_AUTH = 'auth';
    protected Server $server;
    protected int $fd;
    protected string $event = "";
    protected string|array $message = "";
    protected string $route = "";
    protected static array $_instances;
    protected Request $request;
    protected bool $alive = false;


    public function __construct($server, $fd) {
        $this->server = $server;
        $this->fd = $fd;
    }

    public static function instance($server, $fd): static {
        if (!isset(self::$_instances[$fd])) {
            self::$_instances[$fd] = new static($server, $fd);
        }
        return self::$_instances[$fd];
    }

    /**
     * 鉴权,可以直接返回false,或者返回true后延后发送消息再断开连接
     * @return bool
     */
    abstract public function auth(): bool;

    abstract public function onConnect(): bool;

    abstract public function onMessage(): bool;

    abstract public function beforeDisconnect(): void;

    public function setRequest(Request $request): static {
        $this->request = $request;
        $this->route = $request->server['request_uri'];
        return $this;
    }

    public function onDisconnect(): bool {
        $this->alive = false;
        $this->beforeDisconnect();
        unset(self::$_instances[$this->fd]);
        return true;
    }

    protected function isClientAlive(): bool {
        if (!$this->alive) {
            return false;
        }
        return $this->server->isEstablished($this->fd);
    }

    protected function disconnect(): void {
        if ($this->server->isEstablished($this->fd)) {
            $this->server->disconnect($this->fd);
        }
    }

    protected function push(string|array $message): bool {
        return $this->send($message);
    }

    protected function send(string|array $message): bool {
        if (!$this->isClientAlive()) {
            return false;
        }
        if (is_array($message)) {
            $message = JsonHelper::toJson($message);
        }
        return $this->server->push($this->fd, $message);
    }

    public function setEvent($event): void {
        $this->event = $event;
    }

    public function setMessage(string $message): static {
        $this->event = self::EVENT_MESSAGE;
        if (JsonHelper::is($message)) {
            $this->message = JsonHelper::recover($message);
        } else {
            $this->message = $message;
        }
        return $this;
    }

}