<?php

namespace Scf\Mode\Tcp;

use ReflectionMethod;
use Scf\Rpc\Protocol\Protocol;
use Swoole\Coroutine\Socket;
use Swoole\Server;

/**
 * TCP 应用控制器基类。每条连接持有一个实例，连接内的事件按到达顺序执行。
 */
abstract class Connection {

    protected const AUTH_REQUIRED = false;

    private array $routedControllers = [];
    private bool $authenticated = false;
    private bool $closing = false;
    private readonly array $connectionInfo;

    final public function __construct(
        private readonly Server|Socket $transport,
        private readonly int $fd,
        private readonly int $maxPacketLength,
        private readonly ?Connection $owner = null
    ) {
        if ($owner !== null) {
            $this->connectionInfo = $owner->connectionInfo;
        } elseif ($transport instanceof Server) {
            $info = $transport->getClientInfo($fd) ?: [];
            $this->connectionInfo = array_intersect_key($info, array_flip([
                'remote_ip', 'remote_port', 'server_port', 'connect_time',
            ]));
        } else {
            $peer = $transport->getpeername();
            $this->connectionInfo = [
                'remote_ip' => $peer['address'] ?? null,
                'remote_port' => $peer['port'] ?? null,
                'server_port' => $transport->getsockname()['port'] ?? null,
                'connect_time' => time(),
            ];
        }
    }

    /**
     * 首次命中别名时创建控制器，并共享本连接的传输和鉴权状态。
     *
     * @param class-string<Connection> $class 已配置的控制器类
     * @return Connection|null 目标要求认证但尚未通过时返回 null
     */
    final public function routedController(string $alias, string $class): ?Connection {
        if ($class::AUTH_REQUIRED && !$this->authenticated) {
            return null;
        }
        if (!isset($this->routedControllers[$alias])) {
            $controller = new $class($this->transport, $this->fd, $this->maxPacketLength, $this);
            $this->routedControllers[$alias] = $controller;
            try {
                $controller->connected();
            } catch (\Throwable $exception) {
                $this->disconnect();
                throw $exception;
            }
        }
        return $this->routedControllers[$alias];
    }

    /** @return list<Connection> 本连接已初始化的路由控制器，按逆序释放 */
    final public function releaseRoutedControllers(): array {
        $controllers = array_values(array_reverse($this->routedControllers));
        $this->routedControllers = [];
        return $controllers;
    }

    /** 返回 Swoole 连接 ID 和建立连接时记录的地址信息。 */
    final public function getConnectionInfo(): array {
        return ['fd' => $this->fd] + $this->connectionInfo;
    }

    /** 处理 auth 事件的 data。子类应校验凭据，并可保存用户身份。 */
    protected function auth(mixed $credentials): bool {
        return false;
    }

    /** @return bool 当前连接是否允许执行业务事件 */
    final public function isAuthenticated(): bool {
        if ($this->owner !== null) {
            return $this->owner->isAuthenticated() && (!static::AUTH_REQUIRED || $this->owner->authenticated);
        }
        return !static::AUTH_REQUIRED || $this->authenticated;
    }

    /** 仅在首次认证成功时改变连接的认证状态。 */
    final public function authenticate(mixed $credentials): bool {
        if ($this->owner !== null) {
            return $this->owner->authenticate($credentials);
        }
        return $this->authenticated = $this->authenticated || $this->auth($credentials);
    }

    /** @return bool 底层连接是否仍可用于发送 */
    final public function isClientAlive(): bool {
        if ($this->owner !== null) {
            return $this->owner->isClientAlive();
        }
        return !$this->closing && ($this->transport instanceof Server
            ? $this->transport->exist($this->fd)
            : !$this->transport->isClosed());
    }

    /** 发送一帧 JSON，可用于控制器主动推送。 */
    final public function send(array $message): bool {
        if (!$this->isClientAlive()) {
            return false;
        }
        $json = json_encode($message, JSON_INVALID_UTF8_SUBSTITUTE);
        if ($json === false || strlen($json) + 4 > $this->maxPacketLength) {
            return false;
        }
        $packet = Protocol::pack($json);
        return $this->transport instanceof Server
            ? $this->transport->send($this->fd, $packet)
            : $this->transport->sendAll($packet) === strlen($packet);
    }

    /** 主动断开连接，阻止后续事件和发送。 */
    final public function disconnect(): void {
        if ($this->owner !== null) {
            $this->owner->disconnect();
            return;
        }
        if (!$this->isClientAlive()) {
            return;
        }
        $this->closing = true;
        if ($this->transport instanceof Server) {
            $this->transport->close($this->fd);
        } else {
            $this->transport->close();
        }
    }

    /** 连接建立后执行，可初始化会话状态。 */
    public function connected(): void {
    }

    /** 连接关闭后执行，可释放会话资源。 */
    public function disconnected(): void {
    }

    /**
     * 将事件名映射到公开实例方法，例如 chat.message 映射到 onChatMessage。
     *
     * @param string $event 客户端事件名
     * @return ReflectionMethod|null 对应的业务处理方法
     */
    final public function resolveEvent(string $event): ?ReflectionMethod {
        if (!preg_match('/\A[a-z][a-z0-9]*(?:[._-][a-z][a-z0-9]*)*\z/i', $event)) {
            return null;
        }
        $method = 'on' . str_replace(' ', '', ucwords(str_replace(['.', '_', '-'], ' ', $event)));
        if (!method_exists($this, $method)) {
            return null;
        }
        $handler = new ReflectionMethod($this, $method);
        return $handler->isPublic() && !$handler->isStatic() ? $handler : null;
    }
}
