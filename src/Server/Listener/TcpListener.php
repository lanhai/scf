<?php

namespace Scf\Server\Listener;

use ReflectionClass;
use RuntimeException;
use Scf\Mode\Tcp\Connection;
use Scf\Core\App;
use Scf\Rpc\Protocol\Protocol;
use Scf\Util\Dir;
use SplQueue;
use Swoole\Coroutine\Server\Connection as Client;
use Swoole\Coroutine\Socket;
use Swoole\Server;
use Swoole\Server\Port;
use Throwable;

/**
 * TCP 端口回调与帧分发。每条连接在所属 worker 内持有一个应用控制器。
 */
final class TcpListener {
    private array $sessions = [];

    private ?array $controllers = null;

    public function __construct(private readonly int $maxPacketLength) {
    }

    /** 在已挂载应用的 worker 内发现控制器，每个 worker 缓存一次。 */
    private function discoverControllers(): array {
        if ($this->controllers !== null) {
            return $this->controllers;
        }
        $directory = App::libPath();
        $controllers = [];
        foreach (Dir::scanLongFilesGenerator($directory) as $file) {
            $relative = substr($file, strlen($directory) + 1);
            if (!preg_match('~\A(?:[A-Za-z_][A-Za-z0-9_]*/)*([A-Za-z_][A-Za-z0-9_]*)\.php\z~', $relative, $match)) {
                continue;
            }
            $source = file_get_contents($file);
            if ($source === false || !self::declaresSubclass($source, $match[1])) {
                continue;
            }
            $class = 'App\\' . str_replace('/', '\\', substr($relative, 0, -4));
            if (!is_subclass_of($class, Connection::class) || !(new ReflectionClass($class))->isInstantiable()) {
                continue;
            }
            $alias = strtolower($match[1]);
            if (isset($controllers[$alias]) || $alias === 'auth') {
                throw new RuntimeException("TCP 控制器前缀重复或已保留 {$alias}");
            }
            $controllers[$alias] = $class;
        }
        return $this->controllers = $controllers;
    }

    /** 只自动加载声明了父类的候选类，避免执行应用下的配置和脚本。 */
    private static function declaresSubclass(string $source, string $name): bool {
        $tokens = array_values(array_filter(token_get_all($source), static fn($token): bool =>
            !is_array($token) || !in_array($token[0], [T_WHITESPACE, T_COMMENT, T_DOC_COMMENT], true)
        ));
        foreach ($tokens as $i => $token) {
            if (is_array($token) && $token[0] === T_CLASS
                && ($tokens[$i + 1][0] ?? null) === T_STRING
                && $tokens[$i + 1][1] === $name
                && ($tokens[$i + 2][0] ?? null) === T_EXTENDS) {
                return true;
            }
        }
        return false;
    }

    private function newConnection(Server|Socket $transport, int $fd): Connection {
        $class = $this->discoverControllers()['index'] ?? null;
        return $class === null
            ? new class($transport, $fd, $this->maxPacketLength) extends Connection {}
            : new $class($transport, $fd, $this->maxPacketLength);
    }

    /** @return array Swoole 长度帧配置，最大长度包含 4 字节包头 */
    public function framing(): array {
        return [
            'open_length_check' => true,
            'package_length_type' => 'N',
            'package_length_offset' => 0,
            'package_body_offset' => 4,
            'package_max_length' => $this->maxPacketLength,
        ];
    }

    /** 为独立端口注册回调，隔离主 Server 的 WebSocket 和 RPC 回调。 */
    public function bind(Port $port): void {
        $port->on('connect', $this->connect(...));
        $port->on('receive', $this->receive(...));
        $port->on('close', $this->close(...));
    }

    private function connect(Server $server, int $fd): void {
        try {
            $controller = $this->newConnection($server, $fd);
            $session = (object)['controller' => $controller, 'queue' => new SplQueue(), 'busy' => true, 'closed' => false];
            $this->sessions[$fd] = $session;
            $controller->connected();
            $this->drain($session);
        } catch (Throwable $exception) {
            error_log((string)$exception);
            $server->close($fd);
            if (isset($session)) {
                $session->busy = false;
                if ($session->closed) {
                    $this->disconnected($session->controller);
                } else {
                    $this->close($server, $fd);
                }
            }
        }
    }

    private function receive(Server $server, int $fd, int $reactorId, string $packet): void {
        $session = $this->sessions[$fd] ?? null;
        if ($session === null) {
            $server->close($fd);
            return;
        }
        // 控制器可以挂起协程。后续帧排队，避免同连接的鉴权和业务状态交错。
        if ($session->queue->count() >= 16) {
            $session->controller->disconnect();
            return;
        }
        $session->queue->enqueue($packet);
        if (!$session->busy) {
            $session->busy = true;
            $this->drain($session);
        }
    }

    private function drain(object $session): void {
        try {
            while (!$session->closed && !$session->queue->isEmpty() && $session->controller->isClientAlive()) {
                $this->reply($session->controller, $session->queue->dequeue());
            }
        } catch (Throwable $exception) {
            error_log((string)$exception);
            $session->controller->disconnect();
        } finally {
            $session->busy = false;
            if ($session->closed) {
                $this->disconnected($session->controller);
            }
        }
    }

    private function close(Server $server, int $fd): void {
        $session = $this->sessions[$fd] ?? null;
        unset($this->sessions[$fd]);
        if ($session !== null) {
            $session->closed = true;
            if (!$session->busy) {
                $this->disconnected($session->controller);
            }
        }
    }

    /** 在独立 TCP 命令中使用同一套鉴权、路由和连接 API。 */
    public function __invoke(Client $client): void {
        $socket = $client->exportSocket();
        try {
            $controller = $this->newConnection($socket, $socket->fd);
            if (!$socket->setProtocol($this->framing())) {
                return;
            }
            $controller->connected();
            while ($controller->isClientAlive() && ($packet = $socket->recvPacket()) !== false && $packet !== '') {
                $this->reply($controller, $packet);
            }
        } catch (Throwable $exception) {
            error_log((string)$exception);
        } finally {
            if (!$socket->isClosed()) {
                $client->close();
            }
            if (isset($controller)) {
                $this->disconnected($controller);
            }
        }
    }

    private function disconnected(Connection $controller): void {
        foreach ($controller->releaseRoutedControllers() as $routedController) {
            $this->disconnected($routedController);
        }
        try {
            $controller->disconnected();
        } catch (Throwable $exception) {
            error_log((string)$exception);
        }
    }

    private function reply(Connection $controller, string $packet): void {
        $response = $this->dispatch($controller, Protocol::unpack($packet));
        if (!$controller->send($response) && !$controller->send(['ok' => false, 'error' => 'invalid_response'])) {
            $controller->disconnect();
        }
    }

    /** @return array TCP 响应体，业务异常只在服务端记录详情。 */
    private function dispatch(Connection $controller, string $body): array {
        $request = json_decode($body, true);
        if (!is_array($request) || !is_string($request['event'] ?? null)) {
            return ['ok' => false, 'error' => 'invalid_request'];
        }
        $event = $request['event'];
        $data = $request['data'] ?? null;

        try {
            if (strcasecmp($event, 'auth') === 0) {
                return $controller->authenticate($data)
                    ? ['ok' => true, 'data' => null]
                    : ['ok' => false, 'error' => 'authentication_failed'];
            }
            if (!$controller->isAuthenticated()) {
                return ['ok' => false, 'error' => 'authentication_required'];
            }
            // 已注册的前缀优先。其他分段事件继续走默认控制器。
            [$prefix, $action] = array_pad(explode('.', $event, 2), 2, null);
            $alias = strtolower($prefix);
            if ($action !== null && isset($this->controllers[$alias])) {
                $controller = $alias === 'index'
                    ? $controller
                    : $controller->routedController($alias, $this->controllers[$alias]);
                if ($controller === null) {
                    return ['ok' => false, 'error' => 'authentication_required'];
                }
                $event = $action;
            }
            $handler = $controller->resolveEvent($event);
            return $handler === null
                ? ['ok' => false, 'error' => 'unknown_event']
                : ['ok' => true, 'data' => $handler->invoke($controller, $data)];
        } catch (Throwable $exception) {
            error_log((string)$exception);
            return ['ok' => false, 'error' => 'handler_error'];
        }
    }
}
