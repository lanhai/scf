<?php

namespace Scf\Server;

use Scf\Server\Event\Register;
use Scf\Core\Traits\Singleton;
use Swoole\Server;
use Swoole\Server\Port;
use Swoole\WebSocket\Server as WebSocketServer;
use Swoole\Http\Server as HttpServer;

class Launcher {
    use Singleton;

    /**
     * @var Server $swooleServer
     */
    private $swooleServer;
    private $mainServerEventRegister;
    private $subServer = [];
    private $subServerRegister = [];
    private $isStart = false;

    function __construct() {
        $this->mainServerEventRegister = new Register();
    }

    /**
     * @param string|null $serverName
     * @return null|Server|Port|WebSocketServer|HttpServer
     */
    public function getSwooleServer(string $serverName = null): Server\Port|HttpServer|Server|WebSocketServer|null {
        if ($serverName === null) {
            return $this->swooleServer;
        } else {
            if (isset($this->subServer[$serverName])) {
                return $this->subServer[$serverName];
            }
            return null;
        }
    }

    public function createSwooleServer($port, $type, $address = '0.0.0.0', array $setting = [], ...$args): bool {
        switch ($type) {
            case SWOOLE_SERVER:
            {
                $this->swooleServer = new Server($address, $port, ...$args);
                break;
            }
            case SWOOLE_SERVER_HTTP:
            {
                $this->swooleServer = new HttpServer($address, $port, ...$args);
                break;
            }
            case SWOOLE_SERVER_SOCKET:
            {
                $this->swooleServer = new WebSocketServer($address, $port, ...$args);
                break;
            }
            default:
            {
                Trigger::getInstance()->error("unknown server type :{$type}");
                return false;
            }
        }
        if ($this->swooleServer) {
            $this->swooleServer->set($setting);
        }
        return true;
    }


    public function addServer(string $serverName, int $port, int $type = SWOOLE_TCP, string $listenAddress = '0.0.0.0', array $setting = []): Register {
        $eventRegister = new Register();
        $subPort = $this->swooleServer->addlistener($listenAddress, $port, $type);
        if (!empty($setting)) {
            $subPort->set($setting);
        }
        $this->subServer[$serverName] = $subPort;
        $this->subServerRegister[$serverName] = [
            'port' => $port,
            'listenAddress' => $listenAddress,
            'type' => $type,
            'setting' => $setting,
            'eventRegister' => $eventRegister
        ];
        return $eventRegister;
    }

    public function getEventRegister(string $serverName = null): ?Register {
        if ($serverName === null) {
            return $this->mainServerEventRegister;
        } else if (isset($this->subServerRegister[$serverName])) {
            return $this->subServerRegister[$serverName];
        }
        return null;
    }

    public function start(): void {
        //注册主服务事件回调
        $events = $this->getEventRegister()->all();
        foreach ($events as $event => $callback) {
            $this->getSwooleServer()->on($event, function (...$args) use ($callback) {
                foreach ($callback as $item) {
                    call_user_func($item, ...$args);
                }
            });
        }
        //注册子服务的事件回调
        foreach ($this->subServer as $serverName => $subPort) {
            $events = $this->subServerRegister[$serverName]['eventRegister']->all();
            foreach ($events as $event => $callback) {
                $subPort->on($event, function (...$args) use ($callback) {
                    foreach ($callback as $item) {
                        call_user_func($item, ...$args);
                    }
                });
            }
        }
        $this->isStart = true;
        $this->getSwooleServer()->start();
    }

    function isStart(): bool {
        return $this->isStart;
    }
}