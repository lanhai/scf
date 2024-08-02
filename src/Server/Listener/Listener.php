<?php

namespace Scf\Server\Listener;

use Scf\Server\Http;
use Swoole\WebSocket\Server;
use Swoole\Http\Server as HttpServer;

class Listener {
    protected Server|HttpServer $server;

    public function __construct($server) {
        $this->server = $server;
    }

    public static function register($handlers): void {
        $arr = explode("\\", static::class);
        array_pop($arr);
        foreach ($handlers as $listener) {
            $cls = implode("\\", $arr) . "\\" . $listener;
            class_exists($cls) and call_user_func($cls . '::bind');
        }
    }

    public static function bind(): void {
        $server = Http::server();
        $handler = new static($server);
        $methods = get_class_methods($handler);
        foreach ($methods as $action) {
            if (!str_starts_with($action, 'on')) {
                continue;
            }
            $server->on(substr($action, 2), function (...$args) use ($handler, $action) {
                call_user_func([$handler, $action], ...$args);
            });
        }
    }

}