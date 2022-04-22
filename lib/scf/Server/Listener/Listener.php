<?php

namespace Scf\Server\Listener;

use Scf\Helper\StringHelper;
use Scf\Server\Http;
use Swoole\WebSocket\Server;

class Listener {
    protected static Server $server;

    public static function bind(...$events) {
        $handler = new static();
        self::$server = Http::master();
        foreach ($events as $event) {
            $action = 'on' . StringHelper::lower2camel($event);
            self::$server->on($event, function (...$args) use ($handler, $action) {
                call_user_func([$handler, $action], ...$args);
            });
        }
    }

}