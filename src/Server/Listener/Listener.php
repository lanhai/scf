<?php

namespace Scf\Server\Listener;

use Scf\Server\Http;

class Listener {

    public static function register($handlers): void {
        $arr = explode("\\", static::class);
        array_pop($arr);
        foreach ($handlers as $listener) {
            $cls = implode("\\", $arr) . "\\" . $listener;
            class_exists($cls) and call_user_func($cls . '::bind');
        }
    }

    public static function bind(): void {
        $handler = new static();
        $methods = get_class_methods($handler);
        $server = Http::server();
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