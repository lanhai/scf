<?php

namespace Scf\Server\Event;


class Helper {
    public static function register(Register $register, string $event, callable $callback): void {
        $register->set($event, $callback);
    }

    public static function registerWithAdd(Register $register, string $event, callable $callback): void {
        $register->add($event, $callback);
    }

    public static function on(\Swoole\Server $server, string $event, callable $callback) {
        $server->on($event, $callback);
    }
}