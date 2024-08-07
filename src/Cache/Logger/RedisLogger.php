<?php

namespace Scf\Cache\Logger;

use Mix\Redis\LoggerInterface;
use Scf\Core\Console;
use Scf\Server\Http;
use Scf\Server\Worker\ProcessLife;
use Throwable;

class RedisLogger implements LoggerInterface {
    /**
     * redis执行日志
     * @param float $time
     * @param string $cmd
     * @param array $args
     * @param Throwable|null $exception
     * @return void
     */
    public function trace(float $time, string $cmd, array $args, ?Throwable $exception): void {
        if (!is_null(Http::server())) {
            ProcessLife::instance()->addRedis("{$cmd} {$args[0]} " . ($args[1] ?? "") . "【{$time}】ms");
        }
        PRINT_REDIS_LOG and Console::info("【RedisLogger】{$cmd} {$args[0]} " . ($args[1] ?? "") . "【{$time}】ms");
        if (!is_null($exception)) {
            Console::error("【RedisLogger】{$cmd} {$args[0]} " . ($args[1] ?? " ") . "[{$exception->getMessage()}]" . ";file:" . $exception->getLine() . "@" . $exception->getFile());
        }
    }
}