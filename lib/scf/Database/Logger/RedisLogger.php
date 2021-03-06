<?php

namespace Scf\Database\Logger;

use Mix\Redis\LoggerInterface;
use Scf\Mode\Web\Log;
use Throwable;
use Scf\Helper\JsonHelper;

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
        $logger = Log::instance();
        if (!is_null($exception)) {
            $logger->error($exception);
        } else {
            $logger->isDebugEnable() and $logger->addRedis("{$cmd} {$args[0]} " . ($args[1] ?? "") . "【{$time}】ms");
        }
    }
}