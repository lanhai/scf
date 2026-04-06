<?php

namespace Scf\Cache\Logger;

use Mix\Redis\LoggerInterface;
use Scf\Core\Console;
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
        $argSummary = $this->buildArgSummary($args);
        if (ProcessLife::enabled()) {
            ProcessLife::instance()->addRedis("{$cmd} {$argSummary}", $time);
        }
        PRINT_REDIS_LOG and Console::info("【Redis】{$cmd} {$argSummary}t={$time}ms");
        if (!is_null($exception)) {
            Console::error("【Redis】{$cmd} {$argSummary}[{$exception->getMessage()}]" . ";file:" . $exception->getLine() . "@" . $exception->getFile(), false);
        }
    }

    /**
     * 把 Redis 命令参数压成可安全输出的单行摘要。
     *
     * logger 不能假设参数永远是标量；像 HMGET 这类命令会把 field 数组整体透传到底层。
     * 这里只做日志用途的轻量序列化，避免数组直接拼接触发 “Array to string conversion”。
     *
     * @param array $args
     * @return string
     */
    protected function buildArgSummary(array $args): string {
        $parts = array_map(function ($arg): string {
            if (is_array($arg)) {
                $json = json_encode($arg, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                return is_string($json) ? $json : '[array]';
            }
            if (is_bool($arg)) {
                return $arg ? 'true' : 'false';
            }
            if ($arg === null) {
                return 'null';
            }
            if (is_scalar($arg)) {
                return (string)$arg;
            }
            return '[' . get_debug_type($arg) . ']';
        }, $args);
        return trim(implode(' ', $parts)) . ' ';
    }
}
