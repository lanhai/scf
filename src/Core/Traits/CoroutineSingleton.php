<?php

namespace Scf\Core\Traits;

use Swoole\Coroutine;

/**
 * 协程单例
 */
trait CoroutineSingleton {
    private static array $_instances = [];

    /**
     * @param mixed ...$args
     * @return static
     */
    public static function instance(...$args): static {
        $class = static::class;
        $cid = Coroutine::getCid();
        if (!isset(static::$_instances[$class . '_' . $cid])) {
            static::$_instances[$class . '_' . $cid] = new static(...$args);
            if ($cid > 0) {
                Coroutine::defer(function () use ($cid, $class) {
                    unset(static::$_instances[$class . '_' . $cid]);
                });
            }
        }
        return static::$_instances[$class . '_' . $cid];
    }

    public function destroy(int $cid = null): void {
        $class = static::class;
        if ($cid === null) {
            $cid = Coroutine::getCid();
        }
        unset(static::$_instances[$class . '_' . $cid]);
    }
}