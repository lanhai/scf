<?php

namespace Scf\Core;

use Swoole\Coroutine;

abstract class CoroutineComponent {
    use ComponentTrait;

    /**
     * 获取单利
     * @param array|null $conf
     * @return static
     */
    final public static function instance(array $conf = null): static {
        $class = get_called_class();
        $cid = Coroutine::getCid();
        if (!isset(static::$_instances[$class . '_' . $cid])) {
            static::$_instances[$class . '_' . $cid] = new $class($conf);
            if ($cid > 0) {
                Coroutine::defer(function () use ($class, $cid) {
                    unset(static::$_instances[$class . '_' . $cid]);
                });
            }
        }
        return static::$_instances[$class . '_' . $cid];
    }
}
