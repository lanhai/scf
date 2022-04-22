<?php

namespace Scf\Coroutine;

use Scf\Core\ComponentTrait;
use Scf\Core\Config;
use Swoole\Coroutine;

abstract class Component {
    use ComponentTrait;

    /**
     * 获取单利
     * @param array|null $conf
     * @return static
     */
    final public static function instance(array $conf = null): static {
        $class = get_called_class();
        $cid = Coroutine::getCid();
        if (!isset(self::$_instances[$class . '_' . $cid])) {
            $_configs = Config::get('components');
            $config = is_array($conf) ? $conf : ($_configs[$class] ?? []);
            self::$_instances[$class . '_' . $cid] = new $class($config);
            if ($cid > 0) {
                Coroutine::defer(function () use ($class, $cid) {
                    unset(self::$_instances[$class . '_' . $cid]);
                });
            }
        }
        return self::$_instances[$class . '_' . $cid];
    }

    /**
     * 释放协程单例主键
     * @param int|null $cid
     * @return void
     */
    public function destroy(int $cid = null) {
        $class = get_called_class();
        if ($cid === null) {
            $cid = Coroutine::getCid();
        }
        unset(self::$_instances[$class . '_' . $cid]);
    }
}
