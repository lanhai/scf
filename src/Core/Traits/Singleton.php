<?php

namespace Scf\Core\Traits;


trait Singleton {
    private static array $_instances;

    /**
     * @param mixed ...$args
     * @return static
     */
    static function instance(...$args): static {
        $class = static::class;
        if (!isset(static::$_instances[$class])) {
            self::$_instances[$class] = new static(...$args);
        }
        return static::$_instances[$class];
    }

    static function getInstance(...$args):static {
        return self::instance(...$args);
    }
}