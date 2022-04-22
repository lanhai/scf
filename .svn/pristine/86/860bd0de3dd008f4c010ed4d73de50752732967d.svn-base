<?php

namespace Scf\Core;

abstract class Component {
    use ComponentTrait;

    /**
     * 获取单利
     * @param array|null $conf
     * @return static
     */
    final public static function instance(array $conf = null): static {
        $class = get_called_class();
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class($conf);
        }
        return self::$_instances[$class];
    }
}
