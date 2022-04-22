<?php

namespace Scf\Core;


trait Instance {
    private static $instance;

    /**
     * @param mixed ...$args
     * @return static
     */
    static function instance(...$args): static {
        if (!isset(static::$instance)) {
            static::$instance = new static(...$args);
        }
        return static::$instance;
    }
}