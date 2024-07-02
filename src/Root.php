<?php

namespace Scf;

class Root {
    /**
     * 源码目录
     * @return string
     */
    public static function dir(): string {
        return __DIR__;
    }

    public static function root(): string {
        return dirname(__DIR__);
    }

}