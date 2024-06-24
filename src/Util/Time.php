<?php

namespace Scf\Util;

use JetBrains\PhpStorm\Pure;

class Time {

    /**
     * 获取当前毫秒时间
     * @return int
     */
    public static function millisecond(): int {
        list($t1, $t2) = explode(' ', microtime());
        return (int)sprintf('%.0f', (floatval($t1) + floatval($t2)) * 1000);
    }

    /**
     * @param string $format
     * @return string
     */
    #[Pure(true)] public static function now(string $format = 'm-d H:i:s'): string {
        return date($format) . "." . substr(self::millisecond(), -3);
    }

}