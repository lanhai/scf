<?php

namespace Scf\Util;

class Pay {
    /**
     * 将元转换为分
     * @param $num
     * @return float|int
     */
    public static function yuan2fen($num): float|int {
        return intval(ceil(sprintf("%.2f", $num) * 100));
    }

    /**
     * 分转为元
     * @param int $amount
     * @return float|null
     */
    public static function fen2yuan(int $amount): ?float {
        return floatval(Math::div($amount, 100, 2));
    }
}