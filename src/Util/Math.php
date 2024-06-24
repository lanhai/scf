<?php

namespace Scf\Util;

use Brick\Math\BigDecimal;
use Brick\Math\BigNumber;
use Brick\Math\RoundingMode;

class Math {
    /**
     * @param string $num1
     * @param string $num2
     * @param int $scale
     * @return string
     */
    public static function add(string $num1, string $num2, int $scale = 0): string {
        return BigDecimal::of($num1)->plus($num2)->toScale($scale, RoundingMode::HALF_DOWN);
    }

    /**
     * @param string $num1
     * @param string $num2
     * @param int $scale
     * @return string
     */
    public static function sub(string $num1, string $num2, int $scale = 0): string {
        return BigDecimal::of($num1)->minus($num2)->toScale($scale, RoundingMode::HALF_DOWN);
    }

    /**
     * @param string $num1
     * @param string $num2
     * @param int $scale
     * @return string
     */
    public static function mul(string $num1, string $num2, int $scale = 0): string {
        return BigDecimal::of($num1)->multipliedBy($num2)->toScale($scale, RoundingMode::HALF_DOWN);
    }

    /**
     * @param string $num1
     * @param string $num2
     * @param int $scale
     * @return string|null
     */
    public static function div(string $num1, string $num2, int $scale = 0): ?string {
        if ((int)$num1 === 0) {
            return BigDecimal::zero();
        }
        return BigDecimal::of($num1)->dividedBy($num2, $scale, RoundingMode::HALF_DOWN);
    }

    /**
     * @param $value
     * @return BigDecimal|BigNumber
     */
    public static function of($value): BigDecimal|BigNumber {
        return BigDecimal::of($value);
    }

}