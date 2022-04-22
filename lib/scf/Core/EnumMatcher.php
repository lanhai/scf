<?php

namespace Scf\Core;

use Scf\Mode\Web\Request\Verify;

trait EnumMatcher {

    /**
     * @param $value
     * @return null|static
     */
    public static function match($value): null|static {
        $matched = null;
        array_map(
            function (self $e) use ($value, &$matched) {
                if ($value == $e->value) {
                    $matched = $e;
                }
            },
            static::cases()
        );
        return $matched;
    }

    public static function verify($msg): Verify {
        $cases = [];
        array_map(
            function (self $e) use (&$cases) {
                $cases[] = $e->value;
            },
            static::cases()
        );
        return Verify::enum($cases, $msg);
    }

    /**
     * @return string|int
     */
    public function val(): string|int {
        return $this->value;
    }

    /**
     * @return string|int
     */
    public function get(): string|int {
        return $this->value;
    }

    /**
     * @param $val
     * @return bool
     */
    public function is($val): bool {
        return $this->value === $val;
    }

}