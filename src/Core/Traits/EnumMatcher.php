<?php

namespace Scf\Core\Traits;

use Scf\Mode\Web\Request\Validator;

trait EnumMatcher {

    /**
     * @param $value
     * @param string $type
     * @return null|static
     */
    public static function match($value, string $type = 'value'): null|static {
        $matched = null;
        array_map(
            function (self $e) use ($value, $type, &$matched) {
                if ($value == ($type == 'value' ? $e->value : $e->name())) {
                    $matched = $e;
                }
            },
            static::cases()
        );
        return $matched;
    }

    /**
     * 输入验证
     * @param $msg
     * @return Validator
     */
    public static function verify($msg): Validator {
        $cases = [];
        array_map(
            function (self $e) use (&$cases) {
                $cases[] = $e->value;
            },
            static::cases()
        );
        return Validator::enum($cases, $msg);
    }

    /**
     * @return string|int
     */
    public function val(): string|int {
        return $this->value;
    }

    /**
     * @return string
     */
    public function name(): string {
        return $this->name;
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