<?php

namespace Scf\Core\Filters;

use Filterus\Filter;

/** 为 Struct 的 object 规则保留对象类型和可选类约束。 */
class ObjectValue extends Filter {
    protected $defaultOptions = ['class' => ''];

    public function filter($var): ?object {
        $class = $this->options['class'];
        return is_object($var) && (!$class || $var instanceof $class) ? $var : null;
    }

    public function validate($var): bool {
        return $var === $this->filter($var);
    }
}
