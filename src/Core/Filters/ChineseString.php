<?php

namespace Scf\Core\Filters;

use Filterus\Filter;

class ChineseString extends Filter {

    protected $defaultOptions = array(
        'min' => 0,
        'max' => PHP_INT_MAX,
    );

    public function filter($var): ?string {
        if (is_object($var) && method_exists($var, '__toString')) {
            $var = (string)$var;
        }
        if (!is_scalar($var)) {
            return null;
        }
        $var = (string)$var;
        if ($this->options['min'] > mb_strlen($var)) {
            return null;
        } elseif ($this->options['max'] < mb_strlen($var)) {
            return null;
        }
        return $var;
    }

    public function validate($var): bool {
        if (is_object($var) && method_exists($var, '__toString')) {
            $var = (string)$var;
        }
        return parent::validate($var);
    }
}