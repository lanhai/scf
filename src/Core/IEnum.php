<?php

namespace Scf\Core;

interface IEnum {
    public static function match($value);

    public static function verify($msg);

    public function val();

    public function name();

    public function get();

    public function is($val);
}