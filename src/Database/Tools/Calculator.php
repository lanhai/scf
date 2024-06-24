<?php

namespace Scf\Database\Tools;

class Calculator {

    protected int $incValue = 0;
    protected int $redValue = 0;

    /**
     * @param mixed ...$args
     * @return static
     */
    static function instance(...$args): static {
        return new static(...$args);
    }

    protected function _increase(int $value): static {
        $this->incValue = $value;
        return $this;
    }

    protected function _reduce(int $value): static {
        $this->redValue = $value;
        return $this;
    }

    public function getIncValue(): int {
        return $this->incValue;
    }

    public function getRedValue(): int {
        return $this->redValue;
    }

    public static function increase(int $value = 1): Calculator {
        return self::instance()->_increase($value);
    }

    public static function reduce(int $value = 1): Calculator {
        return self::instance()->_reduce($value);
    }


}