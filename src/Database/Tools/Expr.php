<?php

namespace Scf\Database\Tools;

/**
 * Class Creater
 */
class Expr {

    /**
     * @var string
     */
    protected string $expr;

    /**
     * @var array
     */
    protected array $values;

    /**
     * Creater constructor.
     * @param string $expr
     * @param ...$values
     */
    public function __construct(string $expr, ...$values) {
        $this->expr = $expr;
        $this->values = $values;
    }

    /**
     * @return string
     */
    public function __toString(): string {
        $expr = $this->expr;
        foreach ($this->values as $value) {
            $expr = preg_replace('/\?/', is_string($value) ? "'%s'" : "%s", $expr, 1);
        }
        return vsprintf($expr, $this->values);
    }

}
