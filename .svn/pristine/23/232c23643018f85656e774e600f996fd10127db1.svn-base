<?php

namespace Scf\Mode\Web\Request;

use Scf\Core\ComponentTrait;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;

const _MOBILE = '__MOBILE__';
const _EMAIL = '__EMAIL__';
const _REQUIRED = '__REQUIRED__';
const _NUMBER = '__NUMBER__';
const _ARRAY = '__ARRAY__';
const _JSON = '__JSON__';
const _STRING = '__STRING__';
const _ENUM = '__ENUM__';

class Verify {
    use ComponentTrait;

    protected string $filter;
    protected string $message;
    protected int|null $min = null;
    protected int|null $max = null;
    protected array $enumCases = [];

    /**
     * @param string|null $msg
     * @return Verify
     */
    public static function required(string $msg = null): Verify {
        $msg = $msg ?: '参数错误';
        return static::factory()->setFilter(_REQUIRED, $msg);
    }

    /**
     * @param $rule
     * @param $msg
     * @return Verify
     */
    public static function match($rule, $msg): Verify {
        return static::factory()->setFilter($rule, $msg);
    }

    public static function number($msg, $min = null, $max = null): Verify {
        return static::factory()->setFilter(_NUMBER, $msg, $min, $max);
    }

    public static function email($msg = null): Verify {
        return static::factory()->setFilter(_EMAIL, $msg ?: '邮箱地址格式错误');
    }

    public static function mobile($msg = null): Verify {
        return static::factory()->setFilter(_MOBILE, $msg ?: '手机号码格式错误');
    }

    public static function arr($msg): Verify {
        return static::factory()->setFilter(_ARRAY, $msg);
    }

    public static function json($msg): Verify {
        return static::factory()->setFilter(_JSON, $msg);
    }

    public static function string($msg, $min = null, $max = null): Verify {
        return static::factory()->setFilter(_STRING, $msg, $min, $max);
    }

    public static function enum($cases, $msg): Verify {
        return static::factory()->setFilter(_ENUM, $msg, enumCases: $cases);
    }

    /**
     * @param $val
     * @return bool
     */
    public function isValid($val): bool {
        return match ($this->filter) {
            _REQUIRED => !is_null($val) && $val != '',
            _NUMBER => $this->verifyNumber($val),
            _EMAIL => StringHelper::isEmailAddress($val),
            _MOBILE => StringHelper::is_mobile_number($val),
            _ARRAY => is_array($val),
            _JSON => JsonHelper::is($val),
            _STRING => $this->verifyString($val),
            _ENUM => $this->verifyEnmu($val),
            default => true,
        };
    }

    protected function verifyEnmu($val): bool {
        return in_array($val, $this->enumCases);

    }

    protected function verifyString($val): bool {
        if (!is_string($val) || (!is_null($this->min) && mb_strlen($val) < $this->min) || (!is_null($this->max) && mb_strlen($val) > $this->max)) {
            return false;
        }
        return true;
    }

    protected function verifyNumber($val): bool {
        if (!is_numeric($val) || (!is_null($this->min) && $val < $this->min) || (!is_null($this->max) && $val > $this->max)) {
            return false;
        }
        return true;
    }

    /**
     * @param $filter
     * @param $msg
     * @param int|null $min
     * @param int|null $max
     * @param array $enumCases
     * @return $this
     */
    protected function setFilter($filter, $msg, int $min = null, int $max = null, array $enumCases = []): static {
        $this->filter = $filter;
        $this->message = $msg;
        $this->min = $min;
        $this->max = $max;
        $this->enumCases = $enumCases;
        return $this;
    }

    /**
     * @return string
     */
    public function getMessage(): string {
        return $this->message;
    }
}