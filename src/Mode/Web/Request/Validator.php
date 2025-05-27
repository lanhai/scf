<?php

namespace Scf\Mode\Web\Request;

use Scf\Core\Traits\ComponentTrait;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;


class Validator {
    use ComponentTrait;

    const _MOBILE = '__MOBILE__';
    const _EMAIL = '__EMAIL__';
    const _REQUIRED = '__REQUIRED__';
    const _NUMBER = '__NUMBER__';
    const _ARRAY = '__ARRAY__';
    const _JSON = '__JSON__';
    const _STRING = '__STRING__';
    const _ENUM = '__ENUM__';
    const _CODE = '__CODE__';
    protected string $filter;
    protected string $message;
    protected int|null $min = null;
    protected int|null $max = null;
    protected array $enumCases = [];
    protected bool $required = true;

    /**
     * @param string|null $msg
     * @return Validator
     */
    public static function required(string $msg = null): Validator {
        $msg = $msg ?: '参数错误';
        return static::factory()->setFilter(self::_REQUIRED, $msg);
    }

    /**
     * @param $rule
     * @param $msg
     * @param bool $required
     * @return Validator
     */
    public static function match($rule, $msg, bool $required = true): Validator {
        return static::factory()->setFilter($rule, $msg, required: $required);
    }

    public static function number($msg, $min = null, $max = null, $required = true): Validator {
        return static::factory()->setFilter(self::_NUMBER, $msg, $min, $max, required: $required);
    }

    public static function email($msg = null, $required = true): Validator {
        return static::factory()->setFilter(self::_EMAIL, $msg ?: '邮箱地址格式错误', required: $required);
    }

    public static function mobile($msg = null, $required = true): Validator {
        return static::factory()->setFilter(self::_MOBILE, $msg ?: '手机号码格式错误', required: $required);
    }

    public static function arr($msg, $required = true): Validator {
        return static::factory()->setFilter(self::_ARRAY, $msg, required: $required);
    }

    public static function json($msg, $required = true): Validator {
        return static::factory()->setFilter(self::_JSON, $msg, required: $required);
    }

    public static function string($msg, $min = null, $max = null, $required = true): Validator {
        return static::factory()->setFilter(self::_STRING, $msg, $min, $max, required: $required);
    }

    public static function enum($cases, $msg, $required = true): Validator {
        return static::factory()->setFilter(self::_ENUM, $msg, enumCases: $cases, required: $required);
    }

    public static function code($msg = '请输入正确的富文本内容', $required = false): Validator {
        //TODO 验证不支持的代码标签
        return static::factory()->setFilter(self::_CODE, $msg, required: $required);
    }

    /**
     * @param $val
     * @return bool
     */
    public function isValid($val): bool {
        return match ($this->filter) {
            self::_REQUIRED => !is_null($val) && $val !== '',
            self::_NUMBER => $this->verifyNumber($val),
            self::_EMAIL => StringHelper::isEmailAddress($val),
            self::_MOBILE => StringHelper::is_mobile_number($val),
            self::_ARRAY => is_array($val),
            self::_JSON => JsonHelper::is($val),
            self::_STRING => $this->verifyString($val),
            self::_ENUM => $this->verifyEnmu($val),
            self:: _CODE => $this->verifyCode($val),
            default => true,
        };
    }

    public function getFilter(): string {
        return $this->filter;
    }

    protected function verifyCode($val): bool {
        return true;
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
        if ((is_null($val) || $val === '') && $this->required) {
            return false;
        }
        if (($val !== "" && !is_null($val) && !is_numeric($val)) || ($val !== "" && !is_null($val) && !is_null($this->min) && $val < $this->min) || ($val !== "" && !is_null($val) && !is_null($this->max) && $val > $this->max)) {
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
     * @param bool $required
     * @return $this
     */
    protected function setFilter($filter, $msg, int $min = null, int $max = null, array $enumCases = [], bool $required = true): static {
        $this->filter = $filter;
        $this->message = $msg;
        $this->min = $min;
        $this->max = $max;
        $this->enumCases = $enumCases;
        $this->required = $required;
        return $this;
    }

    /**
     * @return string
     */
    public function getMessage(): string {
        return $this->message;
    }
}