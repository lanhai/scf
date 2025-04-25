<?php

namespace Scf\Core;

use JetBrains\PhpStorm\Pure;

class Result {
    private mixed $data;

    public function __construct($data) {
        if (!is_array($data)) {
            $data = [
                'errCode' => 0,
                'message' => 'success',
                'data' => $data
            ];
        } else {
            if (!isset($data['errCode']) || !isset($data['message'])) {
                $data['errCode'] = 'Exception';
                $data['message'] = '数据获取失败,格式错误';
            }
        }
        $this->data = $data;
    }

    /**
     * 结果输出工厂方法
     * @param $data
     * @return Result
     */
    public static function factory($data): static {
        $class = static::class;
        return new $class($data);
    }

    /**
     * @param mixed $data
     * @return Result|static
     */
    public static function success(mixed $data = ''): Result|static {
        return self::factory(self::output()->success($data));
    }

    /**
     * @param IEnum|string $message
     * @param string|array|int $code
     * @param string|array $data
     * @return Result|static
     */
    public static function error(IEnum|string $message, string|array|int $code = 'SERVICE_ERROR', string|array $data = ''): Result|static {
        if ($message instanceof IEnum) {
            $data = $code != 'SERVICE_ERROR' ? $code : '';
            $enum = $message;
            $message = $enum->val();
            $code = $enum->name();
        }
        return self::factory(self::output()->error($message, $code, $data));
    }

    /**
     * @param $content
     * @return string
     */
    public static function raw($content): string {
        return $content;
    }

    /**
     * @return Output
     */
    #[Pure] public static function output(): Output {
        return new Output();
    }

    /**
     * 获取数据结果
     * @param string|null $key
     * @return mixed
     */
    public function getData(string $key = null): mixed {
        if (!is_null($key)) {
            return $this->data['data'][$key] ?? null;
        }
        return $this->data['data'];
    }

    /**
     * 获取错误
     * @return bool
     */
    public function hasError(): bool {
        if (!empty($this->data['errCode'])) {
            return true;
        }
        return false;
    }

    /**
     * 获取结果信息
     * @return mixed
     */
    public function getMessage(): mixed {
        return $this->data['message'];
    }

    /**
     * 获取错误码
     * @return mixed
     */
    public function getErrCode(): mixed {
        return $this->data['errCode'];
    }
}