<?php

namespace Scf\Core;

use JetBrains\PhpStorm\NoReturn;
use JetBrains\PhpStorm\Pure;
use Scf\Mode\Web\Response;

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
        $class = get_called_class();
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
     * @param $message
     * @param string $code
     * @param string|array $data
     * @return Result|static
     */
    public static function error($message, string $code = 'SERVICE_ERROR', string|array $data = ''): Result|static {
        return self::factory(self::output()->error($message, $code, $data));
    }

    /**
     * @param $content
     * @return mixed
     */
    public static function raw($content): mixed {
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
            return $this->data['data'][$key] ?? "";
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