<?php

namespace Scf\Helper;

class JsonHelper {
    /**
     * 将json字符串还原为原始数据
     * @param $data
     * @return false|mixed
     */
    public static function recover($data): mixed {
        try {
            return json_decode($data, true);
        } catch (\Exception $exception) {
            return false;
        }
    }

    /**
     * 将数据转换为JSON格式
     * @param $data
     * @return bool|string
     */
    public static function toJson($data): bool|string {
        if (is_string($data)) {
            return $data;
        }
        return json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    /**
     * 判断是否是json数据
     * @param $data
     * @return bool
     */
    public static function is($data): bool {
        if (!is_string($data)) {
            return false;
        }
        if (function_exists('json_validate')) {
            return json_validate($data);
        }
        try {
            json_decode($data);
            return (json_last_error() == JSON_ERROR_NONE);
        } catch (\Exception $exception) {
            return false;
        }
    }
}