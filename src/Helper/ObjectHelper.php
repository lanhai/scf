<?php

namespace Scf\Helper;

class ObjectHelper {
    /**
     * 将std obj数组转换为数组
     * @param $array
     * @return mixed
     */
    public static function toArray($array): mixed {
        if (is_object($array)) {
            $array = (array)$array;
        }
        if (is_array($array)) {
            foreach ($array as $key => $value) {
                if ($value == "") {
                    $array[$key] = "";
                } else {
                    $array[$key] = self::toArray($value);
                }
            }
        }
        return $array;
    }
}