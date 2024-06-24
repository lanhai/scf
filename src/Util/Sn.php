<?php

namespace Scf\Util;

class Sn {
    /**
     * 创建一个订单号
     * @param string $prefix
     * @return string
     */
    public static function create(string $prefix = ''): string {
        $rel = strtoupper($prefix) . date('YmdHis') . explode('.', microtime('msec'))[1] . rand(1000, 9999);
        if (strlen($rel) < 22) {
            $n = 22 - strlen($rel);
            $rel .= str_repeat('0', $n);
        }
        return $rel;
    }

    /**
     * 生成guid
     * @return string
     */
    public static function create_guid(): string {
        if (function_exists('com_create_guid') === true) {
            return trim(com_create_guid(), '{}');
        }
        return sprintf('%04X%04X-%04X-%04X-%04X-%04X%04X%04X', mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(16384, 20479), mt_rand(32768, 49151), mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(0, 65535));
    }

    /**
     * 生成uuid
     * @param string $prefix
     * @return string
     */
    public static function create_uuid(string $prefix = ""): string { //可以指定前缀
        $str = md5(uniqid(mt_rand(), true));
        //$uuid  = substr($str,0,8) . '-';
        //$uuid .= substr($str,8,4) . '-';
        //$uuid .= substr($str,12,4) . '-';
        //$uuid .= substr($str,16,4) . '-';
        //$uuid .= substr($str,20,12);
        $uuid = $str;
        return $prefix . $uuid;
    }
}