<?php

namespace Scf\Helper;

class StringHelper {
    /**
     * 判断是否是邮件地址
     * @param $str
     * @return bool
     */
    public static function isEmailAddress($str): bool {
        if (empty($str)) {
            return false;
        }
        return !!preg_match("/^([a-z0-9\+_\-]+)(\.[a-z0-9\+_\-]+)*@([a-z0-9\-]+\.)+[a-z]{2,6}$/ix", $str);
    }

    /**
     * 判断是否URL
     * @param $url
     * @return bool
     */
    public static function isUrl($url): bool {
        $preg = "/http[s]?:\/\/[\w.]+[\w\/]*[\w.]*\??[\w=&\+\%]*/is";
        if (preg_match($preg, $url)) {
            return true;
        }
        return false;
    }

    /**
     * 验证手机号码格式是否正确
     * @param $number
     * @return bool
     */
    public static function is_mobile_number($number): bool {
        if (empty($number)) {
            return false;
        }
        if (preg_match("/^1[3456789]{1}\d{9}$/", $number)) {
            return true;
        }
        return false;
    }

    /**
     * 判断字符串是否经过编码方法
     * @param $str
     * @return bool
     */
    public static function is_base64($str): bool {
        if ($str == base64_encode(base64_decode($str))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 安全的base64编码
     * @param $string
     * @return bool|string
     */
    public static function urlsafe_b64encode($string): bool|string {
        $data = base64_encode($string);
        return str_replace(array('+', '/', '='), array('-', '_', ''), $data);
    }

    /**
     * 安全的base64解码
     * @param $string
     * @return bool|string
     */
    public static function urlsafe_b64decode($string): bool|string {
        $data = str_replace(array('-', '_'), array('+', '/'), $string);
        $mod4 = strlen($data) % 4;
        if ($mod4) {
            $data .= substr('====', $mod4);
        }
        return base64_decode($data);
    }

    /**
     * 判断是否是json数据
     * @param $string
     * @return bool
     */
    public static function isJson($string): bool {
        if (is_array($string)) {
            return false;
        }
        try {
            json_decode($string);
            return (json_last_error() == JSON_ERROR_NONE);
        } catch (\Exception $exception) {
            return false;
        }
    }

    /**
     * 变量替换
     * @param $tpl
     * @param array $vars
     * @return mixed
     */
    public static function varsTransform($tpl, array $vars = []): mixed {
        if (empty($vars))
            return $tpl;
        return str_replace(array_keys($vars), array_values($vars), $tpl);
    }

    /**
     * 把JAVA的驼峰风格转换成C的小写字母加下划线风格
     * @param $str
     * @return string
     */
    public static function camel2lower($str): string {
        return strtolower(trim(preg_replace("/[A-Z]/", "_\\0", $str), "_"));
    }

    /**
     * 把C的小写字母加下划线风格转换为JAVA的驼峰风格
     * @param $str
     * @return string
     */
    public static function lower2camel($str): string {
        return ucfirst(preg_replace_callback('/_([a-zA-Z])/', function ($match) {
            return strtoupper($match[1]);
        }, $str));
    }
}