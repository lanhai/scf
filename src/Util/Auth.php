<?php

namespace Scf\Util;

use Scf\Core\App;
use Scf\Helper\StringHelper;

class Auth {

    /**
     * 字符串可逆加密
     * @param $data
     * @param null $key
     * @return string|array|bool
     */
    public static function encode($data, $key = null): string|array|bool {
        return self::encrypt($data, 'E', $key);
    }

    /**
     * 字符串解密
     * @param $data
     * @param null $key
     * @return string|array|bool
     */
    public static function decode($data, $key = null): string|array|bool {
        return self::encrypt($data, 'D', $key);
    }

    /**
     * 函数作用:加密解密字符串
     * @param mixed $string 需要加密解密的字符串
     * @param string $action 判断是加密还是解密:E:加密;D:解密
     * @param null $key
     * @return array|bool|string
     */
    private static function encrypt(mixed $string, string $action, $key = null): array|bool|string {
        $secretKey = !is_null($key) ? $key : (APP_AUTH_KEY ?: App::authKey());
        $secretIv = 'LKYAPP';
        if (is_array($string)) {
            $result = array();
            foreach ($string as $key => $val)
                $result[$key] = self::encrypt($action, $val, "");
            return $result;
        } else {
            $output = false;
            $encrypt_method = "AES-256-CBC";
            $secret_key = $secretKey . "";
            $secret_iv = $secretIv;
            $key = hash('sha256', $secret_key);
            $iv = substr(hash('sha256', $secret_iv), 0, 16);
            if ($action == 'E') {
                $output = openssl_encrypt($string, $encrypt_method, $key, 0, $iv);
                $output = StringHelper::urlsafe_b64encode($output);
                $eski = array("+", "/", "=");
                $yeni = array("b1X4", "x8V7", "F3h7");
                $output = str_replace($eski, $yeni, $output);
            } elseif ($action == 'D') {
                $eski = array("b1X4", "x8V7", "F3h7");
                $yeni = array("+", "/", "=");
                if (!$string) {
                    return false;
                }
                $string = str_replace($eski, $yeni, $string);
                $output = @openssl_decrypt(StringHelper::urlsafe_b64decode($string), $encrypt_method, $key, 0, $iv);
            }
            return $output;
        }
    }
}