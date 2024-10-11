<?php

namespace Scf\Cloud\Wx\Util;


use Scf\Helper\ArrayHelper;

class WxApi {

    /**
     * 创建微信jsapi接口签名串ticket
     * @param array $data
     * @return string
     */
    public static function create_jsapi_signature(array $data): string {
        foreach ($data as $k => $v) {
            $bizParameters[strtolower($k)] = $v;
        }
        ksort($bizParameters);
        $bizString = ArrayHelper::toQueryMap($bizParameters, false);
        return sha1($bizString);
    }

    /**
     * 创建微信api接口签名字符串ticket
     * @param array $data
     * @return string
     */
    public static function create_api_ticket_signature(array $data): string {
        foreach ($data as $k => $v) {
            $bizParameters[strtolower($k)] = $v;
        }
        sort($bizParameters);
        $bizString = implode('', $bizParameters);
        return sha1($bizString);
    }

    /**
     * 生成一个随机字符串
     * @param int $length 字符串长度
     * @return string
     */
    public static function create_noncestr(int $length = 16): string {
        $chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        $str = "";
        for ($i = 0; $i < $length; $i++) {
            $str .= substr($chars, mt_rand(0, strlen($chars) - 1), 1);
        }
        return md5($str);
    }
}
