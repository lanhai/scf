<?php

namespace Scf\Cloud\Wx\Util;

/**
 * 微信公众平台消息加密/解密工具类
 */
class Prpcrypt {

    public string|false $key;

    public function __construct($k) {
        $this->key = base64_decode($k . "=");
    }

    /**
     * 对明文进行加密
     * @param string $text 需要加密的明文
     * @return array 加密后的密文
     */
    public function encrypt(string $text, $appid): array {
        try {
            $random = $this->getRandomStr();
            $text = $random . pack("N", strlen($text)) . $text . $appid;
            $iv = substr($this->key, 0, 16);
            $pkc_encoder = new PKCS7Encoder;
            $text = $pkc_encoder->encode($text);
            $encrypted = openssl_encrypt($text, 'AES-256-CBC', $this->key, OPENSSL_RAW_DATA | OPENSSL_ZERO_PADDING, $iv);
            //使用BASE64对加密后的字符串进行编码
            return array(ErrorCode::$OK, base64_encode($encrypted));
        } catch (\Exception $e) {
            //print $e;
            return array(ErrorCode::$EncryptAESError, null);
        }
    }

    /**
     * 对密文进行解密
     * @param string $encrypted 需要解密的密文
     * @param $appid
     * @return array|string 解密得到的明文
     */
    public function decrypt(string $encrypted, $appid): array|string {

        try {
            $ciphertext_dec = base64_decode($encrypted);
            $iv = substr($this->key, 0, 16);
            $decrypted = openssl_decrypt($ciphertext_dec, 'AES-256-CBC', $this->key, OPENSSL_RAW_DATA | OPENSSL_ZERO_PADDING, $iv);
        } catch (\Exception $e) {
            return array(ErrorCode::$DecryptAESError, null);
        }

        try {
            //去除补位字符
            $pkc_encoder = new PKCS7Encoder;
            $result = $pkc_encoder->decode($decrypted);
            //去除16位随机字符串,网络字节序和AppId
            if (strlen($result) < 16) {
                return "";
            }
            $content = substr($result, 16, strlen($result));
            $len_list = unpack("N", substr($content, 0, 4));
            $xml_len = $len_list[1];
            $xml_content = substr($content, 4, $xml_len);
            $from_appid = substr($content, $xml_len + 4);
        } catch (\Exception $e) {
            //print $e;
            return array(ErrorCode::$IllegalBuffer, null);
        }
        if ($from_appid != $appid) {
            return array(ErrorCode::$ValidateAppidError, null);
        }
        return array(0, $xml_content);
    }

    /**
     * 随机生成16位字符串
     * @return string 生成的字符串
     */
    function getRandomStr() {

        $str = "";
        $str_pol = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
        $max = strlen($str_pol) - 1;
        for ($i = 0; $i < 16; $i++) {
            $str .= $str_pol[mt_rand(0, $max)];
        }
        return $str;
    }

}
