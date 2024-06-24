<?php

namespace Scf\Util;

use Scf\Core\Console;

class Des {
    /**
     * 解密
     * @param $ciphertext
     * @param $key
     * @param string $method
     * @param string $iv
     * @param bool $pad
     * @return string
     */
    public static function decrypt($ciphertext, $key, string $method = 'DES-CBC', string $iv = "", bool $pad = false): string {
        $decrypted = openssl_decrypt(base64_decode($ciphertext), $method, $key, OPENSSL_RAW_DATA, $iv);
        if ($decrypted === false) {
            Console::error("解密错误: " . openssl_error_string());
        }
        $pad and $decrypted = self::pkcsUnpad($decrypted);
        return $decrypted;
    }

    /**
     * 加密
     * @param $plaintext
     * @param $key
     * @param string $method
     * @param string $iv
     * @param bool $pad
     * @return string
     */
    public static function encrypt($plaintext, $key, string $method = 'DES-CBC', string $iv = "", bool $pad = false): string {
        $pad and $plaintext = self::pkcsPad($plaintext, openssl_cipher_iv_length($method));
        $encrypted = openssl_encrypt($plaintext, $method, $key, OPENSSL_RAW_DATA, $iv);
        if ($encrypted === false) {
            Console::error("加密错误: " . openssl_error_string());
        }
        return base64_encode($encrypted);  // 将加密的数据编码为Base64，方便存储和传输
    }

    private static function pkcsPad($text, $blocksize): string {
        $pad = $blocksize - (strlen($text) % $blocksize);
        return $text . str_repeat(chr($pad), $pad);
    }

    private static function pkcsUnpad($text): bool|string {
        $pad = ord($text[strlen($text) - 1]);
        if ($pad < 1 || $pad > strlen($text)) {
            return $text;  // 没有Padding，或Padding无效
        }
        return substr($text, 0, -1 * $pad);
    }

}