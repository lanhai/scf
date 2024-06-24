<?php

namespace Scf\Component\Coroutine;

use Scf\Core\Coroutine\Component;
use Scf\Core\Result;

class Rsa extends Component {

    public ?string $publicKey = null;
    public ?string $privateKey = null;

    /**
     * 使用公钥加密数据
     * @param $originalData
     * @return Result
     */
    public function sign($originalData): Result {
        if (!$this->privateKey) {
            return Result::error('错误的私钥', 'PRIVATE_KEY_NOT_INCORRECT');
        }
        try {
            openssl_sign($originalData, $sign, $this->privateKey, OPENSSL_ALGO_SHA256);
            return Result::success(base64_encode($sign));

        } catch (\Exception $exception) {
            return Result::error('加密失败:' . $exception->getMessage(), 500);
        }
    }

    /**
     * 验签
     * @param $data
     * @param $sign
     * @return Result
     */
    public function checkSign($data, $sign): Result {
        if (!$this->publicKey) {
            return Result::error('错误的公钥', 'PUBLIC_KEY_NOT_INCORRECT');
        }
        try {
            $result = (bool)openssl_verify($data, base64_decode($sign), $this->publicKey, OPENSSL_ALGO_SHA256);
            return Result::success($result);
        } catch (\Exception $exception) {
            return Result::error('验签失败:' . $exception->getMessage(), 500);
        }
    }

    /**
     * 使用公钥加密数据
     * @param $originalData
     * @return Result
     */
    public function encrypt($originalData): Result {
        if (!$this->publicKey) {
            return Result::error('错误的公钥', 'PUBLIC_KEY_NOT_INCORRECT');
        }
        try {
            $crypto = '';
            foreach (str_split($originalData, 117) as $chunk) {
                openssl_public_encrypt($chunk, $encryptData, $this->publicKey);
                $crypto .= $encryptData;
            }
            return Result::success(base64_encode($crypto));
        } catch (\Exception $exception) {
            return Result::error('加密失败:' . $exception->getMessage(), 500);
        }

    }

    /**
     * 使用私钥解密
     * @param $encryptData
     * @return Result
     */
    public function decrypt($encryptData): Result {
        if (!$this->privateKey) {
            return Result::error('错误的私钥', 'PRIVATE_KEY_NOT_INCORRECT');
        }
        try {
            $crypto = '';
            foreach (str_split(base64_decode($encryptData), 256) as $chunk) {
                openssl_private_decrypt($chunk, $decryptData, $this->privateKey);
                $crypto .= $decryptData;
            }
            return Result::success($crypto);
        } catch (\Exception $exception) {
            return Result::error('解密失败:' . $exception->getMessage(), 500);
        }
    }

    /**
     *字符串转十六进制函数
     * @pream string $str='abc';
     */
    public function toHex($str): string {
        $hex = "";
        for ($i = 0; $i < strlen($str); $i++)
            $hex .= dechex(ord($str[$i]));
        return strtoupper($hex);
    }
}