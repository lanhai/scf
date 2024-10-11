<?php

namespace Scf\Cloud\Wx\Util;


/**
 * 微信公众平台消息加密/解密工具类
 */
class MsgCrypt {

    private string $token;
    private string $encodingAesKey;
    private string $appId;

    /**
     * 构造函数
     * @param $token string 公众平台上，开发者设置的token
     * @param $encodingAesKey string 公众平台上，开发者设置的EncodingAESKey
     * @param $appId string 公众平台的appId
     */
    public function __construct(string $token, string $encodingAesKey, string $appId) {
        $this->token = $token;
        $this->encodingAesKey = $encodingAesKey;
        $this->appId = $appId;
    }

    /**
     * 将公众平台回复用户的消息加密打包.
     * <ol>
     *    <li>对要发送的消息进行AES-CBC加密</li>
     *    <li>生成安全签名</li>
     *    <li>将消息密文和安全签名打包成xml格式</li>
     * </ol>
     *
     * @param $replyMsg string 公众平台待回复用户的消息，xml格式的字符串
     * @param $timeStamp string 时间戳，可以自己生成，也可以用URL参数的timestamp
     * @param $nonce string 随机串，可以自己生成，也可以用URL参数的nonce
     * @param &$encryptMsg string 加密后的可以直接回复用户的密文，包括msg_signature, timestamp, nonce, encrypt的xml格式的字符串,
     *                      当return返回0时有效
     *
     * @return int 成功0，失败返回对应的错误码
     */
    public function encryptMsg(string $replyMsg, string $timeStamp, string $nonce, string &$encryptMsg): int {
        $pc = new Prpcrypt($this->encodingAesKey);
        //加密
        $array = $pc->encrypt($replyMsg, $this->appId);
        $ret = $array[0];
        if ($ret != 0) {
            return $ret;
        }

        if ($timeStamp == null) {
            $timeStamp = time();
        }
        $encrypt = $array[1];
        //生成安全签名
        $sha1 = new SHA1;
        $array = $sha1->getSHA1($this->token, $timeStamp, $nonce, $encrypt);
        $ret = $array[0];
        if ($ret != 0) {
            return $ret;
        }
        //生成发送的xml
        $xmlparse = new XMLParse;
        $encryptMsg = $xmlparse->generate($encrypt, $array[1], $timeStamp, $nonce);
        return ErrorCode::$OK;
    }

    /**
     * 检验消息的真实性，并且获取解密后的明文.
     * <ol>
     *    <li>利用收到的密文生成安全签名，进行签名验证</li>
     *    <li>若验证通过，则提取xml中的加密消息</li>
     *    <li>对消息进行解密</li>
     * </ol>
     * @param $msgSignature string 签名串，对应URL参数的msg_signature
     * @param $timestamp string 时间戳 对应URL参数的timestamp
     * @param $nonce string 随机串，对应URL参数的nonce
     * @param $postData string 密文，对应POST请求的数据
     * @param string|null $msg 解密后的原文，当return返回0时有效
     * @return int 成功0，失败返回对应的错误码
     */
    public function decryptMsg(string $msgSignature, string $timestamp, string $nonce, string $postData, string &$msg = null): int {
        if (strlen($this->encodingAesKey) != 43) {
            return ErrorCode::$IllegalAesKey;
        }
        //提取密文
        $xmlparse = new XMLParse;
        $array = $xmlparse->extract($postData);
        $ret = $array[0];
        if ($ret != 0) {
            return $ret;
        }
        $encrypt = $array[1];
        //$touser_name = $array[2];
        //验证安全签名
        $sha1 = new SHA1;
        $array = $sha1->getSHA1($this->token, $timestamp, $nonce, $encrypt);
        $ret = $array[0];
        if ($ret != 0) {
            return $ret;
        }
        if ($array[1] != $msgSignature) {
            return ErrorCode::$ValidateSignatureError;
        }
        $pc = new Prpcrypt($this->encodingAesKey);
        $result = $pc->decrypt($encrypt, $this->appId);
        if ($result[0] != 0) {
            return $result[0];
        }
        $msg = $result[1];
        return ErrorCode::$OK;
    }

    /**
     * 生成随机字符串
     * @return string
     */
    public function CreateNonce(): string {
        $originalcode = "1235467890QWERTYUIPKJHGFDSAZXCVBNMqwertyuipkjhgfdaszxcvbnm";
        $countdistrub = strlen($originalcode);
        $_dscode = "";
        for ($j = 0; $j < 9; $j++) {
            $dscode = $originalcode[rand(0, $countdistrub - 1)];
            $_dscode .= $dscode;
        }
        return $_dscode;
    }
}
