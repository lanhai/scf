<?php

namespace Scf\Cloud\Wx\Util;

/**
 * error code 说明.
 * <ul>
 *    <li>-40001: 签名验证错误</li>
 *    <li>-40002: xml解析失败</li>
 *    <li>-40003: sha加密生成签名失败</li>
 *    <li>-40004: encodingAesKey 非法</li>
 *    <li>-40005: appid 校验错误</li>
 *    <li>-40006: aes 加密失败</li>
 *    <li>-40007: aes 解密失败</li>
 *    <li>-40008: 解密后得到的buffer非法</li>
 *    <li>-40009: base64加密失败</li>
 *    <li>-40010: base64解密失败</li>
 *    <li>-40011: 生成xml失败</li>
 * </ul>
 */
class ErrorCode {
    public static int $OK = 0;
    public static int $ValidateSignatureError = -40001;
    public static int $ParseXmlError = -40002;
    public static int $ComputeSignatureError = -40003;
    public static int $IllegalAesKey = -40004;
    public static int $ValidateAppidError = -40005;
    public static int $EncryptAESError = -40006;
    public static int $DecryptAESError = -40007;
    public static int $IllegalBuffer = -40008;
    public static int $EncodeBase64Error = -40009;
    public static int $DecodeBase64Error = -40010;
    public static int $GenReturnXmlError = -40011;
}
