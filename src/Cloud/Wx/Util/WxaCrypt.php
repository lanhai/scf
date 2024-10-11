<?php

namespace Scf\Cloud\Wx\Util;


use Scf\Core\Component;
use Scf\Core\Result;
use Scf\Helper\JsonHelper;

class WxaCrypt extends Component {
    public string $appid;
    public string $sessionKey;


    /**
     * 检验数据的真实性，并且获取解密后的明文.
     * @param $encryptedData string 加密的用户数据
     * @param $iv string 与用户数据一同返回的初始向量
     * @return Result
     */
    public function decryptData(string $encryptedData, string $iv): Result {
        if (strlen($this->sessionKey) != 24) {
            return Result::error('session_key 非法');
        }
        $aesKey = base64_decode($this->sessionKey);
        if (strlen($iv) != 24) {
            return Result::error('iv 非法');
        }
        $aesIV = base64_decode($iv);
        $aesCipher = base64_decode($encryptedData);
        $result = openssl_decrypt($aesCipher, "AES-128-CBC", $aesKey, 1, $aesIV);
        $dataObj = json_decode($result);
        if (empty($dataObj)) {
            return Result::error('aes 解密失败:数据为空', 'DECRYPT_ERROR_DATA_EMPTY');
        }
        if ($dataObj->watermark->appid != $this->appid) {
            return Result::error('aes 解密失败:appid不匹配', 'DECRYPT_ERROR_APPID_NOT_MATCH');
        }
        return Result::success(JsonHelper::recover($result));
    }
}
