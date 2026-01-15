<?php

namespace Scf\Cloud\Wx;


use App\Common\Model\ConfigModel;
use Scf\Client\Http;
use Scf\Cloud\Wx\Util\MsgCrypt;
use Scf\Core\Result;
use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\Singleton;
use Scf\Helper\ArrayHelper;

class OpenPlatform {
    use Singleton, ComponentTrait;

    protected ?MsgCrypt $cryptor = null;
    protected ?string $appid = null;
    protected ?string $encodingAesKey = null;
    protected ?string $token = null;
    protected ?string $secret = null;

    protected function _init(): void {
        if (is_null($this->cryptor)) {
            $this->appid = $this->_config['appid'];
            $this->encodingAesKey = $this->_config['encoding_key'];
            $this->token = $this->_config['token'];
            $this->secret = $this->_config['secret'];
            $this->cryptor = new MsgCrypt($this->_config['token'], $this->_config['encoding_key'], $this->_config['appid']);
        }
    }

    /**
     * 获取模板列表
     * @return Result
     * @throws \Exception
     */
    public function wxaGetTemplateList(): Result {
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $gateway = $this->_config['gateway'] . '/wxa/gettemplatelist?access_token=' . $token->getData();
        $apiResult = Http::create($gateway)->get();
        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (!empty($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        ArrayHelper::multisort($resultData['template_list'], 'create_time', SORT_DESC);
        return Result::success($resultData['template_list']);
    }

    /**
     * 获取草稿列表
     * @return Result
     */
    public function wxaGetTemplateDraftList(): Result {
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $gateway = $this->_config['gateway'] . '/wxa/gettemplatedraftlist?access_token=' . $token->getData();
        $client = Http::create($gateway);
        $apiResult = $client->get();

        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (!empty($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        return Result::success($resultData['draft_list']);
    }

    /**
     * 删除模板
     * @param $id
     * @return Result
     */
    public function wxaDeleteTemplate($id): Result {
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $gateway = $this->_config['gateway'] . '/wxa/deletetemplate?access_token=' . $token->getData();
        $client = Http::create($gateway);
        $apiResult = $client->JPost([
            'template_id' => $id,
        ]);
        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (!empty($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        return Result::success();
    }

    /**
     * 将草稿设为模板
     * @param int $id
     * @return Result
     */
    public function wxaSetTemplate(int $id): Result {
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $gateway = $this->_config['gateway'] . '/wxa/addtotemplate?access_token=' . $token->getData();
        $client = Http::create($gateway);
        $apiResult = $client->JPost([
            'draft_id' => $id,
            'template_type' => 0
        ]);
        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (!empty($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        return Result::success();
    }

    /**
     * 刷新授权公众号的access_token
     * @param $appid
     * @param $refreshToken
     * @return Result
     */
    public function refreshAuthorizerAccessToken($appid, $refreshToken): Result {
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $accessToken = $token->getData();
        $url = $this->_config['gateway'] . '/cgi-bin/component/api_authorizer_token?component_access_token=' . $accessToken;
        $postData = [
            'component_appid' => $this->appid,
            'authorizer_appid' => $appid,
            'authorizer_refresh_token' => $refreshToken
        ];
        $apiResult = Http::create($url)->JPost($postData);
        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (isset($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        return Result::success($resultData);
    }


    /**
     * 获取授权账号的账户信息
     * @param $appid $appid 公众号APPID
     * @return Result
     */
    public function getAuthAccountInfo($appid): Result {
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $accessToken = $token->getData();
        $url = $this->_config['gateway'] . '/cgi-bin/component/api_get_authorizer_info?component_access_token=' . $accessToken;
        $postData = [
            'component_appid' => $this->appid,
            'authorizer_appid' => $appid
        ];
        $apiResult = Http::create($url)->JPost($postData);
        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (isset($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        return Result::success($resultData);
    }

    /**
     * 查询授权信息
     * @param $code
     * @return Result
     */
    public function getAuthInfo($code): Result {
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $accessToken = $token->getData();
        $url = $this->_config['gateway'] . '/cgi-bin/component/api_query_auth?component_access_token=' . $accessToken;
        $postData = [
            'component_appid' => $this->appid,
            'authorization_code' => $code
        ];
        $client = Http::create($url);
        $apiResult = $client->JPost($postData);
        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (isset($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        return Result::success($resultData);
    }


    /**
     * 获取预授权CODE
     * @return Result
     */
    public function getPreAuthCode(): Result {
        $config = ConfigModel::instance();
        $token = $this->getAccessToken();
        if ($token->hasError()) {
            return $token;
        }
        $accessToken = $token->getData();
        $code = $config->get_value(ConfigModel::KEY_WX_COMPONENT_PREAUTH_CODE);
        $expired = $config->get_value(ConfigModel::KEY_WX_COMPONENT_PREAUTH_CODE_EXPIRED);
        if ((!$code || intval($expired) - time() < 60)) {
            $url = $this->_config['gateway'] . "/cgi-bin/component/api_create_preauthcode?component_access_token=" . $accessToken;
            $data = [
                'component_appid' => $this->appid
            ];
            $client = Http::create($url);
            $apiResult = $client->JPost($data);
            if ($apiResult->hasError()) {
                return Result::error("获取pre_auth_code失败:" . $apiResult->getMessage());
            }
            $json = $apiResult->getData();
            if (!empty($json['errcode'])) {
                return Result::error("获取pre_auth_code失败:" . $json['errmsg']);
            }
            if (empty($json['pre_auth_code'])) {
                return Result::error("获取pre_auth_code失败:未获取到pre_auth_code内容");
            }
            $code = $json['pre_auth_code'];
            $config->update(ConfigModel::KEY_WX_COMPONENT_PREAUTH_CODE, $code);
            $config->update(ConfigModel::KEY_WX_COMPONENT_PREAUTH_CODE_EXPIRED, time() + $json['expires_in']);
        }
        return Result::success($code);
    }

    /**
     * 获取公众号的access_token
     * @return Result
     */
    public function getAccessToken(): Result {
        $config = ConfigModel::instance();
        $token = $config->get_value(ConfigModel::KEY_WX_ACCESS_TOKEN);
        $expired = $config->get_value(ConfigModel::KEY_WX_ACCESS_TOKEN_EXPIRED) ?: 0;
        if ((!$token || (int)$expired - time() < 300) && $ticket = $config->get_value(ConfigModel::KEY_WX_COMPONENT_TICKET)) {
            $url = $this->_config['gateway'] . "/cgi-bin/component/api_component_token";
            $data = [
                'component_appid' => $this->appid,
                'component_appsecret' => $this->secret,
                'component_verify_ticket' => $ticket,
            ];
            $client = Http::create($url);
            $apiResult = $client->JPost($data);
            if ($apiResult->hasError()) {
                return Result::error("获取access_token失败:" . $apiResult->getMessage());
            }
            $json = $apiResult->getData();
            if (!empty($json['errcode'])) {
                return Result::error("获取access_token失败:" . $json['errmsg']);
            }
            if (empty($json['component_access_token'])) {
                return Result::error("获取access_token失败:未获取到token内容");
            }
            $token = $json['component_access_token'];
            $config->update(ConfigModel::KEY_WX_ACCESS_TOKEN, $token);
            $config->update(ConfigModel::KEY_WX_ACCESS_TOKEN_EXPIRED, time() + $json['expires_in']);
        }
        if (!$token) {
            return Result::error('access_token获取失败');
        }
        return Result::success($token);
    }

    /**
     * 推送ticket
     * @return Result
     */
    public function pushTicket(): Result {
        $gateway = $this->_config['gateway'] . '/cgi-bin/component/api_start_push_ticket';
        $postData = [
            'component_appid' => $this->appid,
            'component_secret' => $this->secret,
        ];
        $client = Http::create($gateway);
        $apiResult = $client->JPost($postData);
        if ($apiResult->hasError()) {
            return $apiResult;
        }
        $resultData = $apiResult->getData();
        if (!empty($resultData['errcode'])) {
            return Result::error($resultData['errmsg'], $resultData['errcode']);
        }
        return Result::success($resultData);
    }

    /**
     * 解密消息
     * @param $msgSignature
     * @param $timestamp
     * @param $nonce
     * @param $postData
     * @param $responseData
     * @return int
     */
    public function decryptMsg($msgSignature, $timestamp, $nonce, $postData, &$responseData): int {
        return $this->cryptor->decryptMsg($msgSignature, $timestamp, $nonce, $postData, $responseData);
    }

    /**
     * @param string $ticket
     * @return void
     */
    public function updateTicket(string $ticket): void {
        $config = ConfigModel::instance();
        $config->update(ConfigModel::KEY_WX_COMPONENT_TICKET, $ticket);
        $config->update('wx_verify_ticket_updated', time());
    }

    public function appid(): ?string {
        return $this->appid;
    }

    public function gateway(): ?string {
        return $this->_config['gateway'];
    }

    public function wxaFuncList(): array {
        return $this->_config['wxa_func_list'];
    }
}