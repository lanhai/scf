<?php

namespace Scf\Cloud\Ali;

use Scf\Cache\Redis;
use Scf\Client\Http;
use Scf\Core\Component;
use Scf\Core\Env;
use Scf\Core\Result;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Request;
use Exception;
use Throwable;

class Dingtalk extends Component {
    protected array $_config = [
        'client_id' => '',
        'secret' => '',
        'robot_code' => '',
        'group_template' => ''
    ];

    public function getSettingKey(): string {
        return get_called_class() . ':setting';
    }

    public function setting($key, $value = null): void {
        $this->_config[$key] = $value;
    }

    /**
     * @throws Exception
     */
    protected function getAccessToken() {
        $pool = Redis::pool();
        $token = $pool->hget($this->getSettingKey(), 'access_token') ?: null;
        $tokenExpired = $pool->hget($this->getSettingKey(), 'access_token_expire') ?: 0;
        if (!$token || $tokenExpired <= time()) {
            $apiResponse = $this->request('/v1.0/oauth2/accessToken', [
                'appKey' => $this->_config['client_id'],
                'appSecret' => $this->_config['secret'],
            ]);
            if ($apiResponse->hasError()) {
                throw new Exception($apiResponse->getMessage());
            }
            $responseData = $apiResponse->getData();
            $token = $responseData['accessToken'];
            $pool->hset($this->getSettingKey(), 'access_token', $token);
            $pool->hset($this->getSettingKey(), 'access_token_expire', time() + $responseData['expireIn']);
            //$pool->hset($this->getSettingKey(), 'access_refresh_token', $responseData['refreshToken']);
        }
        return $token;
    }

    /**
     * 发送群消息
     * @param string|array $groupId
     * @param string $messageType
     * @param array|string $messageContent
     * @return Result
     */
    public function sendGroupMessage(string|array $groupId, string $messageType, array|string $messageContent): Result {
        try {
            $token = $this->getAccessToken();
            $params = [
                'target_open_conversation_id' => $groupId,
                'robot_code' => $this->_config['robot_code'],
                'msg_template_id' => $messageType,
                'msg_param_map' => is_array($messageContent) ? JsonHelper::toJson($messageContent) : $messageContent,
            ];
            return $this->request("/topapi/im/chat/scencegroup/message/send_v2?access_token={$token}", $params, gateway: "https://oapi.dingtalk.com/");
        } catch (Throwable $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 创建群聊
     * @param $userId
     * @return Result
     */
    public function createGroup($userId): Result {
        try {
            $token = $this->getAccessToken();
            $params = [
                'title' => '消息通知',
                'template_id' => $this->_config['group_template'],
                'owner_user_id' => $userId
            ];
            return $this->request('/topapi/im/chat/scenegroup/create?access_token=' . $token, $params, gateway: 'https://oapi.dingtalk.com/');
        } catch (Throwable $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 获取用户UID
     * @param $unionId
     * @return Result
     */
    public function queryUserId($unionId): Result {
        try {
            $token = $this->getAccessToken();
            return $this->request("/topapi/user/getbyunionid?access_token={$token}", [
                'unionid' => $unionId
            ], gateway: 'https://oapi.dingtalk.com/');
        } catch (Exception $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 获取用户个人信息
     * @param string $unionId
     * @param $token
     * @return Result
     */
    public function queryUserInfo(string $unionId = 'me', $token = null): Result {
        try {
            return $this->request("/v1.0/contact/users/{$unionId}", method: 'GET', accessToken: $token);
        } catch (Exception $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 查询钉钉授权令牌
     * @param $params
     * @return Result
     */
    public function queryUserAccessToken($params): Result {
        $params['clientId'] = $this->_config['client_id'];
        $params['clientSecret'] = $this->_config['secret'];
        return $this->request('/v1.0/oauth2/userAccessToken', $params);
    }

    /**
     * 发起请求
     * @param $uri
     * @param array $params
     * @param string $method
     * @param string|null $accessToken
     * @param string $gateway
     * @return Result
     */
    protected function request($uri, array $params = [], string $method = 'POST', string $accessToken = null, string $gateway = 'https://api.dingtalk.com'): Result {
        $client = Http::create($gateway . $uri);
        if ($accessToken) {
            $client->setHeader('x-acs-dingtalk-access-token', $accessToken);
        }
        if (strtolower($method) == 'post') {
            $apiResponse = $client->JPost($params);
        } else {
            $apiResponse = $client->get();
        }
        $responseData = $apiResponse->getData();
        $errCode = $responseData['errcode'] ?? 0;
        if ($apiResponse->hasError()) {
            return Result::error($responseData['message'] ?? $apiResponse->getMessage(), $apiResponse->getErrCode(), $responseData);
        }
        if ($errCode) {
            return Result::error($responseData['errmsg'] ?? '未知错误', $errCode, $responseData);
        }
        if (isset($responseData['result'])) {
            return Result::success($responseData['result']);
        }
        return Result::success($responseData);
    }

    /**
     * 获取授权登陆链接
     * @param $uid
     * @return string
     */
    public function getAuthUri($uid): string {
        $redirectHost = Env::isDev() ? 'https://dev.lkyapp.com' : Request::url();
        $redirectUri = urlencode($redirectHost . '/@log:dingtalk:auth:callback@/');
        return "https://login.dingtalk.com/oauth2/auth?redirect_uri={$redirectUri}&response_type=code&client_id={$this->_config['client_id']}&scope=openid&state={$uid}&prompt=consent";
    }
}