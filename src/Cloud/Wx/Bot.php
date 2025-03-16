<?php

namespace Scf\Cloud\Wx;

use Scf\Client\Http;
use Scf\Component\Qrcode;
use Scf\Core\Result;
use Scf\Core\Traits\CoroutineSingleton;
use Scf\Core\Traits\ComponentTrait;
use Swoole\Coroutine;

class Bot {
    use CoroutineSingleton, ComponentTrait;

    protected string $gateway = "";
    protected string $token = "";
    protected string $appid = "";
    protected string $wxid = "";
    protected string $uuid = "";

    public function _init(): void {
        $this->gateway = $this->getConfig('gateway');
        $this->token = $this->getConfig('token');
        $this->appid = $this->getConfig('appid');
        $this->wxid = $this->getConfig('wxid');
        $this->uuid = $this->getConfig('uuid');
    }

    /**
     * 检查在线状态
     * @param $appid
     * @return Result
     */

    public function onlineCheck($appid): Result {
        return $this->request('/login/checkOnline', [
            'appId' => $appid,
        ]);
    }

    /**
     * 发送消息
     * @param $to
     * @param $content
     * @param string $ats
     * @param string $type
     * @return Result|void
     */
    public function sendMessage($to, $content, string $ats = "", string $type = 'text') {
        switch ($type) {
            case 'text':
                $body = [
                    "appId" => $this->appid,
                    "toWxid" => $to,
                    "content" => $content,
                    'ats' => $ats == 'all' ? 'notify@all' : $ats
                ];
                for ($i = 0; $i < 3; $i++) {
                    $result = $this->request('/message/postText', $body);
                    if (!$result->hasError()) {
                        break;
                    }
                    Coroutine::sleep(5);
                }
                return $result;
            case 'image':
                $url = $this->gateway . "/message/sendImageMsg";
                $body = [
                    "appId" => $this->appid,
                ];
                break;
            default:
                return Result::error("不支持的消息类型");
        }
    }

    /**
     * 设置回调地址
     * @param string $url
     * @return Result
     */
    public function setCallbackUrl(string $url = ""): Result {
        return $this->request('/tools/setCallback', [
            'token' => $this->token,
            'callbackUrl' => $url
        ]);
    }

    /**
     * 获取通讯录列表
     * @return Result
     */
    public function contactsList(): Result {
        $cache = $this->request('/contacts/fetchContactsListCache', [
            'appId' => $this->appid
        ]);
        if (!$cache->hasError() && !empty($cache->getData())) {
            return Result::success($cache->getData());
        }
        return $this->request('/contacts/fetchContactsList', [
            'appId' => $this->appid
        ]);
    }

    /**
     * 获取群详情
     * @param $roomId
     * @return Result
     */
    public function roomDetail($roomId): Result {
        return $this->request('/group/getChatroomInfo', [
            'appId' => $this->appid,
            'chatroomId' => $roomId
        ]);
    }

    /**
     * 检查登录状态
     * @param $appid
     * @param $uuid
     * @param string $code
     * @return Result
     */
    public function checkLogin($appid, $uuid, string $code = ""): Result {
        $response = $this->request('/login/checkLogin', [
            'appId' => $appid,
            'uuid' => $uuid,
            'captchCode' => $code
        ]);
        if ($response->hasError()) {
            return $response;
        }
        $result = $response->getData();
        if (!$result) {
            return Result::success(0);
        }
        $status = $result['status'];
        if ($status == 2 && $result['loginInfo']) {
            return Result::success($result);
        }
        return Result::success($status);
    }

    /**
     * 退出登录
     * @param string $appid
     * @return Result
     */
    public function logout(string $appid): Result {
        return $this->request('/login/logout', [
            'appId' => $appid
        ]);
    }

    /**
     * 获取登陆二维码
     * @param string $appid
     * @return Result
     */
    public function login(string $appid = ""): Result {
        $response = $this->request('/login/getLoginQrCode', [
            'appId' => $appid
        ]);
        if ($response->hasError()) {
            return $response;
        }
        $qrData = $response->getData('qrData');
//        $base64Data = preg_replace('#^data:image/\w+;base64,#i', '', $result['qrImgBase64']);
//        $imageData = base64_decode($base64Data);
//        $oss = Oss::instance();
//        $uploadResult = $oss->upload($imageData, 'png');
        $uploadResult = Qrcode::instance()->init($qrData)->upload();
        if ($uploadResult->hasError()) {
            return Result::error('上传登陆二维码失败:' . $uploadResult->getMessage());
        }
        return Result::success([
            'appid' => $response->getData('appId'),
            'qrcode' => $uploadResult->getData(),
            'uuid' => $response->getData('uuid')
        ]);
    }

    /**
     * 获取token
     * @return Result
     */
    public function getToken(): Result {
        return $this->request('/tools/getTokenId');
    }

    /**
     * 请求
     * @param $path
     * @param array $body
     * @return Result
     */
    protected function request($path, array $body = []): Result {
        $url = $this->gateway . $path;
        $client = Http::create($url);
        $client->setHeader('X-GEWE-TOKEN', $this->token);
        $response = $client->JPost($body);
        if ($response->hasError()) {
            return Result::error("请求失败:" . $response->getMessage(), $response->getErrCode());
        }
        $result = $response->getData();
        $code = $result['ret'] ?? 500;
        $msg = $result['msg'] ?? '未知错误';
        if ($code != 200) {
            return Result::error($msg, $code);
        }
        if (!isset($result['data'])) {
            return Result::error($msg ?: "无数据返回", $code);
        }
        return Result::success($result['data']);

    }
}