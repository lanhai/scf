<?php

namespace Scf\Cloud\Ali;

use AlibabaCloud\SDK\Dysmsapi\V20170525\Dysmsapi;
use AlibabaCloud\SDK\Dysmsapi\V20170525\Models\SendSmsRequest;
use AlibabaCloud\Tea\Exception\TeaUnableRetryError;
use Darabonba\OpenApi\Models\Config;
use Scf\Cloud\Aliyun;
use Scf\Core\Result;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Exception\AppError;

class Sms extends Aliyun {
    protected static array $clients;

    /**
     * @throws AppError
     */
    public function _init() {
        parent::_init();
        if (!isset($this->_config['default_server']) || !isset($this->_config['server'])) {
            throw new AppError('阿里云OSS配置信息不存在');
        }
    }

    /**
     * 发送短信验证码
     * @param $mobile
     * @param $code
     * @param string $server
     * @return Result
     */
    public function sendVerifyCode($mobile, $code, string $server = 'default'): Result {
        $server = $this->_config['server'][$server];
        return $this->send($mobile, $server['verify']['sign'], $server['verify']['template'], ['code' => (string)$code]);
    }

    /**
     * @param $mobile
     * @param $sign
     * @param $tplCode
     * @param array $params
     * @param string $server
     * @return Result
     */
    public function send($mobile, $sign, $tplCode, array $params = [], string $server = 'default'): Result {
        if (!isset(self::$clients[$server])) {
            $serverConfig = $this->_config['server'][$server];
            $account = $this->accounts[$serverConfig['account']];
            $config = new Config();
            $config->accessKeyId = $account['accessId'];
            $config->accessKeySecret = $account['accessKey'];
            $config->endpoint = $serverConfig['endpoint'];
            self::$clients[$server] = new Dysmsapi($config);
        }
        $client = self::$clients[$server];
        $sendReq = new SendSmsRequest([
            "phoneNumbers" => $mobile,
            "signName" => $sign,
            "templateCode" => $tplCode,
            'templateParam' => JsonHelper::toJson($params)
        ]);
        try {
            $sendResp = $client->sendSms($sendReq);
            $code = $sendResp->body->code;
            if (strtolower($code) !== 'ok') {
                return Result::error($sendResp->body->message, 'SEND_FAIL');
            }
            return Result::success($sendResp->toMap()['body']);
        } catch (TeaUnableRetryError $e) {
            // 获取报错数据
            //var_dump($e->getErrorInfo());
            // 获取报错信息
            //var_dump($e->getMessage());
            // 获取最后一次报错的 Exception 实例
            //var_dump($e->getLastException());
            // 获取最后一次请求的 Request 实例
            //var_dump($e->getLastRequest());
            return Result::error($e->getMessage());
        }

    }
}