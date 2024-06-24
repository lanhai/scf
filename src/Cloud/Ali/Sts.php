<?php

namespace Scf\Cloud\Ali;

use AlibabaCloud\SDK\Sts\V20150401\Models\AssumeRoleRequest;
use AlibabaCloud\SDK\Sts\V20150401\Sts as StsClient;
use AlibabaCloud\Tea\Exception\TeaUnableRetryError;
use Darabonba\OpenApi\Models\Config;
use Scf\Cloud\Aliyun;
use Scf\Core\Result;
use Scf\Mode\Web\Exception\AppError;

class Sts extends Aliyun {
    /**
     * @var StsClient
     */
    protected StsClient $_client;

    protected array $server;

    protected string $endpoint;

    /**
     * @throws AppError
     */
    public function _init(): void {
        parent::_init();
        if (!isset($this->_config['default_server']) || !isset($this->_config['server'])) {
            throw new AppError('阿里云STS配置信息不存在');
        }
        $this->server = $this->_config['server'][$this->_config['default_server']];
        $account = $this->accounts[$this->server['account']];
        $this->accessId = $account['accessId'];
        $this->accessKey = $account['accessKey'];
        $config = new Config([
            "accessKeyId" => $account['accessId'],
            "accessKeySecret" => $account['accessKey']
        ]);
        // 访问的域名
        //$config->endpoint = 'sts.' . $this->server['regionId'] . '.aliyuncs.com';// "sts.cn-hangzhou.aliyuncs.com";
        $config->regionId = $this->server['regionId'];
        $this->_client = new StsClient($config);

    }

    /**
     * 临时授权
     * @param $usrId
     * @return Result
     */
    public function getOssAuth($usrId = null): Result {
        $usrId = $usrId ?: date('YmdHis') . rand(1000, 9999);
        $assumeRoleRequest = new AssumeRoleRequest([
            "durationSeconds" => $this->server['tokenExpire'],
            //"policy" => $this->server['policy'],
            "roleArn" => $this->server['RoleArn'],
            "roleSessionName" => $usrId
        ]);
        // 复制代码运行请自行打印 API 的返回值
        try {
            $response = $this->client()->assumeRole($assumeRoleRequest);
            return Result::success($response->toMap()['body']);
        } catch (TeaUnableRetryError $e) {
            return Result::error($e->getMessage());
            // 获取报错数据
            //var_dump($e->getErrorInfo());
            // 获取报错信息
            //var_dump($e->getMessage());
//            // 获取最后一次报错的 Exception 实例
//            var_dump($e->getLastException());
//            // 获取最后一次请求的 Request 实例
//            var_dump($e->getLastRequest());
        }
    }

    /**
     * 返回已经初始化的客户端
     * @return StsClient|null
     */
    public function client(): ?StsClient {
        return $this->_client ?? null;
    }
}