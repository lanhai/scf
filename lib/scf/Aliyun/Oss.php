<?php

namespace Scf\Aliyun;

use AlibabaCloud\SDK\Sts\V20150401\Models\AssumeRoleRequest;
use AlibabaCloud\SDK\Sts\V20150401\Sts as StsClient;
use AlibabaCloud\Tea\Exception\TeaUnableRetryError;
use Darabonba\OpenApi\Models\Config;
use JetBrains\PhpStorm\ArrayShape;
use OSS\Core\OssException;
use OSS\OssClient;
use Scf\Client\Http;
use Scf\Core\Aliyun;
use Scf\Core\Result;
use Scf\Mode\Web\Exception\AppError;
use Scf\Util\Date;
use Scf\Util\Sn;

class Oss extends Aliyun {
    /**
     * @var OssClient
     */
    protected OssClient $_client;

    protected array $server;

    protected string $endpoint;
    protected string $serverId;


    /**
     * @throws AppError
     */
    public function _init() {
        parent::_init();
        if (!isset($this->_config['default_server']) || !isset($this->_config['server'])) {
            throw new AppError('阿里云OSS配置信息不存在');
        }
        $this->serverId = $this->_config['default_server'];
        $this->server = $this->_config['server'][$this->_config['default_server']];
        $account = $this->accounts[$this->server['account']];
        $this->accessId = $account['accessId'];
        $this->accessKey = $account['accessKey'];

        //Endpoint以杭州为例，其它Region请按实际情况填写。
        $endpoint = $this->server['ENDPOINT'];
        try {
            $this->_client = new OssClient($this->accessId, $this->accessKey, $endpoint, $this->server['IS_CNNAME']);
        } catch (OssException $e) {
            throw new AppError($e->getMessage());
        }
    }

    /**
     * 获取临时上传授权
     * @param $usrId
     * @return Result
     */
    public function stsAuth($usrId = null): Result {
        try {
            $config = new Config([
                "accessKeyId" => $this->accessId,
                "accessKeySecret" => $this->accessKey
            ]);
            // 访问的域名
            //$config->endpoint = 'sts.'.$this->server['sts']['endPoint'].'.aliyuncs.com';
            $config->regionId = $this->server['sts']['regionId'];
            $stsClient = new StsClient($config);
            $usrId = $usrId ?: date('YmdHis') . rand(1000, 9999);
            $assumeRoleRequest = new AssumeRoleRequest([
                "durationSeconds" => $this->server['sts']['tokenExpire'],
                //"policy" => $this->server['sts']['policy'],
                "roleArn" => $this->server['sts']['RoleArn'],
                "roleSessionName" => $usrId
            ]);
            // 复制代码运行请自行打印 API 的返回值
            $response = $stsClient->assumeRole($assumeRoleRequest);
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

    public function upload(string $content, string $ext, $object = null): Result {
        if (is_null($object)) {
            $object = '/upload/' . Date::today() . '/' . Sn::create_uuid() . '.' . $ext;
        }
        $this->client()->putObject($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $content);
        return Result::success($this->server['CDN_DOMAIN'] . $object);
    }

    /**
     * 上传本地文件
     * @param string $filePath 本地文件绝对路径
     * @param string $object 要存放的路径,示范:/upload/xxxx/xxxx.jpg
     * @return array
     */
    public function uploadFile(string $filePath, string $object): array {
        try {
            $this->client()->uploadFile($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $filePath);
            return Result::output()->success($this->server['CDN_DOMAIN'] . $object);
        } catch (OssException $e) {
            return Result::output()->error($e->getMessage());
        }
    }

    /**
     * 下载文件
     * @param $url
     * @param string|null $object
     * @return Result
     */
    public function downloadFile($url, string $object = null): Result {
        $client = Http::create($url);
        $result = $client->get(60);
        if ($result->hasError()) {
            return Result::error($result->getMessage());
        }
        if (is_null($object)) {
            $arr = explode('.', $url);
            $extension = array_pop($arr);
            $object = '/download/' . Date::today() . '/' . Sn::create_guid() . '.' . $extension;
        }
        try {
            $this->client()->putObject($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $result->getData());
            return Result::success($this->server['CDN_DOMAIN'] . $object);
        } catch (OssException $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * @return array
     */
    #[ArrayShape(['regionId' => "mixed", 'server_id' => "mixed", 'bucket' => "mixed", 'cdn_domain' => "mixed"])]
    public function bucket(): array {
        return [
            'regionId' => $this->server['REGION_ID'],
            'server_id' => $this->serverId,
            'bucket' => $this->server['BUCKET'],
            'cdn_domain' => $this->server['CDN_DOMAIN']
        ];
    }

    /**
     * 返回已经初始化的客户端
     * @return OssClient|null
     */
    public function client(): ?OssClient {
        return $this->_client ?? null;
    }

    /**
     * 获取OSS服务器配置信息
     * @return array
     */
    public function getServer(): array {
        return $this->server;
    }
}