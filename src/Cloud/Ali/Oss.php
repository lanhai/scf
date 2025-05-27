<?php

namespace Scf\Cloud\Ali;

use AlibabaCloud\SDK\Sts\V20150401\Models\AssumeRoleRequest;
use AlibabaCloud\SDK\Sts\V20150401\Sts as StsClient;
use AlibabaCloud\Tea\Exception\TeaUnableRetryError;
use Darabonba\OpenApi\Models\Config;
use JetBrains\PhpStorm\ArrayShape;
use OSS\Core\OssException;
use OSS\Http\RequestCore_Exception;
use OSS\OssClient;
use Scf\Client\Http;
use Scf\Cloud\Aliyun;
use Scf\Core\Result;
use Scf\Database\Dao;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Exception\AppError;
use Scf\Root;
use Scf\Util\Date;
use Scf\Util\MimeTypes;
use Scf\Util\Sn;
use Symfony\Component\Yaml\Yaml;
use Scf\Cloud\Ali\Db\AttachmentTable;
use Throwable;

class Oss extends Aliyun {
    /**
     * @var OssClient
     */
    protected OssClient $_client;

    protected array $server;
    protected string $endpoint;
    protected string $serverId;
    protected array $allowTypes = [
        'jpg' => 'image',
        'jpeg' => 'image',
        'png' => 'image',
        'gif' => 'image',
        'bmp' => 'image',
        'ico' => 'image',
        'amr' => 'media',
        'mp3' => 'media',
        'wmv' => 'media',
        'wav' => 'video',
        'mp4' => 'video',
        'mp5' => 'video',
        'mov' => 'video',
        'avi' => 'video',
        '3gp' => 'video',
        'rm' => 'video',
        'rmvb' => 'video',
        'xlsx' => 'file',
        'xls' => 'file',
        'doc' => 'file',
        'docx' => 'file',
        'zip' => 'file',
        'rar' => 'file',
        'txt' => 'file',
        'csv' => 'file',
        'apk' => 'file',
        'pdf' => 'file',
        'ppt' => 'file',
        'pptx' => 'file'
    ];

    /**
     * @throws AppError
     */
    public function _init(): void {
        parent::_init();
        if (!isset($this->_config['default_server']) || !isset($this->_config['server']) || !$this->accounts) {
            throw new AppError('阿里云OSS配置信息不存在');
        }
        $this->serverId = $this->_config['default_server'];
        $this->server = $this->_config['server'][$this->_config['default_server']];
        $account = $this->accounts[$this->server['account']];
        $this->accessId = $account['accessId'];
        $this->accessKey = $account['accessKey'];
        if (!$this->accessId || !$this->accessKey) {
            throw new AppError('阿里云OSS配置信息不存在');
        }
        //Endpoint以杭州为例，其它Region请按实际情况填写。
        $this->_client = new OssClient($this->accessId, $this->accessKey, $this->server['ENDPOINT'], $this->server['IS_CNNAME']);
    }

    /**
     * 创建数据表
     * @return void
     */
    public function createTable(): void {
        if (!$this->accessId) {
            return;
        }
        $table = Yaml::parseFile(Root::dir() . '/Cloud/Ali/Db/Yml/attachment.yml');
        $arr = explode("/", $table['dao']);
        $cls = implode('\\', $arr);
        /** @var Dao $cls */
        $dao = $cls::factory();
        $dao->updateTable($table);
    }

    /**
     * sts授权
     * @return Result
     */
    public function auth(): Result {
        //TODO 客户端频次以及权限判定
        $result = $this->stsAuth();
        if ($result->hasError()) {
            return $result;
        }
        $data = [
            'credentials' => $result->getData('Credentials'),
            'server' => $this->bucket(),
            'object_path' => 'upload/' . date('Ymd') . '/',
        ];
        return Result::success($data);
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

    /**
     * 给私有文件链接签名
     * @param $object
     * @param int $timeout
     * @param array|null $options
     * @return Result
     */
    public function signUrl($object, int $timeout = 60, array $options = null): Result {
        $domain = $this->bucket()['cdn_domain'];
        if (str_contains($object, PROTOCOL_HTTP) || str_contains($object, PROTOCOL_HTTPS)) {
            $object = str_replace($domain, "", $object);
            if (str_starts_with($object, "/")) {
                $object = substr($object, 1);
            }
        }
        try {

            $result = $this->client()->signUrl($this->bucket()['bucket'], $object, $timeout, options: $options);
            return Result::success($result);
        } catch (OssException $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 获取访问URL
     * @param string $object
     * @return string
     */
    public function getUrl(string $object): string {
        if (!str_contains($object, 'http://') && !str_contains($object, 'https://')) {
            $object = $this->server['CDN_DOMAIN'] . (str_starts_with($object, "/") ? "" : "/") . $object;
        }
        return $object;
    }

    /**
     * @param $source
     * @param $text
     * @param array $options
     * @param bool $upload
     * @return Result
     */
    public function createTextWaterMark($source, $text, array $options = [], bool $upload = false): Result {
        if (!str_contains($source, 'http://') && !str_contains($source, 'https://')) {
            $source = $this->server['CDN_DOMAIN'] . (str_starts_with($source, "/") ? "" : "/") . $source . "?x-oss-process=image";
        }
        if (strlen($text) > 64) {
            return Result::error('文本长度超出限制(最多21个中文字符)');
        }
        $text = StringHelper::urlsafe_b64encode($text);
        $source .= '/watermark';
        $styles = [
            'text' => $text,
            'type' => $options['font'] ?? 'ZmFuZ3poZW5naGVpdGk',
            'g' => $options['position'] ?? 'center',
            'x' => $options['x'] ?? 0,
            'y' => $options['y'] ?? 0,
            'color' => $options['color'] ?? 0,
            'size' => $options['size'] ?? 20,
        ];
        foreach ($styles as $sk => $sv) {
            if (!$sv) {
                continue;
            }
            $source .= ',' . $sk . '_' . $sv;
        }
        if ($upload) {
            return $this->downloadFile($source);
        }
        return Result::success($source);
    }

    /**
     * 执照图片水印
     * @param $source
     * @param $water
     * @param array $options
     * @param bool $upload
     * @return Result
     */
    public function createImgWaterMark($source, $water, array $options = [], bool $upload = false): Result {
        if (!str_contains($source, 'http://') && !str_contains($source, 'https://')) {
            $source = $this->server['CDN_DOMAIN'] . (str_starts_with($source, "/") ? "" : "/") . $source . "?x-oss-process=image";
        }
        $source .= '/watermark';
        $waterImageInfoResult = $this->getImageInfo($water);
        if ($waterImageInfoResult->hasError()) {
            return $waterImageInfoResult;
        }
        $waterImageInfo = $waterImageInfoResult->getData();
        $waterWidth = (int)$waterImageInfo['ImageWidth']['value'];
        $waterHeight = (int)$waterImageInfo['ImageHeight']['value'];
        $waterObject = str_replace($this->server['CDN_DOMAIN'] . "/", "", $water);
        //自动裁剪
        if ($waterWidth != ($options['width'] ?? 100) || $waterHeight != ($options['height'] ?? 100)) {
            $cutImage = $waterObject . '?x-oss-process=image' . '/resize,m_fill,h_' . ($options['height'] ?? 100) . ',w_' . ($options['width'] ?? 100);
            $waterObjectBase64 = StringHelper::urlsafe_b64encode($cutImage);
        } else {
            $waterObjectBase64 = StringHelper::urlsafe_b64encode($waterObject);
        }
        $styles = [
            'image' => $waterObjectBase64,
            'g' => $options['position'] ?? 'center',
            'x' => $options['x'] ?? 0,
            'y' => $options['y'] ?? 0,
        ];
        foreach ($styles as $sk => $sv) {
            if (!$sv) {
                continue;
            }
            $source .= ',' . $sk . '_' . $sv;
        }
        if ($upload) {
            return $this->downloadFile($source);
        }
        return Result::success($source);
    }

    /**
     * 获取图片信息
     * @param $url
     * @return Result
     */
    public function getImageInfo($url): Result {
        if (!str_contains($url, 'http://') && !str_contains($url, 'https://')) {
            $url = $this->server['CDN_DOMAIN'] . (str_starts_with($url, "/") ? "" : "/") . $url;
        }
        $url .= "?x-oss-process=image/info";
        $client = Http::create($url);
        $result = $client->get();
        if ($result->hasError()) {
            return $result;
        }
        return Result::success($result->getData() ?: ['ImageWidth' => ['value' => 0], 'ImageHeight' => ['value' => 0]]);
    }

    /**
     * 上传图片
     * @param string $content
     * @param string $ext
     * @param null $object
     * @param string $return
     * @return Result
     */
    public function upload(string $content, string $ext, $object = null, string $return = "url"): Result {
        if (is_null($object)) {
            $object = '/upload/' . Date::today() . '/' . Sn::create_uuid() . '.' . $ext;
        }
        try {
            $this->client()->putObject($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $content);
            return Result::success($return == 'url' ? ($this->server['CDN_DOMAIN'] . $object) : $object);
        } catch (OssException|RequestCore_Exception $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 上传本地文件
     * @param string $filePath 本地文件绝对路径
     * @param string $object 要存放的路径,示范:/upload/xxxx/xxxx.jpg
     * @return Result
     */
    public function uploadFile(string $filePath, string $object): Result {
        try {
            $this->client()->uploadFile($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $filePath);
            return Result::success($this->server['CDN_DOMAIN'] . $object);
        } catch (OssException|RequestCore_Exception $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 保存文件到数据库
     * @param $data
     * @return Result
     */
    public function saveFile($data): Result {
        $allowTypes = [
            'jpg' => 'image',
            'jpeg' => 'image',
            'png' => 'image',
            'gif' => 'image',
            'bmp' => 'image',
            'mp3' => 'media',
            'wmv' => 'media',
            'wav' => 'video',
            'mp4' => 'video',
            'mov' => 'video',
            'avi' => 'video',
            '3gp' => 'video',
            'rm' => 'video',
            'rmvb' => 'video',
            'xlsx' => 'file',
            'xls' => 'file',
            'doc' => 'file',
            'docx' => 'file',
            'zip' => 'file',
            'gzip' => 'file',
            'tar' => 'file',
            'rar' => 'file',
            'txt' => 'file',
            'csv' => 'file',
            'ppt' => 'file',
            'pptx' => 'file'
        ];
        $object = AttachmentTable::factory($data);
        if (!$object->validate()) {
            return Result::error($object->getError());
        }
        if (!$object->oss_bucket) {
            return Result::error('请选择要保存的bucket');
        }
        if (!$object->file_ext || !isset($allowTypes[strtolower($object->file_ext)])) {
            return Result::error('文件格式不正确,不支持的文件格式:' . $object->file_ext);
        }
        if (!$object->save()) {
            return Result::error('文件保存失败:' . $object->getError());
        }
        $result = [
            'original_name' => $data['file_original_name'],
            'size' => ceil($data['file_size']) . 'KB',
            'ext' => strtolower($data['file_ext']),
            'type' => $data['file_type'],
            'server' => $data['oss_server'],
            'bucket' => $data['oss_bucket'],
            'object' => $data['oss_object'],
            'file' => $object->id
        ];
        $result['object_url'] = $this->getServer()['CDN_DOMAIN'] . '/' . $data['oss_object'];
        return Result::success($result);
    }

    /**
     * @param string $content
     * @param string $object
     * @param string $return
     * @return Result
     */
    public function putFile(string $content, string $object, string $return = "url"): Result {
        //TODO判断文件类型以及大小是否合法
        //创建仓库
        try {
            $this->client()->createBucket($this->server['BUCKET'], OssClient::OSS_ACL_TYPE_PUBLIC_READ_WRITE);
            $this->client()->createObjectDir($this->server['BUCKET'], 'upload/' . date('Ymd'));
            //定义文件信息
            $extension = explode('.', $object);
            $extension = array_pop($extension);
            if (!isset($this->allowTypes[strtolower($extension)])) {
                return Result::error('不支持[' . $extension . ']文件类型上传');
            }
            $content_type = MimeTypes::get_mimetype(strtolower($extension));
            $upload_file_options = [
                //'content' => $content,
                'length' => strlen($content),
                OssClient::OSS_HEADERS => [
                    'Expires' => '2050-10-01 08:00:00',
                ]
            ];
            $this->client()->putObject($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $content, $upload_file_options);
            $fileTypeArr = explode('/', $content_type);
            $fileType = $fileTypeArr[0];
            $pathArr = explode("/", $object);
            $ossAr = AttachmentTable::factory();
            $ossAr->file_original_name = array_pop($pathArr);
            $ossAr->file_size = ceil(strlen($content) / 1024);
            $ossAr->file_ext = strtolower($extension);
            $ossAr->file_type = $fileType;
            $ossAr->oss_server = $this->server['account'];
            $ossAr->oss_bucket = $this->server['BUCKET'];
            $ossAr->oss_object = $object;
            $ossAr->created_scene = 1;
            $ossAr->created_uid = 1;
            $ossAr->created_at = time();
            if (!$ossAr->save()) {
                return Result::error($ossAr->getError());
            }
            return Result::success($return == 'url' ? ($this->server['CDN_DOMAIN'] . $object) : $object);
        } catch (\Throwable $e) {
            return Result::error($e->getMessage());
        }

    }

    public function processObject() {

    }

    /**
     * 下载文件
     * @param $url
     * @param string|null $object
     * @param int $mode
     * @param ?string $app
     * @param int $appid
     * @return Result
     */
    public function downloadFile($url, string $object = null, int $mode = 1, ?string $app = null, int $appid = 0): Result {
        if (is_null($object)) {
            $arr = explode('.', $url);
            $extension = array_pop($arr);
            $object = '/download/' . Date::today() . '/' . Sn::create_guid() . '.' . $extension;
        }
        $client = Http::create($url);
        if ($mode == 1) {
            $result = $client->get(60);
        } else {
            try {
                $data = file_get_contents($url);
                $this->client()->putObject($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $data);
                return Result::success($this->server['CDN_DOMAIN'] . $object);
            } catch (Throwable $e) {
                return Result::error($e->getMessage());
            }
//            $tmpFile = APP_TMP_PATH.'/' . $object;
//            $result = $client->download($tmpFile);
        }
        if ($result->hasError()) {
            return Result::error($result->getMessage());
        }
        $client->close();
        try {
            $this->client()->putObject($this->server['BUCKET'], str_starts_with($object, "/") ? substr($object, 1) : $object, $result->getData());
            if (!is_null($app)) {
                //定义文件信息
                $extension = explode('.', $object);
                $extension = array_pop($extension);
                $content_type = MimeTypes::get_mimetype(strtolower($extension));
                $fileTypeArr = explode('/', $content_type);
                $fileType = $fileTypeArr[0];
                $pathArr = explode("/", $object);
                $ossAr = AttachmentTable::factory();
                $ossAr->file_original_name = array_pop($pathArr);
                $ossAr->file_size = 0;
                $ossAr->oss_server = $this->server['account'];
                $ossAr->oss_bucket = $this->server['BUCKET'];
                $ossAr->file_ext = strtolower($extension);
                $ossAr->file_type = $fileType;
                $ossAr->oss_object = $object;
                $ossAr->created_scene = 1;
                $ossAr->created_uid = 1;
                $ossAr->created_at = time();
                $ossAr->app = $app;
                $ossAr->app_id = $appid;
                if (!$ossAr->save()) {
                    return Result::error($ossAr->getError());
                }
            }
            return Result::success($this->server['CDN_DOMAIN'] . $object);
        } catch (Throwable $e) {
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