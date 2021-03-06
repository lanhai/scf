<?php

namespace Scf\Aliyun;

use AlibabaCloud\Tea\Exception\TeaUnableRetryError;
use OSS\Core\OssException;
use OSS\OssClient;
use Scf\Client\Http;
use Scf\Core\Aliyun;
use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Exception\AppError;
use AlibabaCloud\Client\AlibabaCloud;
use AlibabaCloud\Client\Exception\ClientException;
use AlibabaCloud\Client\Exception\ServerException;
use AlibabaCloud\Green\Green as GreenClient;
use Darabonba\OpenApi\Models\Config;
use AlibabaCloud\Green\Green as AliGreen;
use Scf\Util\Sn;

class Green extends Aliyun {
    /**
     * @var GreenClient
     */
    protected GreenClient $_client;

    protected array $server;

    protected string $endpoint;

    /**
     * @throws AppError|ClientException
     */
    public function _init() {
        if (!isset($this->_config['default_server']) || !isset($this->_config['server'])) {
            throw new AppError('阿里云STS配置信息不存在');
        }
        $this->server = $this->_config['server'][$this->_config['default_server']];
        $account = $this->accounts[$this->server['account']];
        // 设置全局客户端
        AlibabaCloud::accessKeyClient($account['accessId'], $account['accessKey'])->regionId($this->server['regionId'])->asDefaultClient();

    }

    /**
     * 查询视频检测结果
     * @param $taskId
     * @return Result
     */
    public function scanVideoResultQuery($taskId): Result {
        try {
            $response = AliGreen::v20180509()->videoAsyncScanResults()
                ->body(JsonHelper::toJson($taskId))
                ->request();
            if (!$response->isSuccess() || !$result = JsonHelper::recover($response->getBody())) {
                return Result::error('请求失败', 'QUERY_FAIL');
            }
            $finish = false;
            $pass = true;
            $rate = 100;
            $blocks = [];
            $results = [];
            $datas = $result['data'];
            foreach ($datas as $data) {
                if ($data['code'] !== 200) {
                    if ($data['code'] == 280) {
                        return Result::success(compact('finish', 'pass', 'rate', 'blocks', 'results'));
                    }
                    return Result::error($data['msg'], 'SCAN_FAIL');
                }
                foreach ($data['results'] as $videoScanResult) {
                    $rate = min($videoScanResult['rate'], $rate);
                    $results[] = [
                        'label' => $videoScanResult['label'],
                        'video' => $data['url'],
                        'scene' => $videoScanResult['scene'],
                        'rate' => $videoScanResult['rate'],
                        'pass' => $videoScanResult['suggestion'] == 'pass'
                    ];
                    if ($videoScanResult['suggestion'] !== 'pass') {
                        $pass = false;
                        $blocks[] = [
                            'label' => $videoScanResult['label'],
                            'video' => $data['url'],
                            'scene' => $videoScanResult['scene'],
                            'rate' => $videoScanResult['rate'],
                            'pass' => $videoScanResult['suggestion'] == 'pass'
                        ];
                        break;
                    }
                }
            }
            $finish = true;
            return Result::success(compact('finish', 'pass', 'rate', 'blocks', 'results'));
        } catch (ClientException $exception) {
            return Result::error($exception->getMessage(), 'QUERY_FAIL');
        } catch (ServerException $exception) {
            return Result::error($exception->getErrorMessage(), 'QUERY_FAIL');
        }
    }

    /**
     * @param $videos
     * @param $taskId
     * @param $callback
     * @return Result
     */
    public function scanVideo($videos, $taskId, $callback = null): Result {
        try {
            $tasks = [];
            foreach ($videos as $video) {
                $arr = explode('.', $video);
                if (array_pop($arr) == 'mp3') {
                    return Result::error('不支持的文件格式', 'SCAN_FAIL');
                }
                $tasks[] = [
                    'dataId' => Sn::create_guid(),
                    'url' => $video
                ];
            }
            $response = AliGreen::v20180509()->videoAsyncScan()
                ->body(JsonHelper::toJson([
                    'tasks' => $tasks,
                    'scenes' => ["porn", "terrorism"],
                    //'callback' => $callback,
                    'seed' => $taskId
                ]))
                ->request();
            if (!$response->isSuccess() || !$result = JsonHelper::recover($response->getBody())) {
                return Result::error('内容检测失败,请稍后重试', 'SCAN_FAIL');
            }
            return Result::success($result['data']);
        } catch (ClientException $exception) {
            return Result::error('内容检测失败,请稍后重试:' . $exception->getMessage(), 'SCAN_FAIL');
        } catch (ServerException $exception) {
            return Result::error('内容检测失败,请稍后重试:' . $exception->getErrorMessage(), 'SCAN_FAIL');
        }
    }

    /**
     * @param $images
     * @return Result
     */
    public function scanImage($images): Result {
        try {
            $tasks = [];
            foreach ($images as $image) {
                $tasks[] = [
                    'dataId' => Sn::create_guid(),
                    'url' => $image
                ];
            }
            $response = AliGreen::v20180509()->imageSyncScan()
                ->body(JsonHelper::toJson(['tasks' => $tasks, 'scenes' => ["porn", "terrorism", "live", "qrcode"], 'bizType' => '业务场景']))
                ->request();
            if (!$response->isSuccess() || !$result = JsonHelper::recover($response->getBody())) {
                return Result::error('内容检测失败,请稍后重试', 'SCAN_FAIL');
            }
            $pass = true;
            $rate = 100;
            $blocks = [];
            $results = [];

            $datas = $result['data']??[];
            foreach ($datas as $data) {
                if ($data['code'] !== 200) {
                    return Result::error('图片内容检测失败:' . $data['msg'], 'SCAN_FAIL');
                }
                foreach ($data['results'] as $scanResult) {
                    $rate = min($scanResult['rate'], $rate);
                    $results[] = [
                        'label' => $scanResult['label'],
                        'image' => $data['url'],
                        'scene' => $scanResult['scene'],
                        'rate' => $scanResult['rate'],
                        'pass' => $scanResult['suggestion'] == 'pass'
                    ];
                    if ($scanResult['suggestion'] !== 'pass') {
                        $pass = false;
                        $blocks[] = [
                            'label' => $scanResult['label'],
                            'image' => $data['url'],
                            'scene' => $scanResult['scene'],
                            'rate' => $scanResult['rate'],
                            'pass' => $scanResult['suggestion'] == 'pass'
                        ];
                        break;
                    }
                }
            }
            return Result::success(compact('pass', 'rate', 'blocks', 'results'));
        } catch (ClientException $exception) {
            return Result::error('内容检测失败,请稍后重试:' . $exception->getMessage(), 'SCAN_FAIL');
        } catch (ServerException $exception) {
            return Result::error('内容检测失败,请稍后重试:' . $exception->getErrorMessage(), 'SCAN_FAIL');
        }
    }

    /**
     * @param $text
     * @return Result
     */
    public function scanText($text): Result {
        try {
            $task = [
                'dataId' => Sn::create_guid(),
                'content' => $text
            ];
            $response = AliGreen::v20180509()->textScan()
                ->body(JsonHelper::toJson(['tasks' => [$task], 'scenes' => ['antispam'], 'bizType' => 'text']))
                ->request();
            if (!$response->isSuccess() || !$result = JsonHelper::recover($response->getBody())) {
                return Result::error('文本内容检测失败,请稍后重试', 'SCAN_FAIL');
            }
            $pass = true;
            $rate = 100;
            $labels = [];
            $words = [];
            $results = $result['data'][0]['results'];
            foreach ($results as $result) {
                $rate = $result['rate'];
                if ($result['suggestion'] !== 'pass') {
                    foreach ($result['details'] as $detail) {
                        $labels[] = $detail['label'];
                        if (isset($detail['contexts'])) {
                            $contexts = ArrayHelper::getColumn($detail['contexts'], 'context');
                            $words = $words ? [...$words, ...$contexts] : $contexts;
                        }
                    }
                    $pass = false;
                    break;
                }
            }
            return Result::success(compact('pass', 'rate', 'labels', 'words', 'results'));
        } catch (ClientException $exception) {
            return Result::error('内容检测失败,请稍后重试:' . $exception->getMessage(), 'SCAN_FAIL');
        } catch (ServerException $exception) {
            return Result::error('内容检测失败,请稍后重试:' . $exception->getErrorMessage(), 'SCAN_FAIL');
        }
    }

}