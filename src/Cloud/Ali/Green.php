<?php

namespace Scf\Cloud\Ali;

use AlibabaCloud\Client\AlibabaCloud;
use AlibabaCloud\Client\Exception\ClientException;
use AlibabaCloud\Client\Exception\ServerException;
use Scf\Cloud\Aliyun;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Helper\ObjectHelper;
use Scf\Mode\Web\Exception\AppError;
use Scf\Util\Sn;
use Throwable;
use AlibabaCloud\SDK\Green\V20220302\Models\ImageModerationResponse;
use Darabonba\OpenApi\Models\Config;
use AlibabaCloud\Tea\Utils\Utils\RuntimeOptions;
use AlibabaCloud\SDK\Green\V20220302\Green as AliGreen;
use AlibabaCloud\SDK\Green\V20220302\Models\ImageModerationRequest;
use AlibabaCloud\Tea\Utils\Utils;

class Green extends Aliyun {
    /**
     * @var AliGreen
     */
    protected AliGreen $_client;

    protected array $server;

    protected string $endpoint;


    /**
     * @throws AppError
     */
    public function _init(): void {
        if (!isset($this->_config['default_server']) || !isset($this->_config['server'])) {
            throw new AppError('阿里云内容完全配置信息不存在');
        }
        $this->server = $this->_config['server'][$this->_config['default_server']];
        $account = $this->accounts[$this->server['account']];
        // 设置全局客户端
        $config = new Config([
            "accessKeyId" => $account['accessId'],
            "accessKeySecret" => $account['accessKey'],
            "endpoint" => $this->server['endpoint'],
        ]);
        $this->_client = new AliGreen($config);
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
        $pass = true;
        $rate = 100;
        $blocks = [];
        foreach ($images as $image) {
            // 创建RuntimeObject实例并设置运行参数。
            $runtime = new RuntimeOptions([]);
            // 检测参数构造。
            $request = new ImageModerationRequest();
            $serviceParameters = [
                'imageUrl' => $image,
                // 数据唯一标识。
                'dataId' => uniqid()
            ];
            // 图片检测service：内容安全控制台图片增强版规则配置的serviceCode，示例：baselineCheck
            $request->service = "baselineCheck";
            $request->serviceParameters = json_encode($serviceParameters);
            try {
                // 提交检测
                $response = $this->_client->imageModerationWithOptions($request, $runtime);
                if (Utils::equalNumber(500, $response->statusCode) || Utils::equalNumber(500, $response->body->code)) {
                    return Result::error('内容检测服务不可用');
                }
                $results = JsonHelper::recover(json_encode($response->body, JSON_UNESCAPED_UNICODE))['data']['result'] ?? [];
                foreach ($results as $result) {
                    if ($result['label'] !== 'nonLabel') {
                        $rate = min($result['confidence'], $rate);
                        $pass = false;
                        $blocks[] = [
                            'label' => $result['label'],
                            'file' => $image,
                            'confidence' => $result['confidence']
                        ];
                    }
                }
            } catch (\Exception $exception) {
                return Result::error('内容检测失败,请稍后重试(' . $exception->getMessage() . ')', 'SCAN_FAIL', $exception->getMessage());
            }
        }
        return Result::success(compact('pass', 'rate', 'blocks'));
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