<?php

namespace Scf\Cloud\Ali\LLM;

use Scf\Cloud\Ali\LLM as DashScope;
use Scf\Cloud\Ali\LLM\Parameters\VideoParameters;
use Scf\Core\Result;

/**
 * DashScope 视频能力对象。
 *
 * 当前 Wan 视频生成官方 HTTP 协议以异步任务为主，因此该对象统一创建任务并
 * 返回 task_id；调用方随后可用 `LLM::instance()->task($taskId)` 轮询结果。
 */
class Video extends AbstractAbility {
    /**
     * 官方模型说明（Wan 文生视频模型）：
     * https://help.aliyun.com/zh/model-studio/text-to-video-api-reference
     */

    /**
     * Wan 文生视频模型，适合通过文本提示词生成视频任务。
     */
    public const ModelWan27TextToVideo = 'wan2.7-t2v';
    /**
     * Wan 文生视频模型，适合通过文本提示词生成视频任务。
     */
    public const ModelWan26TextToVideo = 'wan2.6-t2v';
    /**
     * Wan 文生视频模型，适合通过文本提示词生成视频任务。
     */
    public const ModelWan26TextToVideoFlash = 'wan2.6-t2v-flash';

    /**
     * 官方模型说明（Wan 图生视频模型）：
     * https://help.aliyun.com/zh/model-studio/image-to-video-general-api-reference
     */

    /**
     * Wan 图生视频模型，适合基于首帧、首尾帧或图片素材生成视频。
     */
    public const ModelWan27ImageToVideo = 'wan2.7-i2v';
    /**
     * Wan 图生视频模型，适合基于首帧、首尾帧或图片素材生成视频。
     */
    public const ModelWan26ImageToVideo = 'wan2.6-i2v';
    /**
     * Wan 图生视频模型，适合基于首帧、首尾帧或图片素材生成视频。
     */
    public const ModelWan26ImageToVideoFlash = 'wan2.6-i2v-flash';

    /**
     * 官方模型说明（Wan 参考生视频模型）：
     * https://help.aliyun.com/zh/model-studio/wan-video-to-video-api-reference
     */

    /**
     * Wan 参考生视频模型，适合基于参考图、参考视频和素材生成视频。
     */
    public const ModelWan27ReferenceToVideo = 'wan2.7-r2v';
    /**
     * Wan 参考生视频模型，适合基于参考图、参考视频和素材生成视频。
     */
    public const ModelWan26ReferenceToVideo = 'wan2.6-r2v';
    /**
     * Wan 参考生视频模型，适合基于参考图、参考视频和素材生成视频。
     */
    public const ModelWan26ReferenceToVideoFlash = 'wan2.6-r2v-flash';

    protected string $model = self::ModelWan26TextToVideo;
    protected ?string $imageUrl = null;
    protected array $referenceUrls = [];
    protected array $media = [];
    protected ?string $audioUrl = null;

    /**
     * 设置视频生成参数对象。
     *
     * VideoParameters 中的 negative_prompt 会自动进入 DashScope input，其余字段
     * 进入 parameters，调用方不用关心官方请求体分层。
     *
     * @param VideoParameters|array<string,mixed> $parameters
     * @return static
     */
    public function generation(VideoParameters|array $parameters): static {
        return $this->parameters($parameters);
    }

    /**
     * 设置图生视频首帧图片。
     *
     * @param string $url
     * @return static
     */
    public function image(string $url): static {
        $this->imageUrl = trim($url) ?: null;
        return $this;
    }

    /**
     * 设置参考素材 URL 列表。
     *
     * @param array<int,string> $urls
     * @return static
     */
    public function references(array $urls): static {
        $this->referenceUrls = array_values(array_filter(array_map('trim', $urls)));
        return $this;
    }

    /**
     * 设置 Wan 2.7 新协议媒体素材列表。
     *
     * 每个媒体对象需遵循官方 `media` 结构，例如
     * `['type' => 'first_frame', 'url' => 'https://...']`、
     * `['type' => 'reference_image', 'url' => 'https://...']`、
     * `['type' => 'reference_video', 'url' => 'https://...']`。
     * 该方法用于需要精确控制素材类型、参考音色 reference_voice 或 driving_audio 的场景。
     *
     * @param array<int,array<string,mixed>> $media
     * @return static
     */
    public function media(array $media): static {
        $this->media = $media;
        return $this;
    }

    /**
     * 追加参考素材 URL。
     *
     * @param string $url
     * @return static
     */
    public function reference(string $url): static {
        $url = trim($url);
        if ($url !== '') {
            $this->referenceUrls[] = $url;
        }
        return $this;
    }

    /**
     * 设置音频 URL。
     *
     * @param string $url
     * @return static
     */
    public function audio(string $url): static {
        $this->audioUrl = trim($url) ?: null;
        return $this;
    }

    /**
     * 设置视频尺寸。
     *
     * @param string $size
     * @return static
     */
    public function size(string $size): static {
        return $this->parameter('size', $size);
    }

    /**
     * 设置 Wan 2.7 分辨率档位。
     *
     * @param string $resolution 可选值：720P、1080P
     * @return static
     */
    public function resolution(string $resolution): static {
        return $this->parameter('resolution', $resolution);
    }

    /**
     * 设置 Wan 2.7 输出宽高比。
     *
     * @param string $ratio 可选值：16:9、9:16、1:1、4:3、3:4
     * @return static
     */
    public function ratio(string $ratio): static {
        return $this->parameter('ratio', $ratio);
    }

    /**
     * 设置视频时长。
     *
     * @param int $seconds
     * @return static
     */
    public function duration(int $seconds): static {
        return $this->parameter('duration', $seconds);
    }

    /**
     * 设置反向提示词。
     *
     * @param string $prompt
     * @return static
     */
    public function negative(string $prompt): static {
        return $this->inputExtras(['negative_prompt' => $prompt]);
    }

    /**
     * 执行视频生成任务。
     *
     * @return Result
     */
    public function getResult(): Result {
        if ($invalid = $this->validateParameterCompatibility()) {
            return $invalid;
        }

        $result = DashScope::instance()->generateVideo($this->buildInput(), $this->model, $this->defaultParameters());
        if ($result->hasError()) {
            return $result;
        }

        return Result::success([
            'task_id' => $result->getData('output')['task_id'] ?? null,
            'task_status' => $result->getData('output')['task_status'] ?? null,
            'request_id' => $result->getData('request_id') ?? null,
            'raw' => $result->getData(),
        ]);
    }

    /**
     * `getResult()` 的语义化别名。
     *
     * @return Result
     */
    public function result(): Result {
        return $this->getResult();
    }

    /**
     * 构造视频任务 input。
     *
     * @return array<string,mixed>
     */
    protected function buildInput(): array {
        $input = array_merge(['prompt' => $this->composePrompt()], $this->inputExtras);
        if ($this->usesWan27Protocol()) {
            $media = $this->media;
            if ($this->imageUrl !== null) {
                $media[] = [
                    'type' => 'first_frame',
                    'url' => $this->imageUrl,
                ];
            }
            foreach ($this->referenceUrls as $url) {
                $media[] = [
                    'type' => 'reference_image',
                    'url' => $url,
                ];
            }
            if ($media) {
                $input['media'] = $media;
            }
        } else {
            if ($this->imageUrl !== null) {
                $input['img_url'] = $this->imageUrl;
            }
            if ($this->referenceUrls) {
                $input['reference_urls'] = $this->referenceUrls;
            }
        }
        if ($this->audioUrl !== null) {
            $input['audio_url'] = $this->audioUrl;
        }
        return $input;
    }

    /**
     * 合并视频默认参数。
     *
     * @return array<string,mixed>
     */
    protected function defaultParameters(): array {
        return array_merge([
            'prompt_extend' => true,
            'watermark' => false,
        ], $this->parameters);
    }

    /**
     * 判断当前模型是否使用 Wan 2.7 新版媒体协议。
     *
     * @return bool
     */
    protected function usesWan27Protocol(): bool {
        return str_starts_with($this->model, 'wan2.7-');
    }

    /**
     * 校验当前模型协议与参数字段是否匹配官方文档。
     *
     * @return Result|null
     */
    protected function validateParameterCompatibility(): ?Result {
        if ($this->usesWan27Protocol()) {
            $unsupported = $this->intersectParameterKeys(['size']);
            if ($unsupported) {
                return Result::error('Wan 2.7 视频模型不支持 size 参数，请改用 resolution 与 ratio', 'DASHSCOPE_PARAMETER_UNSUPPORTED');
            }
            return null;
        }

        $unsupported = $this->intersectParameterKeys(['resolution', 'ratio']);
        if ($unsupported) {
            return Result::error('Wan 2.6 视频模型不支持参数: ' . implode(', ', $unsupported) . '，请使用 size', 'DASHSCOPE_PARAMETER_UNSUPPORTED');
        }

        return null;
    }

    /**
     * 取当前已设置参数和指定字段集合的交集。
     *
     * @param array<int,string> $keys
     * @return array<int,string>
     */
    protected function intersectParameterKeys(array $keys): array {
        return array_values(array_intersect($keys, array_keys($this->parameters)));
    }
}
