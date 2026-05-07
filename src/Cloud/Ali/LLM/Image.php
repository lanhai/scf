<?php

namespace Scf\Cloud\Ali\LLM;

use Scf\Cloud\Ali\LLM as DashScope;
use Scf\Cloud\Ali\LLM\Parameters\ImageParameters;
use Scf\Core\Result;

/**
 * DashScope 图片能力对象。
 *
 * 支持文生图、参考图编辑和异步生图。调用方只需要设置提示词上下文和图片参数，
 * 具体 DashScope endpoint、鉴权、错误归一化由底层 LLM 组件承担。
 */
class Image extends AbstractAbility {
    /**
     * 官方模型说明（Qwen-Image 图片生成与编辑模型）：
     * https://help.aliyun.com/zh/model-studio/image-model
     */

    /**
     * Qwen-Image 图片生成模型，适合文生图和基础图像创作。
     */
    public const ModelQwenImage = 'qwen-image';
    /**
     * Qwen-Image Plus 图片生成模型，适合稳定文生图和常规商业素材生成。
     */
    public const ModelQwenImagePlus = 'qwen-image-plus';
    /**
     * Qwen-Image Plus 图片生成模型，适合稳定文生图和常规商业素材生成。
     */
    public const ModelQwenImagePlus20260109 = 'qwen-image-plus-2026-01-09';
    /**
     * Qwen-Image Max 图片生成模型，适合高质量图像生成。
     */
    public const ModelQwenImageMax = 'qwen-image-max';
    /**
     * Qwen-Image Max 图片生成模型，适合高质量图像生成。
     */
    public const ModelQwenImageMax20251230 = 'qwen-image-max-2025-12-30';
    /**
     * Qwen-Image 2.0 图片生成模型，适合快速文生图和常规图像创作。
     */
    public const ModelQwenImage20 = 'qwen-image-2.0';
    /**
     * Qwen-Image 2.0 图片生成模型，适合快速文生图和常规图像创作。
     */
    public const ModelQwenImage20_20260303 = 'qwen-image-2.0-2026-03-03';
    /**
     * Qwen-Image 2.0 Pro 图片生成模型，适合高质量文生图和图像创作。
     */
    public const ModelQwenImage20Pro = 'qwen-image-2.0-pro';
    /**
     * Qwen-Image 2.0 Pro 图片生成模型，适合高质量文生图和图像创作。
     */
    public const ModelQwenImage20Pro20260422 = 'qwen-image-2.0-pro-2026-04-22';
    /**
     * Qwen-Image 2.0 Pro 图片生成模型，适合高质量文生图和图像创作。
     */
    public const ModelQwenImage20Pro20260303 = 'qwen-image-2.0-pro-2026-03-03';

    /**
     * Qwen-Image 图片编辑模型，适合基础指令改图和参考图编辑。
     */
    public const ModelQwenImageEdit = 'qwen-image-edit';
    /**
     * Qwen-Image Edit Plus 图片编辑模型，适合参考图编辑、风格调整和局部修改。
     */
    public const ModelQwenImageEditPlus = 'qwen-image-edit-plus';
    /**
     * Qwen-Image Edit Plus 图片编辑模型，适合参考图编辑、风格调整和局部修改。
     */
    public const ModelQwenImageEditPlus20251215 = 'qwen-image-edit-plus-2025-12-15';
    /**
     * Qwen-Image Edit Plus 图片编辑模型，适合参考图编辑、风格调整和局部修改。
     */
    public const ModelQwenImageEditPlus20251030 = 'qwen-image-edit-plus-2025-10-30';
    /**
     * Qwen-Image Edit Max 图片编辑模型，适合高质量参考图改图和局部重绘。
     */
    public const ModelQwenImageEditMax = 'qwen-image-edit-max';
    /**
     * Qwen-Image Edit Max 图片编辑模型，适合高质量参考图改图和局部重绘。
     */
    public const ModelQwenImageEditMax20260116 = 'qwen-image-edit-max-2026-01-16';

    /**
     * 官方模型说明（Wan 2.7 图片生成与编辑模型）：
     * https://help.aliyun.com/zh/model-studio/wan-image-generation-and-editing-api-reference
     */

    /**
     * Wan 2.7 图片生成与编辑模型，适合快速文生图、改图和组图。
     */
    public const ModelWanImage = 'wan2.7-image';
    /**
     * Wan 2.7 专业图片生成与编辑模型，适合高质量文生图、改图和组图。
     */
    public const ModelWanImagePro = 'wan2.7-image-pro';

    protected string $model = self::ModelQwenImage20Pro;
    protected array $imageUrls = [];
    protected bool $async = false;

    /**
     * 设置图片生成参数对象。
     *
     * @param ImageParameters|array<string,mixed> $parameters
     * @return static
     */
    public function generation(ImageParameters|array $parameters): static {
        return $this->parameters($parameters);
    }

    /**
     * 设置参考图 URL 列表。
     *
     * @param array<int,string> $urls
     * @return static
     */
    public function images(array $urls): static {
        $this->imageUrls = array_values(array_filter(array_map('trim', $urls)));
        return $this;
    }

    /**
     * 追加一张参考图 URL。
     *
     * @param string $url
     * @return static
     */
    public function image(string $url): static {
        $url = trim($url);
        if ($url !== '') {
            $this->imageUrls[] = $url;
        }
        return $this;
    }

    /**
     * 设置是否使用异步图片接口。
     *
     * @param bool $async
     * @return static
     */
    public function async(bool $async = true): static {
        $this->async = $async;
        return $this;
    }

    /**
     * 设置图片尺寸。
     *
     * @param string $size
     * @return static
     */
    public function size(string $size): static {
        return $this->parameter('size', $size);
    }

    /**
     * 设置输出图片数量。
     *
     * @param int $count
     * @return static
     */
    public function count(int $count): static {
        return $this->parameter('n', $count);
    }

    /**
     * 设置反向提示词。
     *
     * @param string $prompt
     * @return static
     */
    public function negative(string $prompt): static {
        return $this->parameter('negative_prompt', $prompt);
    }

    /**
     * 执行图片任务。
     *
     * @return Result
     */
    public function getResult(): Result {
        if ($invalid = $this->validateParameterCompatibility()) {
            return $invalid;
        }

        $prompt = $this->composePrompt();
        if ($this->usesWanImageGeneration()) {
            $result = DashScope::instance()->generateWanImage($prompt, $this->model, $this->defaultParameters(), $this->imageUrls, $this->inputExtras, $this->async);
        } elseif ($this->async && !$this->imageUrls) {
            $result = DashScope::instance()->generateImageAsync($prompt, $this->model, $this->defaultParameters(), $this->inputExtras);
        } else {
            $result = DashScope::instance()->generateImage($prompt, $this->model, $this->defaultParameters(), $this->imageUrls);
        }
        if ($result->hasError()) {
            return $result;
        }

        return Result::success([
            'images' => $this->extractImages($result->getData()),
            'task' => $result->getData('output') ?? null,
            'usage' => $result->getData('usage') ?? null,
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
     * 合并图片默认参数。
     *
     * @return array<string,mixed>
     */
    protected function defaultParameters(): array {
        if ($this->usesWanImageGeneration() || $this->model === self::ModelQwenImageEdit) {
            return array_merge([
                'watermark' => false,
            ], $this->parameters);
        }

        return array_merge([
            'prompt_extend' => true,
            'watermark' => false,
        ], $this->parameters);
    }

    /**
     * 校验当前模型族与参数字段是否匹配官方文档。
     *
     * @return Result|null
     */
    protected function validateParameterCompatibility(): ?Result {
        if ($this->usesWanImageGeneration()) {
            $unsupported = $this->intersectParameterKeys(['negative_prompt', 'prompt_extend']);
            if ($unsupported) {
                return Result::error('Wan 2.7 图片模型不支持参数: ' . implode(', ', $unsupported), 'DASHSCOPE_PARAMETER_UNSUPPORTED');
            }
            return null;
        }

        $wanOnly = $this->intersectParameterKeys(['enable_sequential', 'thinking_mode', 'color_palette', 'bbox_list']);
        if ($wanOnly) {
            return Result::error('Qwen-Image 模型不支持 Wan 图片专属参数: ' . implode(', ', $wanOnly), 'DASHSCOPE_PARAMETER_UNSUPPORTED');
        }

        if ($this->model === self::ModelQwenImageEdit) {
            $unsupported = $this->intersectParameterKeys(['size', 'prompt_extend']);
            if ($unsupported) {
                return Result::error('qwen-image-edit 基础模型不支持参数: ' . implode(', ', $unsupported), 'DASHSCOPE_PARAMETER_UNSUPPORTED');
            }
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

    /**
     * 从同步图片结果中抽取图片 URL。
     *
     * @param mixed $data
     * @return array<int,string>
     */
    protected function extractImages(mixed $data): array {
        $contents = $data['output']['choices'][0]['message']['content'] ?? [];
        $images = [];
        foreach ($contents as $content) {
            if (!empty($content['image'])) {
                $images[] = (string)$content['image'];
            }
        }
        return $images;
    }

    /**
     * 判断当前模型是否使用 Wan 图片生成新版 endpoint。
     *
     * @return bool
     */
    protected function usesWanImageGeneration(): bool {
        return str_starts_with($this->model, 'wan2.7-image');
    }
}
