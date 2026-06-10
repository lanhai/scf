<?php

namespace Scf\Cloud\Ali\LLM;

use Scf\Cloud\Ali\LLM as DashScope;
use Scf\Cloud\Ali\LLM\Parameters\TextOptions;
use Scf\Core\Result;

/**
 * DashScope 文本能力对象。
 *
 * 业务侧按一次任务创建一个 Text 对象，设置角色、数据、需求和结果格式后调用
 * `getResult()` 即可拿到归一化文本结果；底层默认走 OpenAI-compatible Chat。
 */
class Text extends AbstractAbility {
    /**
     * 官方模型说明（文本生成模型列表）：
     * https://help.aliyun.com/zh/model-studio/text-generation-model
     */

    /**
     * 通义千问通用文本模型，适合摘要、分类、预警和常规对话任务。
     */
    public const ModelQwen = 'qwen-plus';
    /**
     * 高能力文本模型，适合复杂分析、高质量生成和深度推理。
     */
    public const ModelQwenMax = 'qwen-max';
    /**
     * 高能力文本模型，适合复杂分析、高质量生成和深度推理。
     */
    public const ModelQwenMaxLatest = 'qwen-max-latest';
    /**
     * 通义千问通用文本模型，适合摘要、分类、预警和常规对话任务。
     */
    public const ModelQwenPlus = 'qwen-plus';
    /**
     * 通义千问通用文本模型，适合摘要、分类、预警和常规对话任务。
     */
    public const ModelQwenPlusLatest = 'qwen-plus-latest';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwenFlash = 'qwen-flash';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwenFlash20250728 = 'qwen-flash-2025-07-28';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwenTurbo = 'qwen-turbo';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwenTurboLatest = 'qwen-turbo-latest';
    /**
     * 长上下文文本模型，适合长文档、长聊天记录和知识整理。
     */
    public const ModelQwenLong = 'qwen-long';
    /**
     * 长上下文文本模型，适合长文档、长聊天记录和知识整理。
     */
    public const ModelQwenLongLatest = 'qwen-long-latest';

    /**
     * 数学专项文本模型，适合数学推理、公式理解和解题场景。
     */
    public const ModelQwenMathPlus = 'qwen-math-plus';
    /**
     * 数学专项文本模型，适合数学推理、公式理解和解题场景。
     */
    public const ModelQwenMathPlusLatest = 'qwen-math-plus-latest';
    /**
     * 数学专项文本模型，适合数学推理、公式理解和解题场景。
     */
    public const ModelQwenMathTurbo = 'qwen-math-turbo';
    /**
     * 数学专项文本模型，适合数学推理、公式理解和解题场景。
     */
    public const ModelQwenMathTurboLatest = 'qwen-math-turbo-latest';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwenCoderPlus = 'qwen-coder-plus';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwenCoderPlusLatest = 'qwen-coder-plus-latest';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwenCoderTurbo = 'qwen-coder-turbo';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwenCoderTurboLatest = 'qwen-coder-turbo-latest';

    /**
     * 高能力文本模型，适合复杂分析、高质量生成和深度推理。
     */
    public const ModelQwen36MaxPreview = 'qwen3.6-max-preview';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen36Plus = 'qwen3.6-plus';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen36Plus20260402 = 'qwen3.6-plus-2026-04-02';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwen36Flash = 'qwen3.6-flash';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwen36Flash20260416 = 'qwen3.6-flash-2026-04-16';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen36_35bA3b = 'qwen3.6-35b-a3b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen35Plus = 'qwen3.5-plus';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen35Plus20260215 = 'qwen3.5-plus-2026-02-15';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwen35Flash = 'qwen3.5-flash';
    /**
     * 轻量高速文本模型，适合低延迟、成本敏感的摘要、分类和对话。
     */
    public const ModelQwen35Flash20260223 = 'qwen3.5-flash-2026-02-23';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen35_397bA17b = 'qwen3.5-397b-a17b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen35_122bA10b = 'qwen3.5-122b-a10b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen35_35bA3b = 'qwen3.5-35b-a3b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen35_27b = 'qwen3.5-27b';
    /**
     * 高能力文本模型，适合复杂分析、高质量生成和深度推理。
     */
    public const ModelQwen3Max = 'qwen3-max';
    /**
     * 高能力文本模型，适合复杂分析、高质量生成和深度推理。
     */
    public const ModelQwen3MaxPreview = 'qwen3-max-preview';
    /**
     * 高能力文本模型，适合复杂分析、高质量生成和深度推理。
     */
    public const ModelQwen3Max20260123 = 'qwen3-max-2026-01-23';
    /**
     * 高能力文本模型，适合复杂分析、高质量生成和深度推理。
     */
    public const ModelQwen3Max20250923 = 'qwen3-max-2025-09-23';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwen3CoderPlus = 'qwen3-coder-plus';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwen3CoderPlus20250722 = 'qwen3-coder-plus-2025-07-22';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwen3CoderFlash = 'qwen3-coder-flash';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwen3CoderFlash20250728 = 'qwen3-coder-flash-2025-07-28';
    /**
     * 代码专项文本模型，适合代码生成、调试、解释和工程分析。
     */
    public const ModelQwen3CoderNext = 'qwen3-coder-next';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3Next80bA3bThinking = 'qwen3-next-80b-a3b-thinking';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3Next80bA3bInstruct = 'qwen3-next-80b-a3b-instruct';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_235bA22bThinking2507 = 'qwen3-235b-a22b-thinking-2507';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_235bA22bInstruct2507 = 'qwen3-235b-a22b-instruct-2507';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_30bA3bThinking2507 = 'qwen3-30b-a3b-thinking-2507';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_30bA3bInstruct2507 = 'qwen3-30b-a3b-instruct-2507';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_235bA22b = 'qwen3-235b-a22b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_32b = 'qwen3-32b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_30bA3b = 'qwen3-30b-a3b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_14b = 'qwen3-14b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_8b = 'qwen3-8b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_4b = 'qwen3-4b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_17b = 'qwen3-1.7b';
    /**
     * Qwen 3.x 文本模型，适合通用生成、推理、工具调用和业务分析。
     */
    public const ModelQwen3_06b = 'qwen3-0.6b';

    /**
     * DeepSeek 文本模型，适合复杂推理、代码分析和通用对话。
     */
    public const ModelDeepseekV4 = 'deepseek-v4-pro';
    /**
     * DeepSeek 文本模型，适合复杂推理、代码分析和通用对话。
     */
    public const ModelDeepseekV4Pro = 'deepseek-v4-pro';
    /**
     * DeepSeek 文本模型，适合复杂推理、代码分析和通用对话。
     */
    public const ModelDeepseekV4Flash = 'deepseek-v4-flash';
    /**
     * DeepSeek 文本模型，适合复杂推理、代码分析和通用对话。
     */
    public const ModelDeepseekV32 = 'deepseek-v3.2';
    /**
     * DeepSeek 文本模型，适合复杂推理、代码分析和通用对话。
     */
    public const ModelDeepseekV32Exp = 'deepseek-v3.2-exp';
    /**
     * DeepSeek 文本模型，适合复杂推理、代码分析和通用对话。
     */
    public const ModelDeepseekV31 = 'deepseek-v3.1';
    /**
     * DeepSeek 文本模型，适合复杂推理、代码分析和通用对话。
     */
    public const ModelDeepseekR10528 = 'deepseek-r1-0528';
    /**
     * GLM 兼容文本模型，适合通用对话、推理和内容生成。
     */
    public const ModelGlm51 = 'glm-5.1';
    /**
     * Kimi 兼容文本模型，适合长文本理解、推理和内容生成。
     */
    public const ModelKimiK26 = 'kimi-k2.6';
    /**
     * MiniMax 兼容文本模型，适合通用对话、推理和内容生成。
     */
    public const ModelMinimaxM25 = 'MiniMax-M2.5';

    /**
     * 官方模型说明（视觉理解模型）：
     * https://help.aliyun.com/zh/model-studio/vision
     */

    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen36VisionPlus = 'qwen3.6-plus';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen36VisionFlash = 'qwen3.6-flash';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen35OmniPlus = 'qwen3.5-omni-plus';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen35OmniPlus20260315 = 'qwen3.5-omni-plus-2026-03-15';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen35OmniFlash = 'qwen3.5-omni-flash';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen35OmniFlash20260315 = 'qwen3.5-omni-flash-2026-03-15';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen3OmniFlash = 'qwen3-omni-flash';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen3VlPlus = 'qwen3-vl-plus';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen3VlPlus20251219 = 'qwen3-vl-plus-2025-12-19';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen3VlPlus20250923 = 'qwen3-vl-plus-2025-09-23';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwen3VlFlash = 'qwen3-vl-flash';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwenVlMax = 'qwen-vl-max';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwenVlMaxLatest = 'qwen-vl-max-latest';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwenVlMax20250813 = 'qwen-vl-max-2025-08-13';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwenVlPlus = 'qwen-vl-plus';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwenVlPlusLatest = 'qwen-vl-plus-latest';
    /**
     * 视觉理解/多模态模型，适合图片内容理解、OCR 和图文问答。
     */
    public const ModelQwenVlPlus20250815 = 'qwen-vl-plus-2025-08-15';

    /**
     * 官方模型说明（Qwen-VL OCR 模型）：
     * https://help.aliyun.com/zh/model-studio/qwen-vl-ocr-api-reference
     */

    /**
     * 视觉 OCR 模型，适合图片文字识别、票据识别和结构化抽取。
     */
    public const ModelQwenVlOcr = 'qwen-vl-ocr';
    /**
     * 视觉 OCR 模型，适合图片文字识别、票据识别和结构化抽取。
     */
    public const ModelQwenVlOcrLatest = 'qwen-vl-ocr-latest';
    /**
     * 视觉 OCR 模型，适合图片文字识别、票据识别和结构化抽取。
     */
    public const ModelQwenVlOcr20251120 = 'qwen-vl-ocr-2025-11-20';

    /**
     * 官方模型说明（文本向量模型）：
     * https://help.aliyun.com/zh/model-studio/text-embedding-synchronous-api
     */

    /**
     * 文本向量模型，适合语义检索、聚类、推荐和相似度召回。
     */
    public const ModelTextEmbeddingV4 = 'text-embedding-v4';
    /**
     * 文本向量模型，适合语义检索、聚类、推荐和相似度召回。
     */
    public const ModelTextEmbeddingV3 = 'text-embedding-v3';

    protected string $model = self::ModelQwenPlus;
    protected array $messages = [];

    /**
     * 设置文本生成参数对象。
     *
     * @param TextOptions|array<string,mixed> $options
     * @return static
     */
    public function generation(TextOptions|array $options): static {
        return $this->options($options);
    }

    /**
     * 追加一条原始对话消息。
     *
     * @param string $role
     * @param mixed $content
     * @return static
     */
    public function message(string $role, mixed $content): static {
        $this->messages[] = [
            'role' => $role,
            'content' => $content,
        ];
        return $this;
    }

    /**
     * 批量设置原始对话消息。
     *
     * @param array<int,array<string,mixed>> $messages
     * @return static
     */
    public function messages(array $messages): static {
        $this->messages = $messages;
        return $this;
    }

    /**
     * 执行文本生成任务。
     *
     * @return Result
     */
    public function getResult(): Result {
        $options = array_merge(['stream' => false], $this->options);
        if ($this->jsonResult) {
            $options['response_format'] = $options['response_format'] ?? ['type' => 'json_object'];
        }

        $result = DashScope::instance()->chat($this->buildMessages(), $this->model, $options);
        if ($result->hasError()) {
            return $result;
        }

        $content = (string)($result->getData('choices')[0]['message']['content'] ?? '');
        return Result::success([
            'content' => $content,
            'parsed' => $this->jsonResult ? $this->parseJsonContent($content) : null,
            'finish_reason' => $result->getData('choices')[0]['finish_reason'] ?? null,
            'usage' => $result->getData('usage') ?? null,
            'model' => $result->getData('model') ?? $this->model,
            'raw' => $result->getData(),
        ]);
    }

    /**
     * 执行文本生成流式任务。
     *
     * 该方法是 getResult() 的显式流式版本；getResult()/result() 保持原有同步
     * 完整响应行为。回调返回 false 时会中止本次流式读取。
     *
     * @param callable $onEvent function(array $event): bool|void
     * @return Result
     */
    public function stream(callable $onEvent): Result {
        $options = array_merge(['stream' => true], $this->options);
        if ($this->jsonResult) {
            $options['response_format'] = $options['response_format'] ?? ['type' => 'json_object'];
        }

        $result = DashScope::instance()->chatStream($this->buildMessages(), $this->model, $options, $onEvent);
        if ($result->hasError()) {
            return $result;
        }

        $content = (string)$result->getData('content');
        return Result::success([
            'content' => $content,
            'parsed' => $this->jsonResult ? $this->parseJsonContent($content) : null,
            'finish_reason' => $result->getData('finish_reason'),
            'usage' => $result->getData('usage'),
            'model' => $result->getData('model') ?? $this->model,
            'raw' => $result->getData('raw'),
        ]);
    }

    /**
     * `stream()` 的语义化别名。
     *
     * @param callable $onEvent
     * @return Result
     */
    public function streamResult(callable $onEvent): Result {
        return $this->stream($onEvent);
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
     * 构造 Chat Completions 消息。
     *
     * @return array<int,array<string,mixed>>
     */
    protected function buildMessages(): array {
        $messages = $this->messages;
        if ($this->rolePrompt !== '' && (($messages[0]['role'] ?? '') !== 'system')) {
            array_unshift($messages, ['role' => 'system', 'content' => $this->rolePrompt]);
        }
        $prompt = $this->composePrompt(false);
        if (!$messages && $prompt !== '') {
            return [['role' => 'user', 'content' => $prompt]];
        }
        if ($prompt !== '') {
            $messages[] = ['role' => 'user', 'content' => $prompt];
        }
        if ($messages && (($messages[array_key_last($messages)]['role'] ?? '') === 'system')) {
            $messages[] = ['role' => 'user', 'content' => '请根据角色要求完成本次文本任务。'];
        }
        return $messages ?: [['role' => 'user', 'content' => '请根据上下文完成任务。']];
    }
}
