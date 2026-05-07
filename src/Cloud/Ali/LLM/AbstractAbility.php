<?php

namespace Scf\Cloud\Ali\LLM;

use Scf\Cloud\Ali\LLM\Parameters\GenerationParameters;
use Scf\Core\Struct;
use Scf\Helper\JsonHelper;

/**
 * DashScope 能力对象的提示词与参数装配基类。
 *
 * 这一层不直接关心 HTTP 协议，而是为 Text/Image/Video 三类能力提供统一的
 * “角色提示词 + 业务数据 + 需求提示词 + 结果格式定义”上下文结构。对象本身
 * 带有可变属性，必须按一次调用创建一个实例，避免 Swoole 长驻进程串状态。
 */
abstract class AbstractAbility {
    protected string $model = '';
    protected string $rolePrompt = '';
    protected mixed $businessData = null;
    protected string $businessDataLabel = '业务数据';
    protected string $requirementPrompt = '';
    protected mixed $resultFormatDefinition = null;
    protected bool $jsonResult = false;
    protected array $options = [];
    protected array $parameters = [];
    protected array $inputExtras = [];

    /**
     * 创建一次性能力对象。
     *
     * @param string|null $model
     * @return static
     */
    public static function create(?string $model = null): static {
        $instance = new static();
        if ($model !== null && $model !== '') {
            $instance->model($model);
        }
        return $instance;
    }

    /**
     * 设置模型名称。
     *
     * 模型名完全由调用方传入，不在框架层做枚举限制，以兼容 DashScope 官方
     * 新增或下线模型时的动态调整。
     *
     * @param string $model
     * @return static
     */
    public function model(string $model): static {
        $this->model = $model;
        return $this;
    }

    /**
     * 设置角色提示词。
     *
     * @param string $prompt
     * @return static
     */
    public function role(string $prompt): static {
        $this->rolePrompt = trim($prompt);
        return $this;
    }

    /**
     * 设置业务数据。
     *
     * @param mixed $data
     * @param string $label
     * @return static
     */
    public function data(mixed $data, string $label = '业务数据'): static {
        $this->businessData = $data;
        $this->businessDataLabel = trim($label) ?: '业务数据';
        return $this;
    }

    /**
     * 设置需求提示词。
     *
     * @param string $prompt
     * @return static
     */
    public function requirement(string $prompt): static {
        $this->requirementPrompt = trim($prompt);
        return $this;
    }

    /**
     * 设置结果格式定义。
     *
     * 文本能力默认会在设置格式后请求 JSON object；图片/视频能力则把格式定义
     * 写进提示词，用来约束画面或任务产物描述。
     *
     * @param mixed $definition
     * @param bool $jsonResult
     * @return static
     */
    public function format(mixed $definition, bool $jsonResult = true): static {
        $this->resultFormatDefinition = $definition;
        $this->jsonResult = $jsonResult;
        return $this;
    }

    /**
     * 合并模型调用选项。
     *
     * @param array<string,mixed>|Struct $options
     * @return static
     */
    public function options(array|Struct $options): static {
        $this->options = array_merge($this->options, $this->normalizeParameterPayload($options));
        return $this;
    }

    /**
     * 设置单个模型调用选项。
     *
     * @param string $key
     * @param mixed $value
     * @return static
     */
    public function option(string $key, mixed $value): static {
        $this->options[$key] = $value;
        return $this;
    }

    /**
     * 合并原生模型参数。
     *
     * @param array<string,mixed>|Struct $parameters
     * @return static
     */
    public function parameters(array|Struct $parameters): static {
        if ($parameters instanceof GenerationParameters) {
            $this->inputExtras = array_merge($this->inputExtras, $parameters->toInputExtras());
        }
        $this->parameters = array_merge($this->parameters, $this->normalizeParameterPayload($parameters));
        return $this;
    }

    /**
     * 设置单个原生模型参数。
     *
     * @param string $key
     * @param mixed $value
     * @return static
     */
    public function parameter(string $key, mixed $value): static {
        $this->parameters[$key] = $value;
        return $this;
    }

    /**
     * 合并 DashScope input 扩展字段。
     *
     * @param array<string,mixed>|Struct $extras
     * @return static
     */
    public function inputExtras(array|Struct $extras): static {
        $this->inputExtras = array_merge($this->inputExtras, $this->normalizeParameterPayload($extras));
        return $this;
    }

    /**
     * 设置温度参数。
     *
     * @param float $temperature
     * @return static
     */
    public function temperature(float $temperature): static {
        $this->options['temperature'] = $temperature;
        $this->parameters['temperature'] = $temperature;
        return $this;
    }

    /**
     * 设置最大输出 token。
     *
     * @param int $maxTokens
     * @return static
     */
    public function maxTokens(int $maxTokens): static {
        $this->options['max_tokens'] = $maxTokens;
        return $this;
    }

    /**
     * 归一化数组或 Struct 参数对象。
     *
     * @param array<string,mixed>|Struct $parameters
     * @return array<string,mixed>
     */
    protected function normalizeParameterPayload(array|Struct $parameters): array {
        if ($parameters instanceof GenerationParameters) {
            return $parameters->toParameters();
        }
        if ($parameters instanceof Struct) {
            return $this->filterEmptyParameters($parameters->toArray());
        }
        return $this->filterEmptyParameters($parameters);
    }

    /**
     * 过滤参数对象中的空值，保留 false 和 0 等有效配置。
     *
     * @param array<string,mixed> $parameters
     * @return array<string,mixed>
     */
    protected function filterEmptyParameters(array $parameters): array {
        $payload = [];
        foreach ($parameters as $key => $value) {
            if ($value === null || $value === '') {
                continue;
            }
            if (is_array($value)) {
                $value = $this->filterEmptyParameters($value);
                if (!$value) {
                    continue;
                }
            }
            $payload[$key] = $value;
        }
        return $payload;
    }

    /**
     * 组合标准提示词文本。
     *
     * @return string
     */
    protected function composePrompt(bool $includeRole = true): string {
        $sections = [];
        if ($includeRole && $this->rolePrompt !== '') {
            $sections[] = "【角色提示词】\n" . $this->rolePrompt;
        }
        if ($this->businessData !== null) {
            $sections[] = "【{$this->businessDataLabel}】\n" . $this->stringify($this->businessData);
        }
        if ($this->requirementPrompt !== '') {
            $sections[] = "【需求提示词】\n" . $this->requirementPrompt;
        }
        if ($this->resultFormatDefinition !== null) {
            $sections[] = "【结果格式定义】\n" . $this->stringify($this->resultFormatDefinition);
        }
        return implode("\n\n", $sections);
    }

    /**
     * 把任意业务数据转成模型可读文本。
     *
     * @param mixed $value
     * @return string
     */
    protected function stringify(mixed $value): string {
        if (is_string($value)) {
            return trim($value);
        }
        return (string)JsonHelper::toJson($value);
    }

    /**
     * 从模型文本中尝试恢复 JSON。
     *
     * @param string $content
     * @return mixed
     */
    protected function parseJsonContent(string $content): mixed {
        $content = trim($content);
        if ($content === '') {
            return null;
        }
        if (JsonHelper::is($content)) {
            return JsonHelper::recover($content);
        }
        if (preg_match('/```(?:json)?\s*(.*?)```/is', $content, $matches) && JsonHelper::is(trim($matches[1]))) {
            return JsonHelper::recover(trim($matches[1]));
        }
        return null;
    }
}
