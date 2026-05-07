<?php

namespace Scf\Cloud\Ali\LLM\Parameters;

/**
 * 文本生成可选参数结构。
 *
 * 该结构对应百炼 OpenAI-compatible Chat Completions 请求体中除
 * `model/messages` 之外、官方当前文档明确列出的同步文本生成参数。当前 Text
 * 能力封装为非流式结果解析，因此不在结构中暴露 `stream/stream_options`；
 * 需要 SSE 时应单独扩展流式能力，避免业务侧误传后无法解析响应。
 */
class TextOptions extends GenerationParameters {

    /**
     * @var float|null 采样温度，控制输出随机性。官方取值范围为 `[0, 2)`，
     * temperature 与 top_p 都会影响多样性，官方建议二者只设置一个。CRM 摘要、
     * 分类、预警等稳定任务建议使用 0.1 到 0.4；创意写作可适当提高。
     */
    public ?float $temperature = null;

    /**
     * @var float|null 核采样概率阈值。官方取值范围为 `(0, 1.0]`，值越大输出越
     * 多样，值越小输出越确定。与 temperature 功能相近，通常只调整其中一个。
     */
    public ?float $top_p = null;

    /**
     * @var int|null 生成回复的最大 Token 数。不同模型上限不同，超出模型支持的
     * 输出长度会被服务端拒绝或截断；聊天记录分析建议按输出 schema 预估设置，
     * 避免无界生成增加成本。
     */
    public ?int $max_tokens = null;

    /**
     * @var string|array<int,string>|null 停止序列。可传单个字符串或字符串数组，
     * 模型生成内容命中任一停止序列时提前结束。适合模板边界控制；停止序列本身
     * 通常不会出现在最终输出中。
     */
    public string|array|null $stop = null;

    /**
     * @var int|null top-k 采样候选数量。官方要求为大于或等于 0 的整数；传 null
     * 或大于 100 时禁用 top_k 策略，仅 top_p 生效。该字段不是 OpenAI 标准参数，
     * 但百炼兼容接口支持作为顶层扩展参数传入。
     */
    public ?int $top_k = null;

    /**
     * @var int|null 随机数种子。官方取值范围为 `[0, 2147483647]`。相同输入与
     * 相同 seed 会尽量返回可复现结果，但模型生成仍具有概率性，不能保证完全一致。
     */
    public ?int $seed = null;

    /**
     * @var float|null 内容重复惩罚。官方取值范围为 `[-2.0, 2.0]`。正值会降低
     * 模型重复已出现内容的概率；负值会增强重复倾向。CRM 分析通常保持默认即可。
     */
    public ?float $presence_penalty = null;

    /**
     * @var float|null 连续序列重复惩罚。官方要求大于 0，`1.0` 表示不做惩罚；
     * 值越大越不容易重复。该字段不是 OpenAI 标准参数，但百炼兼容接口支持。
     */
    public ?float $repetition_penalty = null;

    /**
     * @var array<string,mixed>|null 响应格式约束。常用枚举为
     * `['type' => 'json_object']`，用于要求模型输出 JSON 对象；部分模型也支持
     * JSON Schema 结构化输出，具体结构以百炼 Chat Completions 文档为准。
     * Text::format() 会自动补充 `json_object`。
     */
    public ?array $response_format = null;

    /**
     * @var array<int,array<string,mixed>>|null 工具定义列表。每项遵循 OpenAI-compatible
     * tools 结构，常见格式为 `['type'=>'function','function'=>['name'=>..., 'description'=>..., 'parameters'=>...]]`。
     * 模型只会返回 tool_calls，真正的函数执行、重试和结果回填必须由业务侧完成。
     */
    public ?array $tools = null;

    /**
     * @var mixed 工具选择策略。官方兼容 OpenAI 取值：`'none'` 表示不调用工具，
     * `'auto'` 表示模型自行判断，或传入指定工具结构
     * `['type'=>'function','function'=>['name'=>'tool_name']]` 强制选择某个工具。
     */
    public mixed $tool_choice = null;

    /**
     * @var bool|null 是否允许并行工具调用。`true` 表示模型可在一次响应里返回多个
     * tool_calls；`false` 表示一次只选择一个工具。需要严格顺序执行的业务流程建议设为 false。
     */
    public ?bool $parallel_tool_calls = null;

    /**
     * @var int|null 生成候选响应数量。官方取值范围为 `1-4`，默认值 1；仅支持
     * Qwen3 非思考模式和 qwen-plus-character。传入 tools 时官方要求 n 为 1。
     * 增大 n 会增加输出 Token 消耗。
     */
    public ?int $n = null;

    /**
     * @var bool|null 是否启用混合思考模型的思考模式。可选值：`true` 开启、
     * `false` 关闭；适用于 Qwen3.6、Qwen3.5、Qwen3、Qwen3-Omni-Flash、
     * Qwen3-VL 等支持混合思考的模型。开启后思考内容通过 reasoning_content 返回。
     */
    public ?bool $enable_thinking = null;

    /**
     * @var bool|null 是否把历史 assistant 消息中的 reasoning_content 拼回模型输入。
     * 官方默认 `false`；目前支持 qwen3.6-max-preview、qwen3.6-plus、
     * qwen3.6-plus-2026-04-02、kimi-k2.6 等模型。开启后历史思考内容会计入输入
     * Token 并计费。
     */
    public ?bool $preserve_thinking = null;

    /**
     * @var int|null 思考过程最大 Token 数。适用于 Qwen3.6、Qwen3.5、Qwen3-VL、
     * Qwen3 的商业版与开源版模型；默认值为模型最大思维链长度。该字段不是
     * OpenAI 标准参数，但百炼兼容接口支持。
     */
    public ?int $thinking_budget = null;

    /**
     * @var bool|null 是否开启代码解释器。官方默认 `false`；可选值为 `true` 或
     * `false`。该能力依赖模型和账号侧支持，不适合作为普通 CRM 文本分析默认项。
     */
    public ?bool $enable_code_interpreter = null;

    /**
     * @var bool|null 是否返回输出 Token 的对数概率。可选值为 `true` 或 `false`；
     * 仅对支持 logprobs 的模型生效，常用于分类置信度估计或调试采样行为。
     */
    public ?bool $logprobs = null;

    /**
     * @var int|null 每个输出 Token 返回的候选 Token 对数概率数量。需要与
     * logprobs=true 配合使用；官方兼容 OpenAI 语义，通常取 0 到 20 之间的整数。
     */
    public ?int $top_logprobs = null;

    /**
     * @var bool|null 是否开启视觉模型高分辨率图片输入。可选值为 `true` 或
     * `false`；仅适用于 Qwen-VL、QVQ 等视觉模型。开启时部分模型输入图像像素
     * 上限提高到 16777216 或 12845056，且 max_pixels 可能被忽略。
     */
    public ?bool $vl_high_resolution_images = null;

    /**
     * @var bool|null 是否启用模型联网搜索。可选值为 `true` 或 `false`，仅对支持
     * 搜索增强的模型和账号能力生效；CRM 内部聊天记录分析默认不建议开启，避免引入外部不确定信息。
     */
    public ?bool $enable_search = null;

    /**
     * @var array<string,mixed>|null 搜索增强配置。仅在 enable_search=true 且模型支持
     * 搜索增强时生效；具体键值随官方搜索能力演进，建议只按百炼文档传入明确支持的字段。
     */
    public ?array $search_options = null;
}
