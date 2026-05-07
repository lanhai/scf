<?php

namespace Scf\Cloud\Ali;

use Scf\Client\Http;
use Scf\Cloud\Aliyun;
use Scf\Cloud\Ali\LLM\Image;
use Scf\Cloud\Ali\LLM\Parameters\GenerationParameters;
use Scf\Cloud\Ali\LLM\Text;
use Scf\Cloud\Ali\LLM\Video;
use Scf\Core\Result;
use Scf\Core\Struct;
use Scf\Mode\Web\Exception\AppError;

/**
 * 统一封装阿里云百炼 DashScope 大模型服务。
 *
 * 该组件位于 SCF 框架云服务层，负责把业务侧的聊天、视觉理解、图片生成、
 * 视频生成和异步任务查询统一收敛到 DashScope HTTP 协议。模型常量按能力归属
 * 定义在 Text、Image、Video 对象中，底层入口仍接受字符串模型名，便于官方
 * 新增模型时业务先行接入。
 */
class LLM extends Aliyun {
    protected string $apiKey = '';
    protected string $baseUrl = 'https://dashscope.aliyuncs.com/api/v1';
    protected string $compatibleBaseUrl = 'https://dashscope.aliyuncs.com/compatible-mode/v1';
    protected int $timeout = 300;

    /**
     * 初始化 DashScope 服务端点与 API Key。
     *
     * DashScope 是按模型动态选择能力的统一服务入口，不像 OSS 那样存在多个
     * 业务服务器节点，因此这里直接读取 LLM 组件配置上的统一 API Key 和端点
     * 参数，避免把调用方引到 `default_server/server` 的多节点配置语义里。
     *
     * @return void
     * @throws AppError
     */
    public function _init(): void {
        parent::_init();

        $this->baseUrl = rtrim((string)($this->_config['base_url'] ?? $this->baseUrl), '/');
        $this->compatibleBaseUrl = rtrim((string)($this->_config['compatible_base_url'] ?? $this->compatibleBaseUrl), '/');
        $this->timeout = max(1, (int)($this->_config['timeout'] ?? $this->timeout));
        $this->apiKey = $this->resolveApiKey();

        if ($this->apiKey === '') {
            throw new AppError('阿里云DashScope API Key配置信息不存在');
        }
    }

    /**
     * 通过 OpenAI-compatible Chat Completions 协议发起对话。
     *
     * 该入口覆盖文本、视觉语言、工具调用、JSON mode 等官方兼容参数；除
     * `model/messages` 外的参数原样透传，调用方可随官方文档演进动态传入。
     *
     * @param array<int,array<string,mixed>> $messages
     * @param string $model
     * @param array<string,mixed>|Struct $options
     * @return Result
     */
    public function chat(array $messages, string $model = 'qwen-plus', array|Struct $options = []): Result {
        return $this->postCompatible('/chat/completions', array_merge($this->normalizeParameterPayload($options), [
            'model' => $model,
            'messages' => $messages,
        ]));
    }

    /**
     * 发送单轮文本聊天并抽取最常用的回复字段。
     *
     * 这是业务侧做摘要、分类、预警判断时的轻量入口；需要更完整响应时应调用
     * `chat()`，避免在框架层丢失 token、工具调用或多候选等信息。
     *
     * @param string $content
     * @param string $model
     * @param array<string,mixed>|Struct $options
     * @param string $systemPrompt
     * @return Result
     */
    public function chatText(string $content, string $model = 'qwen-plus', array|Struct $options = [], string $systemPrompt = ''): Result {
        $messages = [];
        if ($systemPrompt !== '') {
            $messages[] = ['role' => 'system', 'content' => $systemPrompt];
        }
        $messages[] = ['role' => 'user', 'content' => $content];

        $result = $this->chat($messages, $model, $options);
        if ($result->hasError()) {
            return $result;
        }

        return Result::success([
            'message' => $result->getData('choices')[0]['message']['content'] ?? null,
            'finish_reason' => $result->getData('choices')[0]['finish_reason'] ?? null,
            'usage' => $result->getData('usage') ?? null,
            'raw' => $result->getData(),
        ]);
    }

    /**
     * 通过 OpenAI-compatible Responses 协议发起请求。
     *
     * Responses API 是 Chat Completions 的后续统一接口之一，框架层只固定地址
     * 和鉴权，模型、输入结构、工具、推理参数等全部由调用方按官方协议传入。
     *
     * @param string|array<string,mixed>|array<int,mixed> $input
     * @param string $model
     * @param array<string,mixed>|Struct $options
     * @return Result
     */
    public function responses(string|array $input, string $model = 'qwen-plus', array|Struct $options = []): Result {
        return $this->postCompatible('/responses', array_merge($this->normalizeParameterPayload($options), [
            'model' => $model,
            'input' => $input,
        ]));
    }

    /**
     * 通过 OpenAI-compatible Embeddings 协议生成向量。
     *
     * CRM 后续做聊天记录检索、相似会话召回或客户画像聚类时，可以直接动态传入
     * 官方支持的 embedding 模型，不需要在框架层维护固定模型枚举。
     *
     * @param string|array<int,string> $input
     * @param string $model
     * @param array<string,mixed>|Struct $options
     * @return Result
     */
    public function embeddings(string|array $input, string $model = 'text-embedding-v4', array|Struct $options = []): Result {
        return $this->postCompatible('/embeddings', array_merge($this->normalizeParameterPayload($options), [
            'model' => $model,
            'input' => $input,
        ]));
    }

    /**
     * 通过 DashScope 原生文本生成接口调用 Qwen 文本模型。
     *
     * 原生协议适合仍按 `input/messages + parameters` 组织的场景；这里不限制模型
     * 名称，调用方可传 `qwen-plus`、`qwen-flash` 或任意官方新增文本模型。
     *
     * @param string|array<int,array<string,mixed>> $messages
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @param array<string,mixed>|Struct $inputExtras
     * @return Result
     */
    public function textGeneration(string|array $messages, string $model = 'qwen-plus', array|Struct $parameters = [], array|Struct $inputExtras = []): Result {
        $input = is_string($messages)
            ? ['messages' => [['role' => 'user', 'content' => $messages]]]
            : ['messages' => $messages];

        return $this->postDashScope('/services/aigc/text-generation/generation', [
            'model' => $model,
            'input' => array_merge($input, $this->normalizeParameterPayload($inputExtras)),
            'parameters' => $this->normalizeParameterPayload($parameters),
        ]);
    }

    /**
     * 通过 DashScope 原生多模态生成接口调用视觉理解、图片生成和图片编辑模型。
     *
     * DashScope 把 Qwen-VL、Qwen-Image 同步文生图、图片编辑等都收敛在这个
     * endpoint 下；调用方只需要按模型要求传入 message content 中的 text/image/video。
     *
     * @param array<int,array<string,mixed>> $messages
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @param array<string,mixed>|Struct $inputExtras
     * @return Result
     */
    public function multimodalGeneration(array $messages, string $model, array|Struct $parameters = [], array|Struct $inputExtras = []): Result {
        return $this->postDashScope('/services/aigc/multimodal-generation/generation', [
            'model' => $model,
            'input' => array_merge(['messages' => $messages], $this->normalizeParameterPayload($inputExtras)),
            'parameters' => $this->normalizeParameterPayload($parameters),
        ]);
    }

    /**
     * 同步生成或编辑图片。
     *
     * 没有传入 `$imageUrls` 时是文生图；传入 1 到 3 张参考图时按官方
     * Qwen-Image-Edit/Qwen-Image 多模态格式组织输入。
     *
     * @param string $prompt
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @param array<int,string> $imageUrls
     * @return Result
     */
    public function generateImage(string $prompt, string $model = 'qwen-image-2.0-pro', array|Struct $parameters = [], array $imageUrls = []): Result {
        $content = [];
        foreach ($imageUrls as $imageUrl) {
            $content[] = ['image' => $imageUrl];
        }
        $content[] = ['text' => $prompt];

        return $this->multimodalGeneration([
            [
                'role' => 'user',
                'content' => $content,
            ]
        ], $model, array_merge([
            'prompt_extend' => true,
            'watermark' => false,
        ], $this->normalizeParameterPayload($parameters)));
    }

    /**
     * 创建图片异步生成任务。
     *
     * 官方异步图片接口当前主要用于 `qwen-image-plus/qwen-image` 一类模型；
     * 框架不做硬编码校验，避免后续官方扩展模型时需要同步发布框架。
     *
     * @param string $prompt
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @param array<string,mixed>|Struct $inputExtras
     * @return Result
     */
    public function generateImageAsync(string $prompt, string $model = 'qwen-image-plus', array|Struct $parameters = [], array|Struct $inputExtras = []): Result {
        return $this->postDashScope('/services/aigc/text2image/image-synthesis', [
            'model' => $model,
            'input' => array_merge(['prompt' => $prompt], $this->normalizeParameterPayload($inputExtras)),
            'parameters' => array_merge([
                'n' => 1,
                'prompt_extend' => true,
                'watermark' => false,
            ], $this->normalizeParameterPayload($parameters)),
        ], ['X-DashScope-Async' => 'enable']);
    }

    /**
     * 创建 Wan 图片生成或编辑异步任务。
     *
     * Wan 2.7 图片模型使用官方 `image-generation/generation` endpoint，输入结构为
     * 单轮 `messages`，并要求 HTTP 异步头；该入口用于承接 wan2.7-image、
     * wan2.7-image-pro 的文生图、参考图编辑、交互式编辑和组图生成。
     *
     * @param string $prompt
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @param array<int,string> $imageUrls
     * @param array<string,mixed>|Struct $inputExtras
     * @return Result
     */
    public function generateWanImage(string $prompt, string $model = 'wan2.7-image-pro', array|Struct $parameters = [], array $imageUrls = [], array|Struct $inputExtras = [], bool $async = false): Result {
        $content = [];
        foreach ($imageUrls as $imageUrl) {
            $content[] = ['image' => $imageUrl];
        }
        $content[] = ['text' => $prompt];

        $body = [
            'model' => $model,
            'input' => array_merge([
                'messages' => [[
                    'role' => 'user',
                    'content' => $content,
                ]],
            ], $this->normalizeParameterPayload($inputExtras)),
            'parameters' => $this->normalizeParameterPayload($parameters),
        ];

        return $async
            ? $this->postDashScope('/services/aigc/image-generation/generation', $body, ['X-DashScope-Async' => 'enable'])
            : $this->postDashScope('/services/aigc/multimodal-generation/generation', $body);
    }

    /**
     * 创建 Wan 视频生成异步任务。
     *
     * 文生视频、图生视频、参考视频生成都复用官方 video-synthesis endpoint，
     * 差异由 `model/input/parameters` 决定，因此这里保留一个通用任务入口。
     *
     * @param array<string,mixed> $input
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @return Result
     */
    public function generateVideo(array $input, string $model, array|Struct $parameters = []): Result {
        if ($parameters instanceof GenerationParameters) {
            $input = array_merge($input, $parameters->toInputExtras());
        }
        return $this->postDashScope('/services/aigc/video-generation/video-synthesis', [
            'model' => $model,
            'input' => $input,
            'parameters' => $this->normalizeParameterPayload($parameters),
        ], ['X-DashScope-Async' => 'enable']);
    }

    /**
     * 创建文生视频任务。
     *
     * @param string $prompt
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @param array<string,mixed>|Struct $inputExtras
     * @return Result
     */
    public function generateVideoFromText(string $prompt, string $model = 'wan2.6-t2v', array|Struct $parameters = [], array|Struct $inputExtras = []): Result {
        return $this->generateVideo(array_merge(['prompt' => $prompt], $this->normalizeParameterPayload($inputExtras)), $model, $parameters);
    }

    /**
     * 创建图生视频任务。
     *
     * @param string $prompt
     * @param string $imageUrl
     * @param string $model
     * @param array<string,mixed>|Struct $parameters
     * @param array<string,mixed>|Struct $inputExtras
     * @return Result
     */
    public function generateVideoFromImage(string $prompt, string $imageUrl, string $model = 'wan2.7-i2v', array|Struct $parameters = [], array|Struct $inputExtras = []): Result {
        $input = ['prompt' => $prompt];
        if (str_starts_with($model, 'wan2.7-')) {
            $input['media'] = [[
                'type' => 'first_frame',
                'url' => $imageUrl,
            ]];
        } else {
            $input['img_url'] = $imageUrl;
        }
        return $this->generateVideo(array_merge($input, $this->normalizeParameterPayload($inputExtras)), $model, $parameters);
    }

    /**
     * 查询 DashScope 异步任务状态和结果。
     *
     * 图片/视频等异步任务均返回 `task_id`，后续统一通过 `/tasks/{task_id}` 查询；
     * 结果 URL 通常仅保留 24 小时，业务侧拿到成功结果后应尽快转存到 OSS。
     *
     * @param string $taskId
     * @return Result
     */
    public function task(string $taskId): Result {
        return $this->getDashScope('/tasks/' . rawurlencode($taskId));
    }

    /**
     * 创建文本能力对象。
     *
     * 能力对象带有本次调用的可变上下文，因此这里每次返回新实例，而不是复用
     * `LLM::instance()` 的长生命周期对象。
     *
     * @param string|null $model
     * @return Text
     */
    public static function text(?string $model = null): Text {
        return Text::create($model);
    }

    /**
     * 创建图片能力对象。
     *
     * @param string|null $model
     * @return Image
     */
    public static function image(?string $model = null): Image {
        return Image::create($model);
    }

    /**
     * 创建视频能力对象。
     *
     * @param string|null $model
     * @return Video
     */
    public static function video(?string $model = null): Video {
        return Video::create($model);
    }

    /**
     * 公开调用 OpenAI-compatible 任意 endpoint。
     *
     * 这个扩展口用于承接官方新增但框架尚未提供语义化方法的能力；业务侧仍然
     * 复用统一鉴权、超时和错误归一化，只需传入 endpoint path 与请求体。
     *
     * @param string $path
     * @param array<string,mixed> $body
     * @param array<string,string> $headers
     * @return Result
     */
    public function requestCompatible(string $path, array $body, array $headers = []): Result {
        return $this->postCompatible($path, $body, $headers);
    }

    /**
     * 公开调用 DashScope 原生任意 POST endpoint。
     *
     * 当官方新增音频、视频、图像或行业模型服务时，业务可先用此方法接入，
     * 后续再按高频场景沉淀成更明确的语义化方法。
     *
     * @param string $path
     * @param array<string,mixed> $body
     * @param array<string,string> $headers
     * @return Result
     */
    public function requestDashScope(string $path, array $body, array $headers = []): Result {
        return $this->postDashScope($path, $body, $headers);
    }

    /**
     * 公开创建 DashScope 原生异步任务。
     *
     * 官方异步服务统一依赖 `X-DashScope-Async: enable`，这里把该生命周期规则
     * 固定在框架层，调用方只需要关心服务 path、模型和 input/parameters。
     *
     * @param string $path
     * @param array<string,mixed> $body
     * @param array<string,string> $headers
     * @return Result
     */
    public function createAsyncTask(string $path, array $body, array $headers = []): Result {
        return $this->postDashScope($path, $body, array_merge(['X-DashScope-Async' => 'enable'], $headers));
    }

    /**
     * 调用 OpenAI-compatible API。
     *
     * @param string $path
     * @param array<string,mixed> $body
     * @param array<string,string> $headers
     * @return Result
     */
    protected function postCompatible(string $path, array $body, array $headers = []): Result {
        return $this->post($this->compatibleBaseUrl . $path, $body, $headers);
    }

    /**
     * 调用 DashScope 原生 POST API。
     *
     * @param string $path
     * @param array<string,mixed> $body
     * @param array<string,string> $headers
     * @return Result
     */
    protected function postDashScope(string $path, array $body, array $headers = []): Result {
        return $this->post($this->baseUrl . $path, $body, $headers);
    }

    /**
     * 调用 DashScope 原生 GET API。
     *
     * @param string $path
     * @param array<string,string> $headers
     * @return Result
     */
    protected function getDashScope(string $path, array $headers = []): Result {
        $client = $this->http($this->baseUrl . $path, $headers);
        $response = $client->get($this->timeout);
        return $this->normalizeResponse($response);
    }

    /**
     * 发送 JSON POST 请求并统一解析 DashScope 错误结构。
     *
     * @param string $url
     * @param array<string,mixed> $body
     * @param array<string,string> $headers
     * @return Result
     */
    protected function post(string $url, array $body, array $headers = []): Result {
        $client = $this->http($url, $headers);
        $response = $client->JPost($body, $this->timeout);
        return $this->normalizeResponse($response);
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
     * 过滤空参数，保留 false 和 0 等有效配置值。
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
     * 创建带 DashScope 鉴权头的 Swoole HTTP 客户端。
     *
     * @param string $url
     * @param array<string,string> $headers
     * @return Http
     */
    protected function http(string $url, array $headers = []): Http {
        $client = Http::create($url);
        $client->setHeader('Authorization', 'Bearer ' . $this->apiKey);
        foreach ($headers as $key => $value) {
            $client->setHeader($key, $value);
        }
        return $client;
    }

    /**
     * 把 DashScope 的 `code/message` 错误响应转换成框架 Result。
     *
     * HTTP 层失败时 `Http` 已经把响应体放进 Result data；这里继续识别官方
     * JSON 错误结构，避免业务侧只能看到笼统的 HTTP 状态码。
     *
     * @param Result $response
     * @return Result
     */
    protected function normalizeResponse(Result $response): Result {
        $data = $response->getData();
        if ($response->hasError()) {
            if (is_array($data)) {
                $error = is_array($data['error'] ?? null) ? $data['error'] : $data;
                return Result::error($error['message'] ?? $response->getMessage(), $error['code'] ?? $response->getErrCode(), $data);
            }
            return $response;
        }
        if (is_array($data) && is_array($data['error'] ?? null)) {
            return Result::error($data['error']['message'] ?? 'DashScope请求失败', $data['error']['code'] ?? 'DASHSCOPE_ERROR', $data);
        }
        if (is_array($data) && isset($data['code']) && $data['code'] !== '') {
            return Result::error($data['message'] ?? 'DashScope请求失败', $data['code'], $data);
        }
        return Result::success($data);
    }

    /**
     * 解析 DashScope API Key。
     *
     * 组件配置只保留一个统一密钥，不支持像 OSS 那样通过 server 节点切换账号；
     * 环境变量仅作为本地或容器部署时的兜底输入。
     *
     * @return string
     */
    protected function resolveApiKey(): string {
        $apiKey = trim((string)($this->_config['api_key'] ?? ''));
        if ($apiKey !== '') {
            return $apiKey;
        }

        return trim((string)getenv('DASHSCOPE_API_KEY'));
    }
}
