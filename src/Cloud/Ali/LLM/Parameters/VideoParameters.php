<?php

namespace Scf\Cloud\Ali\LLM\Parameters;

/**
 * Wan 视频生成可选参数结构。
 *
 * 该结构覆盖 Wan 2.7 文生视频、图生视频、参考生视频，以及仍需兼容的 2.6
 * 老协议字段。Wan 视频 HTTP 接口以异步任务为主，大部分字段进入 DashScope
 * `parameters`；`negative_prompt` 属于 `input`，会由 toInputExtras() 单独导出。
 */
class VideoParameters extends GenerationParameters {

    /**
     * @var string|null 老版 Wan 2.6 及更早模型的视频尺寸，格式为 `"宽*高"`，
     * 例如 `1280*720`、`1920*1080`。Wan 2.7 新协议不再使用 size，而是通过
     * resolution 与 ratio 组合控制输出分辨率；对 Wan 2.7 传 size 可能返回参数错误。
     */
    public ?string $size = null;

    /**
     * @var string|null Wan 2.7 分辨率档位。可选值：`720P`、`1080P`，默认
     * `1080P`。该字段直接影响费用，调用前需按官方价格确认成本。Wan 2.6 旧协议
     * 使用 size，不使用 resolution。
     */
    public ?string $resolution = null;

    /**
     * @var string|null Wan 2.7 输出视频宽高比。可选值：`16:9`、`9:16`、`1:1`、
     * `4:3`、`3:4`，默认 `16:9`。图生视频和参考生视频在传入首帧图像时，
     * 官方会忽略 ratio，并以首帧图像宽高比生成近似比例视频。
     */
    public ?string $ratio = null;

    /**
     * @var int|null 视频时长，单位秒。Wan 2.7 文生视频和图生视频取值为 `[2, 15]`
     * 的整数，默认 5；Wan 2.7 参考生视频包含参考视频时取 `[2, 10]`，不包含参考视频时取
     * `[2, 15]`。duration 按秒计费，设置前需确认费用。
     */
    public ?int $duration = null;

    /**
     * @var bool|null 是否开启 prompt 智能改写。可选值：`true` 开启、`false` 关闭；
     * Wan 2.7 默认 `true`。开启后服务会自动丰富镜头、动作和画面细节，通常能提升短
     * prompt 效果，但会增加耗时；严格脚本生成建议显式设为 false。
     */
    public ?bool $prompt_extend = null;

    /**
     * @var bool|null 是否添加水印标识。可选值：`false` 不添加，`true` 添加；
     * Wan 2.7 默认 `false`，水印位于视频右下角，文案固定为“AI 生成”。
     */
    public ?bool $watermark = null;

    /**
     * @var int|null 随机数种子。官方取值范围为 `[0, 2147483647]`。固定 seed
     * 可提升结果复现性；由于视频生成仍有概率性，即使相同 seed 也不能保证完全一致。
     */
    public ?int $seed = null;

    /**
     * @var string|null 需要随视频生成使用的音频 URL。该字段属于 input，不属于
     * parameters。Wan 2.7 文生视频字段名为 `audio_url`，支持 HTTP/HTTPS 或通过官方上传
     * 得到的 OSS 临时 URL；音频格式为 wav、mp3，时长 `2-30s`，文件大小不超过 15MB。
     */
    public ?string $audio_url = null;

    /**
     * @var string|null 反向提示词，用于约束不希望出现在视频画面中的内容。该字段
     * 属于 DashScope input，不属于 parameters。Wan 2.7 支持中英文，长度不超过 500
     * 个字符，超过部分会自动截断。
     */
    public ?string $negative_prompt = null;

    /**
     * 导出 Wan 视频 parameters 字段。
     *
     * @return array<string,mixed>
     */
    public function toParameters(): array {
        $data = parent::toParameters();
        unset($data['negative_prompt']);
        unset($data['audio_url']);
        return $data;
    }

    /**
     * 导出 Wan 视频 input 扩展字段。
     *
     * @return array<string,mixed>
     */
    public function toInputExtras(): array {
        $input = [];
        if ($this->negative_prompt !== null && $this->negative_prompt !== '') {
            $input['negative_prompt'] = $this->negative_prompt;
        }
        if ($this->audio_url !== null && $this->audio_url !== '') {
            $input['audio_url'] = $this->audio_url;
        }
        return $input;
    }
}
