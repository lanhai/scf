<?php

namespace Scf\Cloud\Ali\LLM\Parameters;

/**
 * 图片生成与编辑可选参数结构。
 *
 * 该结构只收录 Qwen-Image、Qwen-Image-Edit、Wan 图片生成/编辑官方文档中
 * 已明确列出的 `parameters` 字段。图片模型之间差异较大，字段注释会标明
 * 适用模型、取值范围和互斥关系；业务侧仍需按所选模型选择参数，避免服务端返回
 * InvalidParameter。
 */
class ImageParameters extends GenerationParameters {

    /**
     * @var string|null 输出图片尺寸。
     *
     * Qwen-Image-Edit/Qwen-Image-2.0 使用 `"宽*高"` 格式，例如 `1024*1536`、
     * `2048*2048`；Qwen-Image-2.0 总像素需在 `512*512` 到 `2048*2048` 之间，
     * qwen-image-edit-max/plus 宽高均为 `[512, 2048]`。
     * qwen-image-max/plus 仅支持固定预设：`1664*928`、`1472*1104`、`1328*1328`、
     * `1104*1472`、`928*1664`。
     * Wan 2.7 图片模型还支持缩写枚举：`1K`、`2K`，wan2.7-image-pro 文生图可用
     * `4K`；自定义尺寸按模型限制在 `768*768` 到 `4096*4096` 或 `2048*2048` 范围内。
     */
    public ?string $size = null;

    /**
     * @var int|null 输出图片数量。Qwen-Image-2.0、qwen-image-edit-max、
     * qwen-image-edit-plus 系列支持 `1-6`；基础 qwen-image-edit 仅支持 `1`。
     * Wan 2.7 开启组图模式 enable_sequential=true 时通过 n 控制上限，取值范围
     * `1-12`，默认 12，实际数量由模型决定且不超过 n。
     */
    public ?int $n = null;

    /**
     * @var string|null 反向提示词。Qwen-Image-Edit 支持中英文，长度上限 500 个
     * 字符，超过部分会自动截断。Wan 2.7 图片模型不支持 negative_prompt，不希望出现
     * 的元素应在正向提示词中描述；Wan 2.6/2.5 图片编辑模型支持，长度上限 500 个字符。
     */
    public ?string $negative_prompt = null;

    /**
     * @var bool|null 是否开启提示词智能改写。Qwen-Image-Edit 默认 `true`，支持模型
     * 为除基础 `qwen-image-edit` 以外的模型；Wan 2.6 图片编辑和 2.5 i2i 支持。
     * Wan 2.7 图片模型不支持该字段，可通过 thinking_mode 提升出图质量。
     */
    public ?bool $prompt_extend = null;

    /**
     * @var bool|null 是否添加官方水印。Qwen-Image-Edit 默认 `false`，开启后在图像
     * 右下角添加 `Qwen-Image` 水印；Wan 图片模型按官方模型文档添加对应 AI 生成水印。
     */
    public ?bool $watermark = null;

    /**
     * @var int|null 随机数种子。官方取值范围为 `[0, 2147483647]`。相同 prompt、
     * 模型、尺寸和 seed 可提升结果稳定性，但图片生成具有概率性，不保证完全一致。
     */
    public ?int $seed = null;

    /**
     * @var bool|null 是否启用 Wan 2.7 组图生成。仅 `wan2.7-image-pro` 和
     * `wan2.7-image` 支持，默认 `false`。设为 `true` 后模型会一次生成多张有故事
     * 连贯性的图像；此时 n 的取值范围为 `1-12`，且 thinking_mode、color_palette 不可用。
     */
    public ?bool $enable_sequential = null;

    /**
     * @var bool|null 是否启用 Wan 2.7 图片思考模式。仅 `wan2.7-image-pro` 和
     * `wan2.7-image` 支持，默认 `true`。仅在 enable_sequential=false 时可用；开启会
     * 增强推理与出图质量，但会增加生成耗时。
     */
    public ?bool $thinking_mode = null;

    /**
     * @var array<int,array{hex:string,ratio:string}>|null Wan 2.7 自定义颜色主题。
     * 仅 `wan2.7-image-pro` 和 `wan2.7-image` 支持，且仅在 enable_sequential=false
     * 时可用。数组需包含 3 到 10 种颜色，推荐 8 种；每项包含十六进制颜色 `hex`
     * 和百分比字符串 `ratio`，所有 ratio 总和必须为 `100.00%`。
     */
    public ?array $color_palette = null;

    /**
     * @var array<int,array<int,array<int,int>>>|null Wan 图片编辑框选区域。仅 Wan
     * 2.7 图片编辑场景支持，用于指定需要编辑的物品或位置，官方格式为
     * `List[List[List[int]]]`。坐标必须与输入图片尺寸匹配；不做交互式局部编辑时不要传。
     */
    public ?array $bbox_list = null;
}
