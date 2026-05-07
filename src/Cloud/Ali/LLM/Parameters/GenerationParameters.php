<?php

namespace Scf\Cloud\Ali\LLM\Parameters;

use Scf\Core\Struct;

/**
 * DashScope 生成参数结构基类。
 *
 * 该结构位于 LLM 能力对象和底层 HTTP 请求之间，负责把业务侧创建的强语义
 * 参数对象转换为 DashScope 请求体数组。子类只声明公开字段和字段文档，不在
 * 此处绑定具体模型，避免模型参数随官方演进时牵动调用链。
 */
abstract class GenerationParameters extends Struct {

    /**
     * 导出 DashScope parameters 请求体。
     *
     * 默认只输出非空字段，false、0 这类有效值会被保留；子类可以覆写本方法，
     * 将部分字段排除或改名后再交给具体 endpoint。
     *
     * @return array<string,mixed>
     */
    public function toParameters(): array {
        return $this->filterEmpty($this->toArray());
    }

    /**
     * 导出 DashScope input 扩展字段。
     *
     * 大多数参数属于 parameters，只有视频反向提示词等少数字段属于 input；
     * 默认没有 input 扩展，由特定能力参数结构按需覆写。
     *
     * @return array<string,mixed>
     */
    public function toInputExtras(): array {
        return [];
    }

    /**
     * 递归过滤空参数。
     *
     * @param array<string,mixed> $data
     * @return array<string,mixed>
     */
    protected function filterEmpty(array $data): array {
        $payload = [];
        foreach ($data as $key => $value) {
            if ($value === null || $value === '') {
                continue;
            }
            if (is_array($value)) {
                $value = $this->filterEmpty($value);
                if (!$value) {
                    continue;
                }
            }
            $payload[$key] = $value;
        }
        return $payload;
    }
}
