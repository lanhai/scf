<?php

namespace Scf\Service\Enum;

use Scf\Core\Traits\EnumMatcher;

enum QueueStatus: int {
    use EnumMatcher;

    /**
     * 队列中
     */
    case IN = 0;
    /**
     * 已完成
     */
    case FINISHED = 1;
    /**
     * 待重新加入
     */
    case DELAY = 2;
    /**
     * 失败
     */
    case FAILED = -1;

    /**
     * 获取状态key
     * @return string
     */
    public function key(): string {
        return '_QUEUE_LIST_' . $this->value;
    }

    /**
     * 匹配状态key
     * @param $value
     * @return string
     */
    public static function matchKey($value): string {
        $case = self::match($value);
        return $case ? $case->key() : self::IN->key();
    }

}