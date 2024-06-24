<?php

namespace Scf\Server;

use Scf\Core\Traits\EnumMatcher;
use Scf\Util\Date;

enum Keys: string {
    use EnumMatcher;

    /**
     * 收到请求
     */
    case REQUEST_COUNT = '_REQUEST_COUNT_';

    /**
     * 实时请求
     * @return string
     */
    public function currentRequest(): string {
        return self::REQUEST_COUNT->val() . '_' . time();
    }

    /**
     * 上一秒请求
     * @return string
     */
    public function lastSecondRequest(): string {
        return self::REQUEST_COUNT->val() . '_' . time() - 1;
    }

    /**
     * 今日请求
     * @return string
     */
    public function todayRequest(): string {
        return self::REQUEST_COUNT->val() . '_' . Date::today();
    }
}