<?php

namespace Scf\Util;

/**
 * 对 SCF 的 -key=value 进程参数做边界精确匹配。
 *
 * 进程回收不能使用普通子串判断：app=foo 不得匹配 app=foo2，
 * port=9680 也不得匹配 port=96800。
 */
final class ProcessCommandLine {
    public static function hasOptionValue(string $command, string $option, string|int $expected): bool {
        $option = ltrim(trim($option), '-');
        if ($command === '' || $option === '') {
            return false;
        }
        $pattern = '/(?:^|\s)-' . preg_quote($option, '/') . '=([^\s]+)/';
        if (!preg_match_all($pattern, $command, $matches)) {
            return false;
        }
        $expected = (string)$expected;
        foreach ((array)($matches[1] ?? []) as $value) {
            $value = trim((string)$value, "\"'");
            if (hash_equals($expected, $value)) {
                return true;
            }
        }
        return false;
    }

    public static function intOption(string $command, string $option): int {
        $option = ltrim(trim($option), '-');
        if ($command === '' || $option === '') {
            return 0;
        }
        $pattern = '/(?:^|\s)-' . preg_quote($option, '/') . '=(-?\d+)(?:\s|$)/';
        return preg_match($pattern, $command, $matches)
            ? (int)($matches[1] ?? 0)
            : 0;
    }
}
