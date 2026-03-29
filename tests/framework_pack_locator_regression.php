<?php

declare(strict_types=1);

require_once dirname(__DIR__) . '/src/Util/FrameworkPackLocator.php';

use Scf\Util\FrameworkPackLocator;

/**
 * 创建测试目录。
 *
 * @param string $path
 * @return void
 */
function ensure_dir(string $path): void {
    if (!is_dir($path) && !mkdir($path, 0775, true) && !is_dir($path)) {
        throw new RuntimeException('创建目录失败:' . $path);
    }
}

/**
 * 递归删除测试目录。
 *
 * @param string $path
 * @return void
 */
function remove_path(string $path): void {
    if (!file_exists($path)) {
        return;
    }
    if (is_file($path)) {
        @unlink($path);
        return;
    }
    $items = scandir($path) ?: [];
    foreach ($items as $item) {
        if ($item === '.' || $item === '..') {
            continue;
        }
        remove_path($path . '/' . $item);
    }
    @rmdir($path);
}

/**
 * 断言两个值相等。
 *
 * @param mixed $actual
 * @param mixed $expected
 * @param string $message
 * @return void
 */
function assert_same(mixed $actual, mixed $expected, string $message): void {
    if ($actual !== $expected) {
        throw new RuntimeException($message . ' expected=' . var_export($expected, true) . ' actual=' . var_export($actual, true));
    }
}

$tmpRoot = sys_get_temp_dir() . '/scf-framework-pack-regression-' . getmypid() . '-' . bin2hex(random_bytes(4));

try {
    ensure_dir($tmpRoot . '/build/framework');
    file_put_contents($tmpRoot . '/build/src.pack', 'legacy');

    // 场景 1：没有版本记录时，boot 应回退到历史兼容路径 src.pack。
    assert_same(
        FrameworkPackLocator::currentPackPath($tmpRoot),
        $tmpRoot . '/build/src.pack',
        'missing-version-file should fallback to src.pack'
    );

    // 场景 2：有版本记录但版本包尚未落地时，当前包仍应保持在 src.pack，
    // 同时待生效目标应指向对应版本包。
    file_put_contents($tmpRoot . '/build/framework/version.json', json_encode([
        'version' => '1.2.3',
        'build' => '2026-03-29 12:00:00',
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    assert_same(
        FrameworkPackLocator::pendingPackPath($tmpRoot),
        $tmpRoot . '/build/framework/1.2.3.update',
        'pending-pack-path should follow local framework version'
    );
    assert_same(
        FrameworkPackLocator::currentPackPath($tmpRoot),
        $tmpRoot . '/build/src.pack',
        'missing-versioned-pack should still use src.pack'
    );

    // 场景 3：目标版本包落地后，boot 应切到对应版本包。
    file_put_contents($tmpRoot . '/build/framework/1.2.3.update', 'pack-123');
    assert_same(
        FrameworkPackLocator::currentPackPath($tmpRoot),
        $tmpRoot . '/build/framework/1.2.3.update',
        'existing-versioned-pack should become current framework pack'
    );

    // 场景 4：development 版本仍应走历史兼容路径，避免开发态被错误导向 framework 目录。
    file_put_contents($tmpRoot . '/build/framework/version.json', json_encode([
        'version' => 'development',
        'build' => 'development',
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    assert_same(
        FrameworkPackLocator::currentPackPath($tmpRoot),
        $tmpRoot . '/build/src.pack',
        'development version should stay on src.pack'
    );

    fwrite(STDOUT, "framework pack locator regression passed\n");
} finally {
    remove_path($tmpRoot);
}
