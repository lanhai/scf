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
 * 递归清理测试目录。
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
 * 模拟 boot 中使用的原子替换逻辑。
 *
 * @param string $from
 * @param string $to
 * @return void
 */
function atomic_replace(string $from, string $to): void {
    $dir = dirname($to);
    ensure_dir($dir);
    if (@rename($from, $to)) {
        return;
    }
    $tmp = $dir . '/.' . basename($to) . '.tmp.' . getmypid();
    if (!copy($from, $tmp)) {
        throw new RuntimeException('复制临时文件失败');
    }
    if (!@rename($tmp, $to)) {
        @unlink($tmp);
        throw new RuntimeException('临时文件原子切换失败');
    }
    @unlink($from);
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

$tmpRoot = sys_get_temp_dir() . '/scf-framework-promotion-regression-' . getmypid() . '-' . bin2hex(random_bytes(4));

try {
    ensure_dir($tmpRoot . '/build/framework');
    file_put_contents($tmpRoot . '/build/src.pack', 'legacy-src-pack');
    file_put_contents($tmpRoot . '/build/update.pack', 'new-framework-pack');
    file_put_contents($tmpRoot . '/build/framework/version.json', json_encode([
        'version' => '1.2.3',
        'build' => '2026-03-29 12:00:00',
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));

    $targetPack = FrameworkPackLocator::pendingPackPath($tmpRoot);
    assert_same(
        $targetPack,
        $tmpRoot . '/build/framework/1.2.3.update',
        'pending pack should point to versioned framework package'
    );

    atomic_replace($tmpRoot . '/build/update.pack', $targetPack);

    assert_same(
        (string)file_get_contents($tmpRoot . '/build/src.pack'),
        'legacy-src-pack',
        'legacy src.pack should remain untouched after framework promotion'
    );
    assert_same(
        (string)file_get_contents($targetPack),
        'new-framework-pack',
        'new framework package should be promoted to the versioned target path'
    );
    assert_same(
        FrameworkPackLocator::currentPackPath($tmpRoot),
        $targetPack,
        'boot should resolve current framework pack to the promoted versioned package'
    );

    fwrite(STDOUT, "framework update promotion regression passed\n");
} finally {
    remove_path($tmpRoot);
}
