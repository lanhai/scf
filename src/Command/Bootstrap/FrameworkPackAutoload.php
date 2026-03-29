<?php
declare(strict_types=1);

/**
 * framework pack 元数据与 autoload 策略函数。
 */

function scf_compare_framework_pack(string $left, string $right): int {
    $leftInfo = scf_read_framework_pack_version($left);
    $rightInfo = scf_read_framework_pack_version($right);

    $leftVersion = trim((string)($leftInfo['version'] ?? ''));
    $rightVersion = trim((string)($rightInfo['version'] ?? ''));
    if ($leftVersion !== '' && $rightVersion !== '') {
        $versionCompare = version_compare($leftVersion, $rightVersion);
        if ($versionCompare !== 0) {
            return $versionCompare;
        }
    }

    $leftBuild = trim((string)($leftInfo['build'] ?? ''));
    $rightBuild = trim((string)($rightInfo['build'] ?? ''));
    if ($leftBuild !== '' && $rightBuild !== '' && $leftBuild !== $rightBuild) {
        return strcmp($leftBuild, $rightBuild);
    }

    return (filemtime($left) ?: 0) <=> (filemtime($right) ?: 0);
}

function scf_read_framework_pack_version(string $pack): ?array {
    if (!is_file($pack)) {
        return null;
    }

    $versionFile = 'phar://' . $pack . '/version.php';
    if (!@file_exists($versionFile)) {
        return null;
    }

    $data = @require $versionFile;
    return is_array($data) ? $data : null;
}

function scf_framework_pack_identity(string $pack): string {
    if (!is_file($pack)) {
        return '';
    }

    $version = scf_read_framework_pack_version($pack);
    if (is_array($version) && $version) {
        return trim((string)($version['version'] ?? '')) . '@' . trim((string)($version['build'] ?? ''));
    }

    return (string)(filesize($pack) ?: 0) . '@' . (string)(filemtime($pack) ?: 0);
}

function scf_framework_source_dir(): string {
    $sourceDir = SCF_ROOT . '/src/';
    if (is_dir($sourceDir)) {
        return $sourceDir;
    }

    return SCF_ROOT . '/vendor/lhai/scf/src/';
}

function scf_resolve_framework_class_file(string $class, string $srcPack, string $frameworkSourceDir): ?string {
    if (!str_starts_with($class, 'Scf\\')) {
        return null;
    }

    $relativePath = str_replace('\\', '/', substr($class, 4)) . '.php';
    if (!scf_bool_constant('FRAMEWORK_IS_PHAR') || scf_should_load_from_source($class)) {
        return $frameworkSourceDir . $relativePath;
    }

    return 'phar://' . $srcPack . '/' . $relativePath;
}

function scf_should_load_from_source(string $class): bool {
    /**
     * boot 早期真正需要源码直读的启动工具链，已经通过 boot 手工 require 进来了，
     * 不再依赖命名空间 autoload。
     *
     * 因此显式 `-pack` / `FRAMEWORK_IS_PHAR=true` 时，默认所有 `Scf\*` 类都应当
     * 从 active pack 里加载，避免命令入口、CliBootstrap、GatewayServer 等运行链
     * 继续偷偷回退到本地 src，导致“pack 已更新、节点却还在跑旧源码”。
     *
     * 如果未来真的出现某个启动早期基础类必须保留源码直读，再在这里按“具体类名”
     * 加白名单，而不是按整个命名空间兜底。
     */
    static $sourceAllowList = [
    ];

    return in_array($class, $sourceAllowList, true);
}
