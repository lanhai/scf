<?php
declare(strict_types=1);

/**
 * SCF 启动入口。
 *
 * boot 的职责只保留为：
 * - 定义最基础常量
 * - 装载 Command 启动工具链
 * - 解析当前应加载的 framework 包
 * - 注册 framework autoload
 * - 进入命令执行或 server loop
 *
 * 所有复杂的 pack 协调、server loop 与原子文件操作都已经下沉到
 * `src/Command/Bootstrap`，避免 boot 本体继续膨胀，也避免这里误引入
 * Command 之外的框架模块。
 */

if (version_compare(PHP_VERSION, '8.1.0', '<')) {
    fwrite(STDERR, '运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION . PHP_EOL);
    exit(1);
}

const SCF_ROOT = __DIR__;
defined('BUILD_PATH') || define('BUILD_PATH', dirname(SCF_ROOT) . '/build/');
defined('SCF_APPS_ROOT') || define('SCF_APPS_ROOT', dirname(SCF_ROOT) . '/apps');
$bootstrapCandidates = [
    SCF_ROOT . '/src/Command/Bootstrap/bootstrap.php',
    SCF_ROOT . '/vendor/lhai/scf/src/Command/Bootstrap/bootstrap.php',
];
$bootstrapEntry = null;
foreach ($bootstrapCandidates as $candidate) {
    if (is_file($candidate)) {
        $bootstrapEntry = $candidate;
        break;
    }
}
if ($bootstrapEntry === null) {
    fwrite(
        STDERR,
        'SCF bootstrap 工具链不存在, 已检查: ' . implode(', ', $bootstrapCandidates) . PHP_EOL
    );
    exit(1);
}

require_once $bootstrapEntry;

scf_define_runtime_constants($argv);

$srcPack = scf_try_upgrade($argv, true);
defined('FRAMEWORK_ACTIVE_PACK') || define('FRAMEWORK_ACTIVE_PACK', $srcPack);
defined('FRAMEWORK_ACTIVE_PACK_SIGNATURE') || define('FRAMEWORK_ACTIVE_PACK_SIGNATURE', scf_framework_pack_identity($srcPack));
$frameworkSourceDir = scf_framework_source_dir();

spl_autoload_register(static function (string $class) use ($srcPack, $frameworkSourceDir): void {
    $filePath = scf_resolve_framework_class_file($class, $srcPack, $frameworkSourceDir);
    if ($filePath !== null && file_exists($filePath)) {
        require $filePath;
    }
});

date_default_timezone_set(scf_detect_timezone());
require SCF_ROOT . '/vendor/autoload.php';

scf_bootstrap($argv);
