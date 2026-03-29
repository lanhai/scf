<?php
declare(strict_types=1);

/**
 * framework pack 启动工具链聚合层。
 *
 * 对外仍然只暴露稳定的 `scf_*` 函数名，内部已经拆到 runtime/store/autoload
 * 三个子模块，避免 pack 启动逻辑继续堆在一个文件里。
 */

require_once __DIR__ . '/FrameworkPackRuntime.php';
require_once __DIR__ . '/FrameworkPackStore.php';
require_once __DIR__ . '/FrameworkPackAutoload.php';
