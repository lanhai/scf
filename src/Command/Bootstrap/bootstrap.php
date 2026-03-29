<?php
declare(strict_types=1);

/**
 * boot 启动工具链聚合入口。
 *
 * boot 自身只负责最小启动引导，不承载复杂的 pack 选择、server loop、
 * 原子文件操作等实现细节。这里把这些纯启动期工具函数集中挂到 Command 路径，
 * 让 boot 可以在不依赖其它框架层文件的前提下完成启动。
 */

require_once __DIR__ . '/Support.php';
require_once __DIR__ . '/FrameworkPack.php';
require_once __DIR__ . '/ServerRuntime.php';
