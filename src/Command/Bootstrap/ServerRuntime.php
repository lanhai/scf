<?php
declare(strict_types=1);

/**
 * boot 运行时工具链聚合层。
 *
 * 对外仍保持一组稳定的 `scf_*` 运行函数，具体实现已经拆到
 * main/ports 两个子模块，避免 server loop 与端口探测继续堆在一个文件里。
 */

require_once __DIR__ . '/ServerRuntimeMain.php';
require_once __DIR__ . '/ServerRuntimePorts.php';
