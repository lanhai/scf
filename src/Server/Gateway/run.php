<?php

declare(strict_types=1);

// gateway 控制面统一入口：由 CliBootstrap 完成参数、模式和启动编排。
require_once __DIR__ . '/CliBootstrap.php';

\Scf\Server\Gateway\CliBootstrap::run($argv);
