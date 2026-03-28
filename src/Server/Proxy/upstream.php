<?php

declare(strict_types=1);

// 直连 upstream 启动入口：绕过 gateway 托管，直接拉起业务 HTTP server。
require_once __DIR__ . '/DirectAppServerBootstrap.php';

\Scf\Server\Proxy\DirectAppServerBootstrap::run($argv);
