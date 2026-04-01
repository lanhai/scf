<?php

declare(strict_types=1);

namespace Scf\Server\Gateway;

use Scf\Command\Manager;
use Scf\Core\Env;
use Scf\Server\Http;

/**
 * 直连 upstream 的启动入口。
 *
 * 该入口不经过 gateway 的托管与编排逻辑，直接启动业务 HTTP server。
 */
class DirectAppServerBootstrap {

    /**
     * 以独立 upstream 模式启动业务 server。
     *
     * 这里仅完成运行时常量、自动加载和参数初始化，然后交给 Http 启动。
     */
    public static function run(array $argv): void {
        self::ensureBootstrapToolchain();
        scf_define_bootstrap_root_constants(dirname(__DIR__, 3));
        scf_define_runtime_constants($argv, [
            'IS_PACK' => false,
            'NO_PACK' => true,
            'IS_HTTP_SERVER' => true,
            'IS_HTTP_SERVER_START' => true,
            'IS_GATEWAY_SERVER' => false,
            'IS_GATEWAY_SERVER_START' => false,
            'IS_SERVER_PROCESS_LOOP' => true,
            'IS_SERVER_PROCESS_START' => true,
            'RUNNING_BUILD' => false,
            'RUNNING_INSTALL' => false,
            'RUNNING_TOOLBOX' => false,
            'RUNNING_BUILD_FRAMEWORK' => false,
            'RUNNING_CREATE_AR' => false,
            'FRAMEWORK_IS_PHAR' => false,
        ]);
        scf_register_source_framework_autoload();
        require_once SCF_ROOT . '/src/Const.php';
        require_once SCF_ROOT . '/vendor/autoload.php';

        date_default_timezone_set(getenv('TZ') ?: 'Asia/Shanghai');

        [$args, $opts] = scf_parse_cli_args($argv);
        Manager::instance()->setArgs($args);
        Manager::instance()->setOpts($opts);
        defined('PROXY_UPSTREAM_MODE') || define('PROXY_UPSTREAM_MODE', true);
        Env::initialize(MODE_CGI);

        Http::create(SERVER_ROLE, '0.0.0.0', SERVER_PORT)->start();
    }

    /**
     * 确保 direct upstream 的源码入口也能拿到 boot 共用的 bootstrap 工具链。
     *
     * @return void
     */
    protected static function ensureBootstrapToolchain(): void {
        if (function_exists('scf_define_runtime_constants')) {
            return;
        }

        $root = dirname(__DIR__, 3);
        $candidates = [
            $root . '/src/Command/Bootstrap/bootstrap.php',
            $root . '/vendor/lhai/scf/src/Command/Bootstrap/bootstrap.php',
        ];
        foreach ($candidates as $candidate) {
            if (is_file($candidate)) {
                require_once $candidate;
                return;
            }
        }

        throw new \RuntimeException(
            'SCF bootstrap 工具链不存在, 已检查: ' . implode(', ', $candidates)
        );
    }
}
