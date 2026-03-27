<?php

declare(strict_types=1);

namespace Scf\Server\Proxy;

use Scf\Command\Manager;
use Scf\Core\Env;
use Scf\Server\Http;

class DirectAppServerBootstrap {

    public static function run(array $argv): void {
        self::defineBaseConstants($argv);
        self::registerFrameworkAutoload();
        require_once SCF_ROOT . '/src/Const.php';
        require_once SCF_ROOT . '/vendor/autoload.php';

        date_default_timezone_set(getenv('TZ') ?: 'Asia/Shanghai');

        [$args, $opts] = self::parseArgs($argv);
        Manager::instance()->setArgs($args);
        Manager::instance()->setOpts($opts);
        defined('PROXY_UPSTREAM_MODE') || define('PROXY_UPSTREAM_MODE', true);
        Env::initialize(MODE_CGI);

        Http::create(SERVER_ROLE, '0.0.0.0', SERVER_PORT)->start();
    }

    protected static function defineBaseConstants(array $argv): void {
        defined('SCF_ROOT') || define('SCF_ROOT', dirname(__DIR__, 3));
        defined('BUILD_PATH') || define('BUILD_PATH', dirname(SCF_ROOT) . '/build/');
        defined('SCF_APPS_ROOT') || define('SCF_APPS_ROOT', dirname(SCF_ROOT) . '/apps');

        $envVariables = [
            'app_env' => getenv('APP_ENV') ?: '',
            'app_dir' => getenv('APP_DIR') ?: '',
            'app_src' => getenv('APP_SRC') ?: '',
            'server_role' => getenv('SERVER_ROLE') ?: '',
            'static_handler' => getenv('STATIC_HANDLER') ?: '',
            'host_ip' => getenv('HOST_IP') ?: '',
            'os_name' => getenv('OS_NAME') ?: '',
            'network_mode' => getenv('NETWORK_MODE') ?: '',
            'scf_update_server' => getenv('SCF_UPDATE_SERVER') ?: 'https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/version.json',
        ];

        defined('ENV_VARIABLES') || define('ENV_VARIABLES', $envVariables);
        defined('IS_DEV') || define('IS_DEV', in_array('-dev', $argv, true) || ($envVariables['app_env'] === 'dev'));
        defined('IS_PACK') || define('IS_PACK', false);
        defined('NO_PACK') || define('NO_PACK', true);
        defined('IS_HTTP_SERVER') || define('IS_HTTP_SERVER', true);
        defined('IS_HTTP_SERVER_START') || define('IS_HTTP_SERVER_START', true);
        defined('RUNNING_BUILD') || define('RUNNING_BUILD', false);
        defined('RUNNING_INSTALL') || define('RUNNING_INSTALL', false);
        defined('RUNNING_TOOLBOX') || define('RUNNING_TOOLBOX', false);
        defined('RUNNING_BUILD_FRAMEWORK') || define('RUNNING_BUILD_FRAMEWORK', false);
        defined('RUNNING_CREATE_AR') || define('RUNNING_CREATE_AR', false);
        defined('FRAMEWORK_IS_PHAR') || define('FRAMEWORK_IS_PHAR', false);
    }

    protected static function registerFrameworkAutoload(): void {
        spl_autoload_register(static function (string $class): void {
            if (!str_starts_with($class, 'Scf\\')) {
                return;
            }
            $relative = str_replace('\\', '/', substr($class, 4)) . '.php';
            $path = SCF_ROOT . '/src/' . $relative;
            if (is_file($path)) {
                require_once $path;
            }
        });
    }

    protected static function parseArgs(array $argv): array {
        $args = [];
        $opts = [];
        $params = $argv;
        array_shift($params);
        while (($param = current($params)) !== false) {
            next($params);
            if (str_starts_with($param, '-')) {
                $option = ltrim($param, '-');
                $value = null;
                if (str_contains($option, '=')) {
                    [$option, $value] = explode('=', $option, 2);
                } elseif (($peek = current($params)) !== false && !str_starts_with((string)$peek, '-')) {
                    $value = (string)$peek;
                    next($params);
                }
                if ($option !== '') {
                    $opts[$option] = $value;
                }
                continue;
            }
            if (str_contains($param, '=')) {
                [$key, $value] = explode('=', $param, 2);
                $args[$key] = $value;
                continue;
            }
            $args[] = $param;
        }
        return [$args, $opts];
    }
}
