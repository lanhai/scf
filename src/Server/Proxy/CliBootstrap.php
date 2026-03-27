<?php

declare(strict_types=1);

namespace Scf\Server\Proxy;

require_once __DIR__ . '/AppServerLauncher.php';
require_once __DIR__ . '/AppInstanceManager.php';
require_once __DIR__ . '/GatewayServer.php';
require_once __DIR__ . '/UpstreamRegistry.php';
require_once __DIR__ . '/UpstreamSupervisor.php';

use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Env;
use RuntimeException;
use Scf\Server\Http;
use Swoole\Process;

class CliBootstrap {

    public static function bootedRun(): void {
        defined('PROXY_GATEWAY_MODE') || define('PROXY_GATEWAY_MODE', true);
        Env::initialize(MODE_CGI);
        self::mountGatewayAppIfReady();
        self::defineFrameworkBuildConstants();
        self::startGateway(Manager::instance()->getOpts());
    }

    public static function bootedRunUpstream(): void {
        defined('PROXY_UPSTREAM_MODE') || define('PROXY_UPSTREAM_MODE', true);
        Env::initialize(MODE_CGI);
        self::defineFrameworkBuildConstants();
        Http::create(SERVER_ROLE, '127.0.0.1', (int)SERVER_PORT)->start();
    }

    public static function run(array $argv): void {
        self::defineBaseConstants($argv);
        defined('PROXY_GATEWAY_MODE') || define('PROXY_GATEWAY_MODE', true);
        self::registerFrameworkAutoload();
        require_once SCF_ROOT . '/src/Const.php';
        require_once SCF_ROOT . '/vendor/autoload.php';

        date_default_timezone_set(getenv('TZ') ?: 'Asia/Shanghai');

        [$args, $opts] = self::parseArgs($argv);
        Manager::instance()->setArgs($args);
        Manager::instance()->setOpts($opts);
        Env::initialize(MODE_CGI);
        self::mountGatewayAppIfReady();
        self::startGateway($opts);
    }

    protected static function mountGatewayAppIfReady(): void {
        if (!App::installer()->isInstalled()) {
            return;
        }
        App::mount();
    }

    protected static function startGateway(array $opts): void {
        $serverConfig = \Scf\Core\Config::server();
        $upstreamHost = (string)($opts['upstream_host'] ?? '127.0.0.1');
        $bindHost = (string)($opts['host'] ?? '0.0.0.0');
        $configPort = (int)($serverConfig['port'] ?? 9580);
        $configRpcPort = (int)($serverConfig['rpc_port'] ?? 0);
        $bindPort = (int)($opts['port'] ?? $configPort);
        $rpcBindPortExplicit = array_key_exists('rpc_port', $opts);
        if ($rpcBindPortExplicit) {
            $rpcBindPort = (int)$opts['rpc_port'];
        } elseif ($configRpcPort > 0) {
            if ($bindPort === $configPort) {
                $rpcBindPort = $configRpcPort;
            } else {
                $rpcOffset = max(1, $configRpcPort - $configPort);
                $rpcBindPort = (new AppServerLauncher())->findAvailablePort('127.0.0.1', max(1025, $bindPort + $rpcOffset));
            }
        } else {
            $rpcBindPort = 0;
        }
        $workerNum = (int)($opts['worker_num'] ?? 1);
        $version = (string)($opts['upstream_version'] ?? \Scf\Core\App::version() ?? 'current');
        $upstreamRole = (string)($opts['upstream_role'] ?? SERVER_ROLE);
        $stateFile = (string)($opts['state_file'] ?? self::defaultStateFile($bindPort));
        $spawnUpstream = self::optEnabled($opts, 'spawn_upstream', true);
        $reuseExistingUpstream = self::optEnabled($opts, 'reuse_upstream', false);
        $bootstrapActive = !isset($opts['register_only']);
        $registry = new UpstreamRegistry($stateFile);
        $manager = new AppInstanceManager($registry);

        $launcher = new AppServerLauncher();
        $upstreamPortExplicit = isset($opts['upstream_port']);
        $upstreamPort = $upstreamPortExplicit
            ? (int)$opts['upstream_port']
            : $launcher->findAvailablePort('127.0.0.1', max($bindPort + 1, (int)($serverConfig['proxy_upstream_port'] ?? ($bindPort + 1))));
        $upstreamRpcPortExplicit = isset($opts['upstream_rpc_port']);
        $upstreamRpcPort = $rpcBindPort > 0
            ? ($upstreamRpcPortExplicit
                ? (int)$opts['upstream_rpc_port']
                : $launcher->findAvailablePort('127.0.0.1', max($rpcBindPort + 1, (int)($serverConfig['proxy_upstream_rpc_port'] ?? ($rpcBindPort + 1)))))
            : 0;

        if ($upstreamPort > 0 && $upstreamPort === $bindPort) {
            throw new RuntimeException("upstream HTTP 端口不能与 Gateway 监听端口相同: {$bindPort}");
        }
        if ($rpcBindPort > 0 && $upstreamRpcPort > 0 && $upstreamRpcPort === $rpcBindPort) {
            throw new RuntimeException("upstream RPC 端口不能与 Gateway RPC 端口相同: {$rpcBindPort}");
        }

        $spawnMetadata = [];
        $managedUpstreamPlans = [];

        if ($spawnUpstream) {
            $portInUse = $launcher->isListening($upstreamHost, $upstreamPort, 0.2);
            if ($portInUse && !$upstreamPortExplicit) {
                $upstreamPort = $launcher->findAvailablePort('127.0.0.1', $upstreamPort + 1);
                $portInUse = false;
            }

            if (!$portInUse) {
                $spawnMetadata = [
                    'managed' => true,
                    'role' => $upstreamRole,
                    'started_at' => time(),
                    'managed_mode' => 'gateway_supervisor',
                ];
                $managedUpstreamPlans[] = [
                    'app' => APP_DIR_NAME,
                    'env' => SERVER_RUN_ENV,
                    'host' => $upstreamHost,
                    'version' => $version,
                    'weight' => (int)($opts['weight'] ?? 100),
                    'role' => $upstreamRole,
                    'port' => $upstreamPort,
                    'rpc_port' => $upstreamRpcPort,
                    'src' => APP_SRC_TYPE,
                    'metadata' => $spawnMetadata,
                    'start_timeout' => (int)($opts['upstream_start_timeout'] ?? 25),
                    'extra' => self::forwardedFlags($opts, $upstreamRpcPort, $bindPort),
                ];
            } else {
                $existingRpcPort = $upstreamRpcPortExplicit ? $upstreamRpcPort : self::findExistingRpcPort($manager, $version, $upstreamHost, $upstreamPort);
                if ($rpcBindPort > 0) {
                    if ($existingRpcPort <= 0) {
                        throw new RuntimeException("复用已有 upstream 时未发现可用 RPC 端口: {$upstreamHost}:{$upstreamPort}");
                    }
                    if (!$launcher->isListening($upstreamHost, $existingRpcPort, 0.2)) {
                        throw new RuntimeException("复用已有 upstream 时 RPC 端口不可用: {$upstreamHost}:{$existingRpcPort}");
                    }
                }
                if (!$reuseExistingUpstream) {
                    throw new RuntimeException("upstream 端口已被占用且未允许复用: {$upstreamHost}:{$upstreamPort}");
                }
                $spawnMetadata = [
                    'managed' => false,
                    'rpc_port' => $existingRpcPort,
                    'message' => 'upstream 端口已存在监听，直接复用',
                ];
            }
        }

        if (!$spawnUpstream && $upstreamPort > 0) {
            if (!$launcher->isListening($upstreamHost, $upstreamPort, 0.2)) {
                throw new RuntimeException("指定的 upstream HTTP 端口不可用: {$upstreamHost}:{$upstreamPort}");
            }
            if ($rpcBindPort > 0) {
                if ($upstreamRpcPort <= 0) {
                    throw new RuntimeException("spawn_upstream=off 时必须显式提供可用的 -upstream_rpc_port");
                }
                if (!$launcher->isListening($upstreamHost, $upstreamRpcPort, 0.2)) {
                    throw new RuntimeException("指定的 upstream RPC 端口不可用: {$upstreamHost}:{$upstreamRpcPort}");
                }
            }
            $spawnMetadata = [
                'managed' => false,
                'rpc_port' => $upstreamRpcPort,
                'message' => '使用外部已有 upstream',
            ];
        }

        self::reconcileRegistry($manager, $launcher, $bindHost, $bindPort);
        self::cleanupManagedUpstreams($manager, $launcher);
        $manager->removeInstance($bindHost === '0.0.0.0' ? '127.0.0.1' : $bindHost, $bindPort);
        $manager->removeInstance('127.0.0.1', $bindPort);
        $manager->removeInstance('0.0.0.0', $bindPort);
        if ($spawnMetadata && !$managedUpstreamPlans) {
            foreach ($manager->otherInstances($version, $upstreamHost, $upstreamPort) as $instance) {
                $launcher->stop($instance, 2);
            }
        }
        if (!$managedUpstreamPlans) {
            $manager->bootstrap($version, $upstreamHost, $upstreamPort, $bootstrapActive, (int)($opts['weight'] ?? 100), $spawnMetadata);
        }
        if ($spawnMetadata && !$managedUpstreamPlans) {
            $manager->removeOtherInstances($version, $upstreamHost, $upstreamPort);
        }

        $server = new GatewayServer($manager, $bindHost, $bindPort, $workerNum, $launcher, $managedUpstreamPlans, $rpcBindPort);
        $server->start();
    }

    protected static function reconcileRegistry(AppInstanceManager $manager, AppServerLauncher $launcher, string $bindHost, int $bindPort): void {
        $normalizedBindHost = $bindHost === '0.0.0.0' ? '127.0.0.1' : $bindHost;
        $manager->reconcileInstances(static function (array $instance) use ($launcher, $normalizedBindHost, $bindPort) {
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            if ($port <= 0) {
                return false;
            }

            if ((($host === $normalizedBindHost) || ($host === '127.0.0.1' && $normalizedBindHost === '127.0.0.1')) && $port === $bindPort) {
                return false;
            }

            $metadata = (array)($instance['metadata'] ?? []);
            if (($metadata['managed'] ?? false) === true) {
                $pid = (int)($metadata['pid'] ?? 0);
                if ($pid <= 0 || !Process::kill($pid, 0)) {
                    return false;
                }
            }

            if (!$launcher->isListening($host, $port, 0.2)) {
                return false;
            }

            $rpcPort = (int)($metadata['rpc_port'] ?? 0);
            if ($rpcPort > 0 && !$launcher->isListening($host, $rpcPort, 0.2)) {
                return false;
            }

            return true;
        });
    }

    protected static function cleanupManagedUpstreams(AppInstanceManager $manager, AppServerLauncher $launcher): void {
        $managedInstances = $manager->managedInstances();
        if (!$managedInstances) {
            return;
        }

        echo date('m-d H:i:s') . " 【Gateway】启动前清理历史托管实例: " . count($managedInstances) . PHP_EOL;
        foreach ($managedInstances as $instance) {
            $launcher->stop($instance, 2);
        }
        $manager->removeManagedInstances();
    }

    protected static function findExistingRpcPort(AppInstanceManager $manager, string $version, string $host, int $port): int {
        $snapshot = $manager->snapshot();
        foreach (($snapshot['generations'] ?? []) as $itemVersion => $generation) {
            if ($itemVersion !== $version) {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                if (($instance['host'] ?? '') !== $host || (int)($instance['port'] ?? 0) !== $port) {
                    continue;
                }
                return (int)(($instance['metadata']['rpc_port'] ?? 0));
            }
        }
        return 0;
    }

    protected static function defaultStateFile(int $bindPort): string {
        $role = strtolower((string)SERVER_ROLE);
        if ($bindPort > 0) {
            return APP_UPDATE_DIR . "/gateway_registry_{$role}_{$bindPort}.json";
        }

        return APP_UPDATE_DIR . "/gateway_registry_{$role}.json";
    }

    protected static function defineFrameworkBuildConstants(): void {
        if (defined('FRAMEWORK_BUILD_TIME') && defined('FRAMEWORK_BUILD_VERSION')) {
            return;
        }
        $buildVersion = ['build' => 'development', 'version' => SCF_COMPOSER_VERSION];
        $versionFile = SCF_ROOT . '/src/version.php';
        $localVersionFile = SCF_ROOT . '/build/framework/version.json';
        $packVersionFile = 'phar://' . SCF_ROOT . '/build/src.pack/version.php';
        if (defined('IS_DEV') && IS_DEV && is_file($versionFile)) {
            $buildVersion = require $versionFile;
        } elseif (is_file($localVersionFile)) {
            $data = json_decode((string)file_get_contents($localVersionFile), true);
            if (is_array($data) && $data) {
                $buildVersion = $data;
            }
        } elseif (@file_exists($packVersionFile)) {
            $buildVersion = require $packVersionFile;
        } elseif (is_file($versionFile)) {
            $buildVersion = require $versionFile;
        }
        defined('FRAMEWORK_BUILD_TIME') || define('FRAMEWORK_BUILD_TIME', $buildVersion['build'] ?? 'development');
        defined('FRAMEWORK_BUILD_VERSION') || define('FRAMEWORK_BUILD_VERSION', $buildVersion['version'] ?? SCF_COMPOSER_VERSION);
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

    protected static function optEnabled(array $opts, string $name, bool $default = false): bool {
        if (!array_key_exists($name, $opts)) {
            return $default;
        }
        $value = $opts[$name];
        if ($value === null) {
            return true;
        }
        return !in_array(strtolower((string)$value), ['0', 'false', 'off', 'no'], true);
    }

    protected static function forwardedFlags(array $opts, int $upstreamRpcPort = 0, int $gatewayPort = 0): array {
        $forward = [];
        $reserved = array_fill_keys([
            'app',
            'env',
            'role',
            'host',
            'port',
            'rpc_port',
            'upstream_host',
            'upstream_port',
            'upstream_rpc_port',
            'upstream_version',
            'upstream_role',
            'spawn_upstream',
            'reuse_upstream',
            'register_only',
            'weight',
            'worker_num',
            'state_file',
            'upstream_start_timeout',
            'gateway_port',
            'rport',
            'src',
        ], true);

        foreach ($opts as $name => $value) {
            if (isset($reserved[$name])) {
                continue;
            }
            if ($value === null) {
                $forward[] = '-' . $name;
                continue;
            }
            $forward[] = '-' . $name . '=' . $value;
        }
        if ($upstreamRpcPort > 0) {
            $forward[] = '-rport=' . $upstreamRpcPort;
        }
        if ($gatewayPort > 0) {
            $forward[] = '-gateway_port=' . $gatewayPort;
        }
        return array_values(array_unique($forward));
    }
}
