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
use Scf\Core\Console;
use Scf\Core\Env;
use RuntimeException;
use Scf\Server\Http;
use Swoole\Process;

/**
 * Gateway 启动引导器。
 *
 * 该类只负责 gateway 进程的启动编排边界：解析 CLI 参数、建立运行常量、
 * 装载应用上下文，并把 gateway、managed upstream 与 registry 的生命周期串联起来。
 * 具体业务请求处理不在这里实现。
 */
class CliBootstrap {

    /**
     * gateway 控制面模式的入口。
     *
     * gateway 主进程只负责控制面入口，不应在最外层提前挂载业务应用。
     * 业务代码的装载应留给 managed upstream、crontab、redis queue 等
     * 真实需要业务上下文的子进程各自完成，避免后续子进程重启时继承旧业务镜像。
     *
     * @return void
     */
    public static function bootedRun(): void {
        defined('PROXY_GATEWAY_MODE') || define('PROXY_GATEWAY_MODE', true);
        Env::initialize(MODE_CGI);
        self::defineFrameworkBuildConstants();
        self::startGateway(Manager::instance()->getOpts());
    }

    /**
     * 被 gateway 子进程调用的 upstream 入口。
     *
     * 该入口只初始化 upstream 所需的最小运行环境，避免重复执行 gateway
     * 控制面编排逻辑。
     *
     * @return void
     */
    public static function bootedRunUpstream(): void {
        defined('PROXY_UPSTREAM_MODE') || define('PROXY_UPSTREAM_MODE', true);
        Env::initialize(MODE_CGI);
        self::defineFrameworkBuildConstants();
        Http::create(SERVER_ROLE, '127.0.0.1', (int)SERVER_PORT)->start();
    }

    /**
     * 普通 CLI 启动入口。
     *
     * 负责注入基础常量、注册自动加载和解析参数，再交给 gateway 启动。
     * 这里同样保持 gateway 主进程不提前挂载业务应用。
     *
     * @param array $argv CLI 参数原始数组，通常来自 `$_SERVER['argv']`
     * @return void
     */
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
        self::startGateway($opts);
    }

    /**
     * 构建 gateway 运行参数并启动主 server。
     *
     * 这里会顺带准备 managed upstream、registry 和 supervisor 所需的初始状态，
     * 是 gateway 生命周期的核心编排入口。
     *
     * @param array $opts 命令行解析后的选项集合
     * @return void
     * @throws RuntimeException 当端口冲突或 upstream 状态不满足启动前置条件时抛出
     */
    protected static function startGateway(array $opts): void {
        $serverConfig = \Scf\Core\Config::server();
        $upstreamHost = (string)($opts['upstream_host'] ?? '127.0.0.1');
        $bindHost = (string)($opts['host'] ?? '0.0.0.0');
        $configPort = (int)($serverConfig['port'] ?? 9580);
        $configRpcPort = (int)($serverConfig['rpc_port'] ?? 0);
        $bindPort = (int)($opts['port'] ?? $configPort);
        $trafficMode = self::resolveTrafficMode($opts, $serverConfig);
        $controlBindPort = self::resolveControlPort($opts, $serverConfig, $bindPort, $configPort);
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
        if (in_array($trafficMode, ['tcp', 'nginx'], true)) {
            if ($controlBindPort === $bindPort) {
                throw new RuntimeException("{$trafficMode}流量模式下控制面端口不能与业务端口相同: {$bindPort}");
            }
            if ($rpcBindPort > 0 && $controlBindPort === $rpcBindPort) {
                throw new RuntimeException("{$trafficMode}流量模式下控制面端口不能与Gateway RPC端口相同: {$rpcBindPort}");
            }
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
                // 启动 managed upstream 时，把 gateway 侧的运行语义向子进程继承下去，
                // 包括 dev / pack / 源码类型 / gateway_port 等关键信息。
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
        self::cleanupOrphanManagedUpstreams($launcher, $bindPort, array_values(array_unique(array_filter([
            $upstreamPort > 0 ? $upstreamPort : 0,
        ]))));
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

        $server = new GatewayServer(
            $manager,
            $bindHost,
            $bindPort,
            $workerNum,
            $launcher,
            $managedUpstreamPlans,
            $rpcBindPort,
            $controlBindPort,
            $trafficMode
        );
        $server->start();
    }

    /**
     * 解析 gateway 的流量转发模式。
     *
     * @param array $opts 命令行选项集合
     * @param array $serverConfig 框架 server 配置
     * @return string 返回 `tcp` 或 `nginx`，非法值会回退为 `nginx`
     */
    protected static function resolveTrafficMode(array $opts, array $serverConfig): string {
        $mode = strtolower(trim((string)($opts['traffic_mode'] ?? $opts['gateway_traffic_mode'] ?? ($serverConfig['gateway_traffic_mode'] ?? 'nginx'))));
        return in_array($mode, ['tcp', 'nginx'], true) ? $mode : 'nginx';
    }

    /**
     * 解析 gateway 控制面端口。
     *
     * @param array $opts 命令行选项集合
     * @param array $serverConfig 框架 server 配置
     * @param int $bindPort gateway 业务监听端口
     * @param int $configPort 配置中的业务监听端口
     * @return int 控制面监听端口
     */
    protected static function resolveControlPort(array $opts, array $serverConfig, int $bindPort, int $configPort): int {
        if (array_key_exists('control_port', $opts) || array_key_exists('gateway_control_port', $opts)) {
            $configured = (int)($opts['control_port'] ?? $opts['gateway_control_port'] ?? 0);
            return $configured > 0 ? $configured : ($bindPort + 1000);
        }
        $configured = (int)($serverConfig['gateway_control_port'] ?? 0);
        if ($configured > 0) {
            $offset = max(1, $configured - $configPort);
            return $bindPort + $offset;
        }
        return $bindPort + 1000;
    }

    /**
     * 启动前回收 registry 中残留的无效实例。
     *
     * @param AppInstanceManager $manager registry 管理器
     * @param AppServerLauncher $launcher 进程和端口探测工具
     * @param string $bindHost gateway 业务绑定地址
     * @param int $bindPort gateway 业务监听端口
     * @return void
     */
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

    /**
     * 清理 registry 中记录的历史托管 upstream。
     *
     * @param AppInstanceManager $manager registry 管理器
     * @param AppServerLauncher $launcher 进程和端口探测工具
     * @return void
     */
    protected static function cleanupManagedUpstreams(AppInstanceManager $manager, AppServerLauncher $launcher): void {
        $managedInstances = $manager->managedInstances();
        if (!$managedInstances) {
            return;
        }

        echo Console::timestamp() . " 【Gateway】启动前清理历史托管实例: " . count($managedInstances) . PHP_EOL;
        foreach ($managedInstances as $instance) {
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            $rpcPort = (int)(($instance['metadata']['rpc_port'] ?? 0));
            $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
            echo Console::timestamp() . " 【Gateway】清理历史托管实例: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
            $launcher->stop($instance, 2);
        }
        $manager->removeManagedInstances();
        echo Console::timestamp() . " 【Gateway】历史托管实例清理完成" . PHP_EOL;
    }

    /**
     * 清理不在 registry 里的孤儿 upstream。
     *
     * @param AppServerLauncher $launcher 进程和端口探测工具
     * @param int $gatewayPort 当前 gateway 业务端口
     * @param array $keepPorts 需要保留的 upstream 端口列表
     * @return void
     */
    protected static function cleanupOrphanManagedUpstreams(AppServerLauncher $launcher, int $gatewayPort, array $keepPorts = []): void {
        $instances = self::discoverOrphanManagedUpstreams($gatewayPort, $keepPorts);
        if (!$instances) {
            return;
        }

        echo Console::timestamp() . " 【Gateway】启动前清理孤儿业务实例: " . count($instances) . PHP_EOL;
        foreach ($instances as $instance) {
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            $rpcPort = (int)(($instance['metadata']['rpc_port'] ?? 0));
            $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
            echo Console::timestamp() . " 【Gateway】清理孤儿业务实例: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
            $launcher->stop($instance, 2);
        }
        echo Console::timestamp() . " 【Gateway】孤儿业务实例清理完成" . PHP_EOL;
    }

    /**
     * 从进程列表中发现不受 registry 管理的 upstream。
     *
     * @param int $gatewayPort 当前 gateway 业务端口
     * @param array $keepPorts 需要保留的 upstream 端口列表
     * @return array<int, array<string, mixed>>
     */
    protected static function discoverOrphanManagedUpstreams(int $gatewayPort, array $keepPorts = []): array {
        if ($gatewayPort <= 0) {
            return [];
        }
        $output = @shell_exec('ps -eo pid=,command= 2>/dev/null');
        if (!is_string($output) || trim($output) === '') {
            return [];
        }

        $keep = array_fill_keys(array_map('intval', array_filter($keepPorts, static fn($port) => (int)$port > 0)), true);
        $instances = [];
        foreach (preg_split('/\r?\n/', trim($output)) as $line) {
            $line = trim((string)$line);
            if ($line === '' || !preg_match('/^(\d+)\s+(.+)$/', $line, $matches)) {
                continue;
            }
            $pid = (int)$matches[1];
            $command = (string)$matches[2];
            if (!str_contains($command, '/boot gateway_upstream start')) {
                continue;
            }
            if (!str_contains($command, '-app=' . APP_DIR_NAME)) {
                continue;
            }
            if (self::extractIntFlag($command, 'gateway_port') !== $gatewayPort) {
                continue;
            }
            $port = self::extractIntFlag($command, 'port');
            if ($port <= 0 || isset($keep[$port])) {
                continue;
            }
            $instances[] = [
                'host' => '127.0.0.1',
                'port' => $port,
                'metadata' => [
                    'managed' => true,
                    'pid' => $pid,
                    'rpc_port' => self::extractIntFlag($command, 'rport'),
                ],
            ];
        }
        return $instances;
    }

    /**
     * 从启动命令中提取整型参数。
     *
     * @param string $command 进程启动命令行
     * @param string $flag 目标参数名
     * @return int 提取到的整型值，缺失时返回 0
     */
    protected static function extractIntFlag(string $command, string $flag): int {
        if (preg_match('/(?:^|\s)-' . preg_quote($flag, '/') . '=(\d+)/', $command, $matches)) {
            return (int)($matches[1] ?? 0);
        }
        return 0;
    }

    /**
     * 在 registry 中查找已有 upstream 的 RPC 端口。
     *
     * @param AppInstanceManager $manager registry 管理器
     * @param string $version upstream 版本标识
     * @param string $host upstream 主机地址
     * @param int $port upstream 业务端口
     * @return int 对应的 RPC 端口，未找到时返回 0
     */
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

    /**
     * 计算当前 gateway 对应的 registry 状态文件路径。
     *
     * @param int $bindPort gateway 业务监听端口
     * @return string registry 状态文件路径
     */
    protected static function defaultStateFile(int $bindPort): string {
        $role = strtolower((string)SERVER_ROLE);
        if ($bindPort > 0) {
            return APP_UPDATE_DIR . "/gateway_registry_{$role}_{$bindPort}.json";
        }

        return APP_UPDATE_DIR . "/gateway_registry_{$role}.json";
    }

    /**
     * 载入 framework 构建版本常量，区分源码 / 本地 build / phar。
     *
     * @return void
     */
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

    /**
     * 定义 gateway 启动所需的基础常量。
     *
     * @param array $argv 原始 CLI 参数数组
     * @return void
     */
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

    /**
     * 注册 Scf 命名空间的源码自动加载。
     *
     * @return void
     */
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

    /**
     * 解析 gateway 启动参数与键值型命令行参数。
     *
     * @param array $argv 原始 CLI 参数数组
     * @return array{0: array<int, string>, 1: array<string, mixed>} 返回解析后的位置参数和选项参数
     */
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

    /**
     * 读取布尔型开关参数。
     *
     * @param array $opts 选项集合
     * @param string $name 选项名
     * @param bool $default 未设置时的默认值
     * @return bool 解析后的开关值
     */
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

    /**
     * 继承 gateway 的运行参数到 managed upstream。
     *
     * 只保留会影响运行语义的开关，避免把编排参数原样透传给子进程。
     *
     * @param array $opts gateway 启动选项
     * @param int $upstreamRpcPort upstream RPC 端口
     * @param int $gatewayPort gateway 业务端口
     * @return array<int, string> 需要透传给 upstream 的参数列表
     */
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
