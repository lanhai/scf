<?php

declare(strict_types=1);

namespace Scf\Server\Proxy;

require_once __DIR__ . '/AppServerLauncher.php';
require_once __DIR__ . '/AppInstanceManager.php';
require_once __DIR__ . '/GatewayLease.php';
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
        self::ensureBootstrapToolchain();
        defined('PROXY_GATEWAY_MODE') || define('PROXY_GATEWAY_MODE', true);
        Env::initialize(MODE_CGI);
        scf_define_framework_build_constants();
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
        self::ensureBootstrapToolchain();
        defined('PROXY_UPSTREAM_MODE') || define('PROXY_UPSTREAM_MODE', true);
        Env::initialize(MODE_CGI);
        scf_define_framework_build_constants();
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
        defined('PROXY_GATEWAY_MODE') || define('PROXY_GATEWAY_MODE', true);
        scf_register_source_framework_autoload();
        require_once SCF_ROOT . '/src/Const.php';
        require_once SCF_ROOT . '/vendor/autoload.php';

        date_default_timezone_set(getenv('TZ') ?: 'Asia/Shanghai');

        [$args, $opts] = scf_parse_cli_args($argv);
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
        $launcher = new AppServerLauncher();
        $startup = self::buildGatewayStartupContext($opts, $launcher);
        $registry = new UpstreamRegistry($startup['state_file']);
        $manager = new AppInstanceManager($registry);
        $upstreamBootstrap = self::resolveUpstreamBootstrap($opts, $startup, $manager, $launcher);
        $preservedUpstreamPorts = [];
        if ($upstreamBootstrap['spawn_metadata'] && !$upstreamBootstrap['managed_upstream_plans']) {
            $preservedPort = (int)($upstreamBootstrap['upstream_port'] ?? 0);
            if ($preservedPort > 0) {
                // 只有“复用已有 upstream / 使用外部 upstream”这类不由 gateway 再次拉起的场景，
                // 才需要在启动前清理孤儿实例时保留当前端口；managed spawn 场景后面会在清理完成后
                // 重新解析一次 plan，避免拿着 cleanup 之前的旧端口继续启动。
                $preservedUpstreamPorts[] = $preservedPort;
            }
        }

        self::reconcileRegistry($manager, $launcher, $startup['bind_host'], $startup['bind_port']);
        self::cleanupManagedUpstreams($manager, $launcher);
        self::cleanupOrphanManagedUpstreams($launcher, $startup['bind_port'], $preservedUpstreamPorts);
        self::forgetGatewayRegistryEntries($manager, $startup['bind_host'], $startup['bind_port']);

        // upstream 端口选择必须基于 cleanup 之后的真实监听态；否则一旦历史实例在
        // resolve 之后、spawn 之前才被清理或仍未完全退出，就会拿着过期端口继续启动。
        $upstreamBootstrap = self::resolveUpstreamBootstrap($opts, $startup, $manager, $launcher);

        if ($upstreamBootstrap['spawn_metadata'] && !$upstreamBootstrap['managed_upstream_plans']) {
            foreach ($manager->otherInstances($startup['version'], $startup['upstream_host'], $upstreamBootstrap['upstream_port']) as $instance) {
                $launcher->stop($instance, 2);
            }
        }
        if (!$upstreamBootstrap['managed_upstream_plans']) {
            $manager->bootstrap(
                $startup['version'],
                $startup['upstream_host'],
                $upstreamBootstrap['upstream_port'],
                $startup['bootstrap_active'],
                $startup['weight'],
                $upstreamBootstrap['spawn_metadata']
            );
        }
        if ($upstreamBootstrap['spawn_metadata'] && !$upstreamBootstrap['managed_upstream_plans']) {
            $manager->removeOtherInstances($startup['version'], $startup['upstream_host'], $upstreamBootstrap['upstream_port']);
        }

        $server = new GatewayServer(
            $manager,
            $startup['bind_host'],
            $startup['bind_port'],
            $startup['worker_num'],
            $launcher,
            $upstreamBootstrap['managed_upstream_plans'],
            $startup['rpc_bind_port'],
            $startup['control_bind_port'],
            $startup['traffic_mode']
        );
        $server->start();
    }

    /**
     * 归一化 gateway 启动上下文。
     *
     * 这个方法只处理 gateway 自身的监听端口、控制面端口、RPC 端口、registry
     * 状态文件等“控制面运行参数”，不掺杂 upstream 是否拉起、是否复用等业务决策。
     *
     * @param array<string, mixed> $opts CLI 选项
     * @param AppServerLauncher $launcher 端口探测与分配工具
     * @return array<string, int|string|bool|array>
     */
    protected static function buildGatewayStartupContext(array $opts, AppServerLauncher $launcher): array {
        $serverConfig = \Scf\Core\Config::server();
        $configPort = (int)($serverConfig['port'] ?? 9580);
        $bindPort = (int)($opts['port'] ?? $configPort);
        $rpcBindPort = self::resolveGatewayRpcPort($opts, $serverConfig, $bindPort, $configPort, $launcher);
        $trafficMode = self::resolveTrafficMode($opts, $serverConfig);
        $controlBindPort = self::resolveControlPort($opts, $serverConfig, $bindPort, $configPort);

        self::validateGatewayListenerLayout($trafficMode, $bindPort, $controlBindPort, $rpcBindPort);

        return [
            'server_config' => $serverConfig,
            'upstream_host' => (string)($opts['upstream_host'] ?? '127.0.0.1'),
            'bind_host' => (string)($opts['host'] ?? '0.0.0.0'),
            'bind_port' => $bindPort,
            'rpc_bind_port' => $rpcBindPort,
            'control_bind_port' => $controlBindPort,
            'traffic_mode' => $trafficMode,
            'worker_num' => (int)($opts['worker_num'] ?? 1),
            'version' => (string)($opts['upstream_version'] ?? \Scf\Core\App::version() ?? 'current'),
            'upstream_role' => (string)($opts['upstream_role'] ?? SERVER_ROLE),
            'state_file' => (string)($opts['state_file'] ?? self::defaultStateFile($bindPort)),
            'bootstrap_active' => !isset($opts['register_only']),
            'weight' => (int)($opts['weight'] ?? 100),
            'upstream_start_timeout' => (int)($opts['upstream_start_timeout'] ?? 25),
            'spawn_upstream' => self::optEnabled($opts, 'spawn_upstream', true),
            'reuse_existing_upstream' => self::optEnabled($opts, 'reuse_upstream', false),
        ];
    }

    /**
     * 解析 gateway RPC 监听端口。
     *
     * 规则保持和原来一致：显式 `-rpc_port` 优先，其次复用配置偏移，
     * 否则在当前 bindPort 的对应偏移位置上寻找一个可用端口。
     *
     * @param array<string, mixed> $opts CLI 选项
     * @param array<string, mixed> $serverConfig server 配置
     * @param int $bindPort gateway 业务端口
     * @param int $configPort 默认配置业务端口
     * @param AppServerLauncher $launcher 端口分配工具
     * @return int
     */
    protected static function resolveGatewayRpcPort(array $opts, array $serverConfig, int $bindPort, int $configPort, AppServerLauncher $launcher): int {
        if (array_key_exists('rpc_port', $opts)) {
            return (int)$opts['rpc_port'];
        }

        $configRpcPort = (int)($serverConfig['rpc_port'] ?? 0);
        if ($configRpcPort <= 0) {
            return 0;
        }
        if ($bindPort === $configPort) {
            return $configRpcPort;
        }

        $rpcOffset = max(1, $configRpcPort - $configPort);
        return $launcher->findAvailablePort('127.0.0.1', max(1025, $bindPort + $rpcOffset));
    }

    /**
     * 校验 gateway 控制面、业务入口和 RPC 监听布局。
     *
     * @param string $trafficMode 当前流量模式
     * @param int $bindPort gateway 业务端口
     * @param int $controlBindPort gateway 控制面端口
     * @param int $rpcBindPort gateway RPC 端口
     * @return void
     */
    protected static function validateGatewayListenerLayout(string $trafficMode, int $bindPort, int $controlBindPort, int $rpcBindPort): void {
        if (!in_array($trafficMode, ['tcp', 'nginx'], true)) {
            return;
        }
        if ($controlBindPort === $bindPort) {
            throw new RuntimeException("{$trafficMode}流量模式下控制面端口不能与业务端口相同: {$bindPort}");
        }
        if ($rpcBindPort > 0 && $controlBindPort === $rpcBindPort) {
            throw new RuntimeException("{$trafficMode}流量模式下控制面端口不能与Gateway RPC端口相同: {$rpcBindPort}");
        }
    }

    /**
     * 解析 gateway 启动时对 upstream 的处理策略。
     *
     * 这一步只回答三件事：
     * 1. upstream 该监听哪个 HTTP/RPC 端口；
     * 2. 本轮是由 gateway 托管拉起，还是复用/接入外部已有 upstream；
     * 3. registry 初始化时该写入什么 metadata。
     *
     * @param array<string, mixed> $opts CLI 选项
     * @param array<string, int|string|bool|array> $startup gateway 启动上下文
     * @param AppInstanceManager $manager registry 管理器
     * @param AppServerLauncher $launcher 进程与端口工具
     * @return array{
     *     upstream_port:int,
     *     upstream_rpc_port:int,
     *     spawn_metadata:array<string,mixed>,
     *     managed_upstream_plans:array<int, array<string,mixed>>
     * }
     */
    protected static function resolveUpstreamBootstrap(array $opts, array $startup, AppInstanceManager $manager, AppServerLauncher $launcher): array {
        $serverConfig = (array)($startup['server_config'] ?? []);
        $bindPort = (int)($startup['bind_port'] ?? 0);
        $rpcBindPort = (int)($startup['rpc_bind_port'] ?? 0);
        $controlBindPort = (int)($startup['control_bind_port'] ?? 0);
        $upstreamHost = (string)($startup['upstream_host'] ?? '127.0.0.1');
        $version = (string)($startup['version'] ?? 'current');
        $upstreamRole = (string)($startup['upstream_role'] ?? SERVER_ROLE);
        $weight = (int)($startup['weight'] ?? 100);
        $spawnUpstream = (bool)($startup['spawn_upstream'] ?? true);
        $reuseExistingUpstream = (bool)($startup['reuse_existing_upstream'] ?? false);
        $upstreamStartTimeout = (int)($startup['upstream_start_timeout'] ?? 25);
        $reservedGatewayPorts = array_values(array_unique(array_filter([
            $bindPort,
            $rpcBindPort,
            $controlBindPort,
        ], static fn(int $port): bool => $port > 0)));

        $upstreamPortExplicit = isset($opts['upstream_port']);
        $upstreamPort = $upstreamPortExplicit
            ? (int)$opts['upstream_port']
            : $launcher->findAvailablePortAvoiding(
                '127.0.0.1',
                max($bindPort + 1, (int)($serverConfig['proxy_upstream_port'] ?? ($bindPort + 1))),
                $reservedGatewayPorts,
                2000
            );
        $upstreamRpcPortExplicit = isset($opts['upstream_rpc_port']);
        $upstreamRpcPort = $rpcBindPort > 0
            ? ($upstreamRpcPortExplicit
                ? (int)$opts['upstream_rpc_port']
                : $launcher->findAvailablePortAvoiding(
                    '127.0.0.1',
                    max($rpcBindPort + 1, (int)($serverConfig['proxy_upstream_rpc_port'] ?? ($rpcBindPort + 1))),
                    array_merge($reservedGatewayPorts, [$upstreamPort]),
                    2000
                ))
            : 0;

        self::validateUpstreamListenerLayout($bindPort, $rpcBindPort, $controlBindPort, $upstreamPort, $upstreamRpcPort);

        if ($spawnUpstream) {
            return self::resolveManagedUpstreamBootstrap(
                $opts,
                $startup,
                $manager,
                $launcher,
                $upstreamHost,
                $version,
                $upstreamRole,
                $weight,
                $upstreamStartTimeout,
                $upstreamPort,
                $upstreamPortExplicit,
                $upstreamRpcPort,
                $upstreamRpcPortExplicit,
                $reuseExistingUpstream
            );
        }

        self::assertExternalUpstreamAvailable($launcher, $upstreamHost, $upstreamPort, $rpcBindPort, $upstreamRpcPort);
        return [
            'upstream_port' => $upstreamPort,
            'upstream_rpc_port' => $upstreamRpcPort,
            'spawn_metadata' => [
                'managed' => false,
                'rpc_port' => $upstreamRpcPort,
                'message' => '使用外部已有 upstream',
            ],
            'managed_upstream_plans' => [],
        ];
    }

    /**
     * 校验 upstream 与 gateway 的监听端口布局。
     *
     * @param int $bindPort gateway 业务端口
     * @param int $rpcBindPort gateway RPC 端口
     * @param int $controlBindPort gateway 控制面端口
     * @param int $upstreamPort upstream HTTP 端口
     * @param int $upstreamRpcPort upstream RPC 端口
     * @return void
     */
    protected static function validateUpstreamListenerLayout(int $bindPort, int $rpcBindPort, int $controlBindPort, int $upstreamPort, int $upstreamRpcPort): void {
        if ($upstreamPort > 0 && $upstreamPort === $bindPort) {
            throw new RuntimeException("upstream HTTP 端口不能与 Gateway 监听端口相同: {$bindPort}");
        }
        if ($rpcBindPort > 0 && $upstreamPort > 0 && $upstreamPort === $rpcBindPort) {
            throw new RuntimeException("upstream HTTP 端口不能与 Gateway RPC 端口相同: {$rpcBindPort}");
        }
        if ($controlBindPort > 0 && $upstreamPort > 0 && $upstreamPort === $controlBindPort) {
            throw new RuntimeException("upstream HTTP 端口不能与 Gateway 控制面端口相同: {$controlBindPort}");
        }
        if ($rpcBindPort > 0 && $upstreamRpcPort > 0 && $upstreamRpcPort === $rpcBindPort) {
            throw new RuntimeException("upstream RPC 端口不能与 Gateway RPC 端口相同: {$rpcBindPort}");
        }
        if ($upstreamRpcPort > 0 && $upstreamRpcPort === $bindPort) {
            throw new RuntimeException("upstream RPC 端口不能与 Gateway 监听端口相同: {$bindPort}");
        }
        if ($controlBindPort > 0 && $upstreamRpcPort > 0 && $upstreamRpcPort === $controlBindPort) {
            throw new RuntimeException("upstream RPC 端口不能与 Gateway 控制面端口相同: {$controlBindPort}");
        }
        if ($upstreamPort > 0 && $upstreamRpcPort > 0 && $upstreamRpcPort === $upstreamPort) {
            throw new RuntimeException("upstream RPC 端口不能与 upstream HTTP 端口相同: {$upstreamPort}");
        }
    }

    /**
     * 解析“gateway 自己托管拉起 upstream”时的引导信息。
     *
     * @param array<string, mixed> $opts CLI 选项
     * @param array<string, int|string|bool|array> $startup gateway 启动上下文
     * @param AppInstanceManager $manager registry 管理器
     * @param AppServerLauncher $launcher 端口/进程工具
     * @param string $upstreamHost upstream 绑定地址
     * @param string $version upstream 版本标识
     * @param string $upstreamRole upstream 角色
     * @param int $weight upstream 权重
     * @param int $upstreamStartTimeout upstream 启动超时
     * @param int $upstreamPort 预期 HTTP 端口
     * @param bool $upstreamPortExplicit HTTP 端口是否显式指定
     * @param int $upstreamRpcPort 预期 RPC 端口
     * @param bool $upstreamRpcPortExplicit RPC 端口是否显式指定
     * @param bool $reuseExistingUpstream 是否允许复用已有 upstream
     * @return array{
     *     upstream_port:int,
     *     upstream_rpc_port:int,
     *     spawn_metadata:array<string,mixed>,
     *     managed_upstream_plans:array<int, array<string,mixed>>
     * }
     */
    protected static function resolveManagedUpstreamBootstrap(
        array $opts,
        array $startup,
        AppInstanceManager $manager,
        AppServerLauncher $launcher,
        string $upstreamHost,
        string $version,
        string $upstreamRole,
        int $weight,
        int $upstreamStartTimeout,
        int $upstreamPort,
        bool $upstreamPortExplicit,
        int $upstreamRpcPort,
        bool $upstreamRpcPortExplicit,
        bool $reuseExistingUpstream
    ): array {
        $reservedGatewayPorts = array_values(array_unique(array_filter([
            (int)($startup['bind_port'] ?? 0),
            (int)($startup['rpc_bind_port'] ?? 0),
            (int)($startup['control_bind_port'] ?? 0),
        ], static fn(int $port): bool => $port > 0)));
        $portInUse = $launcher->isListening($upstreamHost, $upstreamPort, 0.2);
        if ($portInUse && !$upstreamPortExplicit) {
            $upstreamPort = $launcher->findAvailablePortAvoiding(
                '127.0.0.1',
                $upstreamPort + 1,
                $reservedGatewayPorts,
                2000
            );
            $portInUse = false;
        }

        if (!$portInUse) {
            $spawnMetadata = [
                'managed' => true,
                'role' => $upstreamRole,
                'started_at' => time(),
                'managed_mode' => 'gateway_supervisor',
                'dynamic_port' => !$upstreamPortExplicit,
                'dynamic_rpc_port' => !$upstreamRpcPortExplicit,
            ];

            return [
                'upstream_port' => $upstreamPort,
                'upstream_rpc_port' => $upstreamRpcPort,
                'spawn_metadata' => $spawnMetadata,
                'managed_upstream_plans' => [[
                    'app' => APP_DIR_NAME,
                    'env' => SERVER_RUN_ENV,
                    'host' => $upstreamHost,
                    'version' => $version,
                    'weight' => $weight,
                    'role' => $upstreamRole,
                    'port' => $upstreamPort,
                    'rpc_port' => $upstreamRpcPort,
                    'src' => APP_SRC_TYPE,
                    'metadata' => $spawnMetadata,
                    'start_timeout' => $upstreamStartTimeout,
                    'extra' => self::forwardedFlags($opts, $upstreamRpcPort, (int)($startup['bind_port'] ?? 0)),
                ]],
            ];
        }

        $existingRpcPort = $upstreamRpcPortExplicit ? $upstreamRpcPort : self::findExistingRpcPort($manager, $version, $upstreamHost, $upstreamPort);
        self::assertReusableUpstreamAvailable(
            $launcher,
            $upstreamHost,
            $upstreamPort,
            (int)($startup['rpc_bind_port'] ?? 0),
            $existingRpcPort,
            $reuseExistingUpstream
        );

        return [
            'upstream_port' => $upstreamPort,
            'upstream_rpc_port' => $existingRpcPort,
            'spawn_metadata' => [
                'managed' => false,
                'rpc_port' => $existingRpcPort,
                'message' => 'upstream 端口已存在监听，直接复用',
            ],
            'managed_upstream_plans' => [],
        ];
    }

    /**
     * 校验复用已有 upstream 时的可达性约束。
     *
     * @param AppServerLauncher $launcher 端口探测工具
     * @param string $upstreamHost upstream 主机
     * @param int $upstreamPort upstream HTTP 端口
     * @param int $rpcBindPort gateway RPC 端口
     * @param int $existingRpcPort 已发现的 upstream RPC 端口
     * @param bool $reuseExistingUpstream 是否允许复用
     * @return void
     */
    protected static function assertReusableUpstreamAvailable(
        AppServerLauncher $launcher,
        string $upstreamHost,
        int $upstreamPort,
        int $rpcBindPort,
        int $existingRpcPort,
        bool $reuseExistingUpstream
    ): void {
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
    }

    /**
     * 校验显式使用外部 upstream 时的可达性。
     *
     * @param AppServerLauncher $launcher 端口探测工具
     * @param string $upstreamHost upstream 主机
     * @param int $upstreamPort upstream HTTP 端口
     * @param int $rpcBindPort gateway RPC 端口
     * @param int $upstreamRpcPort upstream RPC 端口
     * @return void
     */
    protected static function assertExternalUpstreamAvailable(
        AppServerLauncher $launcher,
        string $upstreamHost,
        int $upstreamPort,
        int $rpcBindPort,
        int $upstreamRpcPort
    ): void {
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
    }

    /**
     * 删除 registry 里当前 gateway 自己的残留入口。
     *
     * gateway 当前进程只负责控制面，registry 里的业务实例列表不应该残留旧的
     * gateway 监听端口映射，否则后续状态恢复可能会把控制面误认成业务实例。
     *
     * @param AppInstanceManager $manager registry 管理器
     * @param string $bindHost gateway 绑定地址
     * @param int $bindPort gateway 业务端口
     * @return void
     */
    protected static function forgetGatewayRegistryEntries(AppInstanceManager $manager, string $bindHost, int $bindPort): void {
        $normalizedBindHost = $bindHost === '0.0.0.0' ? '127.0.0.1' : $bindHost;
        $manager->removeInstance($normalizedBindHost, $bindPort);
        $manager->removeInstance('127.0.0.1', $bindPort);
        $manager->removeInstance('0.0.0.0', $bindPort);
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
        $instancesByEndpoint = [];
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
            $host = '127.0.0.1';
            $rpcPort = self::extractIntFlag($command, 'rport');
            $ownerEpoch = self::extractIntFlag($command, 'gateway_epoch');
            $instanceGatewayPort = self::extractIntFlag($command, 'gateway_port');
            $endpoint = $host . ':' . $port;
            $candidate = [
                'host' => '127.0.0.1',
                'port' => $port,
                'metadata' => [
                    'managed' => true,
                    'pid' => $pid,
                    'master_pid' => $pid,
                    'rpc_port' => $rpcPort,
                    'owner_epoch' => $ownerEpoch,
                    'gateway_port' => $instanceGatewayPort,
                    'command' => $command,
                ],
            ];
            if (!isset($instancesByEndpoint[$endpoint])) {
                $instancesByEndpoint[$endpoint] = $candidate;
                continue;
            }

            // `ps` 会把同一 upstream 的 master/manager/worker 都列出来。
            // 孤儿清理必须按 endpoint 去重，只保留“一条根实例”记录，否则会出现
            // 对同一端口反复 stop，触发“关闭中又拉起”的链式噪音。
            $current = $instancesByEndpoint[$endpoint];
            $currentPid = (int)($current['metadata']['pid'] ?? 0);
            if ($pid > 0 && ($currentPid <= 0 || $pid < $currentPid)) {
                $current['metadata']['pid'] = $pid;
                $current['metadata']['master_pid'] = $pid;
                $current['metadata']['command'] = $command;
            }
            if ((int)($current['metadata']['rpc_port'] ?? 0) <= 0 && $rpcPort > 0) {
                $current['metadata']['rpc_port'] = $rpcPort;
            }
            if ((int)($current['metadata']['owner_epoch'] ?? 0) <= 0 && $ownerEpoch > 0) {
                $current['metadata']['owner_epoch'] = $ownerEpoch;
            }
            if ((int)($current['metadata']['gateway_port'] ?? 0) <= 0 && $instanceGatewayPort > 0) {
                $current['metadata']['gateway_port'] = $instanceGatewayPort;
            }
            $instancesByEndpoint[$endpoint] = $current;
        }
        return array_values($instancesByEndpoint);
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
     * 确保源码入口也能拿到 boot 共用的 bootstrap 工具链。
     *
     * `src/Server/Proxy/run.php` 这类调试入口可能绕过 boot 直接调用本类，
     * 因此这里按开发目录 / composer 包目录两种结构补齐 bootstrap 函数定义，
     * 但只引入 Command 启动工具链，不提前装载其它框架模块。
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

        throw new RuntimeException(
            'SCF bootstrap 工具链不存在, 已检查: ' . implode(', ', $candidates)
        );
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
