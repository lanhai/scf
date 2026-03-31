<?php
declare(strict_types=1);

/**
 * boot 主运行时函数。
 */

function scf_bootstrap(array $argv): void {
    if (!scf_bool_constant('IS_SERVER_PROCESS_LOOP')) {
        scf_run($argv);
        return;
    }

    scf_run_server_process_loop($argv);
}

/**
 * 定义 boot 以及独立 CLI 入口共享的根路径常量。
 *
 * 这些常量只描述 SCF 根目录、build 目录和 apps 根目录，
 * 既能服务 boot，也能让 `src/Server/Proxy/*.php` 这类源码入口
 * 在不经过 boot 的情况下复用同一套运行时工具链。
 *
 * @param string $root SCF 包根目录
 * @return void
 */
function scf_define_bootstrap_root_constants(string $root): void {
    defined('SCF_ROOT') || define('SCF_ROOT', rtrim($root, '/'));
    defined('BUILD_PATH') || define('BUILD_PATH', dirname(SCF_ROOT) . '/build/');
    defined('SCF_APPS_ROOT') || define('SCF_APPS_ROOT', dirname(SCF_ROOT) . '/apps');
}

/**
 * 定义启动期运行时常量。
 *
 * boot、自举 server loop、gateway 源码入口与 direct upstream 源码入口
 * 都依赖这一组常量来判断当前运行模式。这里统一收敛到 bootstrap 工具链，
 * 避免多个入口各自维护一套稍有偏差的判定规则。
 *
 * @param array $argv 原始 CLI 参数数组
 * @param array<string, mixed> $overrides 需要强制覆盖的常量值
 * @return void
 */
function scf_define_runtime_constants(array $argv, array $overrides = []): void {
    $envVariables = [
        'app_env' => getenv('APP_ENV') ?: '',
        'app_dir' => getenv('APP_DIR') ?: '',
        'app_src' => getenv('APP_SRC') ?: '',
        'server_role' => getenv('SERVER_ROLE') ?: '',
        'static_handler' => getenv('STATIC_HANDLER') ?: '',
        'host_ip' => getenv('HOST_IP') ?: '',
        'from_cron' => getenv('SCF_FROM_CRON') ?: '',
        'os_name' => getenv('OS_NAME') ?: '',
        'network_mode' => getenv('NETWORK_MODE') ?: '',
        'scf_update_server' => getenv('SCF_UPDATE_SERVER') ?: 'https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/version.json',
    ];

    $argvEnv = strtolower(trim((string)(scf_option_value($argv, 'env') ?? '')));
    $isDev = $envVariables['app_env'] === 'dev' || $argvEnv === 'dev' || scf_has_arg($argv, '-dev');
    $isPack = scf_has_arg($argv, '-pack');
    $isHttpServer = scf_has_arg($argv, 'server');
    $isGatewayServer = scf_has_arg($argv, 'gateway');
    $isHttpServerStart = scf_has_arg($argv, 'start');
    $runningBuild = scf_has_arg($argv, 'build');
    $runningInstall = scf_has_arg($argv, 'install');
    $runningToolbox = scf_has_arg($argv, 'toolbox');
    $runningBuildFramework = scf_has_arg($argv, 'framework');
    $noPack = ($isDev && !$isPack) || scf_has_arg($argv, '-nopack');
    $isGatewayServerStart = $isGatewayServer && $isHttpServerStart;
    $isServerProcessLoop = $isHttpServer || $isGatewayServer;
    $isServerProcessStart = $isHttpServerStart || $isGatewayServerStart;
    $runningCreateAr = $runningToolbox && scf_has_arg($argv, 'ar');
    $frameworkIsPhar = $isPack
        || (!$isDev && !$noPack && !$runningBuild && !$runningInstall && !$runningBuildFramework);

    $defaults = [
        'ENV_VARIABLES' => $envVariables,
        'IS_DEV' => $isDev,
        'IS_PACK' => $isPack,
        'NO_PACK' => $noPack,
        'IS_HTTP_SERVER' => $isHttpServer,
        'IS_GATEWAY_SERVER' => $isGatewayServer,
        'IS_HTTP_SERVER_START' => $isHttpServerStart,
        'IS_GATEWAY_SERVER_START' => $isGatewayServerStart,
        'IS_SERVER_PROCESS_LOOP' => $isServerProcessLoop,
        'IS_SERVER_PROCESS_START' => $isServerProcessStart,
        'RUNNING_BUILD' => $runningBuild,
        'RUNNING_INSTALL' => $runningInstall,
        'RUNNING_TOOLBOX' => $runningToolbox,
        'RUNNING_BUILD_FRAMEWORK' => $runningBuildFramework,
        'RUNNING_CREATE_AR' => $runningCreateAr,
        'FRAMEWORK_IS_PHAR' => $frameworkIsPhar,
    ];

    foreach ($overrides as $name => $value) {
        if (array_key_exists($name, $defaults)) {
            $defaults[$name] = $value;
        }
    }

    foreach ($defaults as $name => $value) {
        defined($name) || define($name, $value);
    }
}

function scf_run(array $argv): void {
    $caller = new Scf\Command\Caller();
    $caller->setScript((string)($argv[0] ?? ''));
    $caller->setCommand($argv[1] ?? false);
    $caller->setParams($argv);

    $ret = Scf\Command\Runner::instance()->run($caller);
    if (!empty($ret->getMsg())) {
        scf_stdout($ret->getMsg());
    }
}

/**
 * 注册源码目录下的 framework autoload。
 *
 * 这个能力只服务源码模式入口，例如 `src/Server/Proxy/run.php` 这种
 * 不经过 boot 的调试链路。显式 `-pack` 的正常运行链仍由 boot 自己的
 * pack-aware autoload 接管。
 *
 * @param string|null $frameworkSourceDir 源码目录，可为空时自动解析
 * @return void
 */
function scf_register_source_framework_autoload(?string $frameworkSourceDir = null): void {
    static $registered = false;
    if ($registered) {
        return;
    }

    $resolvedSourceDir = rtrim($frameworkSourceDir ?: scf_framework_source_dir(), '/') . '/';
    spl_autoload_register(static function (string $class) use ($resolvedSourceDir): void {
        if (!str_starts_with($class, 'Scf\\')) {
            return;
        }
        $relative = str_replace('\\', '/', substr($class, 4)) . '.php';
        $path = $resolvedSourceDir . $relative;
        if (is_file($path)) {
            require_once $path;
        }
    });
    $registered = true;
}

/**
 * 解析 CLI 位置参数与 `-key=value` 风格选项。
 *
 * gateway 控制面与 direct upstream 都沿用同一套命令行约定，
 * 这里统一解析，避免不同入口对参数优先级和布尔开关的处理继续漂移。
 *
 * @param array $argv 原始 CLI 参数数组
 * @return array{0: array<int|string, mixed>, 1: array<string, mixed>}
 */
function scf_parse_cli_args(array $argv): array {
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

function scf_run_server_process_loop(array $argv): void {
    $stopFlag = scf_process_control_flag_path($argv, 'stop');
    if ($stopFlag !== '' && file_exists($stopFlag)) {
        @unlink($stopFlag);
    }
    while (true) {
        if (scf_bool_constant('IS_SERVER_PROCESS_START')) {
            // 新实例拉起前先由 bootstrap 层处理旧监听者，避免把端口冲突处理责任
            // 留给可能仍是旧版本的 pack 运行时逻辑。
            scf_prepare_command_ports_for_start($argv);
        }
        $managerProcess = new \Swoole\Process(static function () use ($argv): void {
            $serverBuildVersion = scf_runtime_build_version();
            define('FRAMEWORK_BUILD_TIME', $serverBuildVersion['build']);
            define('FRAMEWORK_BUILD_VERSION', $serverBuildVersion['version']);
            scf_run($argv);
        });
        $managerProcess->start();

        $ret = \Swoole\Process::wait();
        if ($ret) {
            scf_stdout("【Boot】子进程退出: pid={$ret['pid']}, code={$ret['code']}, signal={$ret['signal']}");
        }
        if (scf_should_stop_server_process_loop($argv)) {
            break;
        }
        if (!scf_bool_constant('IS_SERVER_PROCESS_START')) {
            break;
        }
        $nextPack = defined('FRAMEWORK_ACTIVE_PACK') ? FRAMEWORK_ACTIVE_PACK : scf_try_upgrade($argv);
        try {
            $nextPack = scf_try_upgrade($argv);
        } catch (\Throwable $e) {
            scf_stderr("【Boot】更新检查失败: {$e->getMessage()}");
        }
        scf_wait_command_ports_released($argv);
        if (scf_should_reexec_server_process_loop($nextPack)) {
            scf_reexec_current_boot($argv);
            return;
        }
        sleep(2);
    }
}

/**
 * 读取当前运行 framework 的 build/version。
 *
 * server loop 与 gateway/upstream 业务入口都要向外报告一致的 framework 版本。
 * 优先读取 active pack，再回退到源码 `src/version.php`，确保 pack 模式不会
 * 被本地源码版本污染。
 *
 * @param string|null $fallbackVersion 找不到版本信息时的默认版本号
 * @return array{build: string, version: string}
 */
function scf_runtime_build_version(?string $fallbackVersion = null): array {
    $defaultVersion = $fallbackVersion;
    if ($defaultVersion === null || $defaultVersion === '') {
        $defaultVersion = defined('SCF_COMPOSER_VERSION') ? (string)SCF_COMPOSER_VERSION : 'development';
    }
    $buildVersion = ['build' => 'development', 'version' => $defaultVersion];
    $versionFile = SCF_ROOT . '/src/version.php';
    $activePack = defined('FRAMEWORK_ACTIVE_PACK') ? (string)FRAMEWORK_ACTIVE_PACK : '';
    $packVersion = $activePack !== '' ? (scf_read_framework_pack_version($activePack) ?: null) : null;

    if ($packVersion) {
        return [
            'build' => (string)($packVersion['build'] ?? 'development'),
            'version' => (string)($packVersion['version'] ?? 'development'),
        ];
    }

    if (is_file($versionFile)) {
        $data = require $versionFile;
        if (is_array($data)) {
            return [
                'build' => (string)($data['build'] ?? 'development'),
                'version' => (string)($data['version'] ?? 'development'),
            ];
        }
    }

    return $buildVersion;
}

/**
 * 把 framework build/version 定义成全局常量。
 *
 * gateway 与 upstream 在真正启动业务逻辑前都需要把这两个常量固定下来，
 * 便于 dashboard、cluster 元数据和升级状态机读取一致的运行态版本。
 *
 * @param string|null $fallbackVersion 找不到版本信息时的默认版本号
 * @return void
 */
function scf_define_framework_build_constants(?string $fallbackVersion = null): void {
    if (defined('FRAMEWORK_BUILD_TIME') && defined('FRAMEWORK_BUILD_VERSION')) {
        return;
    }

    $buildVersion = scf_runtime_build_version($fallbackVersion);
    defined('FRAMEWORK_BUILD_TIME') || define('FRAMEWORK_BUILD_TIME', (string)($buildVersion['build'] ?? 'development'));
    defined('FRAMEWORK_BUILD_VERSION') || define('FRAMEWORK_BUILD_VERSION', (string)($buildVersion['version'] ?? (defined('SCF_COMPOSER_VERSION') ? SCF_COMPOSER_VERSION : 'development')));
}

function scf_should_reexec_server_process_loop(string $targetPack): bool {
    if (!scf_bool_constant('FRAMEWORK_IS_PHAR') || !defined('FRAMEWORK_ACTIVE_PACK')) {
        return false;
    }

    $currentPack = (string)FRAMEWORK_ACTIVE_PACK;
    if ($currentPack === '' || $targetPack === '') {
        return false;
    }

    $currentRealPath = realpath($currentPack) ?: $currentPack;
    $targetRealPath = realpath($targetPack) ?: $targetPack;
    if ($currentRealPath !== $targetRealPath) {
        return true;
    }

    $currentIdentity = defined('FRAMEWORK_ACTIVE_PACK_SIGNATURE')
        ? (string)FRAMEWORK_ACTIVE_PACK_SIGNATURE
        : scf_framework_pack_identity($currentPack);
    $targetIdentity = scf_framework_pack_identity($targetPack);
    if ($currentIdentity === '' || $targetIdentity === '') {
        return false;
    }

    return $currentIdentity !== $targetIdentity;
}

function scf_reexec_current_boot(array $argv): never {
    scf_stdout('【Boot】检测到 framework 包已变更，准备重新执行 boot 进程');
    if (!function_exists('pcntl_exec')) {
        scf_stderr('【Boot】当前环境不支持 pcntl_exec，无法在进程内重载 framework 包');
        exit(4);
    }

    pcntl_exec(PHP_BINARY, $argv);

    $error = function_exists('pcntl_get_last_error') ? pcntl_strerror(pcntl_get_last_error()) : 'unknown';
    scf_stderr('【Boot】重新执行 boot 进程失败: ' . $error);
    exit(5);
}

function scf_should_stop_server_process_loop(array $argv): bool {
    $flagFile = scf_process_control_flag_path($argv, 'stop');
    if ($flagFile === '') {
        return false;
    }
    if (!file_exists($flagFile)) {
        return false;
    }
    @unlink($flagFile);
    return true;
}

function scf_process_control_flag_path(array $argv, string $action): string {
    $command = $argv[1] ?? '';
    if (!in_array($command, ['server', 'gateway'], true)) {
        return '';
    }

    $opts = scf_parse_opts($argv);
    $app = $opts['app'] ?? (getenv('APP_DIR') ?: 'app');
    $role = $opts['role'] ?? (getenv('SERVER_ROLE') ?: 'master');
    return dirname(SCF_ROOT) . '/var/' . $app . '_' . $command . '_' . $role . '.' . $action;
}
