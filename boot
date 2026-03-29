<?php
declare(strict_types=1);

use Swoole\Process;

if (version_compare(PHP_VERSION, '8.1.0', '<')) {
    fwrite(STDERR, '运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION . PHP_EOL);
    exit(1);
}

const SCF_ROOT = __DIR__;
require_once SCF_ROOT . '/src/Util/FrameworkPackLocator.php';
defined('BUILD_PATH') || define('BUILD_PATH', dirname(SCF_ROOT) . '/build/');
defined('SCF_APPS_ROOT') || define('SCF_APPS_ROOT', dirname(SCF_ROOT) . '/apps');
scf_define_runtime_constants($argv);

$srcPack = scf_try_upgrade(true);
defined('FRAMEWORK_ACTIVE_PACK') || define('FRAMEWORK_ACTIVE_PACK', $srcPack);
$frameworkSourceDir = scf_framework_source_dir();

spl_autoload_register(static function (string $class) use ($srcPack, $frameworkSourceDir): void {
    $filePath = scf_resolve_framework_class_file($class, $srcPack, $frameworkSourceDir);
    if ($filePath !== null && file_exists($filePath)) {
        require $filePath;
    }
});

date_default_timezone_set(scf_detect_timezone());
require SCF_ROOT . '/vendor/autoload.php';

scf_bootstrap($argv);

function scf_bootstrap(array $argv): void {
    if (!scf_bool_constant('IS_SERVER_PROCESS_LOOP')) {
        scf_run($argv);
        return;
    }

    scf_run_server_process_loop($argv);
}

function scf_define_runtime_constants(array $argv): void {
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

    $argvEnv = strtolower(trim((string)(scf_option_value($argv, 'env') ?? '')));
    defined('ENV_VARIABLES') || define('ENV_VARIABLES', $envVariables);
    defined('IS_DEV') || define('IS_DEV', $envVariables['app_env'] === 'dev' || $argvEnv === 'dev' || scf_has_arg($argv, '-dev'));
    defined('IS_PACK') || define('IS_PACK', scf_has_arg($argv, '-pack'));
    defined('NO_PACK') || define('NO_PACK', (scf_bool_constant('IS_DEV') && !scf_bool_constant('IS_PACK')) || scf_has_arg($argv, '-nopack'));
    defined('IS_HTTP_SERVER') || define('IS_HTTP_SERVER', scf_has_arg($argv, 'server'));
    defined('IS_GATEWAY_SERVER') || define('IS_GATEWAY_SERVER', scf_has_arg($argv, 'gateway'));
    defined('IS_HTTP_SERVER_START') || define('IS_HTTP_SERVER_START', scf_has_arg($argv, 'start'));
    defined('IS_GATEWAY_SERVER_START') || define('IS_GATEWAY_SERVER_START', scf_bool_constant('IS_GATEWAY_SERVER') && scf_has_arg($argv, 'start'));
    defined('IS_SERVER_PROCESS_LOOP') || define('IS_SERVER_PROCESS_LOOP', scf_bool_constant('IS_HTTP_SERVER') || scf_bool_constant('IS_GATEWAY_SERVER'));
    defined('IS_SERVER_PROCESS_START') || define('IS_SERVER_PROCESS_START', scf_bool_constant('IS_HTTP_SERVER_START') || scf_bool_constant('IS_GATEWAY_SERVER_START'));
    defined('RUNNING_BUILD') || define('RUNNING_BUILD', scf_has_arg($argv, 'build'));
    defined('RUNNING_INSTALL') || define('RUNNING_INSTALL', scf_has_arg($argv, 'install'));
    defined('RUNNING_TOOLBOX') || define('RUNNING_TOOLBOX', scf_has_arg($argv, 'toolbox'));
    defined('RUNNING_BUILD_FRAMEWORK') || define('RUNNING_BUILD_FRAMEWORK', scf_has_arg($argv, 'framework'));
    defined('RUNNING_CREATE_AR') || define('RUNNING_CREATE_AR', scf_bool_constant('RUNNING_TOOLBOX') && scf_has_arg($argv, 'ar'));
    defined('FRAMEWORK_IS_PHAR') || define('FRAMEWORK_IS_PHAR', scf_bool_constant('IS_PACK') || (!scf_bool_constant('IS_DEV') && !scf_bool_constant('NO_PACK') && !scf_bool_constant('RUNNING_BUILD') && !scf_bool_constant('RUNNING_INSTALL') && !scf_bool_constant('RUNNING_BUILD_FRAMEWORK')));
}

function scf_try_upgrade(bool $boot = false): string {
    clearstatcache();

    $updatePack = SCF_ROOT . '/build/update.pack';
    $lockFile = SCF_ROOT . '/build/update.lock';
    $srcPack = scf_current_framework_pack_path();

    if (!scf_bool_constant('FRAMEWORK_IS_PHAR')) {
        return $srcPack;
    }

    $lock = scf_open_lock_file($lockFile);
    if (is_resource($lock)) {
        scf_safe_call(static fn() => flock($lock, LOCK_EX));
    }

    if (file_exists($updatePack)) {
        $error = null;
        $targetPack = scf_pending_framework_pack_path();
        if (!scf_atomic_replace($updatePack, $targetPack, $error)) {
            scf_stderr('写入更新文件失败!' . ($error ? ' ' . $error : ''));
            scf_release_lock($lock);
            if ($boot) {
                exit(2);
            }
        } else {
            scf_stdout('框架源码包已更新');
            clearstatcache();
            $srcPack = scf_current_framework_pack_path();
        }
    }

    scf_release_lock($lock);

    if (!file_exists($srcPack)) {
        scf_stderr('内核文件不存在');
        exit(3);
    }

    return $srcPack;
}

/**
 * 读取 framework 的本地版本记录。
 *
 * 应用 phar 模式是通过本地版本信息决定“当前应该挂载哪个版本包”；
 * framework 这里也沿用同样思路，让 boot 不再依赖固定 `src.pack` 文件名。
 *
 * @return array<string, mixed>|null
 */
function scf_read_framework_version_info(): ?array {
    return \Scf\Util\FrameworkPackLocator::readVersionInfo(SCF_ROOT);
}

/**
 * 根据 framework 版本号解析目标包路径。
 *
 * 线上升级后的 framework 包按版本号落在 `build/framework/<version>.update`，
 * boot 启动时只需要根据本地版本记录选中对应包即可。若版本记录缺失，
 * 再回退到历史兼容用的 `build/src.pack`。
 *
 * @param string|null $version framework 版本号
 * @return string
 */
function scf_framework_pack_path(?string $version = null): string {
    return \Scf\Util\FrameworkPackLocator::packPath(SCF_ROOT, $version);
}

/**
 * 返回当前 boot 应该加载的 framework 包路径。
 *
 * @return string
 */
function scf_current_framework_pack_path(): string {
    return \Scf\Util\FrameworkPackLocator::currentPackPath(SCF_ROOT);
}

/**
 * 返回待生效 framework 更新包的落地路径。
 *
 * @return string
 */
function scf_pending_framework_pack_path(): string {
    return \Scf\Util\FrameworkPackLocator::pendingPackPath(SCF_ROOT);
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

function scf_run_server_process_loop(array $argv): void {
    $stopFlag = scf_process_control_flag_path($argv, 'stop');
    if ($stopFlag !== '' && file_exists($stopFlag)) {
        @unlink($stopFlag);
    }
    while (true) {
        $managerProcess = new Process(static function () use ($argv): void {
            $serverBuildVersion = require Scf\Root::dir() . '/version.php';
            define('FRAMEWORK_BUILD_TIME', $serverBuildVersion['build']);
            define('FRAMEWORK_BUILD_VERSION', $serverBuildVersion['version']);
            scf_run($argv);
        });
        $managerProcess->start();

        $ret = Process::wait();
        if ($ret) {
            scf_stdout("[manager] child exit pid={$ret['pid']} code={$ret['code']} signal={$ret['signal']}");
        }
        if (scf_should_stop_server_process_loop($argv)) {
            break;
        }
        if (!scf_bool_constant('IS_SERVER_PROCESS_START')) {
            break;
        }
        $nextPack = defined('FRAMEWORK_ACTIVE_PACK') ? FRAMEWORK_ACTIVE_PACK : scf_current_framework_pack_path();
        try {
            $nextPack = scf_try_upgrade();
        } catch (\Throwable $e) {
            scf_stderr("[manager] update failed: {$e->getMessage()}");
        }
        scf_wait_command_ports_released($argv);
        // server loop 本身是在当前 boot 进程里常驻的；如果 framework 包发生切换，
        // 继续在这个旧进程里 fork 新 child，会把旧 autoload/Root::dir() 一起继承下去。
        // 这里必须整体 re-exec boot，让下一轮 manager 从最新包重新启动。
        if (scf_should_reexec_server_process_loop($nextPack)) {
            scf_reexec_current_boot($argv);
            return;
        }
        sleep(2);
    }
}

/**
 * 判断 server loop 是否需要整体重启 boot 进程。
 *
 * 只要当前运行中的 framework 包路径与本地版本记录选中的目标包不同，
 * 就说明“仅重启 child”还不够，需要把常驻 manager 自身也切到新包。
 *
 * @param string $targetPack 升级检查后应当生效的 framework 包路径
 * @return bool
 */
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
    return $currentRealPath !== $targetRealPath;
}

/**
 * 用当前 CLI 参数整体替换 boot 进程。
 *
 * @param array $argv 原始启动参数
 * @return never
 */
function scf_reexec_current_boot(array $argv): never {
    scf_stdout('[manager] framework pack changed, re-exec boot process');
    if (!function_exists('pcntl_exec')) {
        scf_stderr('[manager] pcntl_exec unavailable, cannot reload framework pack in-process');
        exit(4);
    }

    pcntl_exec(PHP_BINARY, $argv);

    $error = function_exists('pcntl_get_last_error') ? pcntl_strerror(pcntl_get_last_error()) : 'unknown';
    scf_stderr('[manager] re-exec failed: ' . $error);
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

function scf_parse_opts(array $argv): array {
    $opts = [];
    foreach ($argv as $arg) {
        if (!is_string($arg) || !str_starts_with($arg, '-')) {
            continue;
        }
        $option = ltrim($arg, '-');
        $value = null;
        if (str_contains($option, '=')) {
            [$option, $value] = explode('=', $option, 2);
        }
        if ($option !== '') {
            $opts[$option] = $value;
        }
    }
    return $opts;
}

function scf_wait_command_ports_released(array $argv, int $timeoutSeconds = 20, int $intervalMs = 200): void {
    $ports = scf_command_listen_ports($argv);
    if (!$ports) {
        return;
    }

    $deadline = microtime(true) + max(1, $timeoutSeconds);
    while (microtime(true) < $deadline) {
        $occupied = false;
        foreach ($ports as $port) {
            if ($port <= 0) {
                continue;
            }
            if (\Scf\Core\Server::isListeningPortInUse($port)) {
                $occupied = true;
                break;
            }
        }
        if (!$occupied) {
            return;
        }
        usleep(max(50, $intervalMs) * 1000);
    }
}

function scf_command_listen_ports(array $argv): array {
    $command = $argv[1] ?? '';
    $opts = scf_parse_opts($argv);
    $ports = [];

    if ($command === 'gateway') {
        $ports[] = (int)($opts['port'] ?? 9580);
        $rpcPort = (int)($opts['rpc_port'] ?? ($opts['rport'] ?? 0));
        if ($rpcPort > 0) {
            $ports[] = $rpcPort;
        }
    } elseif ($command === 'server') {
        $ports[] = (int)($opts['port'] ?? 9580);
        $rpcPort = (int)($opts['rport'] ?? 0);
        if ($rpcPort > 0) {
            $ports[] = $rpcPort;
        }
    }

    return array_values(array_unique(array_filter(array_map('intval', $ports), static fn(int $port) => $port > 0)));
}

function scf_framework_source_dir(): string {
    $sourceDir = SCF_ROOT . '/src/';
    if (is_dir($sourceDir)) {
        return $sourceDir;
    }

    return SCF_ROOT . '/vendor/lhai/scf/src/';
}

function scf_resolve_framework_class_file(string $class, string $srcPack, string $frameworkSourceDir): ?string {
    if (!str_starts_with($class, 'Scf\\')) {
        return null;
    }

    $relativePath = str_replace('\\', '/', substr($class, 4)) . '.php';
    if (!scf_bool_constant('FRAMEWORK_IS_PHAR') || scf_should_load_from_source($class)) {
        return $frameworkSourceDir . $relativePath;
    }

    return 'phar://' . $srcPack . '/' . $relativePath;
}

function scf_should_load_from_source(string $class): bool {
    return in_array($class, ['Scf\\Command\\Caller', 'Scf\\Command\\Runner'], true);
}

function scf_atomic_replace(string $from, string $to, ?string &$error = null): bool {
    $dir = dirname($to);
    if (!is_dir($dir)) {
        $error = '目标目录不存在:' . $dir;
        return false;
    }

    if (scf_safe_call(static fn() => rename($from, $to), $error)) {
        return true;
    }

    $tmp = $dir . '/.' . basename($to) . '.tmp.' . getmypid();
    if (!scf_safe_call(static fn() => copy($from, $tmp), $error)) {
        return false;
    }

    scf_safe_call(static fn() => chmod($tmp, 0644));
    if (!scf_safe_call(static fn() => rename($tmp, $to), $error)) {
        scf_safe_call(static fn() => unlink($tmp));
        return false;
    }

    scf_safe_call(static fn() => unlink($from));
    return true;
}

function scf_open_lock_file(string $lockFile) {
    return scf_safe_call(static fn() => fopen($lockFile, 'c'));
}

function scf_release_lock($lock): void {
    if (!is_resource($lock)) {
        return;
    }

    scf_safe_call(static fn() => flock($lock, LOCK_UN));
    scf_safe_call(static fn() => fclose($lock));
}

function scf_safe_call(callable $callback, ?string &$error = null): mixed {
    $error = null;
    set_error_handler(static function (int $severity, string $message) use (&$error): bool {
        $error = $message;
        return true;
    });

    try {
        return $callback();
    } finally {
        restore_error_handler();
    }
}

function scf_stdout(string $message): void {
    fwrite(STDOUT, $message . PHP_EOL);
}

function scf_stderr(string $message): void {
    fwrite(STDERR, $message . PHP_EOL);
}

function scf_detect_timezone(): string {
    $candidates = [
        getenv('TZ') ?: null,
        ini_get('date.timezone') ?: null,
    ];

    foreach ($candidates as $timezone) {
        if (is_string($timezone) && $timezone !== '' && in_array($timezone, timezone_identifiers_list(), true)) {
            return $timezone;
        }
    }

    return 'Asia/Shanghai';
}

function scf_has_arg(array $argv, string $needle): bool {
    return in_array($needle, $argv, true);
}

function scf_option_value(array $argv, string $name): ?string {
    foreach ($argv as $index => $arg) {
        if (!is_string($arg) || !str_starts_with($arg, '-')) {
            continue;
        }
        $option = ltrim($arg, '-');
        if (str_contains($option, '=')) {
            [$optionName, $value] = explode('=', $option, 2);
            if ($optionName === $name) {
                return $value;
            }
            continue;
        }
        if ($option === $name) {
            $next = $argv[$index + 1] ?? null;
            if (is_string($next) && !str_starts_with($next, '-')) {
                return $next;
            }
            return null;
        }
    }
    return null;
}

function scf_bool_constant(string $name): bool {
    return defined($name) && (bool)constant($name);
}
