<?php
declare(strict_types=1);

use Swoole\Process;

if (version_compare(PHP_VERSION, '8.1.0', '<')) {
    fwrite(STDERR, '运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION . PHP_EOL);
    exit(1);
}

const SCF_ROOT = __DIR__;
defined('BUILD_PATH') || define('BUILD_PATH', dirname(SCF_ROOT) . '/build/');
defined('SCF_APPS_ROOT') || define('SCF_APPS_ROOT', dirname(SCF_ROOT) . '/apps');
scf_define_runtime_constants($argv);

$srcPack = scf_try_upgrade($argv, true);
defined('FRAMEWORK_ACTIVE_PACK') || define('FRAMEWORK_ACTIVE_PACK', $srcPack);
defined('FRAMEWORK_ACTIVE_PACK_SIGNATURE') || define('FRAMEWORK_ACTIVE_PACK_SIGNATURE', scf_framework_pack_identity($srcPack));
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

/**
 * 统一在 boot 入口协调 framework 运行包。
 *
 * gateway pack 模式固定读取 `gateway.pack`，upstream/普通 CLI pack 模式
 * 固定读取 `src.pack`。这里会在启动前把 `update.pack`、`src.pack`、
 * `gateway.pack` 整理到一个可预测的状态，避免 gateway 在运行时直接命中
 * 正在被更新的 `src.pack`。
 *
 * @param array $argv 原始启动参数
 * @param bool $boot 是否处于 boot 早期；失败时会直接退出
 * @return string 当前进程应该加载的 framework 包路径
 */
function scf_try_upgrade(array $argv, bool $boot = false): string {
    clearstatcache();
    $lockFile = SCF_ROOT . '/build/update.lock';
    $command = scf_runtime_boot_command($argv);
    $srcPack = SCF_ROOT . '/build/src.pack';
    $gatewayPack = SCF_ROOT . '/build/gateway.pack';

    if (!scf_bool_constant('FRAMEWORK_IS_PHAR')) {
        return $command === 'gateway' ? $gatewayPack : $srcPack;
    }

    $lock = scf_open_lock_file($lockFile);
    if (is_resource($lock)) {
        scf_safe_call(static fn() => flock($lock, LOCK_EX));
    }

    $error = null;
    if ($command === 'gateway') {
        $activePack = scf_prepare_gateway_framework_pack($error);
    } else {
        $activePack = scf_prepare_standard_framework_pack($error);
    }
    scf_release_lock($lock);

    if ($activePack === '' || !file_exists($activePack)) {
        if ($error) {
            scf_stderr($error);
        }
        scf_stderr('内核文件不存在');
        exit(3);
    }

    return $activePack;
}

/**
 * 返回当前 boot 的命令类型。
 *
 * `gateway` 需要固定读 `gateway.pack`；`gateway_upstream` 和其它 pack CLI
 * 统一固定读 `src.pack`。
 *
 * @param array $argv 原始 CLI 参数
 * @return string
 */
function scf_runtime_boot_command(array $argv): string {
    $command = trim((string)($argv[1] ?? ''));
    if ($command === 'gateway') {
        return 'gateway';
    }
    if ($command === 'gateway_upstream') {
        return 'gateway_upstream';
    }
    return 'default';
}

/**
 * gateway 运行包协调逻辑。
 *
 * gateway 永远固定读 `gateway.pack`。当 `update.pack` 存在时，gateway
 * 启动会把这份新包同时发布到 `src.pack` 和 `gateway.pack`，确保随后拉起的
 * upstream 直接看到最新 `src.pack`，而 gateway 自己则固定使用独立的
 * `gateway.pack`。只有在没有 `update.pack` 时，才会在 `src.pack` 与
 * `gateway.pack` 之间比较新旧并补齐较旧的一份。
 *
 * @param string|null $error 错误信息输出参数
 * @return string
 */
function scf_prepare_gateway_framework_pack(?string &$error = null): string {
    $updatePack = SCF_ROOT . '/build/update.pack';
    $srcPack = SCF_ROOT . '/build/src.pack';
    $gatewayPack = SCF_ROOT . '/build/gateway.pack';

    // gateway 启动命中 update.pack 时，要把这次更新同时发布给 src/gateway 两个固定包名。
    // 这样新 upstream 读取 src.pack 时就是最新包，而 gateway 仍然使用自己的独立副本。
    if (is_file($updatePack)) {
        if (!scf_atomic_copy($updatePack, $gatewayPack, $error)) {
            return '';
        }
        if (!scf_atomic_replace($updatePack, $srcPack, $error)) {
            return '';
        }
    } elseif (is_file($srcPack) && !is_file($gatewayPack)) {
        // 没有 update 时若 gateway.pack 缺失，则从当前 src.pack 补一份出来，
        // 保证 gateway 继续使用独立包而不是直接引用 src.pack。
        if (!scf_atomic_copy($srcPack, $gatewayPack, $error)) {
            return '';
        }
    }

    if (!is_file($updatePack)) {
        $latestPack = scf_pick_latest_framework_pack($srcPack, $gatewayPack);
        if ($latestPack === '') {
            $error = 'framework 包不存在';
            return '';
        }

        // 当 upstream 先更新了 src.pack 而 gateway 还留在旧包时，等下一次
        // gateway 重启再用“谁更新就复制谁”的规则把两边收敛到同一版本。
        if ($latestPack !== $srcPack && !scf_atomic_copy($latestPack, $srcPack, $error)) {
            return '';
        }
        if ($latestPack !== $gatewayPack && !scf_atomic_copy($latestPack, $gatewayPack, $error)) {
            return '';
        }
    }

    clearstatcache();
    if (is_file($srcPack) && is_file($gatewayPack)) {
        scf_stdout('Gateway framework pack 已同步');
    } elseif (file_exists($updatePack) === false && is_file($gatewayPack)) {
        scf_stdout('Gateway framework pack 已就绪');
    }

    return $gatewayPack;
}

/**
 * upstream/普通 pack CLI 运行包协调逻辑。
 *
 * upstream 固定读 `src.pack`。它会优先把 `update.pack` 原子替换成 `src.pack`，
 * 但绝不会去改 `gateway.pack`，避免正在运行中的 gateway 被新的 framework 包
 * 直接顶掉。若没有 update，再在 `src.pack` 与 `gateway.pack` 之间选择较新的一份
 * 同步回 `src.pack`。
 *
 * @param string|null $error 错误信息输出参数
 * @return string
 */
function scf_prepare_standard_framework_pack(?string &$error = null): string {
    $updatePack = SCF_ROOT . '/build/update.pack';
    $srcPack = SCF_ROOT . '/build/src.pack';
    $gatewayPack = SCF_ROOT . '/build/gateway.pack';

    if (file_exists($updatePack) && !scf_atomic_replace($updatePack, $srcPack, $error)) {
        return '';
    }

    $latestPack = scf_pick_latest_framework_pack($srcPack, $gatewayPack);
    if ($latestPack === '') {
        $error = 'framework 包不存在';
        return '';
    }

    if ($latestPack !== $srcPack && !scf_atomic_copy($latestPack, $srcPack, $error)) {
        return '';
    }

    return $srcPack;
}

/**
 * 在 `src.pack` 和 `gateway.pack` 中选出较新的一份。
 *
 * @param string $srcPack `src.pack` 路径
 * @param string $gatewayPack `gateway.pack` 路径
 * @return string
 */
function scf_pick_latest_framework_pack(string $srcPack, string $gatewayPack): string {
    $srcExists = is_file($srcPack);
    $gatewayExists = is_file($gatewayPack);
    if ($srcExists && !$gatewayExists) {
        return $srcPack;
    }
    if (!$srcExists && $gatewayExists) {
        return $gatewayPack;
    }
    if (!$srcExists && !$gatewayExists) {
        return '';
    }

    return scf_compare_framework_pack($srcPack, $gatewayPack) >= 0 ? $srcPack : $gatewayPack;
}

/**
 * 比较两个 framework 包的新旧。
 *
 * 优先比较包内 `version.php` 的版本号；版本相同则比较 build 时间，
 * 最后再回退到文件修改时间。
 *
 * @param string $left 左侧包路径
 * @param string $right 右侧包路径
 * @return int
 */
function scf_compare_framework_pack(string $left, string $right): int {
    $leftInfo = scf_read_framework_pack_version($left);
    $rightInfo = scf_read_framework_pack_version($right);

    $leftVersion = trim((string)($leftInfo['version'] ?? ''));
    $rightVersion = trim((string)($rightInfo['version'] ?? ''));
    if ($leftVersion !== '' && $rightVersion !== '') {
        $versionCompare = version_compare($leftVersion, $rightVersion);
        if ($versionCompare !== 0) {
            return $versionCompare;
        }
    }

    $leftBuild = trim((string)($leftInfo['build'] ?? ''));
    $rightBuild = trim((string)($rightInfo['build'] ?? ''));
    if ($leftBuild !== '' && $rightBuild !== '' && $leftBuild !== $rightBuild) {
        return strcmp($leftBuild, $rightBuild);
    }

    return (filemtime($left) ?: 0) <=> (filemtime($right) ?: 0);
}

/**
 * 读取 framework 包内的版本元数据。
 *
 * @param string $pack framework 包路径
 * @return array<string, mixed>|null
 */
function scf_read_framework_pack_version(string $pack): ?array {
    if (!is_file($pack)) {
        return null;
    }

    $versionFile = 'phar://' . $pack . '/version.php';
    if (!@file_exists($versionFile)) {
        return null;
    }

    $data = @require $versionFile;
    return is_array($data) ? $data : null;
}

/**
 * 计算 framework 包身份签名。
 *
 * @param string $pack framework 包路径
 * @return string
 */
function scf_framework_pack_identity(string $pack): string {
    if (!is_file($pack)) {
        return '';
    }

    $version = scf_read_framework_pack_version($pack);
    if (is_array($version) && $version) {
        return trim((string)($version['version'] ?? '')) . '@' . trim((string)($version['build'] ?? ''));
    }

    return (string)(filesize($pack) ?: 0) . '@' . (string)(filemtime($pack) ?: 0);
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
        $nextPack = defined('FRAMEWORK_ACTIVE_PACK') ? FRAMEWORK_ACTIVE_PACK : scf_try_upgrade($argv);
        try {
            $nextPack = scf_try_upgrade($argv);
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
 * 固定文件名方案下，`gateway.pack/src.pack` 可能在原路径上被换新内容。
 * 这时即便路径不变，常驻 boot 进程也必须整体 re-exec，避免继续 fork 出
 * 带着旧类定义的 child。
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

/**
 * 通过临时文件原子复制 framework 包。
 *
 * @param string $from 源文件
 * @param string $to 目标文件
 * @param string|null $error 错误信息输出参数
 * @return bool
 */
function scf_atomic_copy(string $from, string $to, ?string &$error = null): bool {
    $error = null;
    if (!is_file($from)) {
        $error = '源文件不存在:' . $from;
        return false;
    }

    $fromReal = realpath($from) ?: $from;
    $toReal = realpath($to) ?: $to;
    if ($fromReal === $toReal) {
        return true;
    }

    $dir = dirname($to);
    if (!is_dir($dir)) {
        $error = '目标目录不存在:' . $dir;
        return false;
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
