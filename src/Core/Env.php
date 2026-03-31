<?php

namespace Scf\Core;

use Scf\Command\Manager;
use Scf\Util\File;
use Scf\Util\Random;

class Env {

    protected static function isProxyUpstreamMode(): bool {
        return defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true;
    }

    protected static function buildServerFileBase(string $path, string $role, array $options): string {
        $base = dirname(SCF_ROOT) . '/var/' . $path;
        if (!self::isProxyUpstreamMode()) {
            return $base . '_' . $role;
        }

        $port = (int)($options['port'] ?? 0);
        $instance = $port > 0 ? 'gateway_upstream_' . $role . '_' . $port : 'gateway_upstream_' . $role;
        return $base . '_' . $instance;
    }

    protected static function buildNodeId(string $role, object $app, array $options): string {
        if (!self::isProxyUpstreamMode()) {
            return strtolower($role) . '-' . $app->node_id;
        }

        $port = (int)($options['port'] ?? 0);
        if ($port > 0) {
            return strtolower($role) . '-upstream-' . $port;
        }

        return strtolower($role) . '-upstream-' . $app->node_id;
    }

    /**
     * 初始化
     * @param string $mode
     * @return void
     */
    public static function initialize(string $mode = MODE_CLI): void {
        $commandManager = Manager::instance();
        $options = $commandManager->getOpts();
        !defined('OS_ENV') and define('OS_ENV', file_exists('/.dockerenv') ? 'docker' : PHP_OS);
        !defined('ENV_MODE') and define('ENV_MODE', $mode);
        if (RUNNING_INSTALL) {
            return;
        }
        $path = $options['app'] ?? (ENV_VARIABLES['app_dir'] ?: 'app');
        if (!$path && App::all()) {
            $path = Console::select(array_column(App::all(), 'app_path'), start: 0, label: '请选择应用');
        }
        $role = $commandManager->issetOpt('master') ? NODE_ROLE_MASTER : ($options['role'] ?? (ENV_VARIABLES['server_role'] ?: NODE_ROLE_MASTER));
        !defined('SERVER_ROLE') and define('SERVER_ROLE', $role);
        !defined('SERVER_PORT') and define('SERVER_PORT', $options['port'] ?? ($mode == MODE_NATIVE ? 9501 : 0));
        $serverFileBase = self::buildServerFileBase($path, $role, $options);

        !defined('SERVER_MASTER_PID_FILE') and define('SERVER_MASTER_PID_FILE', $serverFileBase . '.pid');
        !defined('SERVER_MASTER_DB_PID_FILE') and define('SERVER_MASTER_DB_PID_FILE', $serverFileBase . '_master_db.pid');
        !defined('SERVER_RPC_PID_FILE') and define('SERVER_RPC_PID_FILE', $serverFileBase . '_rpc.pid');
        !defined('SERVER_DASHBOARD_PID_FILE') and define('SERVER_DASHBOARD_PID_FILE', $serverFileBase . '_dashboard.pid');
        !defined('SERVER_DASHBOARD_PORT_FILE') and define('SERVER_DASHBOARD_PORT_FILE', $serverFileBase . '_dashboard_port');
        !defined('SERVER_QUEUE_MANAGER_PID_FILE') and define('SERVER_QUEUE_MANAGER_PID_FILE', $serverFileBase . '_queue_manager.pid');
        !defined('SERVER_CRONTAB_MANAGER_PID_FILE') and define('SERVER_CRONTAB_MANAGER_PID_FILE', $serverFileBase . '_crontab_manager.pid');
        //是否开启定时任务
        !defined('SERVER_CRONTAB_ENABLE') and define('SERVER_CRONTAB_ENABLE', $options['crontab'] ?? SWITCH_ON);
        //是否开启日志机器人推送
        !defined('SERVER_LOG_REPORT') and define('SERVER_LOG_REPORT', $options['report'] ?? SWITCH_ON);
        !defined('SERVER_HOST') and define('SERVER_HOST', $options['host'] ?? Env::getIntranetIp());
        !defined('SERVER_HOST_IS_IP') and define('SERVER_HOST_IS_IP', filter_var(SERVER_HOST, FILTER_VALIDATE_IP) !== false);
        !defined('SERVER_APP_FINGERPRINT_FILE') and define('SERVER_APP_FINGERPRINT_FILE', $serverFileBase . '.fingerprint');
        if (RUNNING_BUILD || RUNNING_BUILD_FRAMEWORK || RUNNING_CREATE_AR) {
            $runEnv = 'dev';
        } else {
            $runEnv = $commandManager->issetOpt('dev') ? 'dev' : ($options['env'] ?? (ENV_VARIABLES['app_env'] ?: 'production'));
        }
        !defined('SERVER_RUN_ENV') and define('SERVER_RUN_ENV', $runEnv);
        !defined('MASTER_PORT') and define('MASTER_PORT', $options['mport'] ?? 0);//自定义主节点端口
        !defined('MDB_PORT') and define('MDB_PORT', $options['mdbport'] ?? 16379);
        !defined('RPC_PORT') and define('RPC_PORT', $options['rport'] ?? 0);
        !defined('PRINT_MYSQL_LOG') and define('PRINT_MYSQL_LOG', $commandManager->issetOpt('show_mysql_logs'));
        !defined('PRINT_REDIS_LOG') and define('PRINT_REDIS_LOG', $commandManager->issetOpt('show_redis_logs'));
        //应用硬件指纹
        if (!file_exists(SERVER_APP_FINGERPRINT_FILE) && $mode != MODE_NATIVE) {
            $fingerprint = Random::makeUUIDV4();
            File::write(SERVER_APP_FINGERPRINT_FILE, $fingerprint);
        } else {
            $fingerprint = File::read(SERVER_APP_FINGERPRINT_FILE);
        }
        !defined('APP_PATH') and define('APP_PATH', SCF_APPS_ROOT . '/' . $path);
        !defined('APP_DIR_NAME') and define('APP_DIR_NAME', $path);
        if ($commandManager->issetOpt('phar')) {
            $appSrc = 'phar';
        } elseif ($commandManager->issetOpt('dir')) {
            $appSrc = 'dir';
        } else {
            $appSrc = ENV_VARIABLES['app_src'] ?: ($options['src'] ?? (SERVER_RUN_ENV == 'production' ? 'phar' : (is_dir(SCF_APPS_ROOT . '/' . $path . '/' . 'src/lib') ? 'dir' : 'phar')));
        }
        !defined('APP_SRC_TYPE') and define('APP_SRC_TYPE', $appSrc);
        //应用参数
        $app = App::appoint($path);
        $app->role = $role;
        !defined('APP_PUBLIC_PATH') and define('APP_PUBLIC_PATH', APP_PATH . '/' . $app->public_path);
        !defined('APP_AUTH_KEY') and define('APP_AUTH_KEY', $app->app_auth_key);
        //APP DB
        !defined('APP_RUNTIME_DB') and define('APP_RUNTIME_DB', APP_PATH . '/db/redis');
        //静态文件路径
        !defined('APP_ASSET_PATH') and define('APP_ASSET_PATH', APP_PUBLIC_PATH . 'asset');
        //临时文件目录路径
        !defined('APP_TMP_PATH') and define('APP_TMP_PATH', APP_PATH . '/tmp');
        //日志文件目录路径
        !defined('APP_LOG_PATH') and define('APP_LOG_PATH', APP_PATH . '/log');
        //文件更新目录
        !defined('APP_UPDATE_DIR') and define('APP_UPDATE_DIR', APP_PATH . '/update');
        !defined('APP_BIN_DIR') and define('APP_BIN_DIR', APP_PATH . '/bin');
        //是否启用自动更新
        !defined('APP_AUTO_UPDATE') and define('APP_AUTO_UPDATE', $options['update'] ?? STATUS_OFF);
        !defined('APP_FINGERPRINT') and define('APP_FINGERPRINT', $fingerprint);
        !defined('APP_NODE_ID') and define('APP_NODE_ID', self::buildNodeId($role, $app, $options));

        if (!file_exists(APP_TMP_PATH) && $mode != MODE_NATIVE) {
            mkdir(APP_TMP_PATH, 0775, true);
            mkdir(APP_TMP_PATH . '/template', 0775, true);
        }
        if (!file_exists(dirname(SCF_ROOT) . '/var') && $mode != MODE_NATIVE) {
            mkdir(dirname(SCF_ROOT) . '/var', 0775, true);
        }
        if (!file_exists(APP_LOG_PATH) && $mode != MODE_NATIVE) {
            mkdir(APP_LOG_PATH, 0775, true);
        }
        if (!file_exists(APP_PATH . '/db/updates') && $mode != MODE_NATIVE) {
            mkdir(APP_PATH . '/db/updates', 0775, true);
        }
        if (!file_exists(APP_UPDATE_DIR) && $mode != MODE_NATIVE) {
            mkdir(APP_UPDATE_DIR, 0775, true);
        }
        if (!file_exists(APP_PUBLIC_PATH) && $mode != MODE_NATIVE) {
            mkdir(APP_PUBLIC_PATH, 0775, true);
        }
        if (!file_exists(APP_BIN_DIR) && $mode != MODE_NATIVE) {
            mkdir(APP_BIN_DIR, 0775, true);
        }
        // 清理缓存目录
        App::clearTemplateCache();
        if ($mode != MODE_CGI && $mode != MODE_NATIVE) {
            App::mount($mode);
        }
        if (App::isReady()) {
            $serverConfig = Config::server();
            if (!defined('APP_MODULE_STYLE')) {
                define('APP_MODULE_STYLE', $serverConfig['module_style'] ?? APP_MODULE_STYLE_MULTI);
            }
        }
    }

    /**
     * @return bool 是否开发环境
     */
    public static function isDev(): bool {
        return SERVER_RUN_ENV == 'dev';
    }

    /**
     * 判断是否在docker容器中运行
     * @return bool
     */
    public static function inDocker(): bool {
        return strtolower(OS_ENV) == 'docker';
    }

    /**
     * 获取内网IP
     * @return ?string
     */
    public static function getIntranetIp(): ?string {
        $hostIp = trim((string)(ENV_VARIABLES['host_ip'] ?? ''));
        $networkMode = (string)(ENV_VARIABLES['network_mode'] ?? '');

        if (self::inDocker() || $networkMode === NETWORK_MODE_GROUP) {
            if ($hostIp !== '') {
                return $hostIp;
            }

            // Linux cron 子进程运行时若缺少 HOST_IP，不再阻塞式依赖 myip 服务。
            // 这里优先降级为容器内本机地址，保证一次性任务能继续执行。
            if (self::isCronRuntime()) {
                return self::resolveLocalIntranetIp() ?: '127.0.0.1';
            }

            $serverIp = (array)App::getServerIp();
            $intranet = trim((string)($serverIp['intranet'] ?? ''));
            if ($intranet !== '') {
                return $intranet;
            }
        }

        return self::resolveLocalIntranetIp();
    }

    /**
     * 判断当前是否由 Linux 系统 cron 链路触发。
     *
     * @return bool
     */
    protected static function isCronRuntime(): bool {
        $flag = trim((string)(ENV_VARIABLES['from_cron'] ?? getenv('SCF_FROM_CRON') ?: ''));
        return in_array(strtolower($flag), ['1', 'true', 'yes', 'on'], true);
    }

    /**
     * 解析当前进程可见的本机内网地址。
     *
     * @return string|null
     */
    protected static function resolveLocalIntranetIp(): ?string {
        foreach (swoole_get_local_ip() as $ip) {
            $ip = trim((string)$ip);
            if ($ip !== '') {
                return $ip;
            }
        }
        return null;
    }
}
