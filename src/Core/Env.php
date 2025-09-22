<?php

namespace Scf\Core;

use Scf\Command\Manager;
use Scf\Util\File;
use Scf\Util\Random;

class Env {

    /**
     * 初始化
     * @param string $mode
     * @return void
     */
    public static function initialize(string $mode = MODE_CLI): void {
        $commandManager = Manager::instance();
        $options = $commandManager->getOpts();
        !defined('SCF_APPS_ROOT') and define("SCF_APPS_ROOT", ($options['apps'] ?? dirname(SCF_ROOT)) . '/apps');
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
        !defined('SERVER_PORT_FILE') and define('SERVER_PORT_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_port');
        !defined('SERVER_MASTER_PID_FILE') and define('SERVER_MASTER_PID_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '.pid');
        !defined('SERVER_MANAGER_PID_FILE') and define('SERVER_MANAGER_PID_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_manager.pid');
        !defined('SERVER_MASTER_DB_PID_FILE') and define('SERVER_MASTER_DB_PID_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '_master_db.pid');
        !defined('SERVER_RPC_PID_FILE') and define('SERVER_RPC_PID_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '_rpc.pid');
        !defined('SERVER_DASHBOARD_PID_FILE') and define('SERVER_DASHBOARD_PID_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '_dashboard.pid');
        !defined('SERVER_QUEUE_MANAGER_PID_FILE') and define('SERVER_QUEUE_MANAGER_PID_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '_queue_manager.pid');
        !defined('SERVER_CRONTAB_MANAGER_PID_FILE') and define('SERVER_CRONTAB_MANAGER_PID_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '_crontab_manager.pid');
        //是否开启定时任务
        !defined('SERVER_CRONTAB_ENABLE') and define('SERVER_CRONTAB_ENABLE', $options['crontab'] ?? SWITCH_ON);
        //是否开启日志机器人推送
        !defined('SERVER_LOG_REPORT') and define('SERVER_LOG_REPORT', $options['report'] ?? SWITCH_ON);
        !defined('SERVER_HOST') and define('SERVER_HOST', $options['host'] ?? Env::getIntranetIp());
        !defined('SERVER_HOST_IS_IP') and define('SERVER_HOST_IS_IP', filter_var(SERVER_HOST, FILTER_VALIDATE_IP) !== false);
        !defined('SERVER_APP_FINGERPRINT_FILE') and define('SERVER_APP_FINGERPRINT_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '.fingerprint');
        if (RUNNING_BUILD || RUNNING_PACKAGE || RUNNING_CREATE_AR) {
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
        !defined('APP_NODE_ID') and define('APP_NODE_ID', strtolower(SERVER_ROLE) . '-' . $app->node_id);

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
            define('APP_MODULE_STYLE', $serverConfig['module_style'] ?? APP_MODULE_STYLE_LARGE);
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
        $intranetIp = null;
        foreach (swoole_get_local_ip() as $ip) {
            $intranetIp = $ip;
            break;
        }
        if (!self::inDocker()) {
            return $intranetIp;
        }
        if (ENV_VARIABLES['net_work_mode'] == NETWORK_MODE_GROUP) {
            $intranetIp = ENV_VARIABLES['host_ip'] ?: App::getServerIp()['intranet'];
        }
        return $intranetIp;
    }
}