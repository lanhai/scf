<?php

namespace Scf\Server;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use Scf\Core\App;
use Scf\Command\Manager;
use Scf\Core\Console;
use Scf\Helper\ArrayHelper;
use Scf\Util\File;
use Scf\Util\Random;
use Scf\Util\Sn;
use Swoole\Coroutine\System;
use Swoole\Event;
use function Co\run;

class Core {

    /**
     * 初始化
     * @param string $mode
     * @return void
     */
    public static function initialize(string $mode = MODE_CLI): void {
        !defined('SERVER_ENV') and define('SERVER_ENV', file_exists('/.dockerenv') ? 'docker' : PHP_OS);
        $options = Manager::instance()->getOpts();
        $appEnv = null;
        $appDir = null;
        $serverRole = null;
        $staticHandler = null;
        $runMode = null;
        if (Env::inDocker()) {
            run(function () use (&$appEnv, &$appDir, &$serverRole, &$staticHandler, &$runMode) {
                $appEnv = trim(System::exec('echo ${APP_ENV}')['output']);
                $appDir = trim(System::exec('echo ${APP_DIR}')['output']);
                $serverRole = trim(System::exec('echo ${SERVER_ROLE}')['output']);
                $staticHandler = trim(System::exec('echo ${STATIC_HANDLER}')['output']);
                $runMode = trim(System::exec('echo ${RUN_MODE}')['output']);
            });
            Event::wait();
        }
        $path = $options['app'] ?? ($appDir ?: 'app');
        !defined('APP_RUN_ENV') and define('APP_RUN_ENV', Manager::instance()->issetOpt('dev') ? 'dev' : ($options['env'] ?? ($appEnv ?: 'production')));
        if (App::all() && !isset($options['app']) && !$appDir) {
            $path = Console::select(array_column(App::all(), 'app_path'), start: 0, label: '请选择应用');
        }
        !defined('APP_RUN_MODE') and define('APP_RUN_MODE', $runMode ?: ($options['mode'] ?? (APP_RUN_ENV == 'production' ? 'phar' : (is_dir(SCF_APPS_ROOT . '/' . $path . '/' . 'src/lib') ? 'src' : 'phar'))));
        $app = App::appoint($path);
        $role = Manager::instance()->issetOpt('master') ? 'master' : ($options['role'] ?? ($serverRole ?: ($app->role ?: 'master')));
        if (!$app->role) {
            $app->role = $role;
        }
        if (!$app->node_id || !empty($options['alias'])) {
            $app->node_id = $options['alias'] ?? Sn::create_guid();
        }
        $app->update();
        !defined('SERVER_NAME') and define('SERVER_NAME', $options['alias'] ?? $app->app_path);
        !defined('SERVER_ROLE') and define('SERVER_ROLE', $role);
        !defined('SERVER_MODE') and define('SERVER_MODE', $mode);
        !defined('SERVER_ALIAS') and define('SERVER_ALIAS', $options['alias'] ?? $app->app_path);
        !defined('SERVER_PORT') and define('SERVER_PORT', $options['port'] ?? 9580);
        !defined('MDB_PORT') and define('MDB_PORT', $options['mport'] ?? 16379);
        !defined('SERVER_NODE_ID') and define('SERVER_NODE_ID', strtolower(SERVER_ROLE) . '-' . $app->node_id);
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
        //是否开启静态资源服务
        !defined('SERVER_ENABLE_STATIC_HANDER') and define('SERVER_ENABLE_STATIC_HANDER', $staticHandler ?: Manager::instance()->issetOpt('static'));
        !defined('SERVER_HOST') and define('SERVER_HOST', $options['host'] ?? Env::getIntranetIp());
        !defined('SERVER_HOST_IS_IP') and define('SERVER_HOST_IS_IP', filter_var(SERVER_HOST, FILTER_VALIDATE_IP) !== false);
        !defined('PRINT_MYSQL_LOG') and define('PRINT_MYSQL_LOG', Manager::instance()->issetOpt('print_mysql_logs'));
        !defined('PRINT_REDIS_LOG') and define('PRINT_REDIS_LOG', Manager::instance()->issetOpt('print_redis_logs'));
        !defined('APP_DIR_NAME') and define('APP_DIR_NAME', $path);
        !defined('APP_PATH') and define('APP_PATH', SCF_APPS_ROOT . '/' . $path);

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
        !defined('APP_AUTO_UPDATE') and define('APP_AUTO_UPDATE', $options['update'] ?? 0);

        if (!file_exists(APP_TMP_PATH)) {
            mkdir(APP_TMP_PATH, 0775, true);
            mkdir(APP_TMP_PATH . '/template', 0775, true);
        }
        if (!file_exists(dirname(SCF_ROOT) . '/var')) {
            mkdir(dirname(SCF_ROOT) . '/var', 0775, true);
        }
        if (!file_exists(APP_LOG_PATH)) {
            mkdir(APP_LOG_PATH, 0775, true);
        }
        if (!file_exists(APP_PATH . '/db/updates')) {
            mkdir(APP_PATH . '/db/updates', 0775, true);
        }
        if (!file_exists(APP_UPDATE_DIR)) {
            mkdir(APP_UPDATE_DIR, 0775, true);
        }
        if (!file_exists(APP_PUBLIC_PATH)) {
            mkdir(APP_PUBLIC_PATH, 0775, true);
        }
        if (!file_exists(APP_BIN_DIR)) {
            mkdir(APP_BIN_DIR, 0775, true);
        }
        //应用硬件指纹
        !defined('SERVER_APP_FINGERPRINT_FILE') and define('SERVER_APP_FINGERPRINT_FILE', dirname(SCF_ROOT) . '/var/' . $path . '_' . SERVER_ROLE . '.fingerprint');
        if (!file_exists(SERVER_APP_FINGERPRINT_FILE)) {
            $fingerprint = Random::makeUUIDV4();
            File::write(SERVER_APP_FINGERPRINT_FILE, $fingerprint);
        } else {
            $fingerprint = File::read(SERVER_APP_FINGERPRINT_FILE);
        }
        !defined('APP_FINGERPRINT') and define('APP_FINGERPRINT', $fingerprint);
        // 清理缓存目录
        App::clearTemplateCache();
        if ($mode != MODE_CGI) {
            App::mount($mode);
        }
    }
}