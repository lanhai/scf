<?php

namespace Scf\Server;

use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Env;

class Core {

    /**
     * 初始化
     * @param string $mode
     * @return void
     */
    public static function initialize(string $mode = 'cli') {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $apps = App::all();
        if (!$apps) {
            Console::write('暂无应用');
            exit();
        }
        $path = $options['app'] ?? $apps[0]['app_path'];
        $app = App::findByPath($path);
        if (!$app) {
            Console::write('应用不存在');
            exit();
        }
        !defined('SERVER_NAME') and define('SERVER_NAME', $options['alias'] ?? $app->appid);
        !defined('SERVER_ENV') and define('SERVER_ENV', file_exists('/.dockerenv') ? 'docker' : PHP_OS);
        !defined('SERVER_ROLE') and define('SERVER_ROLE', $options['role'] ?? 'master');
        !defined('SERVER_ALIAS') and define('SERVER_ALIAS', $options['alias'] ?? $app->appid);
        !defined('SERVER_PORT') and define('SERVER_PORT', $options['port'] ?? 9502);
        !defined('SERVER_NODE_ID') and define('SERVER_NODE_ID', strtolower(SERVER_ROLE) . '-' . md5(SERVER_NAME));
        !defined('SERVER_IS_MASTER') and define('SERVER_IS_MASTER', strtolower(SERVER_ROLE) == 'master');
        !defined('SERVER_MASTER_PID_FILE') and define('SERVER_MASTER_PID_FILE', SCF_ROOT . '/var/' . $app->appid . '.pid');
        !defined('SERVER_MASTER_DB_PID_FILE') and define('SERVER_MASTER_DB_PID_FILE', SCF_ROOT . '/var/' . $app->appid . '_master_db.pid');
        !defined('SERVER_QUEUE_MANAGER_PID_FILE') and define('SERVER_QUEUE_MANAGER_PID_FILE', SCF_ROOT . '/var/' . $app->appid . '_queue_manager.pid');
        !defined('SERVER_CRONTAB_MANAGER_PID_FILE') and define('SERVER_CRONTAB_MANAGER_PID_FILE', SCF_ROOT . '/var/' . $app->appid . '_crontab_manager.pid');
        //是否开启定时任务
        !defined('SERVER_CRONTAB_ENABLE') and define('SERVER_CRONTAB_ENABLE', $options['crontab'] ?? SWITCH_ON);
        //是否开启日志机器人推送
        !defined('SERVER_LOG_REPORT') and define('SERVER_LOG_REPORT', $options['report'] ?? SWITCH_ON);
        //是否开启静态资源服务
        !defined('SERVER_ENABLE_STATIC_HANDER') and define('SERVER_ENABLE_STATIC_HANDER', isset($options['static']) && $options['static'] == SWITCH_ON);
        !defined('SERVER_HOST') and define('SERVER_HOST', Env::getIntranetIp());
        !defined('APP_RUN_ENV') and define('APP_RUN_ENV', $options['env'] ?? 'production');
        !defined('APP_ID') and define('APP_ID', $app->appid ?? 'scf_app');
        !defined('APP_RUN_MODE') and define('APP_RUN_MODE', $options['mode'] ?? (APP_RUN_ENV == 'production' ? 'phar' : 'src'));
        !defined('APP_PATH') and define('APP_PATH', SCF_APPS_ROOT . $path . '/');
        !defined('APP_PUBLIC_PATH') and define('APP_PUBLIC_PATH', APP_PATH . $app->public_path);
        !defined('APP_AUTH_KEY') and define('APP_AUTH_KEY', $app->app_auth_key);
        //APP DB
        !defined('APP_RUNTIME_DB') and define('APP_RUNTIME_DB', APP_PATH . 'db/redis');
        //静态文件路径
        !defined('APP_ASSET_PATH') and define('APP_ASSET_PATH', APP_PUBLIC_PATH . 'asset/');
        //临时文件目录路径
        !defined('APP_TMP_PATH') and define('APP_TMP_PATH', APP_PATH . 'tmp/');
        //日志文件目录路径
        !defined('APP_LOG_PATH') and define('APP_LOG_PATH', APP_PATH . 'log/');
        //文件更新目录
        !defined('APP_UPDATE_DIR') and define('APP_UPDATE_DIR', APP_PATH . 'update/');
        !defined('APP_BIN_DIR') and define('APP_BIN_DIR', APP_PATH . 'bin/');
        if (!file_exists(APP_TMP_PATH)) {
            mkdir(APP_TMP_PATH, 0775, true);
            mkdir(APP_TMP_PATH . '/template', 0775, true);
        }
        if (!file_exists(SCF_ROOT . '/var')) {
            mkdir(SCF_ROOT . '/var', 0775, true);
        }
        if (!file_exists(APP_LOG_PATH)) {
            mkdir(APP_LOG_PATH, 0775, true);
        }
        if (!file_exists(APP_PATH . 'db')) {
            mkdir(APP_PATH . 'db', 0775, true);
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
        if (APP_RUN_ENV !== 'dev' && file_exists(SERVER_MASTER_DB_PID_FILE)) {
            unlink(SERVER_MASTER_DB_PID_FILE);
        }
        if (!App::isInstall()) {
            App::install();
        }
        if ($mode != MODE_CGI) {
            App::setPath();
        }
    }
}