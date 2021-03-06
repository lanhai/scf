<?php

namespace Scf\Core;

use JetBrains\PhpStorm\Pure;
use Scf\Command\Color;
use Scf\Command\Handler\AppCreater;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Updater;
use Scf\Util\Dir;
use Scf\Util\File;
use Swoole\Coroutine\Http\Client;
use Swoole\Event;
use Swoole\Runtime;
use Swoole\Timer;
use function Swoole\Coroutine\run;

class App {
    /**
     * @var array 模块
     */
    protected static array $_modules = [];

    /**
     * 更新应用版本号
     * @param $appid
     * @param $version
     * @return bool
     */
    public static function updateVersionFile($appid, $version): bool {
        $config = SCF_APPS_ROOT . 'apps.json';
        $apps = App::all();
        $app = ArrayHelper::findColumn($apps, 'appid', $appid);
        if (!$app) {
            return false;
        }
        $app['version'] = $version;
        $app['updated'] = date('Y-m-d H:i:s');
        foreach ($apps as $k => $item) {
            if ($item['appid'] == $appid) {
                $apps[$k] = $app;
            }
        }
        return File::write($config, JsonHelper::toJson($apps));
    }

    /**
     * @param $appid
     * @return AppCreater|null
     */
    public static function info($appid = null): ?AppCreater {
        $appid = $appid ?: APP_ID;
        if (!$appList = self::all()) {
            return null;
        }
        $app = ArrayHelper::findColumn($appList, 'appid', $appid);
        if (!$app) {
            return null;
        }
        return AppCreater::factory($app);
    }

    /**
     * 根据路径查找
     * @param $path
     * @return AppCreater|null
     */
    public static function findByPath($path): ?AppCreater {
        if (!$apps = self::all()) {
            return null;
        }
        $app = ArrayHelper::findColumn($apps, 'app_path', $path);
        return $app ? AppCreater::factory($app) : null;
    }

    /**
     * 获取应用列表
     * @return array|bool|string
     */
    public static function all(): bool|array|string {
        $appConfig = SCF_APPS_ROOT . 'apps.json';
        if (!file_exists($appConfig)) {
            return false;
//            Console::write(Color::red('未查询到app配置文件:' . SCF_APPS_ROOT . 'apps.json'));
//            exit();
        }
        return File::readJson($appConfig);
    }

    /**
     * @return void
     */
    public static function setPath() {
        //项目源码目录
        !defined('APP_PHAR_PATH') and define('APP_PHAR_PATH', APP_PATH . APP_ID . '.app');
        !defined('APP_SRC_PATH') and define('APP_SRC_PATH', self::srcPath());
        //项目视图目录
        !defined('APP_VIEW_PATH') and define('APP_VIEW_PATH', APP_SRC_PATH . 'template/');
        //项目库目录
        !defined('APP_LIB_PATH') and define('APP_LIB_PATH', APP_SRC_PATH . 'lib/');
    }

    /**
     * 当前版本
     * @return string|null
     */
    public static function version(): ?string {
        return self::info()->version;
    }

    /**
     * 判断是否安装
     * @return bool
     */
    public static function isInstall(): bool {
        if (APP_RUN_MODE != 'phar') {
            return true;
        }
        return file_exists(self::pharPath());
    }

    public static function getServerIp() {
        $ip = null;
        run(function () use (&$ip) {
            $client = new Client('host.docker.internal', '19502');
            $client->set(['timeout' => 3]);
            if (!$client->get('/') || $client->statusCode !== 200) {
                Console::log(Color::red('获取服务器IP地址失败,请确保宿主机myip服务已启动![' . $client->errMsg . ']'));
                return false;
            }
            $ip = JsonHelper::recover($client->getBody());
        });
        Event::wait();
        if (is_null($ip)) {
            Console::log(Color::notice('3秒后重试'));
            sleep(3);
            return self::getServerIp();
        }
        return $ip;
    }

    /**
     * 安装应用
     * @return bool
     */
    public static function install(): bool {
        run(function () {
            $status = Updater::instance()->updateApp(true);
            if (!$status) {
                Log::instance()->error('更新应用失败');
            }
        });
        Event::wait();
        if (!\Scf\Mode\Web\App::isInstall()) {
            Log::instance()->error('更新应用失败');
            return false;
        }
        Log::instance()->info('应用安装完成');
        return true;
    }

    /**
     * 源码路径
     * @return string
     */
    public static function srcPath(): string {
        return APP_RUN_MODE == 'phar' ? 'phar://' . self::pharPath() . '/' : APP_PATH . 'src/';
    }

    /**
     * phar文件路径
     * @param string|null $version
     * @return string
     */
    public static function pharPath(string $version = null): string {
        return APP_BIN_DIR . 'v-' . ($version ?: self::version()) . '.app';
    }

    /**
     * 获取已加载的模块
     * @return array
     */
    public static function getModules(): array {
        return self::$_modules;
    }

    /**
     * 加载模块
     * @param string $mode
     * @return array
     */
    public static function loadModules(string $mode = MODE_CGI): array {
        //注册加载器
        spl_autoload_register([__CLASS__, 'autoload'], true);
        $entryScripts = Dir::getDirFiles(APP_SRC_PATH . 'lib', 2);
        $modules = [];
        if ($entryScripts) {
            foreach ($entryScripts as $file) {
                $arr = explode('/', $file);
                if (array_pop($arr) != 'config.php') {
                    //APP_RUN_MODE == 'phar' and require_once($file);
                    continue;
                }
                $config = require_once($file);
                $config['mode'] == $mode and $modules[] = $config;
            }
        }
        self::$_modules = $modules;
        return $modules;
    }

    /**
     * 自动加载
     * @param $className
     * @throws Exception
     */
    public static function autoload($className) {
        $flags = Runtime::getHookFlags();
        Runtime::setHookFlags(0);
        $className = ltrim($className, '\\');
        $fileName = '';
        $_fileName = '';
        $namespace = '';
        if (($lastNsPos = strrpos($className, '\\')) !== false) {
            $namespace = substr($className, 0, $lastNsPos);
            $className = substr($className, $lastNsPos + 1);
            //$_fileName = $namespace . DIRECTORY_SEPARATOR;
            $_fileName = str_replace(APP_TOP_NAMESPACE . "\\", "", $namespace) . DIRECTORY_SEPARATOR;
        }
        $_fileName .= $className . '.php';
        $namespaceInfo = explode('\\', $namespace);
        $fileName = APP_LIB_PATH . $_fileName;
        if ($fileName) {
            $fileName = str_replace('\\', DIRECTORY_SEPARATOR, $fileName);
            if (file_exists($fileName)) {
                require_once $fileName;
                Runtime::setHookFlags($flags);
            }
        }
    }

    /**
     * @return bool 是否开发环境
     */
    #[Pure] public static function isDevEnv(): bool {
        return Env::isDev();
    }
}
