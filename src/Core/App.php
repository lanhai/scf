<?php

namespace Scf\Core;

use FilesystemIterator;
use JetBrains\PhpStorm\Pure;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use Scf\App\Installer;
use Scf\App\Updater;
use Scf\Client\Http;
use Scf\Database\Dao;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Log;
use Scf\Command\Color;
use Scf\Server\Env;
use Scf\Util\Dir;
use Scf\Util\File;
use Swoole\Coroutine\Http\Client;
use Swoole\Event;
use Swoole\Runtime;
use Symfony\Component\Yaml\Yaml;
use function Swoole\Coroutine\run;

class App {
    /**
     * @var array 模块
     */
    protected static array $_modules = [
        MODE_CGI => [],
        MODE_RPC => [],
        MODE_CLI => [],
        MODE_NATIVE => []
    ];
    protected static ?string $appid = null;
    protected static string $path;
    protected static bool $_ready = false;

    /**
     * 指定APP文件夹
     * @param $path
     * @return Installer
     */
    public static function appoint($path): Installer {
        self::$path = $path;
        return Installer::mount($path);
    }

    /**
     * @return Installer
     */
    public static function installer(): Installer {
        return Installer::mount(defined('APP_DIR_NAME') ? APP_DIR_NAME : 'app');
    }

    /**
     * 判断是否安装
     * @return bool
     */
    public static function isReady(): bool {
        if (self::$_ready) {
            return self::$_ready;
        }
        $status = self::installer()->isInstalled();
        $status and self::$_ready = true;
        return $status;
    }

    /**
     * @param string $mode
     * @return void
     */
    public static function await(string $mode = MODE_CGI): void {
        while (true) {
            if (self::isReady()) {
                break;
            }
            sleep(1);
        }
    }

    /**
     * @return string|null
     */
    public static function path(): ?string {
        return self::installer()->app_path;
    }

    public static function clearTemplateCache(): void {
        $dir = APP_TMP_PATH . '/template';
        if (!is_dir($dir)) {
            return;
        }
        $files = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS),
            RecursiveIteratorIterator::CHILD_FIRST
        );
        foreach ($files as $fileinfo) {
            $todo = ($fileinfo->isDir() ? 'rmdir' : 'unlink');
            if (!@$todo($fileinfo->getRealPath())) {
                Console::error("Failed to delete {$fileinfo->getRealPath()}");
            }
        }
        rmdir($dir);
    }

    /**
     * 挂载到目录
     * @param string $mode
     * @return void
     */
    public static function mount(string $mode = MODE_CGI): void {
        if (!self::installer()->isInstalled() && self::isDevEnv()) {
            Console::error("[LOAD]无法挂载至:" . SCF_APPS_ROOT . '/' . self::installer()->app_path . ",请先使用'./install'命令安装(创建)应用");
            exit();
        }
        //应用ID
        !defined('APP_ID') and define('APP_ID', self::installer()->appid ?? 'scf_app');
        //项目视图路径
        !defined('APP_VIEW_PATH') and define('APP_VIEW_PATH', self::src() . '/template');
        //项目库路径
        !defined('APP_LIB_PATH') and define('APP_LIB_PATH', self::src() . '/lib');
        Config::init();
        //加载应用第三方库
        $vendorLoader = self::src() . '/vendor/autoload.php';
        if (file_exists($vendorLoader)) {
            require $vendorLoader;
        }
        self::loadModules($mode);
    }

    /**
     * 源码路径
     * @return string
     */
    public static function src(): string {
        return self::installer()->src();
    }

    /**
     * @return Installer|null
     */
    public static function info(): ?Installer {
        return self::installer();
    }

    /**
     * @param $version
     * @return string
     */
    public static function core($version = null): string {
        return APP_BIN_DIR . '/v-' . ($version ?: self::version()) . '.app';
    }

    /**
     * 判断是否master节点
     * @return bool
     */
    public static function isMaster(): bool {
        $role = SERVER_ROLE;// self::installer()->role ?: SERVER_ROLE;
        return $role === 'master';
    }

    /**
     * 升级数据库
     * @return void
     */
    public static function updateDatabase(): void {
        //同步数据库表
        if (self::isMaster()) {
            $configDir = App::src() . '/config/db';
            if (is_dir($configDir) && $files = Dir::scan($configDir)) {
                foreach ($files as $file) {
                    $table = Yaml::parseFile($file);
                    $arr = explode("/", $table['dao']);
                    $cls = self::buildControllerPath(...$arr);
                    if (!class_exists($cls)) {
                        Console::error($cls . " not exist");
                    } else {
                        /** @var Dao $cls */
                        $dao = $cls::factory();
                        $dao->updateTable($table);
                    }
                }
            }
            //同步权限节点
            $versionFile = App::src() . '/config/access/nodes.yml';
            if (file_exists($versionFile)) {
                $latest = Yaml::parseFile($versionFile);
                $cls = $latest['dao'];
                $currVersionFile = APP_PATH . '/db/updates/_access_nodes.yml';
                $current = file_exists($currVersionFile) ? Yaml::parseFile($currVersionFile) : null;
                if (!$current || $current['version'] != $latest['version']) {
                    /** @var Dao $cls */
                    $count = $cls::select()->delete();
                    if ($count) {
                        Console::warning("【Database】{$count}条权限节点数据已删除");
                    }
                    foreach ($latest['nodes'] as $node) {
                        $ar = $cls::factory($node);
                        if (!$ar->save(forceInsert: true)) {
                            Console::error($ar->getError());
                        }
                    }
                    Console::log("【Database】" . count($latest['nodes']) . "条权限节点数据已更新至:" . Color::success($latest['version']));
                    File::write($currVersionFile, Yaml::dump([
                        'version' => $latest['version']
                    ]));
                }
            }
        }
    }

    protected static ?string $_authKey = null;

    /**
     * @return string|null
     */
    public static function authKey(): ?string {
        if (empty(self::$_authKey)) {
            self::$_authKey = self::info()->app_auth_key;
        }
        return self::$_authKey;
    }


    /**
     * @return string|null
     */
    public static function id(): ?string {
        return self::installer()->appid;
    }

    /**
     * 列出所有应用
     * @return array
     */
    public static function all(): array {
        $jsonFile = SCF_APPS_ROOT . '/apps.json';
        if (file_exists($jsonFile)) {
            clearstatcache();
            $apps = File::readJson($jsonFile);
            if (!is_array($apps)) {
                goto create;
            }
            return $apps;
        }
        create:
        return [];
        //return self::installer()->apps();
    }

    public static function getServerIp() {
        $ip = null;
        $client = new Client('host.docker.internal', '19502');
        $client->set(['timeout' => 10]);
        if (!$client->get('/') || $client->statusCode !== 200) {
            Console::log(Color::red('获取服务器IP地址失败,请确保宿主机myip服务已启动![' . $client->errMsg . ']'));
        } else {
            $ip = JsonHelper::recover($client->getBody());
        }
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
//            if (!$status) {
//                Log::instance()->error('更新应用失败');
//            }
        });
        Event::wait();
        if (!self::isReady()) {
            Log::instance()->error('应用安装失败');
            return false;
        }
        Log::instance()->info('应用安装完成');
        return true;
    }

    /**
     * @return bool
     */
    public static function update(): bool {
        if (APP_RUN_MODE != 'phar') {
            return true;
        }
        run(function () {
            $status = Updater::instance()->updateApp();
            if (!$status) {
                Log::instance()->error('应用安装失败');
            }
        });
        Event::wait();
        Log::instance()->info('应用安装完成');
        return true;
    }

    public static function profile(): Installer {
        return self::installer();
    }
    /**
     * 当前版本
     * @return string|null
     */
    public static function version(): ?string {
        if (file_exists(App::src() . '/version.php')) {
            $vision = require App::src() . '/version.php';
            return $vision['version'] ?? 'development';
        }
        return self::installer()->version;
    }

    /**
     * 静态资源版本
     * @return string|null
     */
    public static function publicVersion(): ?string {
        return self::installer()->public_version;
    }

    /**
     * 最新版本
     * @return array
     */
    public static function latestVersion(): array {
        $app = null;
        $public = null;
        $server = self::info()->update_server;
        if ($server) {
            $client = Http::create($server . '?time=' . time());
            $result = $client->get();
            if (!$result->hasError()) {
                $remote = $result->getData();
                if ($remote['app']) {
                    foreach ($remote['app'] as $version) {
                        if (($version['build_type'] == 1 || $version['build_type'] == 3) && is_null($app)) {
                            $app = $version['version'];
                        }
                        if (($version['build_type'] == 2 || $version['build_type'] == 3) && is_null($public)) {
                            $public = $version['version'];
                        }
                    }
                }
            }
        }
        return compact('app', 'public');
    }

    /**
     * 获取已加载的模块
     * @param string $mode
     * @return array
     */
    public static function getModules(string $mode = MODE_CGI): array {
        return self::$_modules[$mode];
    }

    /**
     * 加载模块
     * @param string $mode
     * @return array
     */
    public static function loadModules(string $mode = MODE_CGI): array {
        if (!empty(self::$_modules[$mode])) {
            return self::$_modules[$mode];
        }
        //注册加载器
        spl_autoload_register([__CLASS__, 'autoload']);
        $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
        $entryScripts = [];
        if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
            is_dir(APP_LIB_PATH . '/Controller') and $entryScripts = Dir::scan(APP_LIB_PATH . '/Controller', 2);
            is_dir(APP_LIB_PATH . '/Cli') and $mode == MODE_CLI and $entryScripts = [...$entryScripts, ...Dir::scan(APP_LIB_PATH . '/Cli', 2)];
            is_dir(APP_LIB_PATH . '/Rpc') and $mode == MODE_RPC and $entryScripts = [...$entryScripts, ...Dir::scan(APP_LIB_PATH . '/Rpc', 2)];
            is_dir(APP_LIB_PATH . '/Crontab') and $entryScripts = [...$entryScripts, ...Dir::scan(APP_LIB_PATH . '/Crontab', 2)];
        } else {
            $entryScripts = Dir::scan(APP_LIB_PATH, 2);
        }
        $modules = [];
        if ($entryScripts) {
            foreach ($entryScripts as $file) {
                $arr = explode('/', $file);
                $allowFiles = [
                    '_config.php',
                    '_module_.php',
                    'config.php',
                    'service.php'
                ];
                $fileName = array_pop($arr);
                if (!in_array($fileName, $allowFiles) || ($mode == MODE_CGI && $fileName == 'service.php')) {
                    continue;
                }
                $config = require($file);
                if (is_bool($config) || is_numeric($config)) {
                    continue;
                }
                $allowMode = $config['mode'] ?? MODE_CGI;
                $allowModes = is_array($allowMode) ? $allowMode : [$allowMode];
                in_array($mode, $allowModes) and $modules[] = $config;
            }
        }
        self::$_modules[$mode] = $modules;
        return $modules;
    }

    /**
     * 自动加载
     * @param $className
     * @throws Exception
     */
    public static function autoload($className): void {
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
        $fileName = APP_LIB_PATH . '/' . $_fileName;

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

    /**
     * 拼接控制器路径
     * @param string ...$segments
     * @return string
     */
    public static function buildControllerPath(string ...$segments): string {
        return APP_TOP_NAMESPACE . '\\' . implode('\\', $segments);
    }

    /**
     * 拼接路径
     *
     * @param string ...$segments
     * @return string
     */
    public static function buildPath(string ...$segments): string {
        return implode(DIRECTORY_SEPARATOR, array_filter($segments));
    }
}
