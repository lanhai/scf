<?php

namespace Scf\App;

use Scf\Core\Console;
use Scf\Core\Struct;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Command\Color;
use Scf\Command\Util;
use Scf\Root;
use Scf\Util\Auth;
use Scf\Util\File;
use Scf\Util\Sn;

class Installer extends Struct {

    /**
     * @var ?string appid
     * @required true|appid不能为空
     */
    public ?string $appid;
    /**
     * @var ?string 源码版路径
     */
    public ?string $app_path;

    /**
     * @var ?string 秘钥
     * @required true|秘钥不能为空
     */
    public ?string $app_auth_key;
    /**
     * @var ?string 公共资源路径
     * @default string:public
     */
    public ?string $public_path;
    /**
     * @var ?string 远程更新服务器
     */
    public ?string $update_server;

    /**
     * @var ?string 当前核心版本
     */
    public ?string $version;
    /**
     * @var string|null 静态文件版本
     */
    public ?string $public_version;
    /**
     * @var ?string 更新时间
     */
    public ?string $updated;
    /**
     * @var string|null
     */
    public ?string $role = null;
    /**
     * @var string|null 节点ID
     */
    public ?string $node_id = null;
    /**
     * @var string|null 仪表盘管理密码
     */
    public ?string $dashboard_password = null;

    protected static array $_apps = [];

    /**
     * 更新应用信息
     * @return bool
     */
    public function update(): bool {
        $isNew = true;
        foreach (self::$_apps as $index => $app) {
            if ($app['app_path'] == $this->app_path) {
                $isNew = false;
                self::$_apps[$index] = $this->toArray();
            }
        }
        if ($isNew) {
            self::$_apps[] = $this->toArray();
        }
        $result = File::write(SCF_APPS_ROOT . 'apps.json', JsonHelper::toJson(self::$_apps));
        clearstatcache();
        return $result;
    }

    /**
     * 所有同目录安装的应用
     * @return array
     */
    public function apps(): array {
        return self::$_apps;
    }

    /**
     * 挂载目标目录应用
     * @param string $path
     * @param int $try
     * @param bool $create
     * @return static
     */
    public static function mount(string $path = 'app', int $try = 0, bool $create = false): static {
        if (!file_exists(SCF_APPS_ROOT)) {
            mkdir(SCF_APPS_ROOT, 0775, true);
        }
        $jsonFile = SCF_APPS_ROOT . 'apps.json';
        $appPath = SCF_APPS_ROOT . $path;
        if (!file_exists($appPath)) {
            if (!$create) {
                Console::error("无法挂载至:" . $appPath . ",请先使用'./install'命令安装(创建)应用");
                exit();
            }
            mkdir($appPath, 0775, true);
        }
        if (file_exists($jsonFile)) {
            $try++;
            $apps = File::readJson($jsonFile);
            if (!is_array($apps) && $try < 3) {
                clearstatcache();
                return self::mount($path, $try, $create);
            }
            if ($try >= 3) {
                Console::error("应用配置文件内容非法");
                exit();
            }
            self::$_apps = $apps;
        }
        $info = self::match($path) ?: [
            'appid' => "",
            'app_path' => $path,
            'app_auth_key' => "",
            'public_path' => "public",
            'update_server' => "",
            'version' => "0.0.0",
            'public_version' => "0.0.0",
            'updated' => date('Y-m-d H:i:s'),
            'node_id' => Sn::create_guid(),
            'dashboard_password' => Auth::encode(time(), Sn::create_uuid())
        ];
        !self::$_apps and self::$_apps[] = $info;
        return self::factory($info);
    }

    /**
     * @return string
     */
    public function src(): string {
        $package = APP_BIN_DIR . 'v-' . $this->version . '.app';
        return APP_RUN_MODE === 'phar' ? 'phar://' . $package . '/' : APP_PATH . 'src/';
    }

    /**
     * 应用是否安装
     * @return bool
     */
    public function isInstalled(): bool {
        return !empty($this->app_auth_key) && !empty($this->appid) && file_exists($this->src());
    }

    /**
     * 准备好安装
     * @return bool
     */
    public function readyToInstall(): bool {
        return !empty($this->app_auth_key) && !empty($this->update_server);
    }

    /**
     * @param $path
     * @return array|null
     */
    protected static function match($path): ?array {
        if (!self::$_apps) {
            return null;
        }
        return ArrayHelper::findColumn(self::$_apps, 'app_path', $path);
    }


    /**
     * 添加一个新应用
     * @return bool
     */
    public function add(): bool {
        if (!$this->validate()) {
            Console::write(Color::warning($this->getError()));
        }
        $this->update_server = $this->update_server ?? "";
        if (!file_exists(SCF_APPS_ROOT . $this->app_path)) {
            mkdir(SCF_APPS_ROOT . $this->app_path, 0755, true);
        }
        if (!$this->update_server) {
            Util::releaseResource(Root::dir() . '/Command/Resource/config.php', SCF_APPS_ROOT . $this->app_path . '/src/config/app.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/config_dev.php', SCF_APPS_ROOT . $this->app_path . '/src/config/app_dev.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/server.php', SCF_APPS_ROOT . $this->app_path . '/src/config/server.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/version.php', SCF_APPS_ROOT . $this->app_path . '/src/version.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/controller/_config.php', SCF_APPS_ROOT . $this->app_path . '/src/lib/Demo/_config.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/controller/index.php', SCF_APPS_ROOT . $this->app_path . '/src/lib/Demo/Controller/Index.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/template/common/error.html', SCF_APPS_ROOT . $this->app_path . '/src/template/common/error.html', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/template/common/error_404.html', SCF_APPS_ROOT . $this->app_path . '/src/template/common/error_404.html', true);
        }
        return $this->update();
    }

}