<?php

namespace Scf\App;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Struct;
use Scf\Core\Table\Runtime;
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
        clearstatcache();
        $jsonFile = SCF_APPS_ROOT . '/apps.json';
        $apps = File::readJson($jsonFile) ?: [];
        $apps = ArrayHelper::index($apps, 'app_path');
        $apps[$this->app_path] = $this->asArray();
        $result = File::write(SCF_APPS_ROOT . '/apps.json', JsonHelper::toJson(array_values($apps)));
        Runtime::instance()->set('_APP_PROFILE_', $this->asArray());
        self::$_apps = $apps;
        return $result;
    }

    /**
     * 挂载目标目录应用
     * @param string $path
     * @param int $try
     * @return static
     */
    public static function mount(string $path = 'app', int $try = 0): static {
        if ($appProfile = Runtime::instance()->get('_APP_PROFILE_')) {
            return self::factory($appProfile);
        }
        if (!file_exists(SCF_APPS_ROOT)) {
            mkdir(SCF_APPS_ROOT, 0775, true);
        }
        $jsonFile = SCF_APPS_ROOT . '/apps.json';
        $appPath = SCF_APPS_ROOT . '/' . $path;
        $apps = [];
        if (!file_exists($appPath) && !RUNNING_INSTALL) {
            if (!RUNNING_SERVER) {
                Console::error("应用目录不存在:" . $appPath);
                exit();
            }
            if (!mkdir($appPath, 0775, true)) {
                Console::error("无法挂载至:" . $appPath . ",请先确保应用目录可写");
                exit();
            }
        }
        clearstatcache();
        if (file_exists($jsonFile)) {
            $try++;
            $apps = File::readJson($jsonFile);
            if (!is_array($apps) && $try < 3) {
                clearstatcache();
                return self::mount($path, $try);
            }
            if ($try >= 3) {
                Console::error("应用配置文件内容非法");
                exit();
            }
            $apps = ArrayHelper::index($apps, 'app_path');
        }
        $profile = $apps[$path] ?? [
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
        $apps[$path] = $profile;
        self::$_apps = $apps;
        Runtime::instance()->set('_APP_PROFILE_', $profile);
        return self::factory($profile);
    }

    /**
     * 所有同目录安装的应用
     * @return array
     */
    public function apps(): array {
        return self::$_apps ? array_values(self::$_apps) : [];
    }

    /**
     * @return string
     */
    public function src(): string {
        return APP_SRC_TYPE === 'phar' ? 'phar://' . App::core($this->version) : APP_PATH . '/src';
    }

    /**
     * 应用是否安装
     * @return bool
     */
    public function isInstalled(): bool {
        return file_exists($this->src());
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
        return self::$_apps[$path] ?? null;
        //return ArrayHelper::findColumn(self::$_apps, 'app_path', $path);
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
        if (!file_exists(SCF_APPS_ROOT . '/' . $this->app_path)) {
            mkdir(SCF_APPS_ROOT . '/' . $this->app_path, 0755, true);
        }
        if (!$this->update_server) {
            Util::releaseResource(Root::dir() . '/Command/Resource/config.php', SCF_APPS_ROOT . '/' . $this->app_path . '/src/config/app.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/config_dev.php', SCF_APPS_ROOT . '/' . $this->app_path . '/src/config/app_dev.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/server.php', SCF_APPS_ROOT . '/' . $this->app_path . '/src/config/server.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/version.php', SCF_APPS_ROOT . '/' . $this->app_path . '/src/version.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/controller/_module_.php', SCF_APPS_ROOT . '/' . $this->app_path . '/src/lib/Demo/_module_.php', true);
            Util::releaseResource(Root::dir() . '/Command/Resource/controller/Index.php', SCF_APPS_ROOT . '/' . $this->app_path . '/src/lib/Demo/Controller/Index.php', true);
        }
        return $this->update();
    }

}