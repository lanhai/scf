<?php

namespace Scf\Command\Handler;

use Scf\Command\Color;
use Scf\Command\Util;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Struct;
use Scf\Helper\JsonHelper;
use Scf\Server\Core;
use Scf\Util\File;
use const SCF_ROOT;

class AppCreater extends Struct {

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
     * @var ?string 当前版本
     */
    public ?string $version;
    /**
     * @var ?string 更新时间
     */
    public ?string $updated;


    /**
     * 执行创建应用
     * @return void
     */
    public function execute() {
        if (!$this->validate()) {
            Console::write(Color::warning($this->getError()));
        }
        $this->update_server = $this->update_server ?? "";
        if (!file_exists(SCF_APPS_ROOT . $this->app_path)) {
            mkdir(SCF_APPS_ROOT . $this->app_path, 0755, true);
        }
        if (!$this->update_server) {
            Util::releaseResource(SCF_ROOT . '/lib/scf/Command/Resource/config.php', SCF_APPS_ROOT . $this->app_path . '/src/config/app_dev.php', true);
            Util::releaseResource(SCF_ROOT . '/lib/scf/Command/Resource/server.php', SCF_APPS_ROOT . $this->app_path . '/src/config/server.php', true);
            Util::releaseResource(SCF_ROOT . '/lib/scf/Command/Resource/version.php', SCF_APPS_ROOT . $this->app_path . '/src/version.php', true);
            Util::releaseResource(SCF_ROOT . '/lib/scf/Command/Resource/controller/config.php', SCF_APPS_ROOT . $this->app_path . '/src/lib/Demo/config.php', true);
            Util::releaseResource(SCF_ROOT . '/lib/scf/Command/Resource/controller/index.php', SCF_APPS_ROOT . $this->app_path . '/src/lib/Demo/Controller/Index.php', true);
            Util::releaseResource(SCF_ROOT . '/lib/scf/Command/Resource/template/common/error.html', SCF_APPS_ROOT . $this->app_path . '/src/template/common/error.html', true);
            Util::releaseResource(SCF_ROOT . '/lib/scf/Command/Resource/template/common/error_404.html', SCF_APPS_ROOT . $this->app_path . '/src/template/common/error_404.html', true);
        }
        $appList = SCF_APPS_ROOT . '/apps.json';
        if (!file_exists($appList)) {
            $list = [
                $this->toArray()
            ];
        } else {
            $list = File::readJson($appList);
            if (App::info($this->appid)) {
                foreach ($list as $k => $app) {
                    if ($app['appid'] == $this->appid) {
                        $list[$k] = $this->toArray();
                    }
                }
            } else {
                $list[] = $this->toArray();
            }
        }
        File::write($appList, JsonHelper::toJson($list));
        Console::write(Color::green('应用创建成功'));
    }

}