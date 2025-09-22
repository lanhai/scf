<?php

namespace Scf\Command\DefaultCommand;

use Phar;
use PhpZip\ZipFile;
use Scf\Cloud\Ali\Oss;
use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Root;
use Scf\Util\Auth;
use Scf\Util\Dir;
use Scf\Util\File;


class Build implements CommandInterface {
    public function commandName(): string {
        return 'build';
    }

    public function desc(): string {
        return '应用编译&发布';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('release', '打包应用');
        $commandHelp->addAction('rollback', '版本回滚');
        $commandHelp->addAction('history', '查看发布记录');
        $commandHelp->addAction('framework', '打包框架');
        $apps = App::all();
        $names = [];
        foreach ($apps as $app) {
            $names[] = $app['app_path'];
        }
        $commandHelp->addActionOpt('-app', '要操作的应用[' . implode('|', $names) . '],默认:' . $names[0]);
        $commandHelp->addActionOpt('-file', '[发行参数]要构建的位于build目录下的json配置文件,示范:-file=buildfile');
        $commandHelp->addActionOpt('-version', '[回滚参数]版本号,示范:-version=1.0.1');

        return $commandHelp;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            define("BUILD_PATH", dirname(SCF_ROOT) . '/build/');
            if ($action !== 'framework') {
                Env::initialize();
                define("VERSION_FILE", BUILD_PATH . APP_ID . '-version.json');
            }
            return $this->$action();
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    /**
     * 版本回滚
     * @return void
     */
    public function rollback(): void {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $version = $options['version'] ?? '';
        if (!$version) {
            Console::write(Color::red('请传入版本号参数,示范:-version=1.0.0'));
            exit();
        }
        $this->release($version);
    }


    public function framework(): void {
        Console::log('开始构建框架:' . Root::root());
        $buildDir = BUILD_PATH . 'framework/';
        if (!is_dir($buildDir)) {
            mkdir($buildDir, 0775);
        }
        $buildFilePath = $buildDir . '/scf.phar';
        $versionData = require Root::root() . '/src/version.php';
        $version = StringHelper::incrementVersion($versionData['version']);
        //将版本信息写入version文件
        $versionFile = Root::root() . '/src/version.php';
        $versionInputData = stripslashes(var_export([
            'build' => date('Y-m-d H:i:s'),
            'version' => $version,
            'url' => "https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/" . $version . ".update",
            'boot' => "https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/boot"
        ], true));
        $versionFileContent = "<?php\n  return $versionInputData;";
        if (!File::write($versionFile, $versionFileContent)) {
            Console::log('文件写入失败:' . $versionFile);
            exit();
        }
        Console::log(Color::green('开始打包源码文件'));
        if (file_exists($buildFilePath)) {
            unlink($buildFilePath);
        }
        $phar = new Phar($buildFilePath, 0, 'scf');
        $phar->compress(Phar::GZ);
        $phar->buildFromDirectory(Root::root() . '/src');
        $phar->setDefaultStub('version.php', 'version.php');
        $localFile = $buildDir . "/" . $version . ".update";
        exec('mv ' . $buildFilePath . ' ' . $localFile);
        exec('cp ' . $localFile . ' ' . SCF_ROOT . "/build/src.pack");
        Console::log(Color::green('打包完成'));
        if (!File::write($buildDir . '/version.json', JsonHelper::toJson([
            'build' => date('Y-m-d H:i:s'),
            'version' => $version,
            'url' => "https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/" . $version . ".update",
            'boot' => "https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/boot"
        ]))) {
            Console::warning('版本文件更新失败!');
        }
        Console::log(Color::green('开始上传源码包文件'));
        //上传更新文件到OSS
        $config = require $buildDir . '/config.php';
        $oss = Oss::instance($config);
        $bootUploadResult = $oss->uploadFile(SCF_ROOT . '/boot', "/scf/boot");
        if ($bootUploadResult->hasError()) {
            Console::log('引导文件上传失败:' . Color::red($bootUploadResult->getMessage()));
            exit();
        }
        $packageUploadResult = $oss->uploadFile($localFile, "/scf/{$version}.update");
        if ($packageUploadResult->hasError()) {
            Console::log('源码包上传失败:' . Color::red($packageUploadResult->getMessage()));
            exit();
        }
        $jsonUploadResult = $oss->uploadFile($buildDir . '/version.json', "/scf/version.json");
        if ($jsonUploadResult->hasError()) {
            Console::log('配置文件上传失败:' . Color::red($jsonUploadResult->getMessage()));
            exit();
        }
        Console::log('源码包上传成功:' . Color::green($packageUploadResult->getData()));
        //还原本地配置文件
        $versionInputData = stripslashes(var_export([
            'build' => 'development',
            'version' => $version,
            'url' => "https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/" . $version . ".update",
            'boot' => "https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/boot"
        ], true));
        $versionFileContent = "<?php\n  return $versionInputData;";
        unlink($localFile);
        if (!File::write($versionFile, $versionFileContent)) {
            Console::log('本地版本文件还原写入失败:' . $versionFile);
        }
    }

    /**
     * 版本发布
     * @param string|null $rollbackVersion
     * @return void
     */
    public function release(string $rollbackVersion = null): void {
        try {
            Oss::instance();
        } catch (\Exception $e) {
            Console::warning('请配置OSS服务器后进行打包');
            exit();
        }
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $buildFile = $options['file'] ?? null;
        $resetPassword = false;
        $app = App::info();
        if (!$app->app_auth_key) {
            Console::log('应用未完成初始化安装');
            exit();
        }
        if (!file_exists(BUILD_PATH)) {
            mkdir(BUILD_PATH, 0775);
        }
        if (!file_exists(VERSION_FILE)) {
            File::write(VERSION_FILE, json_encode(['app' => [], 'scf' => ['version' => 'latest', 'release_date' => date('Y-m-d H:i:s')]]));
        }
        $version = $this->getReleaseVersion();
        if (isset($version[0]['version'])) {
            $autoVersionNum = StringHelper::incrementVersion($version[0]['version']);
        } else {
            $autoVersionNum = "0.0.1";
        }
        //版本回滚
        if (!is_null($rollbackVersion)) {
            $app = ArrayHelper::findColumn($version, 'version', $rollbackVersion);
            if (!$app) {
                Console::log(Color::red('未查询到已发布版本:' . $rollbackVersion));
                exit;
            }
            $num = $autoVersionNum;
            $name = APP_ID . "-v" . $num;
            //版本回退为强制更新
            $remark = "版本回退->" . $app['version'];
            $buildType = $app['build_type'];
            $encryptedFilePath = $app['app_object'] ? BUILD_PATH . $name . '.scfupdate' : '';
            $publicFilePath = $app['public_object'] ? BUILD_PATH . $name . '.public.scfupdate' : '';
            if (($buildType == 1 || $buildType == 3) && !file_exists($encryptedFilePath)) {
                Console::log(Color::red('版本打包文件不存在:' . $encryptedFilePath));
                exit();
            }
            if (($buildType == 2 || $buildType == 3) && !file_exists($publicFilePath)) {
                Console::log(Color::red('版本打包文件不存在:' . $publicFilePath));
                exit();
            }
            Console::log('开始回滚至版本:' . $app['version']);
            goto upload;
        }
        //通过配置文件发布
        if (!is_null($buildFile)) {
            $config = File::readJson(BUILD_PATH . $buildFile . '.json');
            if (!$config) {
                Console::log('配置文件不存在:' . $buildFile);
                exit();
            }
            $buildType = $config['type'];
            $num = $config['version'];
            $name = APP_ID . "-v" . $config['version'];
            $remark = $config['remark'];
            Console::log('开始打包至版本:' . $num);
        } else {
            $buildType = Console::select(['源码打包', '静态资源文件打包', '全部打包'], 3, label: "请选择打包类型");
            $num = Console::input("请输入要发布的版本号", $autoVersionNum);
            $num = $num ?: $autoVersionNum;
            $name = APP_ID . "-v" . $num;
            $remark = Console::input("请输入版本说明", required: false, placeholder: '日常更新,暂无说明');
            $remark = $remark ?: "日常更新,暂无说明";
            $resetPasswordChoice = Console::select(['重置', '不重置'], default: 2, label: "是否重置仪表盘管理密码");
            $resetPassword = (int)$resetPasswordChoice == 1;
//            $force = Console::input("请选择是否强制更新 1:是 0:否(缺省)");
//            $force = $force == 1;
        }
        if ($buildType == 1 || $buildType == 3) {
            //将版本信息写入version文件
            $versionFile = APP_PATH . '/src/version.php';
            $versionData = stripslashes(var_export([
                'appid' => APP_ID,
                'version' => $num,
                'build' => date('Y-m-d H:i:s')
            ], true));
            $versionFileContent = "<?php\n  return $versionData;";
            if (!File::write($versionFile, $versionFileContent)) {
                Console::log('文件写入失败');
                exit();
            }
            Console::log(Color::green('开始打包源码文件'));
            $buildFilePath = BUILD_PATH . $name . '.phar';
            $encryptedFilePath = BUILD_PATH . $name . '.scfupdate';
            if (file_exists($buildFilePath)) {
                unlink($buildFilePath);
            }
            if (file_exists($encryptedFilePath)) {
                unlink($encryptedFilePath);
            }
            $phar = new Phar($buildFilePath, 0, 'src');
            $phar->compress(Phar::GZ);
            $phar->buildFromDirectory(APP_PATH . '/src');
            $phar->setDefaultStub('version.php', 'version.php');
            //源代码加密
            $content = File::read($buildFilePath);
            $contentEncrypted = Auth::encode($content);
            if (!File::write($encryptedFilePath, $contentEncrypted)) {
                Console::log('文件写入失败');
                exit();
            }
            unlink($buildFilePath);
        }
        upload:
        $publicFilePath = BUILD_PATH . $name . '.public.scfupdate';
        //更新配置文件
        $ossServer = Oss::instance()->getServer();
        $versionServer = JsonHelper::recover(File::read(VERSION_FILE));
        $latestAppVersion = null;
        $latestPublicVersion = null;
        foreach ($versionServer['app'] as $version) {
            if (!empty($version['app_object']) && is_null($latestAppVersion)) {
                $latestAppVersion = $version['app_object'];
            }
            if (!empty($version['public_object']) && is_null($latestPublicVersion)) {
                $latestPublicVersion = $version['public_object'];
            }
        }
        $releaseVersion = [
            'version' => $num,
            'appid' => APP_ID,
            'release_date' => date('Y-m-d H:i:s'),
            'server' => $ossServer['OSS_HOST'],
            'port' => str_contains($ossServer['CDN_DOMAIN'], 'https://') ? 443 : 80,
            'app_object' => '',// $latestAppVersion,
            'public_object' => '',// $latestPublicVersion,
            'forced' => true,
            'remark' => $remark,
            'build_type' => (int)$buildType
        ];
        if (($buildType == 1 || $buildType == 3) && isset($encryptedFilePath)) {
            Console::log(Color::green('开始上传源码包文件'));
            $versionObject = '/upload/scfapp/' . $name . '.scfupdate';
            //上传更新文件到OSS
            $uploadResult = Oss::instance()->uploadFile($encryptedFilePath, $versionObject);
            if ($uploadResult->hasError()) {
                Console::log('源码包上传失败:' . Color::red($uploadResult->getMessage()));
                exit();
            } else {
                Console::log('源码包上传成功:' . Color::green($uploadResult->getData()));
                unlink($encryptedFilePath);
            }
            $releaseVersion['app_object'] = $versionObject;
        }
        if ($buildType == 2 || $buildType == 3) {
            $publicObject = '/upload/scfapp/' . $name . '.public.scfupdate';
            //打包public文件
            $publicFiles = Dir::scan(APP_PUBLIC_PATH);
            if ($publicFiles) {
                Console::log(Color::blue('开始打包资源文件'));
                $zip = new ZipFile();
                try {
                    Console::line();
                    foreach ($publicFiles as $file) {
                        $arr = explode('.', $file);
                        if (array_pop($arr) == 'map') {
                            continue;
                        }
                        Console::write(str_replace(APP_PUBLIC_PATH, "", $file));
                        $zip->addFile($file, str_replace(APP_PUBLIC_PATH, "", $file));
                    }
                    Console::log(Color::blue('资源文件写入中...'));
                    $zip->setPassword(App::info()->app_auth_key)->saveAsFile($publicFilePath)->close();
                    Console::line();
                    //文件加密
//                    $publicContent = File::read($publicFilePath);
//                    $publicContentEncrypted = Base::encode($publicContent);
//                    if (!File::write($publicFilePath, $publicContentEncrypted)) {
//                        Console::log('资源文件加密失败');
//                        exit();
//                    }
                    //上传OSS
                    Console::log(Color::blue('开始上传资源包文件'));
                    $uploadResult = Oss::instance()->uploadFile($publicFilePath, $publicObject);
                    if ($uploadResult->hasError()) {
                        Console::log('资源文件包上传失败:' . Color::red($uploadResult->getMessage()));
                        exit();
                    } else {
                        Console::log('资源文件包上传成功:' . Color::green($uploadResult->getData()));
                    }
                    $releaseVersion['public_object'] = $publicObject;
                    unlink($publicFilePath);
                } catch (\Exception $exception) {
                    Console::log('打包资源文件失败:' . Color::red($exception->getMessage()));
                    exit();
                } finally {
                    $zip->close();
                }
            } else {
                Console::log(Color::warning('打包public文件失败:暂无文件'));
            }
        }
        if (count($versionServer['app']) >= 100) {
            //最多保留100条版本记录
            unset($versionServer['app'][count($versionServer['app']) - 1]);
        }
        array_unshift($versionServer['app'], $releaseVersion);
        if (!File::write(VERSION_FILE, JsonHelper::toJson($versionServer))) {
            Console::warning('版本文件更新失败!');
            exit();
        }
        Console::success("版本文件更新成功:" . JsonHelper::toJson($versionServer['app'][0]));
        //上传版本引导文件
        $uploadResult = Oss::instance()->uploadFile(VERSION_FILE, '/upload/scfapp/' . APP_ID . '-version.json');
        if ($uploadResult->hasError()) {
            Console::log('版本配置文件上传失败:' . Color::red($uploadResult->getMessage()));
            exit();
        } else {
            Console::log('版本配置文件上传成功:' . Color::green($uploadResult->getData()));
        }

        if (!$app->dashboard_password || $resetPassword) {
            $app->dashboard_password = Auth::encode(str_shuffle(time()), md5(str_shuffle(time())));
            $app->update();
            Console::log('管理密码:' . Color::notice($app->dashboard_password));
        }
        $installData = [
            str_shuffle(time()) => time(),
            'server' => $uploadResult->getData(),
            'key' => $app->app_auth_key,
            'dashboard_password' => $app->dashboard_password,
            'expired' => time() + 86400,
        ];
        $installSecret = Auth::encode(str_shuffle(time()), 'SCF_APP_INSTALL');
        $installCode = $installSecret . Auth::encode(JsonHelper::toJson($installData), $installSecret);
        Console::log('过期时间:' . Color::notice(date('Y-m-d H:i:s', $installData['expired'])));
        Console::log('安装秘钥:' . Color::green($installCode));

    }


    /**
     * 发布历史
     * @return void
     */
    public function history(): void {
        $data = File::readJson(VERSION_FILE);
        Console::line();
        $data['app'] = array_reverse($data['app']);
        foreach ($data['app'] as $value) {
            if ($value['appid'] != APP_ID) {
                continue;
            }
            Console::write('v' . $value['version'] . ' 发布时间:' . $value['release_date'] . ' 版本说明:' . $value['remark']);
        }
        Console::line();
    }

    /**
     * 版本号检查
     * @return array
     */
    public function getReleaseVersion(): array {
        if (!file_exists(VERSION_FILE)) {
            Console::write(Color::red('未查询到发布配置文件:' . VERSION_FILE));
            exit();
        }
        return File::readJson(VERSION_FILE)['app'];
    }
}