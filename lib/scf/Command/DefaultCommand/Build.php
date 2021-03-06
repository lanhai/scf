<?php

namespace Scf\Command\DefaultCommand;

use PhpZip\ZipFile;
use Scf\Aliyun\Oss;
use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Server\Core;
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
            !defined('APP_RUN_MODE') and define('APP_RUN_MODE', 'src');
            define("Scf\Command\DefaultCommand\APP_RUN_MODE", 'DIR');
            define("Scf\Command\DefaultCommand\BUILD_PATH", SCF_ROOT . '/build/');
            define("Scf\Command\DefaultCommand\VERSION_FILE", BUILD_PATH . 'version.json');
            Core::initialize();
            Config::init();
            return $this->$action();
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    /**
     * 版本回滚
     * @return void
     */
    public function rollback() {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $version = $options['version'] ?? '';
        if (!$version) {
            Console::write(Color::red('请传入版本号参数,示范:-version=1.0.0'));
            exit();
        }
        $this->release($version);
    }

    /**
     * 版本发布
     * @param string|null $rollbackVersion
     * @return void
     */
    public function release(string $rollbackVersion = null) {
        $manager = Manager::instance();
        $options = $manager->getOpts();
        $buildFile = $options['file'] ?? null;
        if (!file_exists(BUILD_PATH)) {
            mkdir(BUILD_PATH, 0775);
        }
        if (!file_exists(VERSION_FILE)) {
            File::write(VERSION_FILE, json_encode(['app' => [], 'scf' => ['version' => 'latest', 'release_date' => date('Y-m-d H:i:s')]]));
        }
        $version = $this->getReleaseVersion();
        //版本回滚
        if (!is_null($rollbackVersion)) {
            $app = ArrayHelper::findColumn($version, 'version', $rollbackVersion);
            if (!$app) {
                Console::log(Color::red('未查询到已发布版本:' . $rollbackVersion));
                exit;
            }
            $num = $app['version'];
            $name = APP_ID . "-v" . $rollbackVersion;
            $force = true;//版本回退为强制更新
            $remark = "版本回退";
            $buildType = $app['build_type'];
            $encryptedFilePath = $app['app_object'] ? BUILD_PATH . $name . '.scfupdate' : '';
            $publicFilePath = $app['public_object'] ? BUILD_PATH . $name . '.public.zip' : '';
            if (($buildType == 1 || $buildType == 3) && !file_exists($encryptedFilePath)) {
                Console::log(Color::red('版本打包文件不存在:' . $encryptedFilePath));
                exit();
            }
            if (($buildType == 2 || $buildType == 3) && !file_exists($publicFilePath)) {
                Console::log(Color::red('版本打包文件不存在:' . $publicFilePath));
                exit();
            }
            Console::log('开始回滚至版本:' . $num);
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
            $buildType = Console::input("请选择打包类型 1:源码打包;2:静态资源文件打包;3:全部打包(缺省)") ?: 3;
            $num = Console::input("请输入要发布的版本号,最近发布版本:" . (isset($version[0]) ? $version[0]['version'] : '暂无发布记录'));
            $name = APP_ID . "-v" . $num;
            $remark = Console::input("请输入版本说明");
            $remark = $remark ?: "暂无说明";
//            $force = Console::input("请选择是否强制更新 1:是 0:否(缺省)");
//            $force = $force == 1;
        }
        if ($buildType == 1 || $buildType == 3) {
            Console::log(Color::green('开始打包源码文件'));
            $buildFilePath = BUILD_PATH . $name . '.phar';
            $encryptedFilePath = BUILD_PATH . $name . '.scfupdate';
            if (file_exists($buildFilePath)) {
                unlink($buildFilePath);
            }
            if (file_exists($encryptedFilePath)) {
                unlink($encryptedFilePath);
            }
            $phar = new \Phar($buildFilePath, 0, 'src');
            $phar->compress(\Phar::GZ);
            $phar->buildFromDirectory(APP_PATH . 'src');
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
            'port' => 443,
            'app_object' => '',// $latestAppVersion,
            'public_object' => '',// $latestPublicVersion,
            'forced' => true,
            'remark' => $remark,
            'build_type' => $buildType
        ];
        if (($buildType == 1 || $buildType == 3) && isset($encryptedFilePath)) {
            Console::log(Color::green('开始上传源码包文件'));
            $versionObject = '/upload/scfapp/' . $name . '.scfupdate';
            //上传更新文件到OSS
            $uplaod = Oss::instance()->uploadFile($encryptedFilePath, $versionObject);
            $uploadResult = Result::factory($uplaod);
            if ($uploadResult->hasError()) {
                Console::log('源码包上传失败:' . Color::red($uploadResult->getMessage()));
                exit();
            } else {
                Console::log('源码包上传成功:' . Color::green($uploadResult->getData()));
            }
            $releaseVersion['app_object'] = $versionObject;
        }
        if ($buildType == 2 || $buildType == 3) {
            $publicObject = '/upload/scfapp/' . $name . '.public.scfupdate';
            //打包public文件
            $publicFiles = Dir::getDirFiles(APP_PUBLIC_PATH);
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
                    $zip->setPassword(APP_AUTH_KEY)->saveAsFile($publicFilePath)->close();
                    Console::line();
                    //文件加密
//                    $publicContent = File::read($publicFilePath);
//                    $publicContentEncrypted = Auth::encode($publicContent);
//                    if (!File::write($publicFilePath, $publicContentEncrypted)) {
//                        Console::log('资源文件加密失败');
//                        exit();
//                    }
                    //上传OSS
                    Console::log(Color::blue('开始上传资源包文件'));
                    $uplaod = Oss::instance()->uploadFile($publicFilePath, $publicObject);
                    $uploadResult = Result::factory($uplaod);
                    if ($uploadResult->hasError()) {
                        Console::log('资源文件包上传失败:' . Color::red($uploadResult->getMessage()));
                        exit();
                    } else {
                        Console::log('资源文件包上传成功:' . Color::green($uploadResult->getData()));
                    }
                    $releaseVersion['public_object'] = $publicObject;
                } catch (\Exception $exception) {
                    Console::log('打包资源文件失败:' . Color::red($exception->getMessage()));
                    exit();
                } finally {
                    $zip->close();
                }
            } else {
                Console::log(Color::red('打包public文件失败:暂无文件'));
                exit();
            }
        }
        array_unshift($versionServer['app'], $releaseVersion);
        File::write(VERSION_FILE, JsonHelper::toJson($versionServer));
        //上传版本引导文件
        $uplaod = Oss::instance()->uploadFile(VERSION_FILE, '/upload/scfapp/' . APP_ID . '-version.json');
        $uploadResult = Result::factory($uplaod);
        if ($uploadResult->hasError()) {
            Console::log('版本配置文件上传失败:' . Color::red($uploadResult->getMessage()));
            exit();
        } else {
            Console::log('版本配置文件上传成功:' . Color::green($uploadResult->getData()));
        }
        $pullConfig = [
            'url' => $uploadResult->getData(),
            'key' => APP_AUTH_KEY
        ];
        Console::log('文件打包完成!发行秘钥:' . Color::notice(Auth::encode(JsonHelper::toJson($pullConfig), 'scfapp')));
        //TODO 监听各个节点更新进度
    }


    /**
     * 发布历史
     * @return void
     */
    public function history() {
        $data = File::readJson(VERSION_FILE);
        Console::line();
        $data['app'] = array_reverse($data['app']);
        foreach ($data['app'] as $value) {
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