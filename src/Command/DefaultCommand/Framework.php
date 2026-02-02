<?php

namespace Scf\Command\DefaultCommand;

use PhpZip\ZipFile;
use Scf\Cloud\Ali\Oss;
use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Console;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Root;
use Scf\Util\Dir;
use Scf\Util\File;
use Swoole\Coroutine\System;
use Phar;
use function Co\run;

class Framework implements CommandInterface {

    public function commandName(): string {
        return 'framework';
    }

    public function desc(): string {
        return '框架包管理';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('build', '打包框架');
        $commandHelp->addAction('dashboard', '打包仪表盘');
        $commandHelp->addAction('push', '框架包推送发布');
        $commandHelp->addAction('version', '查询最新版本');
        return $commandHelp;
    }

    public function push(): void {
        $latestVersion = $this->version();
        Console::info('最新版本:' . $latestVersion);
        Console::line();
        Console::info('正在推送代码到github...');
        System::exec("git add " . SCF_ROOT)['output'];
        System::exec('git commit -m "auto commit at ' . date('Y-m-d H:i:s') . '"')['output'];
        System::exec("git push")['output'];
        $defaultVersionNum = StringHelper::incrementVersion($latestVersion);
        $version = Console::input('请输入版本号', $defaultVersionNum);
        Console::info('正在推送版本标签:v' . $version);
        System::exec("git tag -a v$version -m 'release v$version'")['output'];
        System::exec("git push origin v$version")['output'];
        Console::success('框架composer包发布成功:v' . $version);
    }

    public function dashboard(): void {
        $dashboardDir = SCF_ROOT . '/build/public/dashboard';
        if (!is_dir($dashboardDir)) {
            Console::warning('文件夹不存在:' . $dashboardDir);
            exit();
        }
        $indexFile = $dashboardDir . '/index.html';
        if (!file_exists($indexFile)) {
            Console::warning('入口文件不存在');
            exit();
        }
        $buildDir = BUILD_PATH . 'framework/';
        if (!is_dir($buildDir)) {
            mkdir($buildDir, 0775);
        }
        $buildFilePath = $buildDir . '/dashboard.zip';
        $md5 = md5_file($indexFile);
        $versionJson = $dashboardDir . '/version.json';
        if (!file_exists($versionJson)) {
            $version = [
                'version' => '1.0.0',
            ];
        } else {
            $version = JsonHelper::recover(File::read($versionJson));
            if ($version['md5'] == $md5) {
                Console::success('文件一致,无需更新');
                exit();
            }
        }
        $version['md5'] = $md5;
        $version['version'] = StringHelper::incrementVersion($version['version']);
        //打包文件夹
        $publicObject = '/scf/dashboard.zip';
        //打包public文件
        $publicFiles = Dir::scan($dashboardDir);
        $config = require $buildDir . '/config.php';
        $oss = Oss::instance($config);
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
                    Console::write(str_replace($dashboardDir, "", $file));
                    $zip->addFile($file, str_replace($dashboardDir, "", $file));
                }
                Console::log(Color::blue('文件包写入中...'));
                $zip->setPassword('scfdashboard')->saveAsFile($buildFilePath)->close();
                Console::line();
                //上传OSS
                Console::log(Color::blue('开始上传文件包'));
                $uploadResult = $oss->uploadFile($buildFilePath, $publicObject);
                if ($uploadResult->hasError()) {
                    Console::log('文件包上传失败:' . Color::red($uploadResult->getMessage()));
                    exit();
                } else {
                    $version['server'] = $oss->bucket()['cdn_domain'];
                    $version['file'] = $publicObject;
                    Console::log('文件包上传成功:' . Color::green($uploadResult->getData()));
                }
                unlink($buildFilePath);
            } catch (Exception $exception) {
                Console::log('打包资源文件失败:' . Color::red($exception->getMessage()));
                exit();
            } finally {
                $zip->close();
            }
        } else {
            Console::log(Color::warning('打包public文件失败:暂无文件'));
        }
        if (!File::write($versionJson, JsonHelper::toJson($version))) {
            Console::warning('版本文件更新失败!');
        }
        //上传版本引导文件
        $uploadResult = $oss->uploadFile($versionJson, '/scf/dashboard-version.json');
        if ($uploadResult->hasError()) {
            Console::log('版本配置文件上传失败:' . Color::red($uploadResult->getMessage()));
            exit();
        } else {
            Console::log('版本配置文件上传成功:' . Color::green($uploadResult->getData()));
        }
    }

    public function build(): void {
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

    protected function version(): string {
        $version = '0.0.0';
        Console::startLoading('正在查询最新版本', function ($tid) use (&$version) {
            $tags = trim(System::exec('git ls-remote --tags origin')['output']);
            // 将输出行分割为数组
            $lines = explode("\n", trim($tags));
            // 定义版本号提取正则表达式
            $pattern = '/refs\/tags\/v?(\d+\.\d+\.\d+)/';
            // 用于存储版本号和对应的行
            $versions = [];
            // 提取版本号并存储
            foreach ($lines as $line) {
                if (preg_match($pattern, $line, $matches)) {
                    $version = $matches[1];
                    $versions[] = ['version' => $version, 'line' => $line];
                }
            }
            // 自定义比较函数按版本号排序
            usort($versions, function ($a, $b) {
                return version_compare($b['version'], $a['version']);
            });
            $line = $versions[0]['line'];
            // 正则表达式匹配版本号
            $pattern = '/v(\d+\.\d+\.\d+)/';
            if (preg_match($pattern, $line, $matches)) {
                // 提取的版本号
                $version = $matches[1];
            }
            Console::endLoading($tid);
        });
//        $cid[] = go(function () use (&$version) {
//
//        });
//        Coroutine::join($cid);
        return $version;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            $result = null;
            run(function () use ($action, &$result) {
                $result = $this->$action();
            });
            return $result;
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }
}