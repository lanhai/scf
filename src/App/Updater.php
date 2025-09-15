<?php

namespace Scf\App;

use Error;
use PhpZip\Exception\ZipException;
use PhpZip\ZipFile;
use Scf\Client\Http;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Helper\JsonHelper;
use Scf\Core\App;
use Scf\Mode\Web\Log;
use Scf\Command\Color;
use Scf\Util\Auth;
use Scf\Util\File;
use Swoole\Coroutine\Http\Client;
use Throwable;

class Updater {

    protected static array $_instances;
    protected ?array $_version = null;

    /**
     * @return Updater
     */
    public static function instance(): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class();
        }
        return self::$_instances[$class];
    }

    /**
     * 更新应用到最新版本
     * @param bool $isInstall
     * @return bool
     */
    public function updateApp(bool $isInstall = false): bool {
        $version = $this->getRemoteVersion();
        if (is_null($version) || !isset($version['app'])) {
            Log::instance()->error('【Server】升级失败:获取版本服务器版本号失败!');
            return false;
        }
        return $this->changeAppVersion($version['app']['version'], $isInstall);
    }

    /**
     * 切换应用到指定版本
     * @param $version
     * @param bool $isInstall
     * @param string|null $appoint
     * @return false
     */
    public function changeAppVersion($version, bool $isInstall = false, ?string $appoint = null): bool {
        $appFile = App::core($version);
        $versionInfo = null;
        $app = App::info();
        if (!$app->update_server) {
            Log::instance()->error('【Server】应用未设置更新服务器');
            return false;
        }
        try {
            $client = Http::create($app->update_server . '?time=' . time());
            $result = $client->get();
            if ($result->hasError()) {
                Log::instance()->error('【Server】获取云端版本号失败:' . Color::red($result->getMessage()));
                return false;
            }
            $remote = $result->getData();
            $appVersion = $remote['app'];
        } catch (Error $error) {
            Log::instance()->error('【Server】获取版本服务器版本号失败:' . $error->getMessage());
            return false;
        }
        foreach ($appVersion as $v) {
            if (strcmp($version, $v['version']) == 0) {
                $versionInfo = $v;
                break;
            }
        }
        if (is_null($versionInfo)) {
            Log::instance()->error('【Server】升级失败:未查询到版本:' . $version);
            return false;
        }
        if (!is_null($appoint)) {
            //指定更新
            $appoint == 'app' and $versionInfo['public_object'] = null;
            $appoint == 'public' and $versionInfo['app_object'] = null;
        }
        $publicVersion = $version;
        if ($isInstall) {
            $latestAppVersion = null;
            $latestPublicVersion = null;
            //$versionServer = JsonHelper::recover(File::read(VERSION_FILE));
            foreach ($appVersion as $v) {
                if (!empty($v['app_object']) && is_null($latestAppVersion)) {
                    $latestAppVersion = $v['app_object'];
                    $version = $v['version'];
                }
                if (!empty($v['public_object']) && is_null($latestPublicVersion)) {
                    $latestPublicVersion = $v['public_object'];
                    $publicVersion = $v['version'];
                }
            }
            $versionInfo['public_object'] = $latestPublicVersion;
            $versionInfo['app_object'] = $latestAppVersion;
        }
        if ($versionInfo['public_object']) {
            //Log::instance()->info("开始下载資源包:" . $versionInfo['server'] . $versionInfo['public_object']);
            $publicFilePath = APP_UPDATE_DIR . '/app-v' . $version . '.public.zip';
            $port = $versionInfo['port'] ?? 80;
            $client = new Client($versionInfo['server'], $port, $port == 443);
            $client->set(['timeout' => -1]);
            $client->setHeaders([
                'Host' => $versionInfo['server'],
                'User-Agent' => 'Chrome/49.0.2587.3',
                'Accept' => '*',
                'Accept-Encoding' => 'gzip'
            ]);
            $client->download($versionInfo['public_object'], $publicFilePath);
            $code = $client->getStatusCode();
            if ($code !== 200) {
                Log::instance()->error('【Server】升级失败:资源包下载失败:' . $client->errMsg . '(' . $code . ')');
                return false;
            }
            //Log::instance()->info("资源包下载完成:" . $publicFilePath);
            //解压缩
            $zipFile = new ZipFile();
            try {
                $this->clearPublic();
                $stream = File::read($publicFilePath);
                $zipFile->openFromString($stream)->setReadPassword($app->app_auth_key)->extractTo(APP_PUBLIC_PATH);
                //Log::instance()->info("资源包解压成功");
            } catch (ZipException $e) {
                Log::instance()->error("【Server】资源包解压失败:" . Color::red($e->getMessage()));
                return false;
            } finally {
                $zipFile->close();
            }
            $installer = App::installer();
            $installer->public_version = $publicVersion;
            $installer->updated = date('Y-m-d H:i:s');
            if (!$installer->update()) {
                Log::instance()->error('【Server】升级失败:更新版本配置文件失败');
                return false;
            }
        }
        if ($versionInfo['app_object']) {
            //Log::instance()->info("开始下载源码包:" . $versionInfo['server'] . $versionInfo['app_object']);
            $updateFilePath = APP_UPDATE_DIR . '/app-v' . $version . '.scfupdate';
            $host = $versionInfo['server'];
            $port = $versionInfo['port'] ?? 80;
            $client = new Client($host, $port, $port == 443);
            $client->set(['timeout' => -1]);
            $client->setHeaders([
                'Host' => $host,
                'User-Agent' => 'Chrome/49.0.2587.3',
                'Accept' => '*',
                'Accept-Encoding' => 'gzip'
            ]);
            $client->download($versionInfo['app_object'], $updateFilePath);
            $code = $client->getStatusCode();
            if ($code !== 200) {
                Log::instance()->error('【Server】升级失败:源码包下载失败:' . $client->errMsg . '(' . $code . ')');
                return false;
            }
            //Log::instance()->info("开始写入文件:" . $appFile);
            //Log::instance()->info("源码包下载完成:" . $updateFilePath);
            if (file_exists($appFile)) {
                unlink($appFile);
            }
            $updateContent = File::read($updateFilePath);
            if (!$code = Auth::decode($updateContent, $app->app_auth_key)) {
                Log::instance()->error('【Server】升级失败:源码解析失败');
                return false;
            }
            if (!File::write($appFile, $code)) {
                Log::instance()->error('【Server】升级失败:更新写入失败');
                return false;
            }
            //Log::instance()->info("源码包更新成功");
            //更新本地配置文件
            $app = App::installer();
            $app->version = $version;
            $app->updated = date('Y-m-d H:i:s');
            if (!$app->update()) {
                Log::instance()->error('【Server】升级失败:更新版本配置文件失败');
                return false;
            }
        }
        $log = [
            'date' => date('Y-m-d H:i:s'),
            'version' => $version,
            'remark' => $versionInfo['remark']
        ];
        File::write(APP_PATH . '/update/update.log', JsonHelper::toJson($log), true);
        Log::instance()->info("【Server】{$appoint} 已更新至版本:" . $version);
        clearstatcache();
        return true;
    }

    /**
     * 自定义更新src/public到指定版本
     * @param $type
     * @param $version
     * @return bool
     */
    public function appointUpdateTo($type, $version): bool {
        if ($type == 'framework') {
            $saveDir = SCF_ROOT . '/build';
            if (!is_dir($saveDir) && !mkdir($saveDir, 0775)) {
                Console::warning('【updater】创建更新目录失败');
                return false;
            }
            $client = Http::create(FRAMEWORK_REMOTE_VERSION_SERVER);
            $remoteVersionResponse = $client->get();
            if ($remoteVersionResponse->hasError()) {
                Console::warning('【updater】远程版本获取失败:' . $remoteVersionResponse->getMessage());
                return false;
            }
            $remoteVersion = $remoteVersionResponse->getData();
            $updateFile = $saveDir . '/update.pack';
            $client = Http::create($remoteVersion['url']);
            $downloadResult = $client->download($updateFile, 1800);
            if ($downloadResult->hasError()) {
                Console::warning('【updater】框架升级包下载失败:' . $downloadResult->getMessage());
                return false;
            }
            //下载引导文件
            $bootFile = SCF_ROOT . '/boot';
            $client = Http::create($remoteVersion['boot']);
            $downloadResult = $client->download($bootFile, 1800);
            if ($downloadResult->hasError()) {
                Console::warning('【updater】引导文件下载失败:' . $downloadResult->getMessage());
                return false;
            }
            Log::instance()->info("【Server】框架升级包下载成功:" . $version);
            return true;
        }
        $localVersion = $this->getLocalVersion();
        $current = (int)str_replace('.', '', $localVersion['version']);
        $target = (int)str_replace('.', '', $version);
        if ($current == $target) {
            Console::warning("【Server】已是当前版本:" . $version);
            return false;
        }
        $result = $this->getRemoteVersionsRecord();
        if ($result->hasError()) {
            Console::warning('【updater】' . $result->getMessage());
            return false;
        }
        $versions = $result->getData();
        if (!$versions) {
            Console::warning('【updater】版本清单获取失败');
            return false;
        }
        $versionInfo = null;
        foreach ($versions as $item) {
            if ($item['version'] == $version) {
                $versionInfo = $item;
            }
        }
        if (is_null($versionInfo)) {
            Console::warning('【updater】未匹配到版本记录');
            return false;
        }
        if ($type == 'app' && !$versionInfo['app_object']) {
            Console::warning('【updater】未匹配到内核文件');
            return false;
        } else if ($type == 'public' && !$versionInfo['public_object']) {
            Console::warning('【updater】未匹配到资源文件');
            return false;
        }
        if (!$this->changeAppVersion($version, appoint: $type)) {
            Console::warning('【updater】更新失败');
            return false;
        }
        return true;
    }

    /**
     * APP是否存在新版本
     * @return bool
     */
    public function hasNewAppVersion(): bool {
        if (is_null($this->_version)) {
            $this->getVersion();
        }
        if (!App::isReady() || is_null($this->_version['remote'])) {
            return false;
        }
        return !strcmp($this->_version['local']['version'], $this->_version['remote']['app']['version']) == 0;
    }

    /**
     * SCF是否存在新版本
     * @return bool
     */
    public function hasNewScfVersion(): bool {
        if (is_null($this->_version)) {
            $this->getVersion();
        }
        if (!App::isReady() || is_null($this->_version['remote'])) {
            return false;
        }
        if (!$this->_version['remote']['scf']) {
            return false;
        }
        return !strcmp($this->_version['local']['scf']['version'], $this->_version['remote']['scf']['version']) == 0;
    }

    /**
     * 获取远程服务器版本
     * @return mixed
     */
    public function getRemoteVersion(): mixed {
        if (is_null($this->_version)) {
            $this->getVersion();
        }
        return $this->_version['remote'];
    }

    /**
     * 获取本地应用版本号
     * @return array
     */
    public function getLocalVersion(): array {
        return App::info()->toArray();
    }

    /**
     * 获取远程版本记录
     * @return Result
     */
    public function getRemoteVersionsRecord(): Result {
        $app = App::info();
        if (!$app->update_server) {
            return Result::error('获取版本服务器版本号失败:未设置更新服务器');
        } else {

            $client = Http::create($app->update_server . '?time=' . time());
            $result = $client->get();
            if ($result->hasError()) {
                return Result::error('获取版本服务器版本号失败:' . $result->getMessage());
            } else {
                return Result::success($result->getData('app'));
            }
        }
    }

    /**
     * 版本号检查
     * @return array
     */
    public function getVersion(): array {
        $remote = null;
        $app = App::info();
        if (!$app->update_server) {
            Log::instance()->error('获取版本服务器版本号失败:未设置更新服务器');
            $remote = [];
        } else {
            try {
                $client = Http::create($app->update_server . '?time=' . time());
                $result = $client->get();
                if ($result->hasError()) {
                    Log::instance()->error('获取版本服务器版本号失败:' . $result->getMessage());
                } else {
                    $remote = $result->getData();
                    $remote = [
                        'app' => $remote['app'][0],
                        'scf' => $remote['scf'][0] ?? null,
                    ];
                }
            } catch (Throwable $error) {
                Log::instance()->error('获取版本服务器版本号失败:' . $error->getMessage());
            }
        }
        $local = $this->getLocalVersion();
        $this->_version = compact('local', 'remote');
        return $this->_version;
    }

    /**
     * 是否开启自动更新
     * @return bool
     */
    public function isEnableAutoUpdate(): bool {
        return true;
    }

    /**
     * 清除资源文件夹
     * @param string $dir
     * @return bool
     */
    protected function clearPublic(string $dir = APP_PUBLIC_PATH): bool {
        //先删除目录下的文件：
        $dh = opendir($dir);
        while ($file = readdir($dh)) {
            if ($file != "." && $file != "..") {
                $fullpath = $dir . "/" . $file;
                if (!is_dir($fullpath)) {
                    try {
                        unlink($fullpath);
                    } catch (Throwable) {

                    }
                } else {
                    $this->clearPublic($fullpath);
                }
            }
        }
        closedir($dh);
        return true;
    }
}