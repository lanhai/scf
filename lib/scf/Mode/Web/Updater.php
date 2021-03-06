<?php

namespace Scf\Mode\Web;

use PhpZip\Exception\ZipException;
use PhpZip\ZipFile;
use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Helper\JsonHelper;
use Scf\Server\Core;
use Scf\Util\Auth;
use Scf\Util\File;
use Swoole\Coroutine\Http\Client;

class Updater {

    protected static array $_instances;
    protected ?array $_version = null;

    /**
     * @return Updater
     */
    public static function instance(): static {
        $class = get_called_class();
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
            Log::instance()->info('升级失败:获取版本服务器版本号失败!');
            return false;
        }
        return $this->changeAppVersion($version['app']['version'], $isInstall);
    }

    /**
     * 切换应用到指定版本
     * @param $version
     * @param bool $isInstall
     * @return false
     */
    public function changeAppVersion($version, bool $isInstall = false): bool {
        $appFile = App::pharPath($version);
        $versionInfo = null;
        $app = \Scf\Core\App::info();
        if (!$app->update_server) {
            Log::instance()->error('应用未设置更新服务器');
            return false;
        }
        try {
            $client = Http::create($app->update_server . '?time=' . time());
            $result = $client->get();
            if ($result->hasError()) {
                Log::instance()->error('获取云端版本号失败:' . Color::red($result->getMessage()));
                return false;
            }
            $remote = $result->getData();
            $appVersion = $remote['app'];
        } catch (\Error $error) {
            Log::instance()->error('获取版本服务器版本号失败:' . $error->getMessage());
            return false;
        }
        foreach ($appVersion as $v) {
            if (strcmp($version, $v['version']) == 0) {
                $versionInfo = $v;
                break;
            }
        }
        if (is_null($versionInfo)) {
            Log::instance()->error('升级失败:未查询到版本:' . $version);
            return false;
        }
        if ($isInstall) {
            $latestAppVersion = null;
            $latestPublicVersion = null;
            //$versionServer = JsonHelper::recover(File::read(VERSION_FILE));
            foreach ($appVersion as $v) {
                if (!empty($v['app_object']) && is_null($latestAppVersion)) {
                    $latestAppVersion = $v['app_object'];
                }
                if (!empty($v['public_object']) && is_null($latestPublicVersion)) {
                    $latestPublicVersion = $v['public_object'];
                }
            }
            $versionInfo['public_object'] = $latestPublicVersion;
            $versionInfo['app_object'] = $latestAppVersion;
        }
        if ($versionInfo['public_object']) {
            Log::instance()->info("开始下载資源包:" . $versionInfo['server'] . $versionInfo['public_object']);
            $publicFilePath = APP_UPDATE_DIR . 'app-v' . $version . '.public.zip';
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
                Log::instance()->error('升级失败:资源包下载失败:' . $client->errMsg . '(' . $code . ')');
                return false;
            }
            //Log::instance()->info("资源包下载完成:" . $publicFilePath);
            //解压缩
            $zipFile = new ZipFile();
            try {
                $this->clearPublic();
                $stream = File::read($publicFilePath);
                $zipFile->openFromString($stream)->setReadPassword(APP_AUTH_KEY)->extractTo(APP_PUBLIC_PATH);
                Log::instance()->info("资源包解押成功");
            } catch (ZipException $e) {
                Log::instance()->error("资源包解压失败:" . Color::red($e->getMessage()));
                return false;
            } finally {
                $zipFile->close();
            }
        }
        if ($versionInfo['app_object']) {
            Log::instance()->info("开始下载源码包:" . $versionInfo['server'] . $versionInfo['app_object']);
            $updateFilePath = APP_UPDATE_DIR . 'app-v' . $version . '.scfupdate';
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
                Log::instance()->error('升级失败:源码包下载失败:' . $client->errMsg . '(' . $code . ')');
                return false;
            }
            Log::instance()->info("开始写入文件:" . $appFile);
            //Log::instance()->info("源码包下载完成:" . $updateFilePath);
            if (file_exists($appFile)) {
                unlink($appFile);
            }
            $updateContent = File::read($updateFilePath);
            if (!$code = Auth::decode($updateContent)) {
                Log::instance()->error('升级失败:源码解析失败');
                return false;
            }
            if (!File::write($appFile, $code)) {
                Log::instance()->error('升级失败:更新写入失败');
                return false;
            }
            Log::instance()->info("源码包更新成功");
        }
        //更新本地配置文件
        if (!\Scf\Core\App::updateVersionFile(APP_ID, $version)) {
            Log::instance()->error('升级失败:更新版本配置文件失败');
            return false;
        }
        $log = [
            'date' => date('Y-m-d H:i:s'),
            'version' => $version,
            'remark' => $versionInfo['remark']
        ];
        File::write(APP_PATH . 'update/update.log', JsonHelper::toJson($log), true);
        //Log::instance()->info("已更新至版本:" . $version);
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
        if (is_null($this->_version['remote'])) {
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
        if (is_null($this->_version['remote'])) {
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
     * 版本号检查
     * @return array
     */
    public function getVersion(): array {
        $remote = null;
        $app = \Scf\Core\App::info();
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
            } catch (\Error $error) {
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
                    unlink($fullpath);
                } else {
                    $this->clearPublic($fullpath);
                }
            }
        }
        closedir($dh);
        return true;
    }
}