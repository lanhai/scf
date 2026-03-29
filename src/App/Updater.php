<?php

namespace Scf\App;

use Error;
use PhpZip\ZipFile;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use FilesystemIterator;
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

    /**
     * 业务包/资源包下载的最长等待时间。
     *
     * dashboard 触发的版本升级会同步等待本地 `apply_local_package` 阶段返回，
     * 如果下载层无限等待，gateway worker 会长时间卡在“正在应用本地包”阶段。
     * 这里统一给业务更新下载设置显式超时，避免上游下载抖动时整条升级链挂死。
     */
    public const PACKAGE_DOWNLOAD_TIMEOUT_SECONDS = 300;

    protected static array $_instances;
    protected ?array $_version = null;
    protected ?array $_remoteAppVersions = null;
    protected ?string $_lastError = null;

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

    public function resetLastError(): void {
        $this->_lastError = null;
    }

    public function getLastError(): ?string {
        return $this->_lastError;
    }

    protected function setLastError(string $message): void {
        $this->_lastError = trim($message);
    }

    /**
     * 输出升级链路的细粒度步骤日志。
     *
     * 升级问题经常不是“最终成功/失败”两态，而是卡在版本查询、下载、解压、
     * 原子替换或版本文件写回中的某一步。这里统一用一套前缀把每一步串起来。
     *
     * @param string $message 当前步骤说明
     * @return void
     */
    protected function logUpdateStep(string $message): void {
        Console::info('【updater】' . $message);
    }

    /**
     * 更新应用到最新版本
     * @param bool $isInstall
     * @return bool
     */
    public function updateApp(bool $isInstall = false): bool {
        $this->resetLastError();
        $version = $this->getRemoteVersion();
        if (is_null($version) || !isset($version['app'])) {
            $this->setLastError('获取版本服务器版本号失败');
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
    public function changeAppVersion($version, bool $isInstall = false, ?string $appoint = 'all'): bool {
        $this->resetLastError();
        $this->logUpdateStep("changeAppVersion start: version={$version}, install=" . ($isInstall ? 'yes' : 'no') . ", appoint={$appoint}");
        $requestedVersion = $this->normalizeVersion((string)$version);
        $result = $this->getRemoteVersionsRecord(true);
        if ($result->hasError()) {
            $this->setLastError('获取云端版本号失败:' . $result->getMessage());
            Log::instance()->error('【Server】获取云端版本号失败:' . Color::red($result->getMessage()));
            return false;
        }
        $appVersions = $result->getData();
        $versionInfo = $this->findVersionInfo($appVersions, $requestedVersion, $appoint);
        if (is_null($versionInfo)) {
            $this->setLastError('未查询到版本:' . $version);
            Log::instance()->error('【Server】升级失败:未查询到版本:' . $version);
            return false;
        }
        $this->logUpdateStep("changeAppVersion matched version record: version=" . (string)($versionInfo['version'] ?? '') . ", has_app=" . (!empty($versionInfo['app_object']) ? 'yes' : 'no') . ", has_public=" . (!empty($versionInfo['public_object']) ? 'yes' : 'no'));
        return $this->applyVersionUpdate($versionInfo['version'], $appVersions, $versionInfo, $isInstall, $appoint);
    }

    protected function applyVersionUpdate(string $version, array $appVersions, array $versionInfo, bool $isInstall = false, ?string $appoint = 'all'): bool {
        $this->logUpdateStep("applyVersionUpdate start: version={$version}, install=" . ($isInstall ? 'yes' : 'no') . ", appoint={$appoint}");
        $app = App::info();
        if (!is_null($appoint)) {
            //指定更新
            $appoint == 'app' and $versionInfo['public_object'] = null;
            $appoint == 'public' and $versionInfo['app_object'] = null;
        }
        $publicVersion = $version;
        if ($isInstall) {
            $latestAppVersion = null;
            $latestPublicVersion = null;
            $latestAppPackageVersion = null;
            $latestPublicPackageVersion = null;
            //$versionServer = JsonHelper::recover(File::read(VERSION_FILE));
            foreach ($appVersions as $v) {
                if (!empty($v['app_object']) && is_null($latestAppVersion)) {
                    $latestAppVersion = $v['app_object'];
                    $latestAppPackageVersion = (string)($v['version'] ?? '');
                    $version = $v['version'];
                }
                if (!empty($v['public_object']) && is_null($latestPublicVersion)) {
                    $latestPublicVersion = $v['public_object'];
                    $latestPublicPackageVersion = (string)($v['version'] ?? '');
                    $publicVersion = $v['version'];
                }
            }
            $versionInfo['public_object'] = $latestPublicVersion;
            $versionInfo['app_object'] = $latestAppVersion;
            $requestedInstallVersion = (string)($versionInfo['version'] ?? '');
            if ($latestAppPackageVersion && $latestAppPackageVersion !== $requestedInstallVersion) {
                Console::warning("【updater】安装模式提示: {$requestedInstallVersion} 未提供核心包, 回退使用 {$latestAppPackageVersion} 的核心包");
            }
            if ($latestPublicPackageVersion && $latestPublicPackageVersion !== $requestedInstallVersion) {
                Console::warning("【updater】安装模式提示: {$requestedInstallVersion} 未提供资源包, 回退使用 {$latestPublicPackageVersion} 的资源包");
            }
            if (!$latestAppVersion) {
                Console::warning("【updater】安装模式提示: 当前版本链未找到可用核心包");
            }
            if (!$latestPublicVersion) {
                Console::warning("【updater】安装模式提示: 当前版本链未找到可用资源包");
            }
        }
        $appFile = App::core($version);
        $publicBackupDir = null;
        $appBackupFile = null;
        $publicFilePath = null;
        $publicStageDir = null;
        $updateFilePath = null;
        $appStageFile = null;
        $publicUpdated = false;
        $appUpdated = false;
        $preservePublicBackup = false;
        $preserveAppBackup = false;
        try {
            if ($versionInfo['public_object']) {
                $publicFilePath = APP_UPDATE_DIR . '/app-v' . $version . '.public.zip';
                $publicStageDir = $this->createTemporaryPath(APP_UPDATE_DIR . '/public-stage-' . $version);
                $this->prepareDirectory($publicStageDir);
                $this->logUpdateStep("准备下载资源包: target={$publicFilePath}");
                $this->downloadPackage($versionInfo, $versionInfo['public_object'], $publicFilePath, '资源包');
                $this->logUpdateStep("开始解压资源包: source={$publicFilePath}, target={$publicStageDir}");
                $this->extractZipArchive($publicFilePath, $publicStageDir, $app->app_auth_key);
                if (!$this->directoryHasContent($publicStageDir)) {
                    throw new \RuntimeException('资源包解压后为空');
                }
                $this->logUpdateStep("资源包解压完成: target={$publicStageDir}");
            }
            if ($versionInfo['app_object']) {
                $updateFilePath = APP_UPDATE_DIR . '/app-v' . $version . '.scfupdate';
                $appStageFile = $this->createTemporaryPath(APP_UPDATE_DIR . '/app-v' . $version . '.app');
                $this->logUpdateStep("准备下载源码包: target={$updateFilePath}");
                $this->downloadPackage($versionInfo, $versionInfo['app_object'], $updateFilePath, '源码包');
                $this->logUpdateStep("开始读取源码包: source={$updateFilePath}");
                $updateContent = File::read($updateFilePath);
                if ($updateContent === false) {
                    throw new \RuntimeException('源码包读取失败');
                }
                $code = Auth::decode($updateContent, $app->app_auth_key);
                if ($code === false) {
                    throw new \RuntimeException('源码解析失败');
                }
                if (!File::write($appStageFile, $code)) {
                    throw new \RuntimeException('更新写入失败');
                }
                $this->logUpdateStep("源码包解码完成: stage={$appStageFile}");
            }
            if ($appStageFile) {
                $this->logUpdateStep("开始切换核心文件: target={$appFile}");
                $appBackupFile = $this->replaceFileAtomically($appStageFile, $appFile);
                $appStageFile = null;
                $appUpdated = true;
                $this->logUpdateStep("核心文件切换完成: target={$appFile}");
            }
            if ($publicStageDir) {
                $this->logUpdateStep("开始切换资源目录: target=" . APP_PUBLIC_PATH);
                $publicBackupDir = $this->replaceDirectoryAtomically($publicStageDir, APP_PUBLIC_PATH);
                $publicStageDir = null;
                $publicUpdated = true;
                $this->logUpdateStep("资源目录切换完成: target=" . APP_PUBLIC_PATH);
            }
            if ($publicUpdated || $appUpdated) {
                $installer = App::installer();
                if ($publicUpdated) {
                    $installer->public_version = $publicVersion;
                }
                if ($appUpdated) {
                    $installer->version = $version;
                }
                $installer->updated = date('Y-m-d H:i:s');
                if (!$installer->update()) {
                    throw new \RuntimeException('更新版本配置文件失败');
                }
                $this->logUpdateStep("版本配置文件已更新: version={$version}, public_version={$publicVersion}");
                if ($isInstall && !$installer->isInstalled()) {
                    throw new \RuntimeException('安装未完成: 应用核心文件未就绪');
                }
            }
            $log = [
                'date' => date('Y-m-d H:i:s'),
                'version' => $version,
                'remark' => $versionInfo['remark']
            ];
            File::write(APP_PATH . '/update/update.log', JsonHelper::toJson($log), true);
            clearstatcache();
            $this->logUpdateStep("applyVersionUpdate completed: version={$version}");
            return true;
        } catch (Throwable $e) {
            $this->setLastError($e->getMessage());
            $this->logUpdateStep("applyVersionUpdate failed: version={$version}, error=" . $e->getMessage());
            if ($publicUpdated) {
                if ($this->rollbackDirectory($publicBackupDir, APP_PUBLIC_PATH)) {
                    $publicBackupDir = null;
                } else {
                    $preservePublicBackup = true;
                    Log::instance()->error('【Server】资源目录回滚失败,请检查备份目录:' . Color::red((string)$publicBackupDir));
                }
                $publicUpdated = false;
            }
            if ($appUpdated) {
                if ($this->rollbackFile($appBackupFile, $appFile)) {
                    $appBackupFile = null;
                } else {
                    $preserveAppBackup = true;
                    Log::instance()->error('【Server】核心文件回滚失败,请检查备份文件:' . Color::red((string)$appBackupFile));
                }
                $appUpdated = false;
            }
            Log::instance()->error('【Server】升级失败:' . Color::red($e->getMessage()));
            return false;
        } finally {
            $this->removePath($publicFilePath);
            $this->removePath($updateFilePath);
            $this->removePath($publicStageDir);
            $this->removePath($appStageFile);
            if (!$preservePublicBackup) {
                $this->removePath($publicBackupDir);
            }
            if (!$preserveAppBackup) {
                $this->removePath($appBackupFile);
            }
        }
    }

    /**
     * 自定义更新src/public到指定版本
     * @param $type
     * @param $version
     * @return bool
     */
    public function appointUpdateTo($type, $version): bool {
        $this->resetLastError();
        $this->logUpdateStep("appointUpdateTo start: type={$type}, version={$version}");
        if ($type == 'framework') {
            $saveDir = SCF_ROOT . '/build';
            if (!is_dir($saveDir) && !mkdir($saveDir, 0775)) {
                $this->setLastError('创建更新目录失败');
                Console::warning('【updater】创建更新目录失败');
                return false;
            }
            $client = Http::create(ENV_VARIABLES['scf_update_server']);
            $remoteVersionResponse = $client->get();
            if ($remoteVersionResponse->hasError()) {
                $this->setLastError('远程版本获取失败:' . $remoteVersionResponse->getMessage());
                Console::warning('【updater】远程版本获取失败:' . $remoteVersionResponse->getMessage());
                return false;
            }
            $remoteVersion = $remoteVersionResponse->getData();
            $requestedFrameworkVersion = $this->normalizeVersion((string)$version);
            $downloadUrl = $this->resolveFrameworkPackageDownloadUrl($remoteVersion, $requestedFrameworkVersion);
            if ($downloadUrl === null) {
                $this->setLastError('未匹配到框架升级包下载地址');
                Console::warning('【updater】未匹配到框架升级包下载地址');
                return false;
            }

            $versionedDownloadFile = $saveDir . '/framework-' . ($requestedFrameworkVersion ?: 'latest') . '.download';
            $this->logUpdateStep("开始下载框架升级包: url={$downloadUrl}");
            $client = Http::create($downloadUrl);
            $downloadResult = $client->download($versionedDownloadFile, 1800);
            if ($downloadResult->hasError()) {
                $this->setLastError('框架升级包下载失败:' . $downloadResult->getMessage());
                Console::warning('【updater】框架升级包下载失败:' . $downloadResult->getMessage());
                $this->removePath($versionedDownloadFile);
                return false;
            }
            if (!$this->publishFrameworkPackage($versionedDownloadFile, [
                'version' => $requestedFrameworkVersion ?: (string)($remoteVersion['version'] ?? ''),
                'build' => (string)($remoteVersion['build'] ?? ''),
            ])) {
                $this->removePath($versionedDownloadFile);
                return false;
            }
            //下载引导文件
            $bootFile = SCF_ROOT . '/boot';
            $this->logUpdateStep("开始下载框架引导文件: url=" . (string)($remoteVersion['boot'] ?? ''));
            $client = Http::create($remoteVersion['boot']);
            $downloadResult = $client->download($bootFile, 1800);
            if ($downloadResult->hasError()) {
                $this->setLastError('引导文件下载失败:' . $downloadResult->getMessage());
                Console::warning('【updater】引导文件下载失败:' . $downloadResult->getMessage());
                return false;
            }
            return true;
        }
        $requestedVersion = $this->normalizeVersion((string)$version);
        $currentVersion = $this->normalizeVersion((string)$this->getCurrentInstalledVersionByType($type));
        if ($requestedVersion !== '' && !is_null($currentVersion) && strcmp($currentVersion, $requestedVersion) === 0) {
            $this->setLastError('已是当前版本:' . $version);
            Console::warning("【Server】已是当前版本:" . $version);
            return false;
        }
        // 升级是长驻节点触发，不能复用旧缓存，否则新发布版本会查不到。
        $result = $this->getRemoteVersionsRecord(true);
        if ($result->hasError()) {
            $this->setLastError($result->getMessage());
            Console::warning('【updater】' . $result->getMessage());
            return false;
        }
        $versions = $result->getData();
        if (!$versions) {
            $this->setLastError('版本清单获取失败');
            Console::warning('【updater】版本清单获取失败');
            return false;
        }
        $versionInfo = $this->findVersionInfo($versions, $requestedVersion, $type);
        if (is_null($versionInfo)) {
            $this->setLastError("未匹配到版本记录:type={$type},version={$requestedVersion}");
            Console::warning("【updater】未匹配到版本记录:type={$type},version={$requestedVersion}");
            return false;
        }
        $this->logUpdateStep("appointUpdateTo matched version: version=" . (string)($versionInfo['version'] ?? '') . ", type={$type}, has_app=" . (!empty($versionInfo['app_object']) ? 'yes' : 'no') . ", has_public=" . (!empty($versionInfo['public_object']) ? 'yes' : 'no'));
        if ($type == 'app' && !$versionInfo['app_object']) {
            $this->setLastError('未匹配到内核文件');
            Console::warning('【updater】未匹配到内核文件');
            return false;
        } else if ($type == 'public' && !$versionInfo['public_object']) {
            $this->setLastError('未匹配到资源文件');
            Console::warning('【updater】未匹配到资源文件');
            return false;
        }
        if (!$this->applyVersionUpdate($versionInfo['version'], $versions, $versionInfo, false, $type)) {
            Console::warning('【updater】更新失败');
            return false;
        }
        $this->logUpdateStep("appointUpdateTo completed: type={$type}, version={$version}");
        return true;
    }

    /**
     * 解析 framework 指定版本的下载地址。
     *
     * 远端 `version.json` 默认只给出“当前最新版本”的直链，但 dashboard 升级链路
     * 传进来的是明确的目标版本。这里优先使用远端原始 URL；如果它指向的并不是
     * 当前请求版本，则按同目录 `{version}.update` 规则回推历史版本包地址。
     *
     * @param array $remoteVersion 远端版本清单中的 framework 记录
     * @param string $requestedVersion 用户请求的目标版本
     * @return string|null
     */
    protected function resolveFrameworkPackageDownloadUrl(array $remoteVersion, string $requestedVersion): ?string {
        $downloadUrl = trim((string)($remoteVersion['url'] ?? ''));
        if ($downloadUrl === '') {
            return null;
        }

        $remoteVersionNumber = $this->normalizeVersion((string)($remoteVersion['version'] ?? ''));
        if ($requestedVersion === '' || $remoteVersionNumber === '' || $requestedVersion === $remoteVersionNumber) {
            return $downloadUrl;
        }

        $directory = rtrim((string)dirname($downloadUrl), '/');
        if ($directory === '' || $directory === '.') {
            return null;
        }

        return $directory . '/' . $requestedVersion . '.update';
    }

    /**
     * 将下载完成的 framework 包发布到版本化 runtime 目录。
     *
     * framework 升级从这里开始不再写 `build/update.pack`，而是直接把下载文件
     * 发布为 `build/framework/packs/<version>.pack` 并更新 active 指针。
     * 这样当前正在运行的 gateway 不会被固定文件名热切换影响，后续新启动的
     * gateway/upstream 则统一从 active 指向的最新版本 pack 启动。
     *
     * @param string $downloadFile 下载完成的临时文件
     * @param array $versionInfo 目标版本元数据
     * @return bool
     */
    protected function publishFrameworkPackage(string $downloadFile, array $versionInfo): bool {
        if (function_exists('scf_publish_framework_pack')) {
            $publishError = null;
            $record = \scf_publish_framework_pack($downloadFile, $versionInfo, true, $publishError);
            if ($record) {
                $this->logUpdateStep('框架升级包已发布到版本化目录: version=' . (string)($record['version'] ?? ''));
                return true;
            }
            $this->setLastError('框架升级包发布失败:' . (string)$publishError);
            Console::warning('【updater】框架升级包发布失败:' . (string)$publishError);
            return false;
        }

        $fallbackTarget = SCF_ROOT . '/build/src.pack';
        if (!@rename($downloadFile, $fallbackTarget) && !@copy($downloadFile, $fallbackTarget)) {
            $this->setLastError('框架升级包发布失败: fallback src.pack 写入失败');
            Console::warning('【updater】框架升级包发布失败: fallback src.pack 写入失败');
            return false;
        }
        @unlink($downloadFile);
        $this->logUpdateStep('框架升级包已回退发布到 src.pack');
        return true;
    }

    protected function getCurrentInstalledVersionByType(string $type): ?string {
        $localVersion = $this->getLocalVersion();
        return match ($type) {
            'public' => $localVersion['public_version'] ?? null,
            default => $localVersion['version'] ?? null,
        };
    }

    protected function findVersionInfo(array $versions, string $version, ?string $type = 'all'): ?array {
        $normalizedVersion = $this->normalizeVersion($version);
        if ($normalizedVersion === '') {
            return null;
        }
        $matched = [];
        foreach ($versions as $item) {
            if ($this->normalizeVersion((string)($item['version'] ?? '')) === $normalizedVersion) {
                $matched[] = $item;
            }
        }
        if (!$matched) {
            return null;
        }
        foreach ($matched as $item) {
            if ($this->versionRecordMatchesType($item, $type)) {
                return $item;
            }
        }
        return $matched[0];
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
    public function getRemoteVersionsRecord(bool $refresh = false): Result {
        return $this->fetchRemoteAppVersions($refresh);
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
        if (!is_dir($dir)) {
            return true;
        }
        $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS),
            RecursiveIteratorIterator::CHILD_FIRST
        );
        foreach ($iterator as $item) {
            try {
                if ($item->isDir()) {
                    @rmdir($item->getPathname());
                } else {
                    @unlink($item->getPathname());
                }
            } catch (Throwable) {

            }
        }
        return true;
    }

    protected function prepareDirectory(string $dir): void {
        if (is_dir($dir)) {
            $this->removePath($dir);
        }
        if (!mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new \RuntimeException('创建临时目录失败:' . $dir);
        }
    }

    protected function createTemporaryPath(string $prefix): string {
        return $prefix . '.' . date('YmdHis') . '.' . bin2hex(random_bytes(4));
    }

    protected function downloadPackage(array $versionInfo, string $remotePath, string $savePath, string $label): void {
        $host = $versionInfo['server'];
        $port = $versionInfo['port'] ?? 80;
        $this->logUpdateStep("downloadPackage start: label={$label}, host={$host}, port={$port}, path={$remotePath}, save={$savePath}, timeout=" . self::PACKAGE_DOWNLOAD_TIMEOUT_SECONDS . "s");
        $client = new Client($host, $port, $port == 443);
        $client->set(['timeout' => self::PACKAGE_DOWNLOAD_TIMEOUT_SECONDS]);
        $client->setHeaders([
            'Host' => $host,
            'User-Agent' => 'Chrome/49.0.2587.3',
            'Accept' => '*',
            'Accept-Encoding' => 'gzip'
        ]);
        $client->download($remotePath, $savePath);
        $statusCode = $client->getStatusCode();
        $this->logUpdateStep("downloadPackage response: label={$label}, status={$statusCode}, err=" . (string)($client->errMsg ?: ''));
        if ($statusCode !== 200) {
            $message = $client->errMsg ?: 'unknown error';
            throw new \RuntimeException($label . '下载失败:' . $message . '(' . $statusCode . ')');
        }
        if (!file_exists($savePath) || filesize($savePath) <= 0) {
            throw new \RuntimeException($label . '下载失败:文件为空');
        }
        $this->logUpdateStep("downloadPackage completed: label={$label}, bytes=" . (int)filesize($savePath));
    }

    protected function replaceFileAtomically(string $source, string $target): ?string {
        $backup = null;
        if (file_exists($target)) {
            $backup = $this->createTemporaryPath($target . '.bak');
            if (!@rename($target, $backup)) {
                throw new \RuntimeException('备份旧核心文件失败');
            }
        }
        if (!@rename($source, $target)) {
            if ($backup && file_exists($backup)) {
                @rename($backup, $target);
            }
            throw new \RuntimeException('切换核心文件失败');
        }
        return $backup;
    }

    protected function replaceDirectoryAtomically(string $sourceDir, string $targetDir): ?string {
        $backupDir = null;
        if (is_dir($targetDir)) {
            $backupDir = $this->createTemporaryPath($targetDir . '.bak');
            if (!@rename($targetDir, $backupDir)) {
                throw new \RuntimeException('备份旧资源目录失败');
            }
        }
        if (!@rename($sourceDir, $targetDir)) {
            if ($backupDir && is_dir($backupDir)) {
                @rename($backupDir, $targetDir);
            }
            throw new \RuntimeException('切换资源目录失败');
        }
        return $backupDir;
    }

    protected function rollbackDirectory(?string $backupDir, string $targetDir): bool {
        $this->removePath($targetDir);
        if (!$backupDir) {
            return true;
        }
        if (!is_dir($backupDir)) {
            return false;
        }
        return @rename($backupDir, $targetDir);
    }

    protected function rollbackFile(?string $backupFile, string $targetFile): bool {
        if (file_exists($targetFile)) {
            @unlink($targetFile);
        }
        if (!$backupFile) {
            return true;
        }
        if (!file_exists($backupFile)) {
            return false;
        }
        return @rename($backupFile, $targetFile);
    }

    protected function removePath(?string $path): void {
        if (!$path || !file_exists($path)) {
            return;
        }
        if (is_dir($path)) {
            $this->clearPublic($path);
            @rmdir($path);
            return;
        }
        @unlink($path);
    }

    protected function directoryHasContent(string $dir): bool {
        if (!is_dir($dir)) {
            return false;
        }
        $files = scandir($dir);
        if ($files === false) {
            return false;
        }
        foreach ($files as $file) {
            if ($file !== '.' && $file !== '..') {
                return true;
            }
        }
        return false;
    }

    protected function normalizeVersion(string $version): string {
        $version = trim($version);
        if ($version === '') {
            return '';
        }
        return ltrim($version, "vV");
    }

    protected function versionRecordMatchesType(array $item, ?string $type): bool {
        return match ($type) {
            'app' => !empty($item['app_object']),
            'public' => !empty($item['public_object']),
            default => true,
        };
    }

    protected function fetchRemoteAppVersions(bool $refresh = false): Result {
        if (!$refresh && !is_null($this->_remoteAppVersions)) {
            return Result::success($this->_remoteAppVersions);
        }
        $app = App::info();
        if (!$app->update_server) {
            return Result::error('获取版本服务器版本号失败:未设置更新服务器');
        }
        try {
            $client = Http::create($app->update_server . '?time=' . time());
            $result = $client->get();
            if ($result->hasError()) {
                return Result::error('获取版本服务器版本号失败:' . $result->getMessage());
            }
            $this->_remoteAppVersions = $result->getData('app') ?: [];
            return Result::success($this->_remoteAppVersions);
        } catch (Error $error) {
            return Result::error('获取版本服务器版本号失败:' . $error->getMessage());
        }
    }

    /**
     * 优先使用 ZipArchive 直接从文件解压，避免把整个资源包读入内存。
     * 老环境没有 ZipArchive 时再回退到原有实现。
     */
    protected function extractZipArchive(string $archivePath, string $targetDir, ?string $password = null): void {
        if (class_exists(\ZipArchive::class)) {
            $archive = new \ZipArchive();
            $opened = $archive->open($archivePath);
            if ($opened === true) {
                try {
                    if (!empty($password)) {
                        $archive->setPassword($password);
                    }
                    if (!$archive->extractTo($targetDir)) {
                        throw new \RuntimeException('ZipArchive extract failed');
                    }
                    return;
                } finally {
                    $archive->close();
                }
            }
        }
        $stream = File::read($archivePath);
        if ($stream === false) {
            throw new \RuntimeException('升级包读取失败');
        }
        $zipFile = new ZipFile();
        try {
            $zipFile->openFromString($stream);
            if (!empty($password)) {
                $zipFile->setReadPassword($password);
            }
            $zipFile->extractTo($targetDir);
        } finally {
            $zipFile->close();
        }
    }
}
