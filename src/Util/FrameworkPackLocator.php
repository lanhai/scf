<?php

namespace Scf\Util;

/**
 * 负责根据本地 framework 版本记录解析当前应加载的源码包路径。
 *
 * framework 在线升级后的核心诉求不是“永远读固定文件名”，而是像应用 phar
 * 一样，根据本地版本信息选择本机应该挂载的版本包。这个类只处理路径解析与
 * 版本记录读取，不直接参与 boot、下载或重启控制。
 */
class FrameworkPackLocator {

    /**
     * 读取 framework 的本地版本记录。
     *
     * @param string $scfRoot SCF 根目录
     * @return array<string, mixed>|null
     */
    public static function readVersionInfo(string $scfRoot): ?array {
        $versionFile = rtrim($scfRoot, '/') . '/build/framework/version.json';
        if (!is_file($versionFile)) {
            return null;
        }
        $content = @file_get_contents($versionFile);
        if (!is_string($content) || $content === '') {
            return null;
        }
        $data = json_decode($content, true);
        return is_array($data) ? $data : null;
    }

    /**
     * 根据版本号返回 framework 包的目标路径。
     *
     * @param string $scfRoot SCF 根目录
     * @param string|null $version framework 版本号
     * @return string
     */
    public static function packPath(string $scfRoot, ?string $version = null): string {
        $scfRoot = rtrim($scfRoot, '/');
        $version = trim((string)$version);
        if ($version === '' || strtolower($version) === 'development') {
            return $scfRoot . '/build/src.pack';
        }
        return $scfRoot . '/build/framework/' . $version . '.update';
    }

    /**
     * 返回当前 boot 应该加载的 framework 包路径。
     *
     * 若本地版本记录存在且目标包已落地，则优先返回对应版本包；
     * 否则回退到历史兼容用的 `build/src.pack`。
     *
     * @param string $scfRoot SCF 根目录
     * @return string
     */
    public static function currentPackPath(string $scfRoot): string {
        $pack = self::packPath($scfRoot, (string)(self::readVersionInfo($scfRoot)['version'] ?? ''));
        if (is_file($pack)) {
            return $pack;
        }
        return rtrim($scfRoot, '/') . '/build/src.pack';
    }

    /**
     * 返回待生效 framework 更新包的落地路径。
     *
     * @param string $scfRoot SCF 根目录
     * @return string
     */
    public static function pendingPackPath(string $scfRoot): string {
        return self::packPath($scfRoot, (string)(self::readVersionInfo($scfRoot)['version'] ?? ''));
    }
}
