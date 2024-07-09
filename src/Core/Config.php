<?php

namespace Scf\Core;

use Scf\Util\Arr;
use Symfony\Component\Yaml\Yaml;
use Scf\Util\File;

/**
 * 配置管理
 * Class Config
 * @package HuiYun\Config
 */
class Config {

    /**
     * @var array 缓存配置数组
     */
    protected static array $_cache = [];

    /**
     * 初始化
     * 加载系统配置,环境配置
     */
    public static function init(): void {
        self::load(App::src() . '/config/app.php');
        $file = App::src() . '/config/app_' . strtolower(APP_RUN_ENV) . '.php';
        is_file($file) and self::load($file);

    }

    /**
     * 加载一个配置文件合并到配置缓存中
     * @param array|string $path
     */
    public static function load(array|string $path): void {
        $config = is_array($path) ? $path : require($path);
        self::$_cache = Arr::merge(self::$_cache, $config);
    }

    /**
     * 获取一个配置的值,使用.分割的路径访问
     * @param string|null $path
     * @param mixed|null $default
     * @return mixed
     */
    public static function get(string $path = null, mixed $default = null): mixed {
        return is_null($path) ? self::$_cache : Arr::path(self::$_cache, $path, $default, '.');
    }

    /**
     * 动态设置配置,使用.分割的路径访问
     * @param $path
     * @param $value
     * @return mixed
     */
    public static function set($path, $value): mixed {
        Arr::setPath(self::$_cache, $path, $value);
        return $value;
    }

    /**
     * 读取缓存配置
     * @param $name
     * @param string|null $key
     * @return mixed
     */
    public static function getTemp($name, ?string $key = null): mixed {
        $dir = APP_PATH . '/yml';
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        $cacheFile = $dir . '/' . $name . '.yml';
        if (!file_exists($cacheFile)) {
            return null;
        }
        return $key ? Yaml::parseFile($cacheFile)[$key] ?? null : Yaml::parseFile($cacheFile);
    }

    /**
     * 写入缓存配置
     * @param string $name
     * @param string $key
     * @param string|array $value
     * @return bool
     */
    public static function setTemp(string $name, string $key, string|array $value): bool {
        $data = self::getTemp($name);
        $data[$key] = $value;
        return File::write(APP_PATH . '/yml/' . $name . '.yml', Yaml::dump($data));
    }

}