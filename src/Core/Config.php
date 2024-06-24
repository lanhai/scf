<?php

namespace Scf\Core;

use Scf\Util\Arr;

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
        self::load(App::src() . 'config/app.php');
        $file = App::src() . 'config/app_' . strtolower(APP_RUN_ENV) . '.php';
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

}