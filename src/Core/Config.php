<?php

namespace Scf\Core;

use Scf\Util\Arr;
use Symfony\Component\Yaml\Yaml;
use Scf\Util\File;
use Throwable;

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
     * @return array
     */
    public static function server(): array {
        $serverConfigFile = App::src() . '/config/server.php';
        $serverConfigYmlFile = App::src() . '/config/server.yml';
        if (is_file($serverConfigFile)) {
            $serverConfig = require $serverConfigFile;
        } elseif (is_file($serverConfigYmlFile)) {
            $serverConfig = Yaml::parseFile($serverConfigYmlFile);
        } else {
            $serverConfig = [
                'port' => 9580,
                'enable_coroutine' => true,
                'worker_num' => 8,
                'worker_memory_limit' => 256,
                'max_wait_time' => 60,
                'task_worker_num' => 4,
                'max_connection' => 4096,//最大连接数
                'max_coroutine' => 10240,//最多启动多少个携程
                'max_concurrency' => 2048,//最高并发
                'max_request_limit' => 128,//每秒最大请求量限制,超过此值将拒绝服务
                'max_mysql_execute_limit' => 128 * 20,//每秒最大mysql处理量限制,超过此值将拒绝服务
                'package_max_length' => 10 * 1024 * 1024,//最大请求数据限制 10M
            ];
        }
        return $serverConfig;
    }

    /**
     * 初始化
     * 加载系统配置,环境配置
     */
    public static function init(): void {
        if (is_file(App::src() . '/config/app.yml') && file_get_contents(App::src() . '/config/app.yml')) {
            $arr = Yaml::parseFile(App::src() . '/config/app.yml');
            self::load(is_array($arr) ? $arr : []);
        } else {
            self::load(App::src() . '/config/app.php');
        }
        if (is_file(App::src() . '/config/app_' . strtolower(SERVER_RUN_ENV) . '.yml') && file_get_contents(App::src() . '/config/app_' . strtolower(SERVER_RUN_ENV) . '.yml')) {
            $arr = Yaml::parseFile(App::src() . '/config/app_' . strtolower(SERVER_RUN_ENV) . '.yml');
            self::load(is_array($arr) ? $arr : []);
        } else {
            $file = App::src() . '/config/app_' . strtolower(SERVER_RUN_ENV) . '.php';
            is_file($file) and self::load($file);
        }
    }

    /**
     * 加载一个配置文件合并到配置缓存中
     * @param array|string $path
     */
    public static function load(array|string $path): void {
        $config = is_array($path) ? $path : (is_file($path) ? require($path) : []);
        try {
            self::$_cache = Arr::merge(self::$_cache, is_array($config) ? $config : []);
        } catch (Throwable $e) {
            Console::error($path . "=>" . $e->getMessage());
        }
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
     * 读取数据库表配置
     * @param $name
     * @param string|null $key
     * @return mixed
     */
    public static function getDbTable($name, ?string $key = null): mixed {
        $dir = App::src() . '/config/db';
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
     * @param string|array $key
     * @param string|array|null $value
     * @return bool
     */
    public static function setDbTable(string $name, string|array $key, string|array|null $value = null): bool {
        $data = self::getDbTable($name);
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $data[$k] = $v;
            }
        } else {
            $data[$key] = $value;
        }
        return File::write(App::src() . '/config/db/' . $name . '.yml', Yaml::dump($data, 3));
    }

}