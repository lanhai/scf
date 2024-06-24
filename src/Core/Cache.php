<?php

namespace Scf\Core;

use Scf\Helper\StringHelper;
use Scf\Util\Arr;

abstract class Cache {

    /**
     *  配置项
     * @var array
     */
    protected array $_config = [];
    private static array $_instances = [];

    /**
     * 构造器
     * @param array $config 配置项
     */
    public function __construct(array $config = []) {
        if (!$this->_config) {
            //合并配置
            $this->_config = Arr::merge($this->_config, $config);
        } else {
            //覆盖配置
            foreach ($config as $k => $c) {
                if (isset($this->_config[$k])) {
                    $this->_config[$k] = $c;
                }
            }
        }
        $this->_init();
    }

    /**
     * 模块初始化配置,方法中应确保实例多次调用不存参数副作用
     */
    protected function _init(): void {

    }

    /**
     * 获取单例
     * @param array|null $conf
     * @return static
     */
    public static function instance(array $conf = null): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            $config = is_array($conf) ? $conf : self::loadConfig();
            self::$_instances[$class] = new $class($config);
        }
        return self::$_instances[$class];
    }

    /**
     * @param array|null $conf
     * @return static
     */
    final public static function factory(array $conf = null): static {
        $class = static::class;
        $config = is_array($conf) ? $conf : self::loadConfig();
        return new $class($config);
    }

    /**
     * 加载配置
     * @return array
     */
    protected static function loadConfig(): array {
        $class = static::class;
        $arr = explode("\\", $class);
        $driver = StringHelper::camel2lower(array_pop($arr));
        return Config::get('cache')[$driver] ?? [];
    }

    /**
     * 读取配置
     * @param $key
     * @param mixed|null $default
     * @return mixed
     */
    public function getConfig($key, mixed $default = null): mixed {
        return array_key_exists($key, $this->_config) ? $this->_config[$key] : $default;
    }

    /**
     * 动态改变设置
     * @param $key
     * @param $value
     */
    public function setConfig($key, $value): void {
        $this->_config[$key] = $value;
    }
}
