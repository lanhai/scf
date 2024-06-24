<?php

namespace Scf\Cloud;

use Scf\Core\Config;
use Scf\Util\Arr;

abstract class Aliyun {
    /**
     *  配置项
     * @var array
     */
    protected array $_config = [];
    /**
     * @var array
     */
    private static array $_instances = [];

    protected array $accounts;
    protected string $accessId;
    protected string $accessKey;

    /**
     * 构造器
     * @param array $config 配置项
     */
    public function __construct(array $config = [], $accounts = []) {
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
        $this->accounts = $accounts;
        $this->_init();
    }

    /**
     * 模块初始化配置,方法中应确保实例多次调用不存参数副作用
     */
    protected function _init() {

    }

    /**
     * 获取单利
     * @param array|null $conf
     * @return static
     */
    final public static function instance(array $conf = null): static {
        $class = static::class;
        $instanceName = $class . ($conf['default_server'] ?? '');
        if (!isset(self::$_instances[$instanceName])) {
            $_configs = Config::get('aliyun');
            $insanceConfig = $_configs[$class] ?? [];
            $config = is_array($conf) ? Arr::merge($insanceConfig, $conf) : $insanceConfig;
            self::$_instances[$instanceName] = new $class($config, $_configs['accounts']);
        }
        return self::$_instances[$instanceName];
    }

}