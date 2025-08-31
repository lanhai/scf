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

    protected array $accounts = [];
    protected string $accessId = "";
    protected string $accessKey = "";

    /**
     * 构造器
     * @param array $config 配置项
     */
    public function __construct(array $config = [], $accounts = [], ?string $serverName = null) {
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
        if ($serverName) {
            $this->_config['default_server'] = $serverName;
        }
        $this->_init();
    }

    /**
     * 模块初始化配置,方法中应确保实例多次调用不存参数副作用
     */
    protected function _init() {

    }

    /**
     * 获取单例
     * @param array|null $conf
     * @param string|null $serverName
     * @return static
     */
    final public static function instance(array $conf = null, ?string $serverName = null): static {
        $class = static::class;
        $_configs = !is_null($conf) ? $conf : Config::get('aliyun');
        $instanceName = $class . ($_configs[$class]['default_server'] ?? '') . ($serverName ?: '');
        if (!isset(self::$_instances[$instanceName])) {
            //$config = is_array($conf) ? Arr::merge($instanceConfig, $conf) : $instanceConfig;
            self::$_instances[$instanceName] = new $class($_configs[$class] ?? [], $_configs['accounts'] ?? [], $serverName);
        }
        return self::$_instances[$instanceName];
    }

}