<?php

namespace Scf\Core\Traits;

use Scf\Core\Config;
use Scf\Core\Result;
use Scf\Util\Arr;

trait ComponentTrait {
    /**
     *  配置项
     * @var array
     */
    protected array $_config = [];

    /**
     * 构造器
     * @param array|null $conf 配置项
     */
    public function __construct(array $conf = null) {
        $class = static::class;
        $config = is_array($conf) ? $conf : (Config::get('components')[$class] ?? []);
        if ($this->_config) {
            //合并配置
            $this->_config = Arr::merge($this->_config, $config);
        } else {
            //覆盖配置
            foreach ($config as $k => $c) {
                $this->_config[$k] = $c;
            }
        }
        $this->_init();
    }

    /**
     * 模块初始化配置,方法中应确保实例多次调用不存参数副作用
     */
    protected function _init() {

    }

    /**
     * @param array|null $conf 配置项
     * @return static
     */
    final public static function factory(array $conf = null): static {
        $class = static::class;
        return new $class($conf);
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
     *返回所有配置
     * @return array
     */
    public function config(): array {
        return $this->_config;
    }

    /**
     * 动态改变设置
     * @param $key
     * @param $value
     */
    public function setConfig($key, $value): void {
        $this->_config[$key] = $value;
    }

    /**
     * 获取结果
     * @param $result
     * @return Result
     */
    protected function getResult($result): Result {
        return new Result($result);
    }
}