<?php

namespace Scf\Core;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Util\Arr;

trait ComponentTrait {
    /**
     *  配置项
     * @var array
     */
    protected array $_config = [];
    private static array $_instances = [];

    /**
     * 构造器
     * @param array|null $conf 配置项
     */
    public function __construct(array $conf = null) {
        $class = get_called_class();
        $config = is_array($conf) ? $conf : (Config::get('components')[$class] ?? []);
        if (!$this->_config) {
            //合并配置
            $this->_config = Arr::merge($this->_config, $config);
        } else {
            //覆盖配置
            foreach ($config as $k => $c) {
                $this->_config[$k] = $this->_config[$k] ?? $c;
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
     * @param array $conf 配置项
     * @return static
     */
    final public static function factory(array $conf = []): static {
        $class = get_called_class();
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
    public function config() {
        return $this->_config;
    }

    /**
     * 动态改变设置
     * @param $key
     * @param $value
     */
    public function setConfig($key, $value) {
        $this->_config[$key] = $value;
    }

    /**
     * 输出成功结果
     * @param mixed $data
     * @return array
     */
    #[ArrayShape(['errCode' => "int", 'message' => "string", 'data' => "mixed"])]
    protected function success(mixed $data = ''): array {
        return [
            'errCode' => 0,
            'message' => 'SUCCESS',
            'data' => $data
        ];
    }

    /**
     * 输出错误
     * @param $error
     * @param string $code
     * @param mixed $data
     * @return array
     */
    #[ArrayShape(['errCode' => "mixed|string", 'message' => "mixed", 'data' => "array|bool|mixed"])]
    protected function error($error, string $code = 'SERVICE_ERROR', mixed $data = ''): array {
        if ($error instanceof Result) {
            $output = [
                'errCode' => $error->getErrCode(),
                'message' => $error->getMessage(),
                'data' => $error->getData()
            ];
        } else {
            $output = [
                'errCode' => $code,
                'message' => $error,
                'data' => $data
            ];
        }
        return $output;
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