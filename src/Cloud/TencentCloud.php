<?php

namespace Scf\Cloud;

use Scf\Core\Config;
use Scf\Core\Traits\ComponentTrait;
use Scf\Util\Arr;

abstract class TencentCloud {
    use ComponentTrait;

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
     * 获取单例
     * @param array|null $conf
     * @return static
     */
    final public static function instance(array $conf = null): static {
        $class = static::class;
        $instanceName = $class . ($conf['account'] ?? '');
        if (!isset(self::$_instances[$instanceName])) {
            $_configs = Config::get('tencent');
            $insanceConfig = $_configs[$class] ?? [];
            $config = is_array($conf) ? Arr::merge($insanceConfig, $conf) : $insanceConfig;
            self::$_instances[$instanceName] = new $class($config);
        }
        return self::$_instances[$instanceName];
    }

}