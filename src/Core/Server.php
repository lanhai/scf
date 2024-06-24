<?php

namespace Scf\Core;

abstract class Server {

    protected static Server $_SERVER;
    protected static array $_instances = [];

    abstract public static function create($role, string $host = '0.0.0.0', int $port = 9580);

    /**
     * 获取单例
     * @return static
     * @throws Exception
     */
    public static function instance(): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            throw new Exception('尚未创建服务器对象');
        }
        return self::$_instances[$class];
    }

    /**
     * 向控制台输出消息
     * @param string $str
     */
    protected function log(string $str): void {
        Console::log("【Server】" . $str, false);
    }

}