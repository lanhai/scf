<?php

namespace Scf\Core;

use Throwable;

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
        Console::info("【Server】" . $str, false);
    }

    /**
     * 获取可用端口
     * @param $port
     * @return int
     */
    public static function getUseablePort($port): int {
        try {
            $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
            if ($socket === false) {
                return $port;
            }
            $result = @socket_bind($socket, '0.0.0.0', $port);
            socket_close($socket);
            if ($result === false) {
                return self::getUseablePort($port + 1);
            }
        } catch (Throwable) {
            return self::getUseablePort($port + 1);
        }
        return $port;
    }

}