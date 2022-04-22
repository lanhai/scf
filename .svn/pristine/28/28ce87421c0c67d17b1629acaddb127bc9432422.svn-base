<?php

namespace Scf\Database\Connection;

use Redis;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Database\PDOConfig;
use Swoole\Database\PDOPool;
use Swoole\Database\PDOProxy;
use Swoole\Database\RedisConfig;
use Swoole\Database\RedisPool;
use const DBS_MASTER;

class Pool {
    protected array $redisConfig = [
        'host' => 'localhost',
        'port' => 6379,
        'auth' => '',
        'db_index' => 0,
        'time_out' => 1,
        'size' => 64,
    ];
    protected array $pdoConfig = [
        'host' => 'localhost',
        'port' => 3306,
        'username' => 'root',
        'password' => '',
        'size' => 64,
    ];

    protected array $connections = [
        'redis' => '',
        'pdo' => ''
    ];
    protected RedisPool $redisPools;
    protected PDOPool $pdoPools;

    private static array $instance = [
        'redis' => [],
        'pdo' => []
    ];

    /**
     * 获得一个PDO连接
     * @param array $config
     * @param string $dbName
     * @param int $actor
     * @return PDOProxy
     */
    public static function pdo(array $config, string $dbName = 'default', int $actor = DBS_MASTER): PDOProxy {
        $instanceName = $dbName . '_' . $actor;
        if (empty(self::$instance['pdo'][$instanceName])) {
            if (empty($config)) {
                throw new RuntimeException('pdo config empty');
            }
            $cls = new static();
            $cls->pdoConfig = array_replace_recursive($cls->pdoConfig, $config);
            $cls->pdoPools = new PDOPool((new PDOConfig)
                ->withHost('local.dmool.com')
                ->withPort(3306)
                // ->withUnixSocket('/tmp/mysql.sock')
                ->withDbName('lky_event')
                ->withCharset('utf8mb4')
                ->withUsername('dmool')
                ->withPassword('workgroup')
            );
            self::$instance['pdo'][$instanceName] = $cls;
        }
        $pool = self::$instance['pdo'][$instanceName];
        return $pool->pdoPools->get();
    }

    /**
     * 获得一个Redis连接
     * @param array $config
     * @param string $poolName
     * @return Redis
     */
    public static function redis(array $config, string $poolName = 'default'): Redis {
        $cid = Coroutine::getCid();
        if (empty(self::$instance['redis'][$poolName . '_' . $cid])) {
            if (empty($config)) {
                throw new RuntimeException('redis config empty');
            }
            if (empty($config['size'])) {
                throw new RuntimeException('the size of redis connection pools cannot be empty');
            }
            $cls = new static();
            $cls->redisConfig = array_replace_recursive($cls->redisConfig, $config);
            $cls->redisPools = new RedisPool(
                (new RedisConfig())
                    ->withHost($cls->redisConfig['host'])
                    ->withPort($cls->redisConfig['port'])
                    ->withAuth($cls->redisConfig['auth'])
                    ->withDbIndex($cls->redisConfig['db_index'])
                    ->withTimeout($cls->redisConfig['time_out']),
                $cls->redisConfig['size']
            );
            self::$instance['redis'][$poolName . '_' . $cid] = $cls;// new static($config);
        }
        $pool = self::$instance['redis'][$poolName . '_' . $cid];
        return $pool->redisPools->get();
    }

    /**
     * 回收连接
     * @param null $connection
     * @param string $poolName
     * @return void
     */
    public static function putRedis($connection = null, string $poolName = 'default') {
        $cid = Coroutine::getCid();
        $pool = self::$instance['redis'][$poolName . '_' . $cid];
        $pool->redisPools->put($connection);
    }
}