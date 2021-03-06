<?php

namespace Scf\Database;

use Scf\Command\Color;
use Scf\Core\Cache;
use Scf\Core\Console;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Exception\AppError;
use Scf\Mode\Web\Log;
use Scf\Database\Connection\RedisPool;
use Scf\Database\Logger\RedisLogger;
use Scf\Helper\StringHelper;
use Scf\Util\Arr;

class Redis extends Cache {

    protected array $_config = [
        'ttl' => 3600,//缺省生存时间
        'default_server' => 'main',
        'servers' => [
            'main' => [
                'host' => 'localhost',
                'port' => 6379,
                'auth' => '',
                'db_index' => 0,
                'time_out' => 1,
                'size' => 32,
            ]
        ],
    ];
    public string $server = 'main';

    protected \Redis|RedisPool $connection;
    private static array $pools = [];

    /**
     * 创建一个新的连接,此连接在高并发下存在抢锁可能,只适用于非对外业务场景适用
     * @param string $server
     * @return static
     */
    public static function connect(string $server = 'main'): static {
        $instance = self::instance();
        $instance->server = $server;
        $conf = $instance->getConfig('servers')[$server];
        return $instance->getConnection($conf);
    }

    /**
     * 获取一个连接池的连接
     * @param string $server
     * @param string $poolName
     * @return Redis
     */
    public static function pool(string $server = 'main', string $poolName = 'default'): Redis {
        if (!isset(self::$pools[$server . '_' . $poolName])) {
            //Console::log(Color::red('创建连接池'));
            self::$pools[$server . '_' . $poolName] = self::factory()->getPool($server);
        }
        return self::$pools[$server . '_' . $poolName];
    }

    /**
     * @param string|array $server
     * @return Redis
     */
    public function getPool(string|array $server = 'main'): static {
        $config = is_array($server) ? $server : $this->_config['servers'][$server];
        try {
            $this->connection = new RedisPool($config['host'], $config['port'], $config['auth'], 0);
            $maxIdle = $config['max_idle'] ?? 256;        // 最大闲置连接数
            $maxLifetime = 3600;  // 连接的最长生命周期
            $waitTimeout = 0.0;   // 从池获取连接等待的时间, 0为一直等待
            $this->connection->startPool($config['size'], $maxIdle, $maxLifetime, $waitTimeout);
            $logger = new RedisLogger();
            $this->connection->setLogger($logger);
        } catch (\Exception $exception) {
            Console::log(Color::red('Redis连接[' . $config['host'] . ':' . $config['port'] . ']失败：' . $exception->getMessage() . ' ' . $exception->getFile() . '@' . $exception->getLine()));
            exit('REDIS_CONNECT_FAIL');
            //throw new AppError('Redis连接失败：' . $exception->getMessage());
            //die('Redis连接失败：' . $exception->getMessage());
        }
        return $this;
    }
//    /**
//     * 获取一个连接池的连接
//     * @param string $server
//     * @param string $poolName
//     * @return Redis
//     */
//    public static function pool(string $server = 'main', string $poolName = 'default'): Redis {
//        $factory = self::factory();
//        $factory->server = $server;
//        $config = $factory->getConfig('servers')[$server];
//        return $factory->getPool($config, $poolName);
//    }

//    /**
//     * 获取连接池
//     * @param array|null $config
//     * @param string $poolName
//     * @return Redis
//     */
//    public function getPool(array $config = null, string $poolName = 'default'): static {
//        $config = is_array($config) ? $config : $this->_config['servers'][$this->_config['default_server']];
//        $this->poolName = $poolName;
//        $this->connection = Pool::redis($config, $poolName);
//        return $this;
//    }

    /**
     * 连接memcache缓存主机
     * @param array|null $config
     * @return Redis
     */
    public function getConnection(array $config = null): static {
        $config = is_array($config) ? $config : $this->_config['servers'][$this->_config['default_server']];
        $host = $config['host'];
        $port = $config['port'];
        try {
            $connection = new \Redis;
            !$connection->connect($host, $port) and die('redis server connect failed:' . $host . ':' . $port);
            if (!empty($config['auth'])) {
                $connection->auth($config['auth']);
            }
        } catch (\Exception $exception) {
            die ('redis server connect failed:' . $exception->getMessage()) . PHP_EOL;
        }
        $this->connection = $connection;
        return $this;
    }

    /**
     * 读取配置文件
     * @param $key
     * @return array
     */
    public static function config($key): array {
        $instance = parent::instance();
        return $instance->getConfig($key);
    }

    /**
     * 是否可用
     * @return bool
     */
    public static function available(): bool {
        $config = self::config('servers');
        return $config['main']['host'] != "";
    }

    /**
     * 当前缓存服务器状态
     * @return array|false
     */
    public function info(): bool|array {
        return $this->connection->info();
    }

    /**
     * 设置过期时间
     * @param $key
     * @param $ttl
     * @return bool
     */
    public function expire($key, $ttl): bool {
        return $this->connection->expire($key, $ttl);
    }

    /**
     * 减少一个数值
     * @param $key
     * @param $value
     * @param int $timeout
     * @return int
     */
    public function decrementBy($key, $value, int $timeout = 0): int {
        if ($timeout == -1) {
            $timeout = 0;
        } elseif ($timeout == 0) {
            $timeout = $this->_config['ttl'];
        }
        if (!$newValue = $this->connection->decrBy($key, $value)) {
            return $this->connection->set($key, 0, $timeout);
        }
        return $newValue;
    }

    /**
     * 增加一个数值
     * @param $key
     * @param $value
     * @param int $timeout
     * @return int
     */
    public function incrementBy($key, $value, int $timeout = 0): int {
        if ($timeout == -1) {
            $timeout = 0;
        } elseif ($timeout == 0) {
            $timeout = $this->_config['ttl'];
        }
        if (!$newValue = $this->connection->incrBy($key, $value)) {
            return $this->connection->set($key, $value, $timeout);
        }
        return $newValue;
    }

    /**
     * 增加一个数值
     * @param $key
     * @return int
     */
    public function increment($key): int {
        return $this->connection->incr($key);
    }

    /**
     * 减少一个数值
     * @param $key
     * @return int
     */
    public function decrement($key): int {
        return $this->connection->decr($key);
    }

    /**
     * 写入缓存数据
     * @param string $key 缓存标记
     * @param mixed $value 缓存数据
     * @param int $timeout 生存时间 默认0为配置项值 -1为不过期
     * @return boolean
     */
    public function set(string $key, mixed $value, int $timeout = 0): bool {
        if (!is_null($value) && $value !== '') {
            if ($timeout == 0) {
                $timeout = $this->_config['ttl'];
            }
            if (Arr::isArray($value)) {
                $value = JsonHelper::toJson($value);
            }
            return $this->connection->set($key, $value, $timeout);
        } else {
            return false;
        }
    }

    /**
     * 使用自定义命令
     * @param $cmd
     * @param ...$args
     * @return mixed
     */
    public function command($cmd, ...$args): mixed {
        return $this->connection->rawCommand($cmd, ...$args);
    }

    /**
     * 获取key过期时间
     * @param string $key
     * @return bool|int
     */
    public function ttl(string $key): bool|int {
        return $this->connection->ttl($key);
    }

    /**
     * 存储到hash表
     * @param string $cacheKey
     * @param $hashKey
     * @param $value
     * @return bool|int
     */
    public function hset(string $cacheKey, $hashKey, $value): bool|int {
        if ($value !== null && $value !== '') {
            if (Arr::isArray($value)) {
                $value = json_encode($value, JSON_UNESCAPED_UNICODE);
            }
            return $this->connection->hSet($cacheKey, $hashKey, $value);
        } else {
            return false;
        }
    }

    /**
     * 获取存储在哈希表中指定字段的值
     * @param string $cacheKey
     * @param $hashKey
     * @return false|mixed|string
     */
    public function hget(string $cacheKey, $hashKey): mixed {
        $logger = Log::instance();
        try {
            $data = $this->connection->hGet($cacheKey, $hashKey);
            if (!$data) {
                $logger->isDebugEnable() and $logger->hitRedis(false);
                return false;
            }
        } catch (\Exception) {
            return false;
        }
        $logger->isDebugEnable() and $logger->hitRedis();
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    /**
     * 获取存储在哈希表中所有字段的值
     * @param string $cacheKey
     * @return array|false|mixed
     */
    public function hgetAll(string $cacheKey): mixed {
        $logger = Log::instance();
        try {
            $data = $this->connection->hGetAll($cacheKey);
            if (!$data) {
                $logger->isDebugEnable() and $logger->hitRedis(false);
                return false;
            }
        } catch (\Exception) {
            return false;
        }
        $logger->isDebugEnable() and $logger->hitRedis();
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    /**
     * 删除存储在哈希表中指定字段的值
     * @param $cacheKey
     * @param $hashkey
     * @return bool|int
     */
    public function hdel($cacheKey, $hashkey): bool|int {
        return $this->connection->hDel($cacheKey, $hashkey);
    }

    /**
     * 获得缓存数据
     * @param string $cacheKey 缓存标记
     * @return mixed
     */
    public function get(string $cacheKey): mixed {
        $logger = Log::instance();
        try {
            $data = $this->connection->get($cacheKey);
            if ($data === false) {
                $logger->isDebugEnable() and $logger->hitRedis(false);
                return false;
            }
        } catch (\Exception) {
            return false;
        }
        $logger->isDebugEnable() and $logger->hitRedis();
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    /**
     * 获取列表长度
     * @param $key
     * @return int
     */
    public function lLength($key): int {
        return $this->connection->lLen($key);
    }

    /**
     * 读取列表最后一个元素
     * @param $key
     * @return string
     */
    public function lLast($key): string {
        if (!$this->lLength($key)) {
            return '';
        }
        return $this->connection->lRange($key, -1, -1)[0];
    }

    /**
     * 读取列表
     * @param $key
     * @param int $start
     * @param int $end
     * @return array|false
     */
    public function lRange($key, int $start = 0, int $end = -1): bool|array {
        $cache = $this->connection->lRange($key, $start, $end);
        $list = [];
        if ($cache) {
            foreach ($cache as $string) {
                $list[] = JsonHelper::is($string) ? JsonHelper::recover($string) : $string;
            }
        }
        return $list;
    }

    /**
     * 读取全部列表
     * @param $key
     * @return array|false
     */
    public function lAll($key): bool|array {
        return $this->lRange($key, 0, -1);
    }

    /**
     * 在列表头部加入元素
     * @param $key
     * @param $value
     * @return bool|int
     */
    public function lPush($key, $value): bool|int {
        if (Arr::isArray($value)) {
            $value = json_encode($value, JSON_UNESCAPED_UNICODE);
        }
        if (!$count = $this->connection->lPush($key, $value)) {
            return false;
        }
        return $count;
    }

    /**
     * 移除列表开始元素
     * @param $key
     * @return bool|string|array
     */
    public function lPop($key): bool|string|array {
        $result = $this->connection->lPop($key);
        if ($result && JsonHelper::is($result)) {
            $result = JsonHelper::recover($result);
        }
        return $result;
    }

    /**
     * 在列表末尾加入元素
     * @param $key
     * @param $value
     * @return bool|int
     */
    public function rPush($key, $value): bool|int {
        if (Arr::isArray($value)) {
            $value = json_encode($value, JSON_UNESCAPED_UNICODE);
        }
        if (!$count = $this->connection->rPush($key, $value)) {
            return false;
        }
        return $count;
    }

    /**
     * 移除列表末尾元素
     * @param $key
     * @return bool|string|array
     */
    public function rPop($key): bool|string|array {
        $result = $this->connection->rPop($key);
        if ($result && JsonHelper::is($result)) {
            $result = JsonHelper::recover($result);
        }
        return $result;
    }

    /**
     * 移除第一个数据
     * @param string $key
     * @return array|false
     */
    public function blPop(string $key): bool|array {
        return $this->connection->blPop($key, 10);
    }

    /**
     * 移除第一个数据
     * @param string $key
     * @return array|false
     */
    public function brPop(string $key): bool|array {
        return $this->connection->brPop($key, 10);
    }

    /**
     * 删除指定的key
     * @param string $key
     * @return boolean
     */
    public function delete(string $key): bool {
        return $this->connection->del($key);
    }

    /**
     * 清洗缓存
     * @return bool
     */
    public function flush(): bool {
        return $this->connection->flushAll();
    }

    /**
     * 回收连接
     * @return void
     */
    public function close() {
        if ($this->connection instanceof \Redis) {
            $this->connection->close();
        }
//        if (!empty($this->poolName)) {
//            Pool::putRedis($this->connection, $this->poolName);
//        } else {
//            $this->connection->close();
//        }
    }
}
