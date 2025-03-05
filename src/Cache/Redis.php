<?php

namespace Scf\Cache;

use RedisException;
use Scf\Cache\Connection\RedisPool;
use Scf\Cache\Logger\RedisLogger;
use Scf\Core\Cache;
use Scf\Core\Console;
use Scf\Database\Exception\NullPool;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Exception\AppError;
use Scf\Server\Http;
use Scf\Server\Worker\ProcessLife;
use Scf\Util\Arr;
use Throwable;

class Redis extends Cache {
    protected string $keyPrefix = '';

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
     * 创建一个新的连接,此连接在高并发下存在抢锁可能,只适用于非对外业务场景
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
     * 连接缓存主机
     * @param array|null $config
     * @return Redis
     */
    private function getConnection(array $config = null): static {
        $config = is_array($config) ? $config : $this->_config['servers'][$this->_config['default_server']];
        $host = $config['host'];
        $port = $config['port'];
        $this->keyPrefix = $config["key_prefix"] ?? APP_ID;
        try {
            $connection = new \Redis;
            !$connection->connect($host, $port) and die('redis server connect failed:' . $host . ':' . $port);
            if (!empty($config['auth'])) {
                $connection->auth($config['auth']);
            }
        } catch (RedisException $exception) {
            die ('redis server connect failed:' . $exception->getMessage()) . PHP_EOL;
        }
        $this->connection = $connection;
        $logger = new RedisLogger();
        $this->connection->setLogger($logger);
        return $this;
    }

    /**
     * 获取一个连接池的连接
     * @param string $server
     * @param string $poolName
     * @return Redis|NullPool
     */
    public static function pool(string $server = 'main', string $poolName = 'default'): Redis|NullPool {
        if (!isset(self::$pools[$server . '_' . $poolName])) {
            try {
                self::$pools[$server . '_' . $poolName] = self::factory()->createPool($server);
            } catch (Throwable $exception) {
                return new NullPool($server, $exception->getMessage());
            }
        }
        return self::$pools[$server . '_' . $poolName];
    }

    /**
     * 动态方法创建连接池
     * @param ?array $config
     * @return Redis|NullPool
     */
    public function create(?array $config = null): Redis|NullPool {
        $config = is_array($config) ? $config : $this->_config['servers']['main'];
        if (!isset(self::$pools[md5($config['host'])])) {
            try {
                self::$pools[md5($config['host'])] = $this->createPool($config);
            } catch (Throwable $exception) {
                return new NullPool($config['host'], $exception->getMessage());
            }
        }
        return self::$pools[md5($config['host'])];
    }

    /**
     * @param string|array $server
     * @return Redis
     * @throws AppError
     */
    private function createPool(string|array $server = 'main'): static {
        $config = is_array($server) ? $server : $this->_config['servers'][$server];
        $httpServer = Http::master();
        $isTaskWorker = !is_null($httpServer) && $httpServer->taskworker;
        if ($isTaskWorker && (!isset($config['task_worker_enable']) || !$config['task_worker_enable'])) {
            return static::connect($server);
        }
        try {
            if (!$config['host']) {
                throw new AppError('未配置redis服务器地址');
            }
            $this->connection = new RedisPool($config['host'], $config['port'], $config['auth'], 0);
            if ($isTaskWorker) {
                $maxOpen = $config['task_worker_max_open'] ?? 2;
                $maxIdle = $config['task_worker_max_idle'] ?? 1;
            } else {
                $maxOpen = $config['size'] ?? 4;//最大开启连接数
                $maxIdle = $config['max_idle'] ?? 2;// 最大闲置连接数
            }
            $maxLifetime = $config['max_life_time'] ?? 600;  // 连接的最长生命周期
            $waitTimeout = $config['wait_timeout'] ?? 0.0;   // 从池获取连接等待的时间, 0为一直等待
            $this->connection->start($maxOpen, $maxIdle, $maxLifetime, $waitTimeout);
            $logger = new RedisLogger();
            $this->connection->setLogger($logger);
            $this->keyPrefix = $config["key_prefix"] ?? APP_ID;
        } catch (RedisException $exception) {
            $msg = '【Redis】[' . $config['host'] . ':' . $config['port'] . ']创建连接池失败：' . $exception->getMessage();
            throw new AppError($msg);
        }
        return $this;
    }

    /**
     * 获取连接池状态
     * @return array
     */
    public function poolStats(): array {
        return $this->connection->poolStats();
    }

    /**
     * 获取一个原生驱动连接
     * @return RedisPool|\Redis
     */
    public function conn(): RedisPool|\Redis {
        return $this->connection;
    }

    /**
     * 读取配置文件
     * @param $configKey
     * @return array
     */
    public static function config($configKey): array {
        $instance = parent::instance();
        return $instance->getConfig($configKey);
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
        try {
            return $this->connection->info();
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 设置过期时间
     * @param $key
     * @param $ttl
     * @return bool
     */
    public function expire($key, $ttl): bool {
        try {
            return $this->connection->expire($this->setPrefix($key), $ttl);
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 移除过期时间
     * @param $key
     * @return bool
     */
    public function persist($key): bool {
        try {
            return $this->connection->persist($this->setPrefix($key));
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 减少一个数值
     * @param $key
     * @param $value
     * @param int $timeout
     * @return int|bool
     */
    public function decrementBy($key, $value, int $timeout = 0): int|bool {
        try {
            if ($timeout == 0) {
                $timeout = $this->_config['ttl'];
            }
            if (!$newValue = $this->connection->decrBy($this->setPrefix($key), $value)) {
                if ($timeout == -1) {
                    return $this->connection->set($this->setPrefix($key), 0);
                }
                return $this->connection->set($this->setPrefix($key), 0, $timeout);
            }
            return $newValue;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 增加一个数值
     * @param $key
     * @param $value
     * @param int $timeout
     * @return int|bool
     */
    public function incrementBy($key, $value, int $timeout = 0): int|bool {
        try {
            if ($timeout == 0) {
                $timeout = $this->_config['ttl'];
            }
            if (!$newValue = $this->connection->incrBy($this->setPrefix($key), $value)) {
                if ($timeout == -1) {
                    return $this->connection->set($this->setPrefix($key), 0);
                }
                return $this->connection->set($this->setPrefix($key), $value, $timeout);
            }
            return $newValue;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 增加一个数值
     * @param $key
     * @return int|bool
     */
    public function increment($key): int|bool {
        try {

            return $this->connection->incr($this->setPrefix($key));
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 减少一个数值
     * @param $key
     * @return int|bool
     */
    public function decrement($key): int|bool {
        try {
            return $this->connection->decr($this->setPrefix($key));
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 实现分布式锁或确保某些操作只执行一次
     * @param $key
     * @param int $expire
     * @return bool
     */
    public function lock($key, int $expire = 5): bool {
        try {
            if ($this->connection->get($key) || $this->connection->setNX($key, time()) === false) {
                return false;
            }
            $this->connection->expire($key, $expire);
            return true;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 锁写入
     * @param $key
     * @param mixed $value
     * @return bool
     */
    public function setNX($key, mixed $value): bool {
        try {
            if (!is_null($value) && $value !== '') {
                if (Arr::isArray($value)) {
                    $value = JsonHelper::toJson($value);
                }
                return $this->connection->setnx($this->setPrefix($key), $value);
            } else {
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 读取并设置过期
     * @param $key
     * @param int $timeout
     * @return bool
     */
    public function getEx($key, int $timeout): bool {
        $logger = ProcessLife::instance();
        $data = $this->connection->getEx($this->setPrefix($key), ['EX' => $timeout]);
        if ($data === false) {
            $logger->hitRedis(false);
            return false;
        }
        $logger->hitRedis();
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    /**
     * 写入缓存数据
     * @param string $key 缓存标记
     * @param mixed $value 缓存数据
     * @param int $timeout 生存时间 默认0为配置项值 -1为不过期
     * @return boolean
     */
    public function set(string $key, mixed $value, int $timeout = 0): bool {
        try {
            if (!is_null($value) && $value !== '') {
                if ($timeout == 0) {
                    $timeout = $this->_config['ttl'];
                }
                if (Arr::isArray($value)) {
                    $value = JsonHelper::toJson($value);
                }
                if ($timeout == -1) {
                    return $this->connection->set($this->setPrefix($key), $value);
                }
                return $this->connection->set($this->setPrefix($key), $value, $timeout);
            } else {
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
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
        try {
            return $this->connection->rawCommand($cmd, ...$args);
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 获取key过期时间
     * @param string $key
     * @return bool|int
     */
    public function ttl(string $key): bool|int {

        try {
            return $this->connection->ttl($this->setPrefix($key));
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 添加集合
     * @param string $key
     * @param ...$hashKey
     * @return bool|int|\Redis
     */
    public function sAdd(string $key, ...$hashKey): bool|int|\Redis {
        try {
            if ($hashKey !== '') {
                return $this->connection->sAdd($this->setPrefix($key), ...$hashKey);
            } else {
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 移除集合成员
     * @param string $key
     * @param ...$hashKey
     * @return false|int|\Redis
     */
    public function sRemove(string $key, ...$hashKey): bool|int|\Redis {
        try {
            if ($hashKey !== '') {
                return $this->connection->sRem($this->setPrefix($key), ...$hashKey);
            } else {
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 取集合成员数
     * @param $key
     * @return \Redis|int|bool
     */
    public function sCard($key): \Redis|int|bool {
        $logger = ProcessLife::instance();
        try {
            $data = $this->connection->sCard($this->setPrefix($key));
            if (!$data) {
                $logger->hitRedis(false);
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
        $logger->hitRedis();
        return $data;
    }

    /**
     * 判断是否集合成员
     * @param $key
     * @param $member
     * @return bool|\Redis
     */
    public function sIsMember($key, $member): bool|\Redis {
        try {
            return $this->connection->sIsMember($this->setPrefix($key), $member);
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 获取集合所有成员
     * @param $key
     * @return array|false|\Redis
     */
    public function sMembers($key): bool|array|\Redis {
        $logger = ProcessLife::instance();
        try {
            $data = $this->connection->sMembers($this->setPrefix($key));
            if (!$data) {
                $logger->hitRedis(false);
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
        $logger->hitRedis();
        return $data;
    }

    /**
     * 存储到hash表
     * @param string $key
     * @param $hashKey
     * @param $value
     * @return bool|int
     */
    public function hset(string $key, $hashKey, $value): bool|int {
        try {

            if ($value !== null && $value !== '') {
                if (Arr::isArray($value)) {
                    $value = json_encode($value, JSON_UNESCAPED_UNICODE);
                }
                return $this->connection->hSet($this->setPrefix($key), $hashKey, $value);
            } else {
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 增加hash表字段值
     * @param string $key
     * @param $hashKey
     * @param $value
     * @return bool|int
     */
    public function hInc(string $key, $hashKey, $value): bool|int {
        try {

            if ($value !== null && $value !== '') {
                if (Arr::isArray($value)) {
                    $value = json_encode($value, JSON_UNESCAPED_UNICODE);
                }
                return $this->connection->hIncrBy($this->setPrefix($key), $hashKey, $value);
            } else {
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 获取存储在哈希表中指定字段的值
     * @param string $key
     * @param $hashKey
     * @return false|mixed|string
     */
    public function hget(string $key, $hashKey): mixed {

        $logger = ProcessLife::instance();
        try {
            $data = $this->connection->hGet($this->setPrefix($key), $hashKey);
            if (!$data) {
                $logger->hitRedis(false);
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
        $logger->hitRedis();
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    /**
     * 获取存储在哈希表中所有字段的值
     * @param string $key
     * @return array|false|mixed
     */
    public function hgetAll(string $key): mixed {

        $logger = ProcessLife::instance();
        try {
            $data = $this->connection->hGetAll($this->setPrefix($key));
            if (!$data) {
                $logger->hitRedis(false);
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
        $logger->hitRedis();
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    /**
     * 删除存储在哈希表中指定字段的值
     * @param $key
     * @param $hashkey
     * @return bool|int
     */
    public function hdel($key, $hashkey): bool|int {

        try {
            return $this->connection->hDel($this->setPrefix($key), $hashkey);
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 获得缓存数据
     * @param string $key 缓存标记
     * @return mixed
     */
    public function get(string $key): mixed {
        $logger = ProcessLife::instance();
        try {
            $data = $this->connection->get($this->setPrefix($key));
            if ($data === false) {
                $logger->hitRedis(false);
                return false;
            }
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
        $logger->hitRedis();
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    /**
     * 获取列表长度
     * @param $key
     * @return int
     */
    public function lLength($key): int {

        try {
            return $this->connection->lLen($this->setPrefix($key));
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return 0;
        }
    }

    /**
     * 读取列表最后一个元素
     * @param $key
     * @return string
     */
    public function lLast($key): string {
        try {

            if (!$this->lLength($this->setPrefix($key))) {
                return '';
            }
            return $this->connection->lRange($this->setPrefix($key), -1, -1)[0];
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 读取列表
     * @param $key
     * @param int $start
     * @param int $end
     * @return array|false
     */
    public function lRange($key, int $start = 0, int $end = -1): bool|array {

        try {
            $cache = $this->connection->lRange($this->setPrefix($key), $start, $end);
            $list = [];
            if ($cache) {
                foreach ($cache as $string) {
                    $list[] = JsonHelper::is($string) ? JsonHelper::recover($string) : $string;
                }
            }
            return $list;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 读取全部列表
     * @param $key
     * @return array|false
     */
    public function lAll($key): bool|array {

        return $this->lRange($this->setPrefix($key), 0, -1);
    }

    /**
     * 在列表头部加入元素
     * @param $key
     * @param $value
     * @return bool|int
     */
    public function lPush($key, $value): bool|int {

        try {
            if (Arr::isArray($value)) {
                $value = json_encode($value, JSON_UNESCAPED_UNICODE);
            }
            if (!$count = $this->connection->lPush($this->setPrefix($key), $value)) {
                return false;
            }
            return $count;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 移除列表开始元素
     * @param $key
     * @return bool|string|array
     */
    public function lPop($key): bool|string|array {

        try {
            $result = $this->connection->lPop($this->setPrefix($key));
            if ($result && JsonHelper::is($result)) {
                $result = JsonHelper::recover($result);
            }
            return $result;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 在列表末尾加入元素
     * @param $key
     * @param $value
     * @return bool|int
     */
    public function rPush($key, $value): bool|int {

        try {
            if (Arr::isArray($value)) {
                $value = json_encode($value, JSON_UNESCAPED_UNICODE);
            }
            if (!$count = $this->connection->rPush($this->setPrefix($key), $value)) {
                return false;
            }
            return $count;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 移除列表末尾元素
     * @param $key
     * @return bool|string|array
     */
    public function rPop($key): bool|string|array {

        try {
            $result = $this->connection->rPop($this->setPrefix($key));
            if ($result && JsonHelper::is($result)) {
                $result = JsonHelper::recover($result);
            }
            return $result;
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 移除第一个数据
     * @param string $key
     * @return array|false
     */
    public function blPop(string $key): bool|array {

        try {
            return $this->connection->blPop($this->setPrefix($key), 10);
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 移除第一个数据
     * @param string $key
     * @return array|false
     */
    public function brPop(string $key): bool|array {

        try {
            return $this->connection->brPop($this->setPrefix($key), 10);
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }

    /**
     * 删除指定的key
     * @param string $key
     * @return boolean
     */
    public function delete(string $key): bool {

        try {
            return $this->connection->del($this->setPrefix($key));
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }

    }

    /**
     * 清洗缓存
     * @return bool
     */
    public function flush(): bool {
        try {
            return $this->connection->flushAll();
        } catch (RedisException $exception) {
            $this->onExecuteError($exception);
            return false;
        }
    }


    /**
     * 回收连接
     * @return void
     */
    public function close(): void {
        if ($this->connection instanceof \Redis) {
            try {
                $this->connection->close();
            } catch (RedisException $exception) {
                $this->onExecuteError($exception);
            }
        }
    }

    protected function setPrefix($key): string {
        if (str_starts_with($key, REDIS_IGNORE_KEY_PREFIX)) {
            return str_replace(REDIS_IGNORE_KEY_PREFIX, "", $key);
        }
        return $this->keyPrefix . '_' . $key;
    }

    protected function onExecuteError(RedisException $exception): void {
        self::$pools = [];
        Console::error("【Redis】Execute Error:" . $exception->getMessage() . ";code:" . $exception->getCode() . ";file:" . $exception->getLine() . "@" . $exception->getFile());
    }
}
