<?php

namespace Scf\Database;

use Scf\Core\Config;
use Scf\Database\Logger\PdoLogger;
use Scf\Helper\StringHelper;
use Scf\Util\Arr;
use const DBS_MASTER;
use const DBS_SLAVE;

class Pdo {

    /**
     *  配置项
     * @var array
     */
    protected array $_config = [
        'driver' => 'mysql',
        'pool' => [
            'max_open' => -1,// 最大开启连接数
            'max_idle' => -1,// 最大闲置连接数
            'max_lifetime' => 60,//连接的最长生命周期
            'wait_timeout' => 0.0,// 从池获取连接等待的时间, 0为一直等待
            'connection_auto_ping_interval' => 0,//自动ping间隔时间
            'connection_idle_timeout' => 0,//空闲回收时间
        ],
    ];
    protected int $actor = DBS_MASTER;
    protected bool $enablePool = false;
    private static array $_instances = [];
    protected array $serverConfig = [];
    protected DB|null $database = null;
    protected ?array $overrides = null;

    /**
     * 构造器
     * @param int $actor
     * @param bool $enablePool
     * @param array|null $overrides
     */
    public function __construct(int $actor = DBS_MASTER, bool $enablePool = false, ?array $overrides = null) {
        $this->overrides = $overrides;
        $conf = Config::get('database');
        $this->_config = $conf ? Arr::merge($this->_config, $conf) : $this->_config;
        $this->actor = $actor;
        $this->enablePool = $enablePool;
    }

    /**
     * @param int $actor
     * @param bool $pool
     * @param array|null $config
     * @return static
     */
    final public static function factory(int $actor = DBS_MASTER, bool $pool = false, ?array $config = null): static {
        $class = static::class;
        return new $class($actor, $pool, $config);
    }

    /**
     * 不同的库/角色保存各自的单例副本
     * @param $name
     * @param $actor
     * @param bool $enablePool
     * @return Pdo
     */
    private static function getInstance($name, $actor, bool $enablePool = true): Pdo {
        $ovrerride = SERVER_MODE == MODE_CGI ? Overrider::get() : null;
        $config = null;
        if ($ovrerride && isset($ovrerride[$name])) {
            $instanceKey = $ovrerride['__KEY__'] . $name . '_' . $actor;
            $config = $ovrerride;
        } else {
            $instanceKey = $name . '_' . $actor;
        }
        if (!$enablePool || !isset(self::$_instances[$instanceKey])) {
            $instance = self::factory($actor, $enablePool, $config)->connect($name);
            //非连接池模式不能使用单例,因为非连接池模式未处理断线重连的情况,同时会出现不同请求/协程使用同一个连接导致致命问题
            if (!$enablePool) {
                return $instance;
            }
            self::$_instances[$instanceKey] = $instance;
        }
        return self::$_instances[$instanceKey];
    }

    /**
     * 返回数据库连接构造器
     * @param string|array $config
     * @return DB
     */
    public function getDatabase(string|array $config = 'default'): DB {
        if (is_null($this->database)) {
            $this->connect($config);
        }
        return $this->database;
    }

    /**
     * 创建一个主库对象
     * @param string $name
     * @param bool $enablePool
     * @return Pdo
     */
    public static function master(string $name = 'default', bool $enablePool = true): Pdo {
        return self::getInstance($name, DBS_MASTER, $enablePool);
    }

    /**
     * 创建一个从库对象
     * @param string $name
     * @param bool $enablePool
     * @return Pdo
     */
    public static function slave(string $name = 'default', bool $enablePool = true): Pdo {
        return self::getInstance($name, DBS_SLAVE, $enablePool);
    }

    /**
     * 开启嵌套事务,所有写入操作都将开启事务,谨慎使用
     */
    public static function beginProcessLifeTransaction(): array {
        return NestTransactions::instance()->begin();
    }

    public static function cancelProcessLifeTransaction(): void {
        NestTransactions::instance()->cancel();
    }

    public static function isBeginProcessLifeTransaction(): bool {
        return NestTransactions::instance()->isBegin();
    }

    public static function finishProcessLifeTransaction(): NestTransactions {
        return NestTransactions::instance()->finish();
    }

    /**
     * 连接
     * @param string|array $config
     * @return Pdo
     */
    protected function connect(string|array $config = 'default'): Pdo {
        $this->serverConfig = is_array($config) ? $config : $this->getServer($config);
        if ($this->actor == DBS_SLAVE) {
            $hosts = explode(',', $this->serverConfig['slave']);
        } else {
            $hosts = explode(',', $this->serverConfig['master']);
        }
        $host = $hosts[rand(0, count($hosts) - 1)];
        $this->database = new DB("mysql:host={$host};port={$this->serverConfig['port']};charset={$this->serverConfig['charset']};dbname={$this->serverConfig['name']}", $this->serverConfig['username'], $this->serverConfig['password'], [\PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC], $this->actor);//, \PDO::ATTR_PERSISTENT => true, \PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true
        $this->database->setConfig($this->serverConfig);
        $logger = new PdoLogger();
        if ($this->enablePool) {
            $maxOpen = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['max_open'] : $this->_config['pool']['max_open'];
            $maxIdle = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['max_idle'] : $this->_config['pool']['max_idle'];
            $maxLifetime = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['max_lifetime'] : $this->_config['pool']['max_lifetime'];
            $waitTimeout = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['wait_timeout'] : $this->_config['pool']['wait_timeout'];
            $autoPing = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['connection_auto_ping_interval'] : $this->_config['pool']['connection_auto_ping_interval'];
            $idleTimeout = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['connection_idle_timeout'] : $this->_config['pool']['connection_idle_timeout'];
            $this->database->startPool($maxOpen, $maxIdle, $maxLifetime, $waitTimeout, $autoPing, $idleTimeout, is_array($config) ? $this->serverConfig['name'] : $config);
        }
        $this->database->setLogger($logger);
        return $this;
    }

    /**
     * 加载配置
     * @param string $name
     * @return array
     */
    protected function getServer(string $name = 'default'): array {
        $driver = StringHelper::camel2lower($this->_config['driver']);
        $config = $this->_config[$driver];
        if ($this->overrides) {
            $config = Arr::merge($config, $this->overrides);
        }
        $server = $config[$name] ?? null;
        if (!$config) {
            throw new \PDOException('未配置数据库:' . $name);
        }
        $this->serverConfig = $server;
        return $this->serverConfig;
    }

    /**
     * @param $key
     * @return array|mixed
     */
    public function getConfig($key = null): mixed {
        if (!is_null($key)) {
            return $this->serverConfig[$key];
        }
        return $this->serverConfig;
    }


}