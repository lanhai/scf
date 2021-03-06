<?php

namespace Scf\Database;

use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Database\Connection\PdoPool;
use Scf\Database\Logger\PdoLogger;
use Scf\Helper\StringHelper;
use Scf\Util\Arr;
use Swoole\Coroutine;
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
            'max_open' => 50,// 最大开启连接数
            'max_idle' => 20,// 最大闲置连接数
            'max_lifetime' => 3600,//连接的最长生命周期
            'wait_timeout' => 0.0// 从池获取连接等待的时间, 0为一直等待
        ],
    ];
    protected int $actor = DBS_MASTER;
    protected bool $enablePool = false;
    private static array $pools = [];
    private static array $transactions = [];
    protected array $serverConfig = [];
    protected PdoPool|null $conn = null;

    /**
     * 构造器
     * @param int $actor
     * @param bool $enablePool
     */
    public function __construct(int $actor = DBS_MASTER, bool $enablePool = false) {
        $conf = Config::get('database');
        $this->_config = $conf ? Arr::merge($this->_config, $conf) : $this->_config;
        $this->actor = $actor;
        $this->enablePool = $enablePool;
    }

    /**
     * @param int $actor
     * @param bool $pool
     * @return static
     */
    final public static function factory(int $actor = DBS_MASTER, bool $pool = false): static {
        $class = get_called_class();
        return new $class($actor, $pool);
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

    /**
     * 返回数据库连接构造器
     * @param string|array $config
     * @return PdoPool
     */
    public function connection(string|array $config = 'default'): PdoPool {
        if (is_null($this->conn)) {
            $this->connect($config);
        }
        return $this->conn;
    }

    /**
     * 创建一个开启了事务的连接对象
     * @param string $name
     * @param bool $enablePool
     * @return PdoPool
     */
    public static function beginTransaction(string $name = 'default', bool $enablePool = true): PdoPool {
        return self::factory(DBS_MASTER, $enablePool)->connect($name)->connection()->_beginTransaction();
//        $cid = Coroutine::getCid();
//        if (!isset(self::$transactions[$name . '_' . DBS_MASTER . '_' . $cid])) {
//            self::$transactions[$name . '_' . DBS_MASTER . '_' . $cid] = self::factory(DBS_MASTER, $enablePool)->connect($name)->connection()->_beginTransaction();
//            if ($cid > 0) {
//                Coroutine::defer(function () use ($cid, $name) {
//                    Console::log('释放事务资源:' . $cid);
//                    unset(self::$transactions[$name . '_' . DBS_MASTER . '_' . $cid]);
//                });
//            }
//        }
//        return self::$transactions[$name . '_' . DBS_MASTER . '_' . $cid];
    }

    /**
     * 创建一个主库对象
     * @param string $name
     * @param bool $enablePool
     * @return Pdo
     */
    public static function master(string $name = 'default', bool $enablePool = true): Pdo {
        if (!isset(self::$pools[$name . '_' . DBS_MASTER])) {
            self::$pools[$name . '_' . DBS_MASTER] = self::factory(DBS_MASTER, $enablePool)->connect($name);
        }
        return self::$pools[$name . '_' . DBS_MASTER];
    }

    /**
     * 创建一个从库对象
     * @param string $name
     * @param bool $enablePool
     * @return Pdo
     */
    public static function slave(string $name = 'default', bool $enablePool = true): Pdo {
        if (!isset(self::$pools[$name . '_' . DBS_SLAVE])) {
            self::$pools[$name . '_' . DBS_SLAVE] = self::factory(DBS_SLAVE, $enablePool)->connect($name);
        }
        return self::$pools[$name . '_' . DBS_SLAVE];
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
        $this->conn = new PdoPool("mysql:host={$host};port={$this->serverConfig['port']};charset={$this->serverConfig['charset']};dbname={$this->serverConfig['name']}", $this->serverConfig['username'], $this->serverConfig['password'], [\PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC]);
        $this->conn->setConfig($this->serverConfig);
        if ($this->enablePool) {
            $maxOpen = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['max_open'] : $this->_config['pool']['max_open'];
            $maxIdle = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['max_idle'] : $this->_config['pool']['max_idle'];
            $maxLifetime = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['max_lifetime'] : $this->_config['pool']['max_lifetime'];
            $waitTimeout = isset($this->serverConfig['pool']) ? $this->serverConfig['pool']['wait_timeout'] : $this->_config['pool']['wait_timeout'];
            $this->conn->startPool($maxOpen, $maxIdle, $maxLifetime, $waitTimeout);
        }
        $logger = new PdoLogger();
        $this->conn->setLogger($logger);
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
        $server = $config[$name] ?? null;
        if (!$config) {
            throw new \PDOException('未配置数据库:' . $name);
        }
        $this->serverConfig = $server;
        return $this->serverConfig;
    }
}