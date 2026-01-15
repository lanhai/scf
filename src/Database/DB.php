<?php

namespace Scf\Database;

use Closure;
use PDOException;
use Scf\Core\Table\Counter;
use Scf\Core\Table\PdoPoolTable;
use Scf\Database\Logger\PdoLogger;
use Scf\Database\Pool\ConnectionPool;
use Scf\Database\Pool\Dialer;
use Scf\Mode\Web\Log;
use Swoole\Timer;
use Throwable;

class DB {
    const PDO_POOL_ID_KEY = APP_ID . '_PDO_POOL_ID_';
    protected array $config = [];

    /**
     * 连接池
     * @var ConnectionPool|null
     */
    protected ConnectionPool|null $pool = null;
    /**
     * @var string
     */
    protected string $dbName = '';

    /**
     * 数据源格式
     * @var string
     */
    protected string $dsn = '';

    /**
     * 数据库用户名
     * @var string
     */
    protected string $username = 'root';

    /**
     * 数据库密码
     * @var string
     */
    protected string $password = '';

    /**
     * 驱动连接选项
     * @var array
     */
    protected array $options = [];

    /**
     * 最大活跃数
     * "0" 为不限制，"-1" 等于cpu数量
     * @var int
     */
    protected int $maxOpen = -1;

    /**
     * 最多可空闲连接数
     * "-1" 等于cpu数量
     * @var int
     */
    protected int $maxIdle = -1;

    /**
     * 连接可复用的最长时间
     * "0" 为不限制
     * @var int
     */
    protected int $maxLifetime = 0;

    /**
     * 等待新连接超时时间
     * "0" 为不限制
     * @var float
     */
    protected float $waitTimeout = 0.0;

    /**
     * @var Driver
     */
    protected Driver $dialer;

    /**
     * @var Driver|null
     */
    protected Driver|null $driver = null;

    /**
     * @var PdoLogger
     */
    protected PdoLogger $logger;

    protected int $poolId = 0;
    /**
     * @var int 主从角色
     */
    protected int $actor = DBS_MASTER;

    /**
     * Database constructor.
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array $options
     * @param int $actor
     */
    public function __construct(string $dsn, string $username, string $password, array $options = [], int $actor = DBS_MASTER) {
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->options = $options;
        $this->actor = $actor;
        $this->driver = new Driver(
            $this->dsn,
            $this->username,
            $this->password,
            $this->options
        );

    }

    /**
     * 开启连接池
     * @param int $maxOpen
     * @param int $maxIdle
     * @param int $maxLifetime
     * @param float $waitTimeout
     * @param int $autoPing
     * @param int $idleTimeout
     * @param string $dbName
     */
    public function startPool(int $maxOpen, int $maxIdle, int $maxLifetime = 0, float $waitTimeout = 0.0, int $autoPing = 0, int $idleTimeout = 0, string $dbName = ''): void {
        $this->dbName = $dbName;
        $this->maxOpen = $maxOpen;
        $this->maxIdle = $maxIdle;
        $this->maxLifetime = $maxLifetime;
        $this->waitTimeout = $waitTimeout;
        $this->createPool($autoPing, $idleTimeout);
    }

    /**
     * 创建连接池
     * @param int $autoPing
     * @param int $idleTimeout
     * @return void
     */
    protected function createPool(int $autoPing = 0, int $idleTimeout = 0): void {
        if ($this->driver) {
            $this->driver->close();
            $this->driver = null;
        }
        $this->poolId = Counter::instance()->incr(self::PDO_POOL_ID_KEY);
        $this->pool = new ConnectionPool(
            new Dialer(
                $this->dsn,
                $this->username,
                $this->password,
                $this->options
            ),
            $this->maxOpen,
            $this->maxIdle,
            $this->maxLifetime,
            $this->waitTimeout
        );
        $this->pool->setPingInterval($autoPing);
        $this->pool->setIdleTimeout($idleTimeout);
        $this->pool->setId($this->poolId);
//        $server = Http::server();
//        if ($autoPing && (is_null($server) || $server->taskworker === false) && !RUNNING_TOOLBOX && !defined('IS_CRONTAB_PROCESS')) {
//            $table = PdoPoolTable::instance();
//            $idleCheckTimerId = $this->pool->idleCheck();
//            $table->set($this->poolId, [
//                'id' => $this->poolId,
//                'hash' => substr(md5(spl_object_hash($this) . getmypid()), 8, 16),
//                'object_id' => spl_object_hash($this->pool),
//                'db' => $this->dbName,
//                'dsn' => $this->dsn,
//                'pid' => getmypid(),
//                'created' => date('Y-m-d H:i:s'),
//                'timer_id' => $idleCheckTimerId
//            ]);
//        }
//        Console::success("#" . $this->poolId . " 连接池创建成功,db:" . $this->dbName);
//        Coroutine::defer(function () {
//            $this->destory();
//        });
    }

    /**
     * 手动触发销毁
     * @return void
     */
    public function destory(): void {
        if ($timerId = PdoPoolTable::instance()->get($this->poolId, 'timer_id')) {
            Timer::clear($timerId);
            PdoPoolTable::instance()->delete($this->poolId);
        }
    }

    /**
     * 销毁
     */
    public function __destruct() {
        $this->destory();
    }

    /**
     * 如果当前db在事务,取出一个事务连接
     * @throws Throwable
     */
    protected function getTransaction(): ?Transaction {
        $manager = NestTransactions::instance();
        if (!$manager->isBegin() || $manager->isEnd()) {
            return null;
        }
        $dsn = explode(";", $this->dsn);
        $connectionKey = array_pop($dsn) . "_" . md5($this->dsn);
        if (!$transaction = $manager->getConnenction($connectionKey)) {
            $driver = $this->pool ? $this->pool->borrow() : $this->driver;
            $connection = new Connection($driver, $this->logger, ['prefix' => $this->config['prefix'] ?? ''], $this->actor);
            $transaction = $connection->beginTransaction();
            $manager->addConnection($connectionKey, $transaction);
        }
        return $transaction;
    }

    /**
     * 取出连接
     * Borrow connection
     * @return Connection|Transaction
     */
    protected function borrow(): Connection|Transaction {
        if ($this->actor == DBS_MASTER) {
            try {
                $transaction = $this->getTransaction();
                if ($transaction) {
                    //避免取出不同表前缀的事务连接,重新设置前缀
                    $transaction->setPrefix($this->config['prefix'] ?? '');
                    return $transaction;
                }
            } catch (Throwable $exception) {
                Log::instance()->error("事务开启失败:" . $exception->getMessage());
            }
        }
        if ($this->pool) {
            $driver = $this->pool->borrow();
            $conn = new Connection($driver, $this->logger, ['prefix' => $this->config['prefix'] ?? ''], $this->actor);
        } else {
            $conn = new Connection($this->driver, $this->logger, ['prefix' => $this->config['prefix'] ?? ''], $this->actor);
        }
        return $conn;
    }

    /**
     * @param int $maxOpen
     */
    public function setMaxOpenConns(int $maxOpen): void {
        if ($this->maxOpen == $maxOpen) {
            return;
        }
        $this->maxOpen = $maxOpen;
        $this->createPool();
    }

    /**
     * @param int $maxIdle
     */
    public function setMaxIdleConns(int $maxIdle): void {
        if ($this->maxIdle == $maxIdle) {
            return;
        }
        $this->maxIdle = $maxIdle;
        $this->createPool();
    }

    /**
     * @param int $maxLifetime
     */
    public function setConnMaxLifetime(int $maxLifetime): void {
        if ($this->maxLifetime == $maxLifetime) {
            return;
        }
        $this->maxLifetime = $maxLifetime;
        $this->createPool();
    }

    /**
     * @param float $waitTimeout
     */
    public function setPoolWaitTimeout(float $waitTimeout): void {
        if ($this->waitTimeout == $waitTimeout) {
            return;
        }
        $this->waitTimeout = $waitTimeout;
        $this->createPool();
    }

    /**
     * @return array
     */
    public function poolStats(): array {
        if (!$this->pool) {
            return [];
        }
        return $this->pool->stats();
    }

    /**
     * @param  $logger
     */
    public function setLogger($logger): void {
        $this->logger = $logger;
        $this->pool?->setLogger($logger);

    }

    /**
     * @param Closure $func
     * @return IConnection
     */
    public function debug(Closure $func): IConnection {
        return $this->borrow()->debug($func);
    }

    /**
     * @param string $sql
     * @param ...$values
     * @return IConnection
     * @throws Throwable
     */
    public function raw(string $sql, ...$values): IConnection {
        return $this->borrow()->raw($sql, ...$values);
    }

    /**
     * @param string $sql
     * @param ...$values
     * @return IConnection
     * @throws Throwable
     */
    public function exec(string $sql, ...$values): IConnection {
        return $this->borrow()->exec($sql, ...$values);
    }

    /**
     * 批量插入
     * @param string $table
     * @param array $data
     * @param string $insert
     * @return IConnection
     */
    public function batchInsert(string $table, array $data, string $insert = 'INSERT INTO'): IConnection {
        return $this->borrow()->batchInsert($table, $data, $insert);
    }

    /**
     * 自动事务
     * @param Closure $closure
     * @throws Throwable
     */
    public function transaction(Closure $closure): void {
        $this->borrow()->transaction($closure);
    }


    /**
     * 获得数据库ORM
     * @param string $table
     * @return IConnection
     */
    public function table(string $table): IConnection {
        return $this->borrow()->table($table);
    }

    /**
     * 插入数据
     * @param string $table
     * @param array $data
     * @param string $insert
     * @return IConnection
     */
    public function insert(string $table, array $data, string $insert = 'INSERT INTO'): IConnection {
        return $this->borrow()->insert($table, $data, $insert);
    }

    /**
     * 返回一个事务连接
     * @throws Throwable
     */
    public function beginTransaction(): Transaction {
        if (NestTransactions::instance()->isBegin()) {
            throw new PDOException('开启事务失败:当前线程已开启事务嵌套');
        }
        return $this->borrow()->beginTransaction();
    }

    public function setConfig($config): void {
        $this->config = $config;
    }
}