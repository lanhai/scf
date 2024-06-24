<?php

namespace Scf\Database;

use Closure;
use JetBrains\PhpStorm\ArrayShape;
use JetBrains\PhpStorm\Pure;
use PDOException;
use PDOStatement;
use Scf\Core\Console;
use Scf\Database\Logger\PdoLogger;
use Scf\Database\Tools\Expr;
use Scf\Database\Tools\QueryBuilder;
use Scf\Util\Random;
use Throwable;

/**
 * Class AbstractConnection
 */
abstract class AConnection implements IConnection {

    use QueryBuilder;

    /**
     * 驱动
     * @var Driver|EmptyDriver|null
     */
    protected Driver|EmptyDriver|null $driver = null;

    /**
     * @var PdoLogger|null
     */
    protected PdoLogger|null $logger = null;

    /**
     * @var Closure|null
     */
    protected Closure|null $debug = null;

    /**
     * PDOStatement
     * @var PDOStatement|null
     */
    protected PDOStatement|null $statement = null;

    /**
     * sql
     * @var string
     */
    protected string $sql = '';

    /**
     * params
     * @var array
     */
    protected array $params = [];

    /**
     * values
     * @var array
     */
    protected array $values = [];

    /**
     * 查询数据
     * @var array [$sql, $params, $values, $time]
     */
    protected array $sqlData = [];

    /**
     * 归还连接前缓存处理
     * @var array
     */
    protected array $options = [];

    /**
     * 归还连接前缓存处理
     * @var string|null
     */
    protected string|null $lastInsertId;

    /**
     * 归还连接前缓存处理
     * @var int|null
     */
    protected int|null $rowCount = null;

    /**
     * 因为协程模式下每次执行完，Driver 会被回收，因此不允许复用 Connection，必须每次都从 Database->borrow()
     * 为了保持与同步模式的兼容性，因此限制 Connection 不可多次执行
     * 事务在 commit rollback __destruct 之前可以多次执行
     * @var bool
     */
    protected bool $executed = false;
    /**
     * @var int 主从角色
     */
    protected int $actor = DBS_MASTER;

    protected array $config = [
        'prefix' => ''
    ];

    /**
     * AbstractConnection constructor.
     * @param Driver $driver
     * @param PdoLogger|null $logger
     * @param array $config
     * @param int $actor
     */
    #[Pure] public function __construct(Driver $driver, ?PdoLogger $logger, array $config = [], int $actor = DBS_MASTER) {
        $this->driver = $driver;
        $this->logger = $logger;
        $this->actor = $actor;
        $config and $this->config = $config;
        $this->options = $driver->options();
    }

    public function setPrefix($prefix): void {
        $this->config['prefix'] = $prefix;
    }

    /**
     * 连接
     * @throws PDOException
     */
    public function connect(): void {
        $this->driver->connect();
    }

    /**
     * 关闭连接
     */
    public function close(): void {
        $this->statement = null;
        $this->driver->close();
    }

    /**
     * 重新连接
     * @throws PDOException
     */
    protected function reconnect(): void {
        $this->close();
        $this->connect();
    }

    /**
     * 判断是否为断开连接异常
     * @param Throwable $ex
     * @return bool
     */
    protected static function isDisconnectException(Throwable $ex): bool {
        $disconnectMessages = [
            'server has gone away',
            'no connection to the server',
            'Lost connection',
            'is dead or not enabled',
            'Error while sending',
            'decryption failed or bad record mac',
            'server closed the connection unexpectedly',
            'SSL connection has been closed unexpectedly',
            'Error writing data to the connection',
            'Resource deadlock avoided',
            'failed with errno',
        ];
        $errorMessage = $ex->getMessage();
        foreach ($disconnectMessages as $message) {
            if (false !== stripos($errorMessage, $message)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param string $sql
     * @param ...$values
     * @return IConnection
     * @throws Throwable
     */
    public function raw(string $sql, ...$values): IConnection {
        // 保存SQL
        $this->sql = $sql;
        $this->values = $values;
        $this->sqlData = [$this->sql, $this->params, $this->values, 0];

        // 执行
        return $this->execute();
    }

    /**
     * @param string $sql
     * @param ...$values
     * @return IConnection
     * @throws Throwable
     */
    public function exec(string $sql, ...$values): IConnection {
        return $this->raw($sql, ...$values);
    }

    /**
     * @return IConnection
     * @throws Throwable
     */
    public function execute(): IConnection {
        if ($this->executed) {
            throw new \RuntimeException('The Connection::class cannot be executed repeatedly, please use the Database::class call');
        }
        $beginTime = microtime(true);

        $isBeginTransactions = false;
        $transactionsManager = null;
        $pointId = null;
        if ($this->actor == DBS_MASTER) {
            $transactionsManager = NestTransactions::instance();
            if ($transactionsManager->isBegin()) {
                $isBeginTransactions = true;
                $pointId = Random::character();
                $point = [
                    'id' => $pointId,
                    'sql' => $this->sqlData[0],
                    'values' => $this->sqlData[2] ?? $this->sqlData[1] ?? []
                ];
                $transactionsManager->addPoint($point);
            }
        }

        try {
            $this->prepare();
            $success = $this->statement->execute();
            if (!$success) {
                list($flag, $code, $message) = $this->statement->errorInfo();
                Console::warning("PDO execute Failed:" . $message);
                $isBeginTransactions and $transactionsManager->addError($pointId, $message);
                throw new PDOException(sprintf('%s %d %s', $flag, $code, $message), $code);
            }
        } catch (\Error $error) {
            $isBeginTransactions and $transactionsManager->addError($pointId, $error->getMessage());
            Console::warning("PDO execute Error:" . $error->getMessage());
            throw new PDOException("PDO execute Error:" . $error->getMessage());
        } catch (Throwable $ex) {
            $isBeginTransactions and $transactionsManager->addError($pointId, $ex->getMessage());
            Console::warning("PDO execute Throw:" . $ex->getMessage());
            throw $ex;
        } finally {
            // 只可执行一次
            // 事务除外，事务在 commit rollback __destruct 中处理
            if (!$this instanceof Transaction) {
                $this->executed = true;
            }
            // 记录执行时间
            $time = round((microtime(true) - $beginTime) * 1000, 2);
            $this->sqlData[3] = $time;
            // 缓存常用数据，让资源可以提前回收
            // 包含 pool 并且在非事务的情况下才执行缓存，并且提前归还连接到池，提高并发性能并且降低死锁概率
            isset($this->lastInsertId) and $this->lastInsertId = null;
            isset($this->rowCount) and $this->rowCount = null;
            if (isset($ex)) {
                // 有异常: 使用默认值, 不调用 driver, statement
                $this->lastInsertId = '';
                $this->rowCount = 0;
                // 开启了协程事务重置参数
                if ($isBeginTransactions && $this instanceof Transaction) {
                    $this->params = [];
                    $this->values = [];
                }
            } elseif ($this->driver->pool && !$this instanceof Transaction) {
                // 有pool: 提前缓存 lastInsertId, rowCount 让连接提前归还
                try {
                    if (stripos($this->sql, 'INSERT INTO') !== false) {
                        $this->lastInsertId = $this->driver->instance()->lastInsertId();
                    } else {
                        $this->lastInsertId = '';
                    }
                } catch (Throwable $ex) {
                    // pgsql: SQLSTATE[55000]: Object not in prerequisite state: 7 ERROR:  lastval is not yet defined in this session
                    $this->lastInsertId = '';
                }
                $this->rowCount = $this->statement->rowCount();
            }
            // logger
            if ($this->logger) {
                $log = $this->queryLog();
                $this->logger->trace(
                    $log['time'],
                    $log['sql'],
                    $log['bindings'],
                    $this->rowCount(),
                    $ex ?? null
                );
            }
            // debug
            $debug = $this->debug;
            $debug and $debug($this);
        }

        // 事务还是要复用 Connection 清理依然需要
        // 抛出异常时不清理，因为需要重连后重试
        $this->clear();

        // 执行完立即回收
        // 抛出异常时不回收，重连那里还需要验证是否在事务中
        // 事务除外，事务在 commit rollback __destruct 中回收
        if ($this->driver->pool && !$this instanceof Transaction) {
            $this->driver->__return();
            $this->driver = new EmptyDriver();
        }
        return $this;
    }

    /**
     * 事务还是要复用 Connection 清理依然需要
     */
    protected function clear(): void {
        $this->debug = null;
        $this->sql = '';
        $this->params = [];
        $this->values = [];
    }

    protected function prepare(): void {
        if (!empty($this->params)) { // 参数绑定
            // 支持insert里面带函数
            foreach ($this->params as $k => $v) {
                if ($v instanceof Expr) {
                    unset($this->params[$k]);
                    $k = str_starts_with($k, ':') ? $k : ":{$k}";
                    $this->sql = str_replace($k, $v->__toString(), $this->sql);
                }
            }
            $statement = $this->driver->instance()->prepare($this->sql);
            if (!$statement) {
                throw new PDOException('PDO prepare failed');
            }
            $this->statement = $statement;
            $this->sqlData = [$this->sql, $this->params, [], 0,]; // 必须在 bindParam 前，才能避免类型被转换
            foreach ($this->params as $key => &$value) {
                if (!$this->statement->bindParam($key, $value, static::bindType($value))) {
                    throw new PDOException('PDOStatement bindParam failed');
                }
            }
        } elseif (!empty($this->values)) { // 值绑定
            $statement = $this->driver->instance()->prepare($this->sql);
            if (!$statement) {
                throw new PDOException('PDO prepare failed');
            }
            $this->statement = $statement;
            $this->sqlData = [$this->sql, [], $this->values, 0];
            foreach ($this->values as $key => $value) {
                if (!$this->statement->bindValue($key + 1, $value, static::bindType($value))) {
                    throw new PDOException('PDOStatement bindValue failed');
                }
            }
        } else { // 无参数
            $statement = $this->driver->instance()->prepare($this->sql);
            if (!$statement) {
                throw new PDOException('PDO prepare failed');
            }
            $this->statement = $statement;
            $this->sqlData = [$this->sql, [], [], 0];
        }
    }

    /**
     * @param $value
     * @return int
     */
    protected static function bindType($value): int {
        return match (gettype($value)) {
            'boolean' => \PDO::PARAM_BOOL,
            'NULL' => \PDO::PARAM_NULL,
            'integer' => \PDO::PARAM_INT,
            default => \PDO::PARAM_STR,
        };
    }

    /**
     * @param Closure $func
     * @return $this
     */
    public function debug(Closure $func): IConnection {
        $this->debug = $func;
        return $this;
    }

    /**
     * 返回多行
     * @return array
     */
    public function get(): array {
        if ($this->table) {
            list($sql, $values) = $this->build('SELECT');
            $this->raw($sql, ...$values);
        }
        return $this->queryAll();
    }

    /**
     * 返回一行
     * @return array|object|false
     */
    public function first(): object|bool|array {
        if ($this->table) {
            list($sql, $values) = $this->build('SELECT');
            $this->raw($sql, ...$values);
        }
        return $this->queryOne();
    }

    /**
     * 返回单个值
     * @param string $field
     * @return mixed
     * @throws PDOException
     */
    public function value(string $field): mixed {
        if ($this->table) {
            list($sql, $values) = $this->build('SELECT');
            $this->raw($sql, ...$values);
        }
        $result = $this->queryOne();
        if (empty($result)) {
            throw new PDOException(sprintf('Field %s not found', $field));
        }
        $isArray = is_array($result);
        if (($isArray && !isset($result[$field])) || (!$isArray && !isset($result->$field))) {
            throw new PDOException(sprintf('Field %s not found', $field));
        }
        return $isArray ? $result[$field] : $result->$field;
    }

    /**
     * @param array $data
     * @return IConnection
     */
    public function updates(array $data): IConnection {
        list($sql, $values) = $this->build('UPDATE', $data);
        return $this->exec($sql, ...$values);
    }

    /**
     * @param string $field
     * @param $value
     * @return IConnection
     */
    public function update(string $field, $value): IConnection {
        list($sql, $values) = $this->build('UPDATE', [
            $field => $value
        ]);
        return $this->exec($sql, ...$values);
    }

    /**
     * @return IConnection
     */
    public function delete(): IConnection {
        list($sql, $values) = $this->build('DELETE');
        return $this->exec($sql, ...$values);
    }

    /**
     * 返回结果集
     * 注意：只能在 debug 闭包中使用，因为连接归还到池后，如果还有调用结果集会有一致性问题
     * @return PDOStatement
     */
    public function statement(): PDOStatement {
        // check debug
        if (!$this->debug) {
            throw new \RuntimeException('Can only be used in debug closure');
        }

        return $this->statement;
    }

    /**
     * 返回一行
     * @param int|null $fetchStyle
     * @return array|object|false
     */
    public function queryOne(int $fetchStyle = null): object|bool|array {
        $fetchStyle = $fetchStyle ?: $this->options[\PDO::ATTR_DEFAULT_FETCH_MODE];
        return $this->statement->fetch($fetchStyle);
    }

    /**
     * 返回多行
     * @param int|null $fetchStyle
     * @return array
     */
    public function queryAll(int $fetchStyle = null): array {
        $fetchStyle = $fetchStyle ?: $this->options[\PDO::ATTR_DEFAULT_FETCH_MODE];
        return $this->statement->fetchAll($fetchStyle);
    }

    /**
     * 返回一列 (默认第一列)
     * @param int $columnNumber
     * @return array
     */
    public function queryColumn(int $columnNumber = 0): array {
        $column = [];
        while ($row = $this->statement->fetchColumn($columnNumber)) {
            $column[] = $row;
        }
        return $column;
    }

    /**
     * 返回一个标量值
     * @return mixed
     */
    public function queryScalar(): mixed {
        return $this->statement->fetchColumn();
    }

    /**
     * 返回最后插入行的ID或序列值
     * @return string
     */
    public function lastInsertId(): string {
        if (!isset($this->lastInsertId) && $this->driver instanceof Driver) {
            $this->lastInsertId = $this->driver->instance()->lastInsertId();
        }
        return $this->lastInsertId;
    }

    /**
     * 返回受上一个 SQL 语句影响的行数
     * @return int
     */
    public function rowCount(): int {
        if (!isset($this->rowCount) && $this->driver instanceof Driver) {
            $this->rowCount = $this->statement->rowCount();
        }
        return $this->rowCount;
    }

    /**
     * 获取查询日志
     * @return array
     */
    #[ArrayShape(['time' => "int|mixed", 'sql' => "mixed|string", 'bindings' => "array|mixed"])] public function queryLog(): array {
        $sql = '';
        $params = $values = [];
        $time = 0;
        !empty($this->sqlData) and list($sql, $params, $values, $time) = $this->sqlData;
        return [
            'time' => $time,
            'sql' => $sql,
            'bindings' => $values ?: $params,
        ];
    }

    /**
     * @param string $table
     * @param array $data
     * @param string $insert
     * @return IConnection
     */
    public function insert(string $table, array $data, string $insert = 'INSERT INTO'): IConnection {
        $prefix = $this->config['prefix'] ?? '';
        $table = $prefix . $table;

        $keys = array_keys($data);
        $fields = array_map(function ($key) {
            return ":{$key}";
        }, $keys);
        $sql = "{$insert} `{$table}` (`" . implode('`, `', $keys) . "`) VALUES (" . implode(', ', $fields) . ")";
        $this->params = array_merge($this->params, $data);
        return $this->exec($sql);
    }

    /**
     * @param string $table
     * @param array $data
     * @param string $insert
     * @return IConnection
     */
    public function batchInsert(string $table, array $data, string $insert = 'INSERT INTO'): IConnection {
        $keys = array_keys($data[0]);
        $sql = "{$insert} `{$table}` (`" . implode('`, `', $keys) . "`) VALUES ";
        $values = [];
        $subSql = [];
        foreach ($data as $item) {
            $placeholder = [];
            foreach ($keys as $key) {
                $value = $item[$key];
                // 原始方法
                if ($value instanceof Expr) {
                    $placeholder[] = $value->__toString();
                    continue;
                }
                $values[] = $value;
                $placeholder[] = '?';
            }
            $subSql[] = "(" . implode(', ', $placeholder) . ")";
        }
        $sql .= implode(', ', $subSql);
        return $this->exec($sql, ...$values);
    }

    /**
     * 返回当前PDO连接是否在事务内（在事务内的连接回池会造成下次开启事务产生错误）
     * @return bool
     */
    public function inTransaction(): bool {
        $pdo = $this->driver->instance();
        return (bool)($pdo->inTransaction());
    }

    /**
     * 自动事务
     * @param Closure $closure
     * @throws Throwable
     */
    public function transaction(Closure $closure) {
        $tx = $this->beginTransaction();
        try {
            call_user_func($closure, $tx);
            $tx->commit();
        } catch (Throwable $ex) {
            $tx->rollback();
            throw $ex;
        }
    }

    /**
     * @return Transaction
     * @throws PDOException
     */
    public function beginTransaction(): Transaction {
        $driver = $this->driver;
        $this->driver = null; // 使其在析构时不回收
        return new Transaction($driver, $this->logger, $this->config);
    }

}
