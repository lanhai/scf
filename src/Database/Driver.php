<?php

namespace Scf\Database;

use PDO;
use PDOException;
use Scf\Core\InflightCounter;

/**
 * 持有单个 PDO 会话，提供建连、关闭和不执行业务 SQL 的可用性探测。
 *
 * 连接池在独占连接时调用探测；本层不重连或重放业务操作，避免改变事务状态。
 */
class Driver {

    use DriverTrait;

    /**
     * @var string
     */
    protected string $dsn = '';

    /**
     * @var string
     */
    protected string $username = 'root';

    /**
     * @var string
     */
    protected string $password = '';

    /**
     * @var array
     */
    protected array $options = [];

    /**
     * @var PDO|null
     */
    protected PDO|null $pdo = null;

    /**
     * 默认驱动连接选项
     * @var array
     */
    protected array $defaultOptions = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_OBJ,
        PDO::ATTR_TIMEOUT => 5,
    ];

    /**
     * Driver constructor.
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array $options
     * @throws PDOException
     */
    public function __construct(string $dsn, string $username, string $password, array $options = []) {
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->options = $options;
        $this->connect();
    }

    /**
     * Get instance
     * @return ?PDO
     */
    public function instance(): ?PDO {
        return $this->pdo;
    }

    /**
     * Get options
     * @return array
     */
    public function options(): array {
        return $this->options + $this->defaultOptions;
    }

    /**
     * Connect
     * @throws PDOException
     */
    public function connect(): void {
        $this->pdo = new PDO(
            $this->dsn,
            $this->username,
            $this->password,
            $this->options()
        );
    }

    /**
     * 在连接交给业务前探测 PDO 会话，完整释放探测结果并登记 I/O 在途计数。
     *
     * @return void
     * @throws PDOException 连接已关闭或数据库拒绝探测时交由连接池处理。
     */
    public function ping(): void {
        if ($this->pdo === null) {
            throw new PDOException('Cannot probe a closed PDO connection');
        }
        InflightCounter::beginMysql();
        try {
            $statement = $this->pdo->query('SELECT 1');
            if ($statement === false) {
                throw new PDOException('PDO connection probe failed');
            }
            $statement->closeCursor();
        } finally {
            InflightCounter::endMysql();
        }
    }

    /**
     * Close
     */
    public function close(): void {
        $this->pdo = null;
    }
}
