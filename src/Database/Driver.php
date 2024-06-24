<?php

namespace Scf\Database;

use PDO;
use PDOException;

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
     * @return PDO
     */
    public function instance(): PDO {
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
     * Close
     */
    public function close(): void {
        $this->pdo = null;
    }
}