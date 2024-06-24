<?php

namespace Scf\Database;

use PDOException;
use Scf\Database\Logger\ILogger;

/**
 * Class Transaction
 */
class Transaction extends Connection {

    /**
     * Transaction constructor.
     * @param Driver $driver
     * @param ILogger|null $logger
     * @param array $config
     */
    public function __construct(Driver $driver, ?ILogger $logger, array $config = []) {
        parent::__construct($driver, $logger, $config);
        if (!$this->driver->instance()->beginTransaction()) {
            throw new PDOException('Begin transaction failed');
        }
    }

    /**
     * 提交事务
     * @throws PDOException
     */
    public function commit(): void {
        if (!$this->driver->instance()->commit()) {
            throw new PDOException('Commit transaction failed');
        }
        $this->__destruct();
    }

    /**
     * 回滚事务
     * @throws PDOException
     */
    public function rollback(): void {
        if (!$this->driver->instance()->rollBack()) {
            throw new PDOException('Rollback transaction failed');
        }
        $this->__destruct();
    }

}
