<?php

namespace Scf\Database\Connection;

use Mix\Database\Connection;
use Mix\Database\ConnectionInterface;
use Mix\Database\Database;
use Mix\Database\Driver;
use Mix\ObjectPool\Exception\WaitTimeoutException;

class PdoPool extends Database {
    protected array $config = [];
    protected bool $inTransaction = false;
    protected PdoConnection $transactionConnection;


    /**
     * 自定义事务开启方法用以适配AR
     * @return PdoPool
     */
    public function _beginTransaction(): PdoPool {
        if ($this->pool) {
            $this->driver = $this->pool->borrow();
        }
        if (isset($this->transactionConnection)) {
            throw new \PDOException('开启事务失败:重复执行!');
        }
        if (!$this->driver->instance()->beginTransaction()) {
            throw new \PDOException('开启事务失败');
        }
        $this->inTransaction = true;
        $this->transactionConnection = new PdoConnection($this->driver, $this->logger);
        return $this;
    }

    /**
     * 提交事务
     * @throws \PDOException
     */
    public function commit() {
        if (!$this->driver->instance()->commit()) {
            throw new \PDOException('事务提交失败');
        }
        $this->transactionConnection->__destruct();
        $this->inTransaction = false;
        unset($this->transactionConnection);
    }

    /**
     * 回滚事务
     * @throws \PDOException
     */
    public function rollback() {
        if (!$this->driver->instance()->rollBack()) {
            throw new \PDOException('事务回滚失败');
        }
        $this->transactionConnection->__destruct();
        $this->inTransaction = false;
        unset($this->transactionConnection);
    }

    /**
     * Borrow connection
     * @return Connection
     * @throws WaitTimeoutException
     */
    protected function borrow(): Connection {
        if ($this->inTransaction && isset($this->transactionConnection)) {
            return $this->transactionConnection;
        }
        if ($this->pool) {
            $driver = $this->pool->borrow();
            return new PdoConnection($driver, $this->logger);
        } else {
            return new PdoConnection($this->driver, $this->logger);
        }
    }

    /**
     * 数量统计
     * @param string $table
     * @param string $key
     * @param string|null $where
     * @return int
     */
    public function count(string $table, string $key, string $where = null): int {
        $tablePrefix = $this->config['prefix'];
        $sql = "SELECT count({$key}) as count FROM " . $tablePrefix . $table;
        if (!is_null($where)) {
            $sql .= "{$where}";
        }
        $result = $this->exec($sql)->first();
        return $result['count'] ?? 0;
    }

    /**
     * 计算总和
     * @param string $table
     * @param array|string $field
     * @param string|null $where
     * @param string|null $group
     * @param string|null $order
     * @return mixed
     */
    public function sum(string $table, array|string $field, string $where = null, string $group = null, string $order = null): mixed {
        $prefix = $this->config['prefix'];
        if (is_array($field)) {
            $arr = [];
            foreach ($field as $f) {
                $arr[] = "sum(`{$f}`) as {$f}_sum";
            }
            $sumSql = implode(",", $arr);
        } else {
            $sumSql = "sum(`{$field}`) as {$field}_sum";
        }
        $sql = "SELECT {$sumSql}" . (!is_null($group) ? ',' . $group : "") . " FROM {$prefix}{$table}";
        if (!is_null($where)) {
            $sql .= "{$where}";
        }
        if (!is_null($group)) {
            $sql .= " GROUP BY ({$group})";
        }
        if (!is_null($order)) {
            $sql .= " ORDER BY SUM({$order}) DESC";
        }
        if (!is_null($group)) {
            $result = $this->exec($sql)->get();
        } else {
            $result = $this->exec($sql)->first();
            if (!is_array($field)) {
                return $result[$field . '_sum'] ?? 0;
            }
        }
        return $result;
    }


    /**
     * 获得数据库ORM
     * @param string $table
     * @return ConnectionInterface
     */
    public function table(string $table): ConnectionInterface {
        $tablePrefix = $this->config['prefix'];
        return $this->borrow()->table($tablePrefix . $table);
    }

    /**
     * 插入数据
     * @param string $table
     * @param array $data
     * @param string $insert
     * @return ConnectionInterface
     */
    public function insert(string $table, array $data, string $insert = 'INSERT INTO'): ConnectionInterface {
        $tablePrefix = $this->config['prefix'];
        return $this->borrow()->insert($tablePrefix . $table, $data, $insert);
    }

    public function setConfig($config) {
        $this->config = $config;
    }
}