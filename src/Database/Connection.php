<?php

namespace Scf\Database;

use Throwable;

class Connection extends AConnection {

    protected bool $exceptional = false;

    /**
     * @throws Throwable
     */
    protected function call($name, $arguments = []) {
        try {
            // 执行父类方法
            return call_user_func_array(parent::class . "::{$name}", $arguments);
        } catch (\Throwable $ex) {
            if (static::isDisconnectException($ex) && !$this->inTransaction()) {
                // 断开连接异常处理
                $this->reconnect();
                // 重连后允许再次执行
                $this->executed = false;
                // 重新执行方法
                return $this->call($name, $arguments);
            } else {
                // 不可在这里处理丢弃连接，会影响用户 try/catch 事务处理业务逻辑
                // 会导致 commit rollback 时为 EmptyDriver
                $this->exceptional = true;

                // 抛出其他异常
                throw $ex;
            }
        }
    }

    /**
     * 数量统计
     * @param string|null $field
     * @return int|array
     * @throws Throwable
     */
    public function count(string|null $field = null): int|array {
        $this->countField = $field ?: '*';
        $isGroup = !empty($this->group);
        if ($this->table) {
            list($sql, $values) = $this->build('COUNT');
            $this->raw($sql, ...$values);
        }
        if ($isGroup) {
            $result = $this->queryAll();
        } else {
            $result = $this->queryOne();
            return $result['count'] ?: 0;
        }
        return $result;
    }

    /**
     * 计算总和
     * @param mixed ...$fields
     * @return int|float|array|false
     * @throws Throwable
     */
    public function sum(...$fields): int|float|array|false {
        $this->sumFields = array_merge($this->sumFields, $fields);
        $isGroup = !empty($this->group);
        $fields = $this->sumFields;
        if ($this->table) {
            list($sql, $values) = $this->build('SUM');
            $this->raw($sql, ...$values);
        }
        if ($isGroup) {
            $result = $this->queryAll();
        } else {
            $result = $this->queryOne();
            if (count($fields) == 1) {
                return $result[$fields[0]] ?: 0;
            }
        }
        return $result;
    }

    /**
     * @param string $sql
     * @param ...$values
     * @return IConnection
     * @throws Throwable
     */
    public function raw(string $sql, ...$values): IConnection {
        $expr = preg_replace('/((FROM|TABLE|INTO|UPDATE|JOIN|BY)\s*) ([a-zA-Z0-9_\.]+)/i', "$2 `$3`", $sql);
        // 保存SQL
        $this->sql = $expr;
        $this->values = $values;
        $this->sqlData = [$this->sql, $this->params, $this->values, 0];
        // 执行
        return $this->execute();
    }

    public function order(string $field, string $order): IConnection {
        $field = "`{$field}`";
        return parent::order($field, $order);
    }

    /**
     * @param array $data
     * @return IConnection
     * @throws Throwable
     */
    public function updates(array $data): IConnection {
        $datas = [];
        foreach ($data as $k => $v) {
            $datas['`' . $k . '`'] = $v;
        }
        list($sql, $values) = $this->build('UPDATE', $datas);
        return $this->exec($sql, ...$values);
    }

    /**
     * @param string $field
     * @param $value
     * @return IConnection
     * @throws Throwable
     */
    public function update(string $field, $value): IConnection {
        $field = "`{$field}`";
        list($sql, $values) = $this->build('UPDATE', [
            $field => $value
        ]);
        return $this->exec($sql, ...$values);
    }

    /**
     * @throws Throwable
     */
    public function queryOne(int $fetchStyle = null): object|bool|array {
        return $this->call(__FUNCTION__, func_get_args());
    }

    /**
     * @throws Throwable
     */
    public function queryAll(int $fetchStyle = null): array {
        return $this->call(__FUNCTION__, func_get_args());
    }

    /**
     * @throws Throwable
     */
    public function queryColumn(int $columnNumber = 0): array {
        return $this->call(__FUNCTION__, func_get_args());
    }

    /**
     * @throws Throwable
     */
    public function queryScalar(): mixed {
        return $this->call(__FUNCTION__);
    }

    public function execute(): IConnection {
        return $this->call(__FUNCTION__);
    }

    /**
     * @throws Throwable
     */
    public function beginTransaction(): Transaction {
        return $this->call(__FUNCTION__);
    }

    /**
     * @throws Throwable
     */
    public function rowCount(): int {
        return $this->call(__FUNCTION__);
    }

    public function __destruct() {
        $this->executed = true;
        // 回收
        if (!$this->driver || $this->driver instanceof EmptyDriver) {
            return;
        }
        if ($this->exceptional || $this->inTransaction()) {
            $this->driver->__discard();
            $this->driver = new EmptyDriver();
            return;
        }
        $this->driver->__return();
        $this->driver = new EmptyDriver();
    }
}