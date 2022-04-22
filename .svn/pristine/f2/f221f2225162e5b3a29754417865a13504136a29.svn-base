<?php

namespace Scf\Database\Connection;

use Mix\Database\Connection;
use Mix\Database\ConnectionInterface;

class PdoConnection extends Connection {
    protected array $config = [];

    /**
     * @param string $sql
     * @param ...$values
     * @return ConnectionInterface
     */
    public function raw(string $sql, ...$values): ConnectionInterface {
        $expr = preg_replace('/((FROM|TABLE|INTO|UPDATE|JOIN|BY)\s*) ([a-zA-Z0-9_\.]+)/i', "$2 `$3`", $sql);
        // 保存SQL
        $this->sql = $expr;
        $this->values = $values;
        $this->sqlData = [$this->sql, $this->params, $this->values, 0];
        // 执行
        return $this->execute();
    }

    public function order(string $field, string $order): ConnectionInterface {
        $field = "`{$field}`";
        return parent::order($field, $order);
    }

    /**
     * @param array $data
     * @return ConnectionInterface
     */
    public function updates(array $data): ConnectionInterface {
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
     * @return ConnectionInterface
     */
    public function update(string $field, $value): ConnectionInterface {
        $field = "`{$field}`";
        list($sql, $values) = $this->build('UPDATE', [
            $field => $value
        ]);
        return $this->exec($sql, ...$values);
    }

    /**
     * @param string $table
     * @param array $data
     * @param string $insert
     * @return ConnectionInterface
     */
    public function insert(string $table, array $data, string $insert = 'INSERT INTO'): ConnectionInterface {
        $keys = array_keys($data);
        $fields = array_map(function ($key) {
            return ":{$key}";
        }, $keys);
        $sql = "{$insert} `{$table}` (`" . implode('`, `', $keys) . "`) VALUES (" . implode(', ', $fields) . ")";
        $this->params = array_merge($this->params, $data);
        return $this->exec($sql);
    }
}