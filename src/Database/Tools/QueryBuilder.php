<?php

namespace Scf\Database\Tools;

use Scf\Database\IConnection;

/**
 * Trait QueryBuilder
 */
trait QueryBuilder {

    /**
     * @var string
     */
    protected string $table = '';

    /**
     * @var array
     */
    protected array $select = [];
    /**
     * @var string
     */
    protected string $countField = '';
    /**
     * @var array
     */
    protected array $sumFields = [];

    /**
     * @var array
     */
    protected array $join = [];

    /**
     * @var array
     */
    protected array $where = [];

    /**
     * @var array
     */
    protected array $order = [];

    /**
     * @var array
     */
    protected array $group = [];

    /**
     * @var array
     */
    protected array $having = [];

    /**
     * @var int
     */
    protected int $offset = 0;

    /**
     * @var int
     */
    protected int $limit = 0;

    /**
     * @var string
     */
    protected string $lock = '';

    /**
     * @param string $table
     * @return IConnection
     */
    public function table(string $table): IConnection {
        $prefix = $this->config['prefix'] ?? '';
        $this->table = $prefix . $table;
        return $this;
    }

    /**
     * @param string ...$fields
     * @return IConnection
     */
    public function select(string ...$fields): IConnection {
        $this->select = array_merge($this->select, $fields);
        return $this;
    }

    /**
     * @param string $table
     * @param string $on
     * @param ...$values
     * @return IConnection
     */
    public function join(string $table, string $on, ...$values): IConnection {
        $this->join[] = ['INNER JOIN', $table, $on, $values];
        return $this;
    }

    /**
     * @param string $table
     * @param string $on
     * @param ...$values
     * @return IConnection
     */
    public function leftJoin(string $table, string $on, ...$values): IConnection {
        $this->join[] = ['LEFT JOIN', $table, $on, $values];
        return $this;
    }

    /**
     * @param string $table
     * @param string $on
     * @param ...$values
     * @return IConnection
     */
    public function rightJoin(string $table, string $on, ...$values): IConnection {
        $this->join[] = ['RIGHT JOIN', $table, $on, $values];
        return $this;
    }

    /**
     * @param string $table
     * @param string $on
     * @param ...$values
     * @return IConnection
     */
    public function fullJoin(string $table, string $on, ...$values): IConnection {
        $this->join[] = ['FULL JOIN', $table, $on, $values];
        return $this;
    }

    /**
     * @param string $expr
     * @param ...$values
     * @return IConnection
     */
    public function where(string $expr, ...$values): IConnection {
        $this->where[] = ['AND', $expr, $values];
        return $this;
    }

    /**
     * @param string $expr
     * @param ...$values
     * @return IConnection
     */
    public function or(string $expr, ...$values): IConnection {
        $this->where[] = ['OR', $expr, $values];
        return $this;
    }

    /**
     * @param string $field
     * @param string $order
     * @return IConnection
     */
    public function order(string $field, string $order): IConnection {
        if (!in_array($order, ['asc', 'desc'])) {
            throw new \RuntimeException('Sort can only be asc or desc.');
        }
        $this->order[] = [$field, strtoupper($order)];
        return $this;
    }

    /**
     * @param string ...$fields
     * @return IConnection
     */
    public function group(string ...$fields): IConnection {
        $this->group = array_merge($this->group, $fields);
        return $this;
    }

    /**
     * @param string $expr
     * @param ...$values
     * @return IConnection
     */
    public function having(string $expr, ...$values): IConnection {
        $this->having[] = [$expr, $values];
        return $this;
    }

    /**
     * offset
     * @param int $length
     * @return IConnection
     */
    public function offset(int $length): IConnection {
        $this->offset = $length;
        return $this;
    }

    /**
     * limit
     * @param int $length
     * @return IConnection
     */
    public function limit(int $length): IConnection {
        $this->limit = $length;
        return $this;
    }

    /**
     * 意向排它锁
     * @return IConnection
     */
    public function lockForUpdate(): IConnection {
        $this->lock = 'FOR UPDATE';
        return $this;
    }

    /**
     * 意向共享锁
     * @return IConnection
     */
    public function sharedLock(): IConnection {
        $this->lock = 'LOCK IN SHARE MODE';
        return $this;
    }

    /**
     * @param string $index
     * @param array $data
     * @return array
     */
    protected function build(string $index, array $data = []): array {
        $sqls = $values = [];

        // select
        if ($index == 'SELECT') {
            if ($this->select) {
                foreach ($this->select as &$k) {
                    !str_starts_with($k, '`') and $k = "`{$k}`";
                }
                $select = implode(', ', $this->select);
                $sqls[] = "SELECT {$select}";
            } else {
                $sqls[] = "SELECT *";
            }
        }
        // count
        if ($index == 'COUNT') {
            if ($this->group) {
                $select = implode(', ', $this->group);
                $sqls[] = "SELECT count({$this->countField}) as count,{$select}";
            } else {
                $sqls[] = "SELECT count({$this->countField}) as count";
            }
        }
        // sum
        if ($index == 'SUM' && $this->sumFields) {
            $arr = [];
            foreach ($this->sumFields as $f) {
                $arr[] = "sum(`{$f}`) as {$f}";
            }
            $sumFileds = implode(",", $arr);
            if ($this->group) {
                $select = implode(', ', $this->group);
                $sqls[] = "SELECT {$sumFileds},{$select}";
            } else {
                $sqls[] = "SELECT {$sumFileds}";
            }
        }
        // delete
        if ($index == 'DELETE') {
            $sqls[] = "DELETE";
        }

        // table
        if ($this->table) {
            // update
            if ($index == 'UPDATE') {
                $set = [];
                unset($k);
                foreach ($data as $k => $v) {
                    if ($v instanceof Calculator) {
                        $c = clone $v;
                        $c->getIncValue() > 0 and $v = new Expr($k . ' + ?', $c->getIncValue());
                        $c->getRedValue() > 0 and $v = new Expr($k . ' - ?', $c->getRedValue());
                    }
                    if ($v instanceof Expr) {
                        $set[] = "$k = {$v->__toString()}";
                    } else {
                        $set[] = "$k = ?";
                        $values[] = $v;
                    }
                }
                $sqls[] = "UPDATE {$this->table} SET " . implode(', ', $set);
            } else {
                $sqls[] = "FROM {$this->table}";
            }
        }

        // join
        if ($this->join) {
            foreach ($this->join as $item) {
                list($keyword, $table, $on, $vals) = $item;
                $sqls[] = "{$keyword} {$table} ON {$on}";
                array_push($values, ...$vals);
            }
        }

        // where
        if ($this->where) {
            $sqls[] = "WHERE";
            foreach ($this->where as $key => $item) {
                list($keyword, $expr, $vals) = $item;

                // in 处理
                foreach ($vals as $k => $val) {
                    if (is_array($val)) {
                        foreach ($val as &$value) {
                            if (is_string($value)) {
                                $value = "'$value'";
                            }
                        }
                        $expr = preg_replace('/\(\?\)/', sprintf('(%s)', implode(',', $val)), $expr, 1);
                        unset($vals[$k]);
                    }
                }

                if ($key == 0) {
                    $sqls[] = "{$expr}";
                } else {
                    $sqls[] = "{$keyword} {$expr}";
                }
                array_push($values, ...$vals);
            }
        }

        // group
        if ($this->group) {
            $sqls[] = "GROUP BY " . implode(', ', $this->group);
        }

        // having
        if ($this->having) {
            $subSql = [];
            foreach ($this->having as $item) {
                list($expr, $vals) = $item;
                $subSql[] = "$expr";
                array_push($values, ...$vals);
            }
            $subSql = count($subSql) == 1 ? array_pop($subSql) : implode(' AND ', $subSql);
            $sqls[] = "HAVING {$subSql}";
        }

        // order
        if ($this->order) {
            $subSql = [];
            foreach ($this->order as $item) {
                list($field, $order) = $item;
                $subSql[] = "{$field} {$order}";
            }
            $sqls[] = "ORDER BY " . implode(', ', $subSql);
        }

        // limit and offset
        if ($this->limit > 0) {
            $sqls[] = 'LIMIT ?, ?';
            array_push($values, $this->offset, $this->limit);
        }

        // lock
        if ($this->lock) {
            $sqls[] = $this->lock;
        }

        // clear
        $this->table = '';
        $this->select = [];
        $this->join = [];
        $this->where = [];
        $this->order = [];
        $this->group = [];
        $this->having = [];
        $this->sumFields = [];
        $this->countField = '';
        $this->offset = 0;
        $this->limit = 0;
        $this->lock = '';

        // 聚合
        return [implode(' ', $sqls), $values];
    }

}
