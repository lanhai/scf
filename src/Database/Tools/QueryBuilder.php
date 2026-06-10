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
                    //修改成判断如果是 MIN或者MAX函数,则替换成 $k as 函数里的字段
                    if (str_starts_with($k, 'MIN') || str_starts_with($k, 'MAX')) {
                        // 提取函数里的字段名
                        preg_match('/\((.*?)\)/', $k, $matches);
                        if (isset($matches[1])) {
                            $field = trim($matches[1], '`');
                            $k = "{$k} AS {$field}";
                        }
                    } else {
                        !str_starts_with($k, '`') and $k = "`{$k}`";
                    }
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
                $bound = [];
                $inPlaceholderReplacements = [];
                $inPlaceholderIndex = 0;
                foreach ($vals as $val) {
                    if (is_array($val)) {
                        $marker = $this->makeInPlaceholderMarker($key, $inPlaceholderIndex++);
                        if (!$val) {
                            $expr = preg_replace('/\(\?\)/', '(' . $marker . ')', $expr, 1, $replaceCount);
                            if ($replaceCount > 0) {
                                $inPlaceholderReplacements[$marker] = 'NULL';
                            }
                            continue;
                        }
                        $placeholders = [];
                        $arrayBound = [];
                        foreach ($val as $inValue) {
                            if ($inValue instanceof Expr) {
                                $placeholders[] = $inValue->getExpr();
                                array_push($arrayBound, ...$inValue->getValues());
                            } else {
                                $placeholders[] = '?';
                                $arrayBound[] = $inValue;
                            }
                        }
                        $expr = preg_replace('/\(\?\)/', '(' . $marker . ')', $expr, 1, $replaceCount);
                        if ($replaceCount > 0) {
                            $inPlaceholderReplacements[$marker] = implode(',', $placeholders);
                            array_push($bound, ...$arrayBound);
                        }
                    } elseif ($val instanceof Expr) {
                        $count = 0;
                        $expr = preg_replace('/\?/', $val->getExpr(), $expr, 1, $count);
                        if ($count > 0) {
                            array_push($bound, ...$val->getValues());
                        }
                    } else {
                        $bound[] = $val;
                    }
                }
                if ($inPlaceholderReplacements) {
                    $expr = str_replace(array_keys($inPlaceholderReplacements), array_values($inPlaceholderReplacements), $expr);
                }

                if ($key == 0) {
                    $sqls[] = "{$expr}";
                } else {
                    $sqls[] = "{$keyword} {$expr}";
                }
                array_push($values, ...$bound);
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

    /**
     * 为 IN 条件生成本次构建过程内唯一的临时标记。
     *
     * QueryBuilder 会逐个展开同一条 where 表达式里的数组参数。当前一个 IN 只有单值时，
     * 直接替换成 `(?)` 会让后续数组参数再次命中同一个占位符；临时标记用于先占位，
     * 等所有数组参数都消费完各自的 IN 位置后再还原成真正的 SQL 占位符。
     *
     * @param int|string $whereKey 当前 where 条目的序号。
     * @param int $index 当前 where 表达式内第几个数组参数。
     * @return string
     */
    protected function makeInPlaceholderMarker(int|string $whereKey, int $index): string {
        return '__SCF_QUERY_BUILDER_IN_PLACEHOLDER_' . spl_object_id($this) . '_' . $whereKey . '_' . $index . '__';
    }
}
