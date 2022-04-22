<?php

namespace Scf\Database;

use Co\Channel;
use JetBrains\PhpStorm\ArrayShape;
use JetBrains\PhpStorm\Pure;
use Swoole\Coroutine;
use Mix\Database\ConnectionInterface;
use Mix\Database\Expr;
use Scf\Core\Log;
use Scf\Core\Struct;
use Scf\Database\QueryBuilder\WhereBuilder;
use Scf\Database\Connection\PdoPool;
use Scf\Database\Exception\NullAR;
use Scf\Helper\JsonHelper;

class Query extends Struct {
    /**
     * 数据库名称
     * @var string
     */
    protected string $_dbName = 'default';
    /**
     * 表名称
     * @var string
     */
    protected string $_table;
    /**
     * 数据库主键名
     * @var string
     */
    protected string $_primaryKey = 'id';

    protected string $_autoIncKey = '';

    protected int $actor = DBS_SLAVE;
    /**
     * @var int 单页数据条数
     */
    protected int $_pageSize = 10;
    /**
     * @var int 当前页码
     */
    protected int $_pn = 1;

    /**
     * @var int 缓存时间
     */
    protected int $cacheLifeTime = 60;

    /**
     * 快照
     * @var ?array
     */
    protected array|null $snapshot = null;


    /**
     * @var ?WhereBuilder 查询语句构造器
     */
    protected WhereBuilder|null $_where = null;
    /**
     * @var ?array 指定读取字段
     */
    protected array|null $_fields = null;
    /**
     * @var ?array 排序
     */
    protected array|null $_order = null;
    protected string $_customPrimaryKey = '';
    protected null|string|int $_primaryVal = null;

    protected static array $ar_instance;
    protected static array $db_instance;
    protected bool $_arExist = false;
    protected PdoPool $transactionConnection;

    /**
     * @param array|string $field
     * @param string|null $group
     * @param string|null $order
     * @return array|false|int|mixed|object
     */
    public function sum(array|string $field, string $group = null, string $order = null): mixed {
        if ($this->actor == DBS_SLAVE) {
            $db = $this->slave();
        } else {
            $db = $this->master();
        }
        try {
            return $db->sum($this->_table, $field, $this->getWhereSql(), $group, $order);
        } catch (\PDOException $exception) {
            $this->addError($this->_table . '_SUM', $exception->getMessage());
            return false;
        }
    }


    /**
     * 统计指定条件的行数
     * @return int
     */
    public function count(): int {
        if ($this->actor == DBS_SLAVE) {
            $db = $this->slave();
        } else {
            $db = $this->master();
        }
        try {
            return $db->count($this->_table, $this->_primaryKey, $this->getWhereSql());
        } catch (\PDOException $exception) {
            $this->addError($this->_table . '_COUNT', $exception->getMessage());
            return 0;
        }
    }

    /**
     * 查询指定记录是否存在
     * @param int $cacheExpired 缓存过期时间,0:为不启用缓存
     * @return bool
     */
    public function found(int $cacheExpired = 0): bool {
        if (is_null($this->getWhereSql()) && !is_null($this->_primaryVal)) {
            $this->where(WhereBuilder::create([$this->getPrimaryKey() => $this->_primaryVal]));
        }
        if (is_null($this->getWhereSql())) {
            return false;
        }
        $key = '_DB_EXIST_' . md5($this->_dbName . $this->_table . $this->getWhereSql());
        if ($cacheExpired && Cache::instance()->get($key) !== false) {
            return true;
        }
        $exist = $this->count() > 0;
        if ($exist && $cacheExpired) {
            Cache::instance()->set($key, time(), $cacheExpired);
        }
        return $exist;
    }

    /**
     * 获取所有数据
     * @param bool $format
     * @return array
     */
    public function all(bool $format = true): array {
        $connection = $this->connection();
        $list = $connection->get();
        if ($list && $format) {
            foreach ($list as &$item) {
                $item = $this->format($item);
            }
        }
        if ($list && !is_null($this->_fields) && count($this->_fields) == 1) {
            $ids = [];
            foreach ($list as $item) {
                $ids[] = $item[$this->_fields[0]];
            }
            return $ids;
        }
        return $list;
    }

    /**
     * 获取随机查询结果
     * @param int $size
     * @param bool $asAR
     * @return static|array|NullAR|null
     */
    public function random(int $size = 1, bool $asAR = true): static|NullAR|array|null {
        //最多取100条
        $size = min($size, 100);
        $cacheKey = '_DB_RANDOM_CACHE_' . md5(JsonHelper::toJson($this->_fields) . JsonHelper::toJson($this->_order) . $this->getWhereSql());
        if (!$collections = Cache::instance()->get($cacheKey)) {
            try {
                $count = $this->count();
                if (!$count) {
                    if ($size == 1 && $asAR) return new NullAR(get_called_class(), $this->getWhereSql());
                    return null;
                }
                $size = min($size, $count);
                //1000条以内直接读取全部id合集存入缓存
                if ($count <= 1000) {
                    $collections = $this->pluck($this->getPrimaryKey());
                } else {
                    $channelSize = 50;
                    $pageSize = 20;
                    $totalPage = ceil($count / $pageSize);
                    $pages = [];
                    for ($p = 0; $p < $totalPage; $p++) {
                        $pages[] = $p;
                    }
                    $randPages = [];
                    //取随机100条页码
                    for ($i = 0; $i < $channelSize; $i++) {
                        shuffle($pages);
                        $randPages[] = $pages[0];
                        array_shift($pages);
                    }
                    //采用随机分页循环100次取数据,每页随机取10条
                    $channel = new Channel($channelSize);
                    for ($i = 0; $i < $channelSize; $i++) {
                        $pn = $randPages[$i];
                        Coroutine::create(function () use ($pn, $channel, $pageSize, $count) {
                            $where = $this->_where ?: [];
                            $channel->push(static::select($this->getPrimaryKey())->where($where)->topPrimarys($pageSize, $pn, $count));
                        });
                    }
                    $ids = [];
                    for ($i = 0; $i < $channelSize; $i++) {
                        $ids = [...$ids, ...$channel->pop()];
                    }
                    $collections = $ids;
                }
                //id合集缓存5分钟
                Cache::instance()->set($cacheKey, $collections, 300);
            } catch (\PDOException $exception) {
                $this->addError($this->_table . '_AR', $exception->getMessage());
                if ($size == 1 && $asAR) return new NullAR(get_called_class(), $this->getWhereSql());
                return null;
            }
        }
        if ($size === 1) {
            shuffle($collections);
            $primaryValue = $collections[0];
            if ($asAR) {
                $this->setPrimaryVal($primaryValue);;
                return $this->ar();
            }
            return $this->where([$this->getPrimaryKey() => $primaryValue])->first();
        }
        $primaryValue = [];
        for ($i = 0; $i < $size; $i++) {
            shuffle($collections);
            $primaryValue[] = $collections[0];
            array_shift($collections);
        }
        return $this->where([$this->getPrimaryKey() => $primaryValue])->all();
    }

    /**
     * 获取第一条数据
     * @return array|null
     */
    public function first(): ?array {
        if (!is_null($this->_primaryVal)) {
            $this->_where = WhereBuilder::create([$this->getPrimaryKey() => $this->_primaryVal]);
        }
        $connection = $this->connection();
        try {
            if (!$result = $connection->first()) {
                return null;
            }
            return $result;
        } catch (\PDOException $exception) {
            $this->addError($this->_table . '_AR', $exception->getMessage());
            return null;
        }
    }

    /**
     * @return NullAR|static
     */
    public function ar(): static|NullAR {
        $primaryVal = $this->_primaryVal ?: $this->queryPrimaryVal();
        if (is_null($primaryVal)) {
            return new NullAR(get_called_class(), $this->getWhereSql());
        }
        //获取存活的记录
        if (!$result = Cache::instance()->get($this->getArCacheKey($primaryVal))) {
            try {
                if (!$result = $this->first()) {
                    return new NullAR(get_called_class(), $this->getWhereSql());
                }
                Cache::instance()->set($this->getArCacheKey($primaryVal), $result, $this->cacheLifeTime);
                Cache::instance()->close();
            } catch (\PDOException $exception) {
                $this->addError($this->_table . '_AR', $exception->getMessage());
                Log::instance()->error($exception->getMessage());
                return new NullAR(get_called_class(), $this->getWhereSql());
            }
        }
        $this->_arExist = true;
        $this->create($result);
        $this->snapshot = $this->toArray();
        return $this;
    }

    /**
     * 获取列表
     * @param bool $format
     * @return array
     */
    #[ArrayShape(['list' => "array", 'pages' => "int", 'pn' => "int", 'total' => "int", 'primarys' => "array"])]
    public function list(bool $format = true): array {
        $total = $this->count();
        $totalPage = $total ? ceil($total / $this->_pageSize) : 0;
        $page = min($this->_pn, $totalPage) ?: 1;
        $list = $this->top($this->_pageSize, $page, $total, $format);
        $primaryKeys = [];
        if ($list) {
            foreach ($list as &$item) {
                $primaryKeys[] = $item[$this->_primaryKey];
            }
        }
        return ['list' => $list, 'pages' => (int)$totalPage, 'pn' => (int)$page, 'total' => $total, 'primarys' => $primaryKeys];
    }

    /**
     * 获取最新记录
     * @param int $size
     * @param int $pn
     * @param int $total
     * @param bool $format
     * @return array
     */
    public function top(int $size = 10, int $pn = 1, int $total = 0, bool $format = true): array {
        $connection = $this->connection();
        if ($total) {
            $totalPage = ceil($total / $size);
            $pn = min($pn, $totalPage) ?: 1;
            $offset = ($pn - 1) * $size;
        } else {
            $offset = 0;
        }
        $connection->offset($offset);
        $connection->limit($size);
        $list = $connection->get();
        $primaryKeys = [];
        if ($list) {
            foreach ($list as &$item) {
                $primaryKeys[] = $item[$this->_primaryKey];
                if ($format) {
                    $item = $this->format($item);
                }
            }
            if (!empty($this->_autoIncKey)) {
                $this->master()->table($this->_table)->where($this->_primaryKey . ' IN (?)', $primaryKeys)->updates([$this->_autoIncKey => new Expr($this->_autoIncKey . ' + ?', 1)]);
            }
        }
        return $list;
    }

    /**
     * 获取最新的主键值合集
     * @param int $size
     * @param int $pn
     * @param int $total
     * @return array
     */
    public function topPrimarys(int $size = 10, int $pn = 1, int $total = 0): array {
        $list = $this->top($size, $pn, $total);
        $primaryKeys = [];
        if ($list) {
            foreach ($list as $item) {
                $primaryKeys[] = $item[$this->_primaryKey];
            }
        }
        return $primaryKeys;
    }

    /**
     * 获取主键合集
     * @return array
     */
    public function primaryKeys(): array {
        return $this->pluck($this->_primaryKey);
    }

    /**
     * 取一列的值
     * @param string|null $key
     * @return array
     */
    public function pluck(string $key = null): array {
        $connection = $this->connection();
        $key = !is_null($key) ? $key : $this->_primaryKey;
        $connection->select($key);
        $column = [];
        try {
            if ($result = $connection->get()) {
                foreach ($result as $item) {
                    $column[] = $item[$key];
                }
            }
        } catch (\PDOException $exception) {
            $this->addError($this->_table . '_AR', $exception->getMessage());
        }
        return $column;
    }

    /**
     * 获取主键值
     * @return mixed
     */
    public function queryPrimaryVal(): mixed {
        $connection = $this->connection();
        $connection->select($this->getPrimaryKey());
        try {
            if (!$result = $connection->first()) {
                return null;
            }
            return $result[$this->getPrimaryKey()];
        } catch (\PDOException $exception) {
            $this->addError($this->_table . '_AR', $exception->getMessage());
            return null;
        }
    }

    /**
     * 批量更新
     * @param $datas
     * @return int
     */
    public function update($datas): int {
        $connection = $this->connection(DBS_MASTER);
        $row = $connection->updates($datas)->rowCount();
        if ($row) {
            $priIds = $this->primaryKeys();
            $this->deleteArCache($priIds);
        }
        return $row;
    }


    /**
     * 删除数据
     * @return int
     */
    public function delete(): int {
        $uniqueKey = $this->getPrimaryKey();
        $uniqueVal = $this->getPrimaryVal();
        if (!is_null($uniqueVal)) {
            $row = $this->master()->table($this->_table)->where("`{$uniqueKey}` = ?", $uniqueVal)->delete()->rowCount();
            if ($row) {
                $this->deleteArCache($uniqueVal);
                $this->snapshot = null;
                if (!empty($this->$uniqueKey)) unset($this->$uniqueKey);
            }
        } else {
            $priIds = $this->primaryKeys();
            $row = $this->connection(DBS_MASTER)->delete()->rowCount();
            if ($row && $priIds) {
                $this->deleteArCache($priIds);
            }
        }
        return $row;
    }


    /**
     * 保存活动记录
     * @param bool $filterNull
     * @return bool
     */
    public function save(bool $filterNull = true): bool {
        try {
            if (!$this->validate()) {
                return false;
            }
            //数据格式化
            $primaryKey = $this->getPrimaryKey();
            $datas = $this->format($this->toArray($filterNull));
            if (!$datas) {
                return false;
            }
            if (!empty($this->$primaryKey) && self::has($datas[$primaryKey])) {
                $arCacheKey = $this->getArCacheKey($this->$primaryKey);
                //如果存在快照只更新值变动过的字段
                if (!is_null($this->snapshot)) {
                    foreach ($datas as $k => $v) {
                        $v = is_array($v) ? JsonHelper::toJson($v) : $v;
                        if (isset($this->snapshot[$k])) {
                            $snapshotVaule = is_array($this->snapshot[$k]) ? JsonHelper::toJson($this->snapshot[$k]) : $this->snapshot[$k];
                            if (md5(strval($v)) == md5(strval($snapshotVaule))) {
                                unset($datas[$k]);
                            } else {
                                //更新快照里对应字段的值
                                $this->snapshot[$k] = $datas[$k];
                            }
                        }
                    }
                }
                if ($datas) {
                    $row = $this->master()->table($this->_table)->where("`{$primaryKey}` = ?", $this->$primaryKey)->updates($datas)->rowCount();
                    if ($row && $cache = Cache::instance()->get($arCacheKey)) {
                        foreach ($datas as $k => $v) {
                            $cache[$k] = $v;
                        }
                        //换新缓存
                        Cache::instance()->set($arCacheKey, $cache, $this->cacheLifeTime);
                        Cache::instance()->close();
                    }
                }
            } else {
                $this->$primaryKey = $this->master()->insert($this->_table, $datas)->lastInsertId();
            }
        } catch (\PDOException $error) {
            $this->addError('save', $error->getMessage());
            return false;
        }
        return true;
    }

    /**
     * @param $fields
     * @return static
     */
    public static function select(...$fields): static {
        $name = get_called_class();
        /**
         * @var Query $cls
         */
        $cls = new $name();
        if ($fields) {
            $cls->setFields(...$fields);
        }
        return $cls;
    }

    /**
     * 查询唯一数据
     * @param int|string $val
     * @param int $actor 数据库类型
     * @param string|null $key 字段名称
     * @return static
     */
    public static function unique(int|string $val, int $actor = DBS_SLAVE, string $key = null): static {
        $cls = static::select();
        $cls->setActor($actor);
        $cls->setPrimaryKey(is_null($key) ? $cls->_primaryKey : $key);
        $cls->setPrimaryVal($val);
        return $cls;
    }

    /**
     * 是否是空值
     * @return bool
     */
    public function notExist(): bool {
        return !$this->_arExist;
    }

    /**
     * @return bool
     */
    public function exist(): bool {
        return $this->_arExist;
    }

    /**
     * 读取协程AR
     * @param $id
     * @return static|null
     */
    protected static function getInstanceAr($id): static|null {
        $key = self::getInstanceArKey($id);
        return self::$ar_instance[$key] ?? null;
    }

    /**
     * 设置协程AR
     * @param $id
     * @param $ar
     * @return void
     */
    protected static function setInstanceAr($id, $ar) {
        $cid = Coroutine::getCid();
        $key = self::getInstanceArKey($id);
        self::$ar_instance[$key] = $ar;
        if ($cid > 0) {
            Coroutine::defer(function () use ($key) {
                //Console::log('释放AR资源:' . $key);
                unset(self::$ar_instance[$key]);
            });
        }
    }

    /**
     * 更新协程AR
     * @param $id
     * @param $ar
     * @return void
     */
    protected static function updateInstanceAr($id, $ar) {
        $key = self::getInstanceArKey($id);
        if (isset(self::$ar_instance[$key])) {
            self::$ar_instance[$key] = $ar;
        }
    }

    /**
     * 获取协程AR KEY
     * @param $id
     * @return string
     */
    protected static function getInstanceArKey($id): string {
        $name = get_called_class();
        $cid = Coroutine::getCid();
        return str_replace("\\", "", $name) . '_' . $id . '_' . $cid;
    }

    /**
     * 释放AR
     * @param int|null $cid
     * @return void
     */
    public function destroyInstanceAr($id, int $cid = null) {
        $name = get_called_class();
        if ($cid === null) {
            $cid = Coroutine::getCid();
        }
        $instanceKey = $name . '_' . $id . '_' . $cid;
        unset(self::$ar_instance[$instanceKey]);
    }

    /**
     * 清除AR缓存
     * @param int|array $id
     * @return void
     */
    protected function deleteArCache(int|array $id) {
        if (!$id) {
            return;
        }
        if (is_array($id)) {
            foreach ($id as $i) {
                $arCacheKey = $this->getArCacheKey($i);
                Cache::instance()->delete($arCacheKey);
                //$this->destroyInstanceAr($id);
            }
        } else {
            $arCacheKey = $this->getArCacheKey($id);
            Cache::instance()->delete($arCacheKey);
            //$this->destroyInstanceAr($id);
        }
        Cache::instance()->close();
    }

    /**
     * 判断是否存在
     * @param $id
     * @param int $actor
     * @return bool
     */
    public static function has($id, int $actor = DBS_SLAVE): bool {
        $name = get_called_class();
        /**
         * @var Query $cls
         */
        $cls = new $name();
        $cls->setActor($actor);
//        if (Cache::instance()->get($cls->getArCacheKey($id))) {
//            return true;
//        }
        if ($actor == DBS_SLAVE) {
            $db = $cls->slave();
        } else {
            $db = $cls->master();
        }
        $result = $db->table($cls->_table)->where("`{$cls->_primaryKey}` = ?", $id)->select($cls->_primaryKey)->first();
        if ($result) {
            return true;
        }
        return false;
    }

    /**
     * 创建一个AR实例
     * @param array|null $data
     * @param string $scene
     * @return static
     */
    public static function factory(array $data = null, string $scene = ''): static {
        return parent::factory($data, $scene);
    }

    /**
     * 设定页码
     * @param int $pn
     * @param int $size
     * @return static
     */
    public function page(int $pn = 1, int $size = 10): static {
        $this->_pn = $pn;
        $this->_pageSize = $size;
        return $this;
    }

    /**
     * @param WhereBuilder|array $where
     * @return static
     */
    public function where(WhereBuilder|array $where): static {
        if (is_array($where)) {
            $where = new WhereBuilder($where);
        }
        $this->_where = $where;
        return $this;
    }

    /**
     * @param ...$fields
     * @return static
     */
    protected function setFields(...$fields): static {
        $this->_fields = $fields;
        return $this;
    }

    /**
     * @param array $order
     * @return static
     */
    public function order(array $order): static {
        $this->_order = $order;
        return $this;
    }

    /**
     * 返回数据库设置
     * @return array
     */
    #[ArrayShape(['db' => "string", 'table' => "string", 'primary_key' => "string"])]
    public function config(): array {
        return [
            'db' => $this->_dbName,
            'table' => $this->_table,
            'primary_key' => $this->_primaryKey
        ];
    }

    /**
     * @param int $actor
     * @return ConnectionInterface
     */
    protected function connection(int $actor = 0): ConnectionInterface {
        $actor = $actor ?: $this->actor;
        if ($actor == DBS_SLAVE) {
            $connection = $this->slave()->table($this->_table);
        } else {
            $connection = $this->master()->table($this->_table);
        }
        if (!is_null($this->_where)) {
            $where = $this->_where->build();
            $where['sql'] and $connection->where($where['sql'], ...$where['match']);
        }
        if (!is_null($this->_order)) {
            foreach ($this->_order as $field => $order) {
                $connection->order($field, strtolower($order));
            }
        }
        if (!is_null($this->_fields)) {
            $connection->select(...$this->_fields);
        }
        $this->resetQueryParams();
        return $connection;
    }

    /**
     * 从库连接
     * @return PdoPool
     */
    protected function slave(): PdoPool {
        $cid = Coroutine::getCid();
        if (!isset(self::$db_instance[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid])) {
            self::$db_instance[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid] = Pdo::slave($this->_dbName)->connection();
            if ($cid > 0) {
                Coroutine::defer(function () use ($cid) {
                    unset(self::$db_instance[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid]);
                });
            }
        }
        return self::$db_instance[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid];
    }

    /**
     * 主库连接
     * @return PdoPool
     */
    protected function master(): PdoPool {
        $cid = Coroutine::getCid();
        if (isset($this->transactionConnection)) {
            return $this->transactionConnection;
        }
        if (!isset(self::$db_instance[DBS_MASTER . '_' . $this->_dbName . '_' . $cid])) {
            self::$db_instance[DBS_MASTER . '_' . $this->_dbName . '_' . $cid] = Pdo::master($this->_dbName)->connection();
            if ($cid > 0) {
                Coroutine::defer(function () use ($cid) {
                    unset(self::$db_instance[DBS_MASTER . '_' . $this->_dbName . '_' . $cid]);
                });
            }
        }
        return self::$db_instance[DBS_MASTER . '_' . $this->_dbName . '_' . $cid];
    }

    /**
     * 开启事务
     * @return void
     */
    public function beginTransaction() {
        $this->transactionConnection = Pdo::beginTransaction($this->_dbName);
    }

    /**
     * 事务回滚
     * @return void
     */
    public function rollback() {
        $this->transactionConnection->rollback();
        unset($this->transactionConnection);
    }

    /**
     * 事务提交
     * @return void
     */
    public function commit() {
        $this->transactionConnection->commit();
        unset($this->transactionConnection);
    }

    /**
     * 获取活动数据记录缓存KEY
     * @param $val
     * @return string
     */
    protected function getArCacheKey($val): string {
        $arKey = $this->getPrimaryKey();
        return APP_ID . '_AR_' . md5(strtoupper($this->_dbName) . '_' . strtoupper($this->_table) . '_' . $arKey . '_' . $val . JsonHelper::toJson($this->_fields));
    }

    /**
     * @return null|string
     */
    protected function getWhereSql(): string|null {
        if (is_null($this->_where)) {
            return null;
        }
        return $this->_where->getWhereSql();
    }

    /**
     * 数据转换
     * @param array|null $data
     * @return array
     */
    protected function format(array $data = null): array {
        if (!$this->_validate) {
            return $data;
        }
        $data = is_null($data) ? $this->toArray(true) : $data;
        foreach ($this->_validate as $f => $v) {
            if (isset($data[$f]) && isset($v['format'])) {
                $dataType = $v['format'][0]['type'];
                if ($dataType == 'json') {
                    if (JsonHelper::is($data[$f])) {
                        $data[$f] = JsonHelper::recover($data[$f]);
                    } else {
                        $data[$f] = JsonHelper::toJson($data[$f]);
                    }
                }
            }
        }
        return $data;
    }

    /**
     * 设置主从库
     * @param $actor
     * @return $this
     */
    protected function setActor($actor): static {
        $this->actor = $actor;
        return $this;
    }


    /**
     * 设置AR缓存字段
     * @param $key
     * @return void
     */
    protected function setPrimaryKey($key) {
        $this->_customPrimaryKey = $key;
    }

    /**
     * 获取AR缓存字段
     * @return string
     */
    protected function getPrimaryKey(): string {
        return $this->_customPrimaryKey ?: $this->_primaryKey;
    }

    /**
     * 设置ar匹配值
     * @param $val
     * @return void
     */
    protected function setPrimaryVal($val) {
        $this->_primaryVal = $val;
    }

    /**
     * @return int|string|null
     */
    #[Pure]
    protected function getPrimaryVal(): int|string|null {
        if (!is_null($this->_primaryVal)) {
            return $this->_primaryVal;
        }
        $uniqueKey = $this->getPrimaryKey();
        return !empty($this->$uniqueKey) ? $this->$uniqueKey : null;
    }

    /**
     * 重置参数
     * @return void
     */
    protected function resetQueryParams() {
        $this->_order = null;
        $this->_fields = null;
        $this->_where = null;
    }
}