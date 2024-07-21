<?php

namespace Scf\Database;

use Co\Channel;
use JetBrains\PhpStorm\ArrayShape;
use JetBrains\PhpStorm\Pure;
use PDOException;
use Scf\Command\Color;
use Scf\Component\Cache;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Core\Struct;
use Scf\Database\Exception\NullAR;
use Scf\Database\Tools\Calculator;
use Scf\Database\Tools\Expr;
use Scf\Database\Tools\WhereBuilder;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Util\File;
use Swoole\Coroutine;
use Symfony\Component\Yaml\Yaml;
use Throwable;

/**
 * 数据库访问对象Database Access Objects
 */
class Dao extends Struct {
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
    protected string $_createSql = '';

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
    /**
     * @var string|array|null
     */
    protected string|array|null $_group = null;
    protected string $_customPrimaryKey = '';
    protected null|string|int $_primaryVal = null;

    protected static array $_instances;
    protected static array $_db_instances;
    protected bool $_arExist = false;
    protected Transaction $transaction;
    private string|null $whereSql = null;


    /**
     * @param mixed ...$fields
     * @return array|bool|int|float
     */
    public function sum(...$fields): array|bool|int|float {
        $connection = $this->connection();
        try {
            return $connection->sum(...$fields);
        } catch (Throwable $exception) {
            $this->addError($this->_table . '_SUM', $exception->getMessage());
            return false;
        }
    }


    /**
     * 统计指定条件的行数
     * @param string|null $field
     * @return int|array
     */
    public function count(string $field = null): int|array {
        $field = $field ?: $this->_primaryKey;
        $connection = $this->connection();
        try {
            return $connection->count($field);
        } catch (Throwable $exception) {
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
        $where = $this->_where ? clone $this->_where : [];
        //最多取100条
        $size = min($size, 100);
        $count = static::select($this->getPrimaryKey())->where($where)->count();
        if ($count == 0) {
            if ($size == 1 && $asAR) return new NullAR(static::class, $this->whereSql);
            return null;
        }
        if ($size == 1) {
            $queryResult = $this->where($where)->page(rand(1, $count), 1)->list(total: $count);
            if ($asAR) {
                $this->_arExist = true;
                $this->install($queryResult['list'][0]);
                return $this;
            }
            return $queryResult['list'][0];
        }
        $cacheKey = '_QUERY_RANDOM_CACHE_' . md5(JsonHelper::toJson($this->_fields) . JsonHelper::toJson($this->_order) . $this->getWhereSql());
        if (!$collections = Cache::instance()->get($cacheKey)) {
            try {
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
                //id合集缓存60秒
                Cache::instance()->set($cacheKey, $collections, 60);
            } catch (PDOException $exception) {
                $this->addError($this->_table . '_AR', $exception->getMessage());
                if ($size == 1 && $asAR) return new NullAR(static::class, $this->whereSql);
                return null;
            }
        }
        $primaryValue = [];
        $size = min($size, count($collections));
        for ($i = 0; $i < $size; $i++) {
            shuffle($collections);
            $primaryValue[] = $collections[0];
            array_shift($collections);
        }
        return $this->where([$this->getPrimaryKey() => $primaryValue])->all();
    }

    /**
     * 获取第一条数据
     * @param bool $format
     * @return array|null
     */
    public function first(bool $format = true): ?array {
        if (!is_null($this->_primaryVal)) {
            $this->_where = WhereBuilder::create([$this->getPrimaryKey() => $this->_primaryVal]);
        }
        $connection = $this->connection();
        try {
            if ($result = $connection->first()) {
                if ($format) {
                    $result = $this->format($result);
                }
                return $result;
            }
        } catch (PDOException $exception) {
            $this->addError($this->_table . '_AR', $exception->getMessage());
        }
        return null;
    }

    /**
     * @return NullAR|static
     */
    public function ar(): static|NullAR {
        $primaryVal = $this->_primaryVal ?: $this->queryPrimaryVal();
        if (is_null($primaryVal)) {
            return new NullAR(static::class, $this->whereSql);
        }
        //获取缓存数据
        if (!$result = Cache::instance()->get($this->getArCacheKey($primaryVal))) {
            try {
                if (!$result = $this->first()) {
                    return new NullAR(static::class, $this->whereSql);
                }
                Cache::instance()->set($this->getArCacheKey($primaryVal), $result, $this->cacheLifeTime);
            } catch (PDOException $exception) {
                $this->addError($this->_table . '_AR', $exception->getMessage());
                Log::instance()->error($exception->getMessage());
                return new NullAR(static::class, $this->whereSql);
            }
        }
        $this->_arExist = true;
        $this->install($result);
        $this->snapshot = $this->toArray();
        return $this;
    }

    /**
     * 获取列表
     * @param bool $format
     * @param int $total
     * @return array
     */
    #[ArrayShape(['list' => "array", 'pages' => "int", 'pn' => "int", 'total' => "int", 'primarys' => "array"])]
    public function list(bool $format = true, int $total = 0): array {
        $select = self::select($this->_primaryKey)->where($this->_where);
        if ($this->_group) {
            $select->group($this->_group);
        }
        $total = $total ?: $select->count();
        $total = is_array($total) ? count($total) : $total;
        $totalPage = $total ? ceil($total / $this->_pageSize) : 0;
        $page = min($this->_pn, $totalPage) ?: 1;
        $list = $this->top($this->_pageSize, $page, $total, $format);
        $primaryKeys = [];
        if ($list) {
            foreach ($list as $item) {
                $primaryKeys[] = $item[$this->_primaryKey] ?? 0;
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
                $primaryKeys[] = $item[$this->_primaryKey] ?? 0;
                if ($format) {
                    $item = $this->format($item);
                }
            }
            if ($this->_fields && !in_array($this->_primaryKey, $this->_fields)) {
                return $list;
            }
            if (!empty($this->_autoIncKey)) {
                $connection = $this->transaction ?? $this->master();
                $connection->table($this->_table)->where($this->_primaryKey . ' IN (?)', $primaryKeys)->updates([$this->_autoIncKey => new Expr($this->_autoIncKey . ' + ?', 1)]);
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
        } catch (PDOException $exception) {
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
        if (!$this->getPrimaryKey()) {
            return null;
        }
        //$connection->select($this->getPrimaryKey());
        $connection->select();
        try {
            if (!$result = $connection->first()) {
                return null;
            }
            $primaryVal = $result[$this->getPrimaryKey()];
            Cache::instance()->set($this->getArCacheKey($primaryVal), $result, $this->cacheLifeTime);
            $this->setPrimaryVal($primaryVal);
            return $primaryVal;
        } catch (PDOException $exception) {
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
        if (is_null($this->_where) && !is_null($this->_primaryVal)) {
            $this->where([$this->_primaryKey => $this->_primaryVal]);
        }
        $connection = $this->connection(DBS_MASTER, false);
        $row = $connection->updates($datas)->rowCount();
        if ($row) {
            $priIds = $this->primaryKeys();
            $this->deleteArCache($priIds);
        }
        $this->resetQueryParams();
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
            $connection = $this->transaction ?? $this->master();
            $row = $connection->table($this->_table)->where("`{$uniqueKey}` = ?", $uniqueVal)->delete()->rowCount();
            if ($row) {
                $this->deleteArCache($uniqueVal);
                $this->snapshot = null;
                if (!empty($this->$uniqueKey)) unset($this->$uniqueKey);
            }
        } else {
            $row = $this->connection(DBS_MASTER, false)->delete()->rowCount();
            $priIds = $this->primaryKeys();
            if ($row && $priIds) {
                $this->deleteArCache($priIds);
            }
            $this->resetQueryParams();
        }
        return $row;
    }

    /**
     * 创建表
     * @param string|null $sql
     * @return bool|array
     */
    public function createTable(?string $sql = null): bool|array {
        $sql = $sql ?: $this->_createSql;
        if ($sql && !$this->tableExist()) {
            try {
                Pdo::master($this->getDb())->getDatabase()->exec($sql);
            } catch (Throwable) {
                return false;
            }
        }
        return true;
    }

    /**
     * 数据表是否存在
     * @return bool
     */
    public function tableExist(): bool {
        try {
            $tablePrefix = Pdo::master($this->_dbName)->getConfig('prefix');
            $completeTable = $tablePrefix . $this->getTable();
            Pdo::master($this->getDb())->getDatabase()->exec('DESCRIBE ' . $completeTable);
            return true;
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * 更新数据表结构
     * @param $latest
     * @return void
     */
    public function updateTable($latest): void {
        $key = $latest['db'] . '_' . $latest['table'];
        $versionFile = APP_PATH . '/db/updates/' . $key . '.yml';
        $current = file_exists($versionFile) ? Yaml::parseFile($versionFile) : null;
        if (!$current) {
            if (!$this->createTable($latest['create'])) {
                Console::log("【Database】{$latest['db']}.{$latest['table']} " . Color::red('创建失败'));
            } else {
                Console::log("【Database】{$latest['db']}.{$latest['table']} " . Color::success('创建成功'));
                File::write($versionFile, Yaml::dump($latest, 3));
            }
        } elseif ($current['version'] !== $latest['version']) {
            if (App::isDevEnv()) goto update;
            //更新字段
            Console::info("【Database】{$latest['db']}.{$latest['table']} " . Color::yellow('需要更新'));
            // 获取当前和新表的字段
            $fields = array_keys($latest['columns']);
            $currentFields = array_keys($current['columns']);
            // 用于存储生成的 SQL 语句
            $sqlStatements = [];
            // 处理新增和更新字段
            foreach ($fields as $field) {
                if (!in_array($field, $currentFields)) {
                    // 新增字段
                    $sqlStatements[] = "ALTER TABLE `{$latest['table']}` ADD COLUMN " . $latest['columns'][$field]['content'];
                } elseif ($current['columns'][$field]['hash'] != $latest['columns'][$field]['hash']) {
                    // 更新字段
                    $sqlStatements[] = "ALTER TABLE `{$latest['table']}` MODIFY COLUMN " . $latest['columns'][$field]['content'];
                }
            }
            // 处理删除字段
            $fieldsToRemove = array_diff($currentFields, $fields);
            foreach ($fieldsToRemove as $cfield) {
                $sqlStatements[] = "ALTER TABLE `{$latest['table']}` DROP COLUMN `{$cfield}`";
            }
            //更新索引
            $indexes = array_keys($latest['index']);
            $currentIndexes = array_keys($current['index']);
            foreach ($indexes as $index) {
                if (!in_array($index, $currentIndexes)) {
                    // 新增字段
                    $sqlStatements[] = "ALTER TABLE `{$latest['table']}` ADD " . $latest['index'][$index]['content'];
                } elseif ($current['index'][$index]['hash'] != $latest['index'][$index]['hash']) {
                    // 更新字段
                    $sqlStatements[] = "ALTER TABLE `{$latest['table']}` DROP INDEX `{$index}`";
                    $sqlStatements[] = "ALTER TABLE `{$latest['table']}` ADD " . $latest['index'][$index]['content'];
                }
            }
            // 删除索引
            $indexesToRemove = array_diff($currentIndexes, $indexes);
            foreach ($indexesToRemove as $dindex) {
                $sqlStatements[] = "ALTER TABLE `{$latest['table']}` DROP INDEX `{$dindex}`";
            }
            // 更新主键
            if ($latest['primary'] !== $current['primary']) {
                $sqlStatements[] = "ALTER TABLE `{$latest['table']}` DROP PRIMARY KEY, ADD PRIMARY KEY (" . implode(',', $latest['primary']) . ") USING BTREE";
            }
            // 输出所有 SQL 语句
            $hasError = false;
            foreach ($sqlStatements as $sql) {
                try {
                    Pdo::master($this->getDb())->getDatabase()->exec($sql)->get();
                    Console::success($sql . "=>执行成功");
                } catch (Throwable $exception) {
                    $hasError = true;
                    Console::error($sql . "=>" . $exception->getMessage());
                }
            }

            //if (!$hasError) {
            update:
            File::write($versionFile, Yaml::dump($latest, 3));
            //}
        } else {
            Console::log("【Database】{$latest['db']}.{$latest['table']}已是最新版本:" . Color::success($latest['version']));
        }
    }

    /**
     * 保存活动记录
     * @param bool $filterNull
     * @param bool $forceInsert
     * @return bool
     */
    public function save(bool $filterNull = true, bool $forceInsert = false): bool {
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
            if (!$forceInsert && !empty($this->$primaryKey) && self::has($datas[$primaryKey])) {
                //如果存在快照只更新值变动过的字段
                if (!is_null($this->snapshot)) {
                    foreach ($datas as $k => $v) {
                        $v = is_array($v) ? JsonHelper::toJson($v) : $v;
                        if (isset($this->snapshot[$k])) {
                            $snapshotVaule = is_array($this->snapshot[$k]) ? JsonHelper::toJson($this->snapshot[$k]) : $this->snapshot[$k];
                            if ($v instanceof Calculator) {
                                $c = clone $v;
                                $c->getIncValue() > 0 and $this->snapshot[$k] += $c->getIncValue();
                                $c->getRedValue() > 0 and $this->snapshot[$k] -= $c->getRedValue();
                                $this->$k = $this->snapshot[$k];
                                continue;
                            }
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
                    try {
                        $connection = $this->transaction ?? $this->master();
                        $row = $connection->table($this->_table)->where("`{$primaryKey}` = ?", $this->$primaryKey)->updates($datas)->rowCount();
                    } catch (Throwable $error) {
                        $this->addError('save', $error->getMessage());
                        return false;
                    }
                    if ($row) {
                        //清除缓存,此处不能直接使用更新后的快照数更新缓存
                        $this->deleteArCache($this->$primaryKey);
                    }
                }
            } else {
                $connection = $this->transaction ?? $this->master();
                $this->$primaryKey = $connection->insert($this->_table, $datas)->lastInsertId() ?: ($datas[$primaryKey] ?? '');
            }
        } catch (Throwable $error) {
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
        $name = static::class;
        $cls = new $name();
        if ($fields) {
            $cls->setFields(...$fields);
        }
        return $cls;
    }


    public function getTable(): string {
        return $this->_table;
    }

    public function getDb(): string {
        return $this->_dbName;
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
        return self::$_instances[$key] ?? null;
    }

    /**
     * 设置协程AR
     * @param $id
     * @param $ar
     * @return void
     */
    protected static function setInstanceAr($id, $ar): void {
        $cid = Coroutine::getCid();
        $key = self::getInstanceArKey($id);
        self::$_instances[$key] = $ar;
        if ($cid > 0) {
            Coroutine::defer(function () use ($key) {
                //Console::log('释放AR资源:' . $key);
                unset(self::$_instances[$key]);
            });
        }
    }

    /**
     * 更新协程AR
     * @param $id
     * @param $ar
     * @return void
     */
    protected static function updateInstanceAr($id, $ar): void {
        $key = self::getInstanceArKey($id);
        if (isset(self::$_instances[$key])) {
            self::$_instances[$key] = $ar;
        }
    }

    /**
     * 获取协程AR KEY
     * @param $id
     * @return string
     */
    protected static function getInstanceArKey($id): string {
        $name = static::class;
        $cid = Coroutine::getCid();
        return str_replace("\\", "", $name) . '_' . $id . '_' . $cid;
    }

    /**
     * 释放AR
     * @param $id
     * @param int|null $cid
     * @return void
     */
    public function destroyInstanceAr($id, int $cid = null): void {
        $name = static::class;
        if ($cid === null) {
            $cid = Coroutine::getCid();
        }
        $instanceKey = $name . '_' . $id . '_' . $cid;
        unset(self::$_instances[$instanceKey]);
    }

    /**
     * 清除AR缓存
     * @param int|array|string $id
     * @return void
     */
    protected function deleteArCache(int|array|string $id): void {
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
    }

    /**
     * 判断是否存在
     * @param $id
     * @param int $actor
     * @return bool
     */
    public static function has($id, int $actor = DBS_SLAVE): bool {
        $name = static::class;
        $cls = new $name();
        $cls->setActor($actor);
        if (Cache::instance()->get($cls->getArCacheKey($id))) {
            return true;
        }
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
     * 字段赋值
     * @param $values
     * @return static
     */
    public static function setValues($values): static {
        $dbcFields = array_keys(self::factory()->toArray());
        $index = 0;
        $item = [];
        foreach ($values as $value) {
            $item[$dbcFields[$index]] = $value;
            $index++;
        }
        return self::factory($item);
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
     * @param WhereBuilder|?array $where
     * @return static
     */
    public function where(WhereBuilder|null|array $where): static {
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
     * @param $group
     * @return $this
     */
    public function group($group): static {
        $this->_group = $group;
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
     * @param bool $resetParams
     * @return IConnection
     */
    protected function connection(int $actor = 0, bool $resetParams = true): IConnection {
        $actor = $actor ?: $this->actor;
        if ($actor == DBS_SLAVE) {
            $connection = $this->slave()->table($this->_table);
        } else {
            $connection = $this->transaction ?? $this->master()->table($this->_table);
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
        if (!is_null($this->_group)) {
            $group = is_array($this->_group) ? $this->_group : [$this->_group];
            $connection->group(...$group);
        }
        if (!is_null($this->_fields)) {
            $connection->select(...$this->_fields);
        }
        $this->whereSql = $this->getWhereSql();
        $resetParams and $this->resetQueryParams();
        return $connection;
    }

    /**
     * 从库连接
     * @return DB
     */
    protected function slave(): DB {
        $cid = Coroutine::getCid();
        if (!isset(self::$_db_instances[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid])) {
            self::$_db_instances[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid] = Pdo::slave($this->_dbName)->getDatabase();
            if ($cid > 0) {
                Coroutine::defer(function () use ($cid) {
                    unset(self::$_db_instances[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid]);
                });
            }
        }
        return self::$_db_instances[DBS_SLAVE . '_' . $this->_dbName . '_' . $cid];
    }

    /**
     * 主库连接
     * @return DB
     */
    protected function master(): DB {
        $cid = Coroutine::getCid();
        if (!isset(self::$_db_instances[DBS_MASTER . '_' . $this->_dbName . '_' . $cid])) {
            self::$_db_instances[DBS_MASTER . '_' . $this->_dbName . '_' . $cid] = Pdo::master($this->_dbName)->getDatabase();
            if ($cid > 0) {
                Coroutine::defer(function () use ($cid) {
                    unset(self::$_db_instances[DBS_MASTER . '_' . $this->_dbName . '_' . $cid]);
                });
            }
        }
        return self::$_db_instances[DBS_MASTER . '_' . $this->_dbName . '_' . $cid];
    }

    /**
     * 为当前对象开启事务
     * @return void
     */
    public function beginTransaction(): void {
        try {
            $this->transaction = $this->master()->beginTransaction();
        } catch (Throwable $exception) {
            Console::error($exception->getMessage());
        }
    }

    /**
     * 事务回滚
     * @return void
     */
    public function rollback(): void {
        $this->transaction->rollback();
        unset($this->transaction);
    }

    /**
     * 事务提交
     * @return void
     */
    public function commit(): void {
        $this->transaction->commit();
        unset($this->transaction);
    }

    /**
     * 获取活动数据记录缓存KEY
     * @param $val
     * @return string
     */
    protected function getArCacheKey($val): string {
        $arKey = $this->getPrimaryKey();
        return 'AR_' . strtoupper($this->_dbName) . '_' . strtoupper($this->_table) . '_' . $arKey . '_' . $val;

        //return 'AR_' . md5(strtoupper($this->_dbName) . '_' . strtoupper($this->_table) . '_' . $arKey . '_' . $val . JsonHelper::toJson($this->_fields));
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
    public function format(array $data = null): array {
        if (!$this->_validate) {
            return $data;
        }
        $data = is_null($data) ? $this->toArray(true) : $data;
        foreach ($this->_validate as $f => $v) {
            if (isset($data[$f]) && isset($v['format'])) {
                $dataType = $v['format'][0]['type'];
                if ($dataType == 'json') {
                    if (is_string($data[$f])) {
                        $data[$f] = htmlspecialchars_decode($data[$f]);
                    }
                    if (JsonHelper::is($data[$f])) {
                        $data[$f] = ArrayHelper::integer(JsonHelper::recover($data[$f]));
                    } else {
                        $data[$f] = JsonHelper::toJson(ArrayHelper::integer($data[$f]));
                    }
                } elseif ($dataType == 'code' || $dataType == 'url') {
                    $data[$f] = htmlspecialchars_decode($data[$f]);
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
    protected function setPrimaryKey($key): void {
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
    protected function setPrimaryVal($val): void {
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
    protected function resetQueryParams(): void {
        $this->_order = null;
        $this->_fields = null;
        $this->_where = null;
        $this->_group = null;
    }
}