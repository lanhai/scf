<?php

namespace Scf\Database;

use Throwable;
use PDOException;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine;
use Scf\Command\Color;
use Scf\Component\Cache;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Log;
use Scf\Core\Struct;
use Scf\Database\Exception\NullAR;
use Scf\Database\Tools\Calculator;
use Scf\Database\Tools\Expr;
use Scf\Database\Tools\WhereBuilder;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Util\File;
use Symfony\Component\Yaml\Yaml;

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
    protected string $_groupAggregateFunction = 'min';
    protected ?string $_groupAggregateField = null;
    protected string $_customPrimaryKey = '';
    protected null|string|int $_primaryVal = null;

    protected bool $_arExist = false;
    protected Transaction $transaction;
    private string|null $whereSql = null;
    protected bool $enablePool = true;

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
    public function count(?string $field = null): int|array {
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
     * 获取一条array格式数据
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
     * 获取一条对象化数据
     * @return NullAR|static
     */
    public function ar(): static|NullAR {
        $primaryVal = $this->_primaryVal ?? $this->queryPrimaryVal();
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
        $this->snapshot = $this->asArray();
        return $this;
    }

    /**
     * 获取所有数据
     * @param bool $format
     * @return array
     */
    public function all(bool $format = true): array {
        $connection = $this->connection(resetParams: is_null($this->_group));
        $list = $connection->get();
        if (!is_null($this->_group)) {
            $this->_group = null;
            $indexes = [];
            foreach ($list as $item) {
                $indexes[] = $item[$this->_groupAggregateField];
            }
            return $this->where([$this->_groupAggregateField => $indexes])->all($format);
        }
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
                        Coroutine::create(function () use ($pn, $channel, $pageSize, $count, $where) {
                            $localWhere = $where ?: [];
                            $channel->push(static::select($this->getPrimaryKey())->where($localWhere)->topPrimarys($pageSize, $pn, $count));
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
        if ($size <= 0) {
            // 保持与历史行为兼容：不选取任何主键值，直接按照空主键集合构造查询
            return $this->where([$this->getPrimaryKey() => $primaryValue])->all();
        }
        // 更高效地从集合中随机抽取指定数量的主键值（等价于原实现的“无放回随机”语义）
        $randomKeys = array_rand($collections, $size);
        if (!is_array($randomKeys)) {
            $randomKeys = [$randomKeys];
        }
        foreach ($randomKeys as $key) {
            $primaryValue[] = $collections[$key];
        }
        return $this->where([$this->getPrimaryKey() => $primaryValue])->all();
    }

    /**
     * 获取列表
     * @param bool $format
     * @param int $total
     * @param string|null $countField
     * @return array
     */
    public function list(bool $format = true, int $total = 0, ?string $countField = null): array {
        $select = self::select($this->_primaryKey)->where($this->_where);
        if ($this->_group) {
            $select->group($this->_group);
        }
        $total = $total ?: $select->count($countField);
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
     * @param bool $forUpdate
     * @return array
     */
    public function top(int $size = 10, int $pn = 1, int $total = 0, bool $format = true, bool $forUpdate = false): array {
        $connection = $this->connection(resetParams: is_null($this->_group));
        if ($forUpdate && is_null($this->_group)) {
            $connection->lockForUpdate();
        }
        if ($total) {
            $totalPage = ceil($total / $size);
            $pn = min($pn, $totalPage) ?: 1;
            $offset = ($pn - 1) * $size;
            $connection->offset($offset);
        }
        $connection->limit($size);
        $list = $connection->get();
        $primaryKeys = [];
        if ($list) {
            if (!is_null($this->_group)) {
                $this->_group = null;
                $indexes = [];
                foreach ($list as $item) {
                    $indexes[] = $item[$this->_groupAggregateField];
                }
                $where = $this->_where;
                if (!$where) {
                    $where = WhereBuilder::create();
                }
                $where->and([$this->_groupAggregateField => $indexes]);
                return $this->where($where)->top($size, $pn, $total, $format, $forUpdate);
            }
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
     * @param int $size
     * @return array
     */
    public function primaryKeys(int $size = 0): array {
        return $this->pluck($this->_primaryKey, $size);
    }

    /**
     * 取一列的值
     * @param string|null $key
     * @param int $size
     * @param bool $forUpdate
     * @return array
     */
    public function pluck(string $key = null, int $size = 0, bool $forUpdate = false): array {
        $connection = $this->connection();
        if ($forUpdate && is_null($this->_group)) {
            $connection->lockForUpdate();
        }
        if ($size) {
            $connection->offset(0);
            $connection->limit($size);
        }
        $key = !is_null($key) ? $key : $this->_primaryKey;
        $connection->select($key);
        $column = [];
        try {
            if ($list = $connection->get()) {
                if (!is_null($this->_group)) {
                    $ids = [];
                    $this->_group = null;
                    foreach ($list as $item) {
                        $ids[] = $item[$this->_groupAggregateField];
                    }
                    $where = $this->_where;
                    if (!$where) {
                        $where = WhereBuilder::create();
                    }
                    $where->and([$this->_groupAggregateField => $ids]);
                    return $this->where($where)->pluck($key, $size, $forUpdate);
                }
                foreach ($list as $item) {
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
     * @param int $size
     * @return int
     */
    public function update($datas, int $size = 0): int {
        $priKey = $this->getPrimaryKey();
        if (is_null($this->_where) && !is_null($this->_primaryVal)) {
            $this->where([$priKey => $this->_primaryVal]);
        }
        $priValues = $this->primaryKeys($size);
        if (!$priValues) {
            return 0;
        }
        $this->where([$priKey => $priValues]);
        $connection = $this->connection(DBS_MASTER);
        try {
            $row = $connection->updates($datas)->rowCount();
            if ($row) {
                $this->deleteArCache($priValues);
            }
            return $row;
        } catch (Throwable $exception) {
            $this->addError($this->_table . '_UPDATE', $exception->getMessage());
            return 0;
        }
    }


    /**
     * 删除数据
     * @param int $size
     * @return int
     */
    public function delete(int $size = 0): int {
        $priKey = $this->getPrimaryKey();
        $uniqueVal = $this->getPrimaryVal();
        if (!is_null($uniqueVal)) {//单条AR数据
            $connection = $this->connection(DBS_MASTER);
            $row = $connection->table($this->_table)->where("`{$priKey}` = ?", $uniqueVal)->delete()->rowCount();
            if ($row) {
                $this->deleteArCache($uniqueVal);
                $this->snapshot = null;
                if (!empty($this->$priKey)) {
                    unset($this->$priKey);
                }
            }
        } else {//批量删除
            $priValues = $this->primaryKeys($size);
            if (!$priValues) {
                return 0;
            }
            $this->where([$priKey => $priValues]);
            $connection = $this->connection(DBS_MASTER);
            $row = $connection->delete()->rowCount();
            if ($row) {
                $this->deleteArCache($priValues);
            }
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
            $pdoMaster = Pdo::master($this->_dbName);
            $tablePrefix = $pdoMaster->getConfig('prefix');
            $completeTable = $tablePrefix . $this->getTable();
            $pdoMaster->getDatabase()->exec('DESCRIBE ' . $completeTable);
            return true;
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * 数据库是否存在
     * @return bool
     */
    public function databaseCheck(): bool {
        $dbName = $this->getDb();  // 获取数据库名称
        return Pdo::factory()->createDatabaseIfNotExists($dbName);
    }


    /**
     * 更新数据表结构
     * @param $latest
     * @return void
     */
    public function updateTableStruct($latest): void {
        $hasError = false;
        if (!$this->databaseCheck()) {
            return;
        }
        $key = $latest['db'] . '_' . $latest['table'];
        $versionFile = APP_PATH . '/db/updates/' . $key . '.yml';
        $current = file_exists($versionFile) ? Yaml::parseFile($versionFile) : null;
        $conf = Config::get('database')['mysql'];
        if (!$current || !$this->tableExist()) {
            if (!$this->createTable($latest['create'])) {
                Console::log("【Database】{$conf[$latest['db']]['name']}.{$latest['table']} " . Color::red('创建失败'));
            } else {
                Console::log("【Database】{$conf[$latest['db']]['name']}.{$latest['table']} " . Color::success('创建成功'));
                File::write($versionFile, Yaml::dump($latest, 3));
            }
        } elseif ($current['version'] !== $latest['version']) {
            if (Env::isDev()) goto update;
            // 更新字段/索引/主键
            Console::info("【Database】{$latest['db']}.{$latest['table']} " . Color::yellow('需要更新'));
            $sqlStatements = $this->buildSchemaUpdateSqlStatements($latest, $current);
            // 执行所有 SQL 语句
            foreach ($sqlStatements as $sql) {
                try {
                    Pdo::master($this->getDb())->getDatabase()->exec($sql)->get();
                    Console::info("【Database】" . $sql . " => " . Color::green('执行成功'));
                } catch (Throwable $exception) {
                    $msg = $exception->getMessage();
                    if (!$this->isIgnorableSchemaError($msg)) {
                        $hasError = true;
                        Console::info("【Database】" . $sql . " => " . Color::red($msg));
                    } else {
                        Console::info("【Database】(忽略) " . $sql . " => " . $msg);
                    }
                }
            }
            update:
            if (!$hasError) {
                File::write($versionFile, Yaml::dump($latest, 3));
            }
        }
    }

    /**
     * 根据当前/最新表结构配置生成表结构变更 SQL 列表
     * @param array $latest
     * @param array $current
     * @return array
     */
    private function buildSchemaUpdateSqlStatements(array $latest, array $current): array {
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
        // 更新索引
        $indexes = array_keys($latest['index']);
        $currentIndexes = array_keys($current['index']);
        foreach ($indexes as $index) {
            if (!in_array($index, $currentIndexes)) {
                // 新增索引
                $sqlStatements[] = "ALTER TABLE `{$latest['table']}` ADD " . $latest['index'][$index]['content'];
            } elseif ($current['index'][$index]['hash'] != $latest['index'][$index]['hash']) {
                // 更新索引
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
        return $sqlStatements;
    }

    /**
     * 对象数据落库
     * @param bool $filterNull
     * @param bool $forceInsert
     * @return bool
     */
    public function save(bool $filterNull = true, bool $forceInsert = false): bool {
        try {
            if (!$this->validate()) {
                return false;
            }
            $primaryKey = $this->getPrimaryKey();
            $datas = $this->format($this->asArray($filterNull));
            if (!$datas) {
                return false;
            }
            if (!$forceInsert && !empty($this->$primaryKey) && static::has($this->$primaryKey)) {
                $changedDatas = $datas;
                $nextSnapshot = null;
                if (!is_null($this->snapshot)) {
                    [$changedDatas, $nextSnapshot] = $this->getChangedFieldsFromSnapshot($datas);
                }
                if ($changedDatas) {
                    try {
                        $connection = $this->transaction ?? $this->master();
                        $row = $connection->table($this->_table)
                            ->where("`{$primaryKey}` = ?", $this->$primaryKey)
                            ->updates($changedDatas)
                            ->rowCount();
                        if ($row === 0) {
                            $this->addError('save', "数据不存在或没有任何变化");
                            return false;
                        } else {
                            $this->deleteArCache($this->$primaryKey);
                            if (!is_null($nextSnapshot)) {
                                $this->snapshot = $nextSnapshot;
                            }
                        }
                    } catch (Throwable $error) {
                        $this->addError('save', $error->getMessage());
                        return false;
                    }
                }
            } else { // Insert operation
                $connection = $this->transaction ?? $this->master();
                $this->$primaryKey = $connection->insert($this->_table, $datas)->lastInsertId() ?: ($datas[$primaryKey] ?? '');
                // After insert, set snapshot for future updates
                $this->snapshot = $this->asArray();
            }
        } catch (Throwable $error) {
            $this->addError('save', $error->getMessage());
            return false;
        }
        return true;
    }

    /**
     * 根据快照对比当前数据, 返回发生变化的字段数组与更新后的快照
     * @param array $currentDatas 从当前对象获取的格式化数据
     * @return array{0: array, 1: array} [changedFields, nextSnapshot]
     */
    private function getChangedFieldsFromSnapshot(array $currentDatas): array {
        $changedFields = [];
        $nextSnapshot = $this->snapshot ?? [];
        foreach ($currentDatas as $field => $newValue) {
            // If field is not in snapshot, it's a new field or was null, consider it changed
            if (!array_key_exists($field, $nextSnapshot)) {
                $changedFields[$field] = $newValue;
                $nextSnapshot[$field] = $newValue;
                continue;
            }

            $oldValue = $nextSnapshot[$field];

            // Handle Calculator objects
            if ($newValue instanceof Calculator) {
                $calculator = clone $newValue;
                // Apply calculation to snapshot value
                $currentValue = is_numeric($oldValue) ? (int)$oldValue : 0;
                if ($calculator->getIncValue() > 0) {
                    $currentValue += $calculator->getIncValue();
                }
                if ($calculator->getRedValue() > 0) {
                    $currentValue -= $calculator->getRedValue();
                }
                $nextSnapshot[$field] = $currentValue;
                $this->$field = $currentValue; // Update current object property
                $changedFields[$field] = $newValue; // Calculator objects always result in a change
                continue;
            }

            // Use the new strict comparison method
            if ($this->isValueChanged($newValue, $oldValue)) {
                $changedFields[$field] = $newValue;
                $nextSnapshot[$field] = $newValue;
            }
        }
        return [$changedFields, $nextSnapshot];
    }

    /**
     * 严格比较两个值是否发生变化，处理对象和数组
     * @param mixed $newValue
     * @param mixed $oldValue
     * @return bool
     */
    private function isValueChanged(mixed $newValue, mixed $oldValue): bool {
        if ($newValue === $oldValue) {
            return false;
        }
        if (is_array($newValue) || is_object($newValue)) {
            return serialize($newValue) !== serialize($oldValue);
        }
        if (gettype($newValue) !== gettype($oldValue)) {
            if (is_numeric($newValue) && is_numeric($oldValue)) {
                return (string)$newValue !== (string)$oldValue; // Compare as strings to handle '0' vs 0
            }
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

    /**
     * 开启事务
     * @return Transaction
     * @throws Throwable
     */
    public static function transaction(): Transaction {
        return static::select()->master()->beginTransaction();
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
     * 清除AR缓存
     * @param int|array|string $id
     * @return void
     */
    protected function deleteArCache(int|array|string $id): void {
        if (!$id) {
            return;
        }
        if (is_array($id)) {
            $keys = [];
            foreach ($id as $i) {
                $keys[] = $this->getArCacheKey($i);
            }
            Cache::instance()->deleteMultiple($keys);
        } else {
            $arCacheKey = $this->getArCacheKey($id);
            Cache::instance()->delete($arCacheKey);
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
        $db = $actor == DBS_SLAVE ? $cls->slave() : $cls->master();
        $count = $db->table($cls->_table)->where("`{$cls->_primaryKey}` = ?", $id)->select($cls->_primaryKey)->count();
        return $count > 0;
    }

    /**
     * 创建一个Dao实例
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
     * @param null $field
     * @param string $af
     * @return $this
     */
    public function group($group, $field = null, string $af = 'MIN'): static {
        $this->_group = $group;
        $this->_groupAggregateFunction = $af;
        $this->_groupAggregateField = $field ?: $this->_primaryKey;
        return $this;
    }

    /**
     * 返回数据库设置
     * @return array
     */
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
            $connection->select(strtoupper($this->_groupAggregateFunction) == 'MIN' ? 'MIN(' . ($this->_groupAggregateField ?: $this->_primaryKey) . ')' : 'MAX(' . ($this->_groupAggregateField ?: $this->_primaryKey) . ')');
        } elseif (!is_null($this->_fields)) {
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
        return Pdo::slave($this->_dbName, $this->enablePool)->getDatabase();
    }

    /**
     * 主库连接
     * @return DB
     */
    protected function master(): DB {
        return Pdo::master($this->_dbName, $this->enablePool)->getDatabase();
    }

    /**
     * 为当前对象开启事务
     * @return void
     * @throws Throwable
     */
    public function beginTransaction(): void {
        $this->transaction = $this->master()->beginTransaction();

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
            if (isset($data[$f])) {
                if (isset($v['format'])) {
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
                } elseif (is_numeric($data[$f]) && !is_int($data[$f]) && !is_float($data[$f]) && strlen($data[$f]) < 10) {
                    $data[$f] = (float)$data[$f];
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
    protected function getPrimaryVal(): int|string|null {
        if (!is_null($this->_primaryVal)) {
            return $this->_primaryVal;
        }
        $uniqueKey = $this->getPrimaryKey();
        return !empty($this->$uniqueKey) ? $this->$uniqueKey : null;
    }

    /**
     * 判断是否为可忽略的表结构变更错误
     * 典型场景：
     * - 字段已存在 / 不存在（ADD/MODIFY/DROP COLUMN）
     * - 索引已存在 / 不存在（ADD/DROP INDEX）
     * - 主键重复定义 / 不存在（PRIMARY KEY）
     */
    private function isIgnorableSchemaError(string $message): bool {
        $m = strtolower($message);
        return
            // Column cases
            str_contains($m, 'column already exists') ||
            str_contains($m, 'duplicate column name') ||
            str_contains($m, 'unknown column') ||                              // e.g. MODIFY/DROP 时列不存在
            // Index cases
            str_contains($m, 'duplicate key name') ||                          // 索引已存在
            str_contains($m, "can't drop index") ||                            // 索引不存在
            (str_contains($m, "can't drop") && str_contains($m, 'check that') && str_contains($m, 'exists')) ||
            // Primary key cases
            str_contains($m, 'multiple primary key defined') ||                // 已有主键
            str_contains($m, "can't drop 'primary'");                          // 没有主键可删
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
        $this->_groupAggregateField = null;
        $this->_groupAggregateFunction = 'min';
    }
}
