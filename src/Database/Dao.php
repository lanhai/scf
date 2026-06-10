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
     * @var ?WhereBuilder 候选集查询条件，仅用于两段式分页的第一段收敛范围
     */
    protected WhereBuilder|null $_candidateWhere = null;
    /**
     * @var ?array 候选集原始数组条件，仅用于在 Dao 内部推断最合适的联合索引
     */
    protected array|null $_candidateSource = null;
    /**
     * @var string|array|null
     */
    protected string|array|null $_group = null;
    protected string $_groupAggregateFunction = 'min';
    protected ?string $_groupAggregateField = null;
    protected string $_customPrimaryKey = '';
    protected null|string|int $_primaryVal = null;
    /**
     * @var array<int, array{dao: self, source_field: string, target_field: string, assign_field: string, aggregate: ?array{type: string, field: string, result_key: string, default: mixed}}>
     * 记录待在主表结果集回填的关联查询配置。
     */
    protected array $_joins = [];

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
        $joins = $this->_joins;
        $connection = $this->connection();
        try {
            if ($result = $connection->first()) {
                if ($format) {
                    $result = $this->format($result);
                }
                if ($joins) {
                    $rows = $this->applyJoinsToRows([$result], $joins);
                    $result = $rows[0] ?? null;
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
        $joins = $this->_joins;
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
        if ($list && $joins) {
            $list = $this->applyJoinsToRows($list, $joins);
        }
        if ($list && !is_null($this->_fields) && count($this->_fields) == 1 && !$joins) {
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
        if (!is_null($this->_candidateWhere)) {
            return $this->buildCandidateListResult($format, $total);
        }
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
        $joins = $this->_joins;
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
            if ($joins) {
                $list = $this->applyJoinsToRows($list, $joins);
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
                            // MySQL 将“命中但值未变化”和“未命中记录”都表现为 0 行更新，
                            // 这里用同一连接复查主键，事务内删除也能被正确分类。
                            $exists = $connection->table($this->_table)
                                ->where("`{$primaryKey}` = ?", $this->$primaryKey)
                                ->select($primaryKey)
                                ->count() > 0;
                            $this->addError('save', $exists ? "数据没有任何变化" : "数据不存在");
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
     * 为当前查询追加一个结果回填式 join 定义。
     * 该 join 不改写主表 SQL，而是在主表结果集取回后批量查询目标表并按匹配键回填。
     *
     * @param self $dao 目标表访问对象，可携带自己的 where/order 等预置条件。
     * @param string|array $match 关联键配置。字符串表示主表字段匹配目标表主键；数组表示 [主表字段 => 目标表字段]。
     * @param string $assignField 回填到主表单行数据中的字段名。
     * @param string|array|null $aggregate 聚合模式。`null` 为普通 join，`count` 为分组计数，
     *                                     `['type' => 'sum', 'field' => 'xxx']` 为分组求和，
     *                                     `['type' => 'max', 'field' => 'xxx']` / `min` 为分组极值。
     * @return static
     */
    public function join(self $dao, string|array $match, string $assignField, string|array|null $aggregate = null): static {
        [$sourceField, $targetField] = $this->normalizeJoinMatch($dao, $match);
        if (!$sourceField || !$targetField) {
            $this->addError('join', 'Join关联字段配置错误');
            return $this;
        }
        $aggregateConfig = $this->normalizeJoinAggregate($dao, $aggregate);
        if ($aggregateConfig === false) {
            $this->addError('join', 'Join聚合配置错误');
            return $this;
        }
        $this->_joins[] = [
            'dao' => clone $dao,
            'source_field' => $sourceField,
            'target_field' => $targetField,
            'assign_field' => $assignField,
            'aggregate' => $aggregateConfig,
        ];
        return $this;
    }

    /**
     * 为当前查询声明一段候选集。
     *
     * 候选集只负责先把分页候选范围压缩到较小的数据子集，最终结果仍然会在第二段查询里
     * 继续应用主查询的 `where/join/format` 逻辑。因此业务层只需要表达“先在哪个候选集里
     * 取主键页”，而不需要关心底层索引名或二段式 SQL 的组织方式。
     *
     * @param WhereBuilder|array $where 候选集条件
     * @return static
     */
    public function candidate(WhereBuilder|array $where): static {
        if (is_array($where)) {
            $this->_candidateSource = $where;
            $where = WhereBuilder::create($where);
        } else {
            $this->_candidateSource = null;
        }
        $this->_candidateWhere = $where;
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
     * 按调用方提供的有序主键集合构造标准分页结果。
     *
     * 该方法负责把“主键分页策略”和“主表格式化/join 回填”拆开：
     * 调用方只需要先求出这一页的有序主键及总量，Dao 再统一完成主表回填、
     * join 组装以及顺序恢复，避免控制器重复实现同样的二段式查询流程。
     *
     * @param array $primaryKeys 当前页主键集合，顺序即最终结果顺序
     * @param int $total 当前完整筛选条件下的总量
     * @param bool $format 是否执行 Dao 的格式化逻辑
     * @return array{list: array, pages: int, pn: int, total: int, primarys: array}
     */
    protected function buildListResultByPrimaryKeys(array $primaryKeys, int $total, bool $format = true): array {
        $total = max(0, $total);
        $totalPage = $total ? (int)ceil($total / $this->_pageSize) : 0;
        $page = min($this->_pn, $totalPage) ?: 1;
        $primaryKeys = array_values(array_filter($primaryKeys, static fn($primaryKey) => $primaryKey !== null && $primaryKey !== ''));
        if (!$primaryKeys) {
            return ['list' => [], 'pages' => $totalPage, 'pn' => $page, 'total' => $total, 'primarys' => []];
        }

        $rowsDao = clone $this;
        // 主键分页已经在外层确定顺序，这里只负责按主键批量取回明细，避免再次走慢排序路径。
        $rowsDao->_order = null;
        [$queryFields, $removePrimaryFieldAfterHydrate] = $this->prepareJoinQueryFields($rowsDao->_fields, $this->getPrimaryKey());
        if (!is_null($queryFields)) {
            $rowsDao->setFields(...$queryFields);
        }
        $rows = $rowsDao->where([$this->getPrimaryKey() => $primaryKeys])->all($format);
        $rowsMap = [];
        foreach ($rows as $row) {
            if (!is_array($row) || !array_key_exists($this->getPrimaryKey(), $row)) {
                continue;
            }
            $rowsMap[(string)$row[$this->getPrimaryKey()]] = $row;
        }

        $orderedRows = [];
        foreach ($primaryKeys as $primaryKey) {
            $key = (string)$primaryKey;
            if (!array_key_exists($key, $rowsMap)) {
                continue;
            }
            $row = $rowsMap[$key];
            if ($removePrimaryFieldAfterHydrate) {
                unset($row[$this->getPrimaryKey()]);
            }
            $orderedRows[] = $row;
        }

        return ['list' => $orderedRows, 'pages' => $totalPage, 'pn' => $page, 'total' => $total, 'primarys' => $primaryKeys];
    }

    /**
     * 返回当前 Dao 对应的数据表完整名称（含前缀）。
     *
     * 候选集二段分页会在 Dao 内部直接拼装原生子查询，这里统一负责拿到真实表名，
     * 避免控制器或业务代码再接触底层表前缀细节。
     *
     * @param int $actor 读取哪一个角色的数据库配置
     * @return string
     */
    protected function getCompleteTableName(int $actor = DBS_SLAVE): string {
        $pdo = $actor == DBS_MASTER ? Pdo::master($this->_dbName) : Pdo::slave($this->_dbName);
        $prefix = (string)($pdo->getConfig('prefix') ?? '');
        return $prefix . $this->_table;
    }

    /**
     * 针对声明了候选集的列表查询，执行数据库内完成交集的二段式分页。
     *
     * 第一段候选集负责把扫描范围压到更小的数据子集；第二段在数据库内部基于候选集
     * 与主查询的完整 `where` 做交集，再按既定顺序取当前页主键。这样既保持 `list()`
     * 的统一入口，也避免把成千上万的候选主键先拉回 PHP 再拼成大 `IN (...)`。
     *
     * @param bool $format 是否执行 Dao 的格式化逻辑
     * @param int $total 外部已知的最终总量；为 0 时由本方法自行统计
     * @return array{list: array, pages: int, pn: int, total: int, primarys: array}
     */
    protected function buildCandidateListResult(bool $format = true, int $total = 0): array {
        $candidateWhere = $this->_candidateWhere ? clone $this->_candidateWhere : null;
        if (!$candidateWhere) {
            return $this->list($format);
        }
        $orders = $this->_order ?: [$this->getPrimaryKey() => 'DESC'];
        $primaryKey = $this->getPrimaryKey();
        $table = $this->getCompleteTableName();
        $page = max(1, $this->_pn);
        $size = max(1, $this->_pageSize);
        $candidateBuild = $candidateWhere->build();
        $candidateWhereSql = $candidateBuild['sql'] ? 'WHERE ' . $candidateBuild['sql'] : '';
        $qualifiedCandidateWhereSql = $candidateBuild['sql'] ? $this->qualifyCandidateWhereSql($candidateBuild['sql'], 'p') : '';
        $where = $this->_where ? clone $this->_where : WhereBuilder::create();
        $whereBuild = $where->build();
        $qualifiedWhereSql = $whereBuild['sql'] ? $this->qualifyCandidateWhereSql($whereBuild['sql'], 'p') : '';
        $whereSql = $qualifiedWhereSql ? 'WHERE ' . $qualifiedWhereSql : '';
        $orderFields = [];
        $orderClauses = [];
        $outerOrderSelects = [];
        $orderAliasIndex = 0;
        foreach ($orders as $field => $direction) {
            if (!is_string($field) || trim($field) === '') {
                continue;
            }
            $orderFields[$field] = $field;
            $sort = strtoupper((string)$direction) === 'ASC' ? 'ASC' : 'DESC';
            $alias = "_candidate_order_{$orderAliasIndex}";
            $outerOrderSelects[] = "x.`{$field}` AS `{$alias}`";
            $orderClauses[] = "`{$alias}` {$sort}";
            $orderAliasIndex++;
        }
        if (!$orderClauses) {
            $orderFields[$primaryKey] = $primaryKey;
            $outerOrderSelects[] = "x.`{$primaryKey}` AS `_candidate_order_0`";
            $orderClauses[] = "`_candidate_order_0` DESC";
        }
        $subqueryFields = array_unique(array_merge([$primaryKey], array_values($orderFields)));
        $subquerySelect = implode(', ', array_map(static fn(string $field) => "`{$field}`", $subqueryFields));
        $orderSql = implode(', ', $orderClauses);
        $indexHintSql = $this->resolveCandidateIndexHintSql(array_values($orderFields));
        $database = Pdo::slave($this->_dbName)->getDatabase();
        $countWhereSql = $this->mergeQualifiedCandidateWhereSql($qualifiedCandidateWhereSql, $qualifiedWhereSql);

        $countSql = <<<SQL
SELECT COUNT(*) AS total
FROM `{$table}` p
{$countWhereSql}
SQL;
        if (!$total) {
            $countResult = $database->raw($countSql, ...$candidateBuild['match'], ...$whereBuild['match'])->queryOne();
            $total = (int)($countResult['total'] ?? 0);
        }
        if (!$total) {
            return $this->buildListResultByPrimaryKeys([], 0, $format);
        }

        $page = min($page, (int)ceil($total / $size)) ?: 1;
        $offset = ($page - 1) * $size;
        $outerSelect = array_merge(["p.`{$primaryKey}`"], $outerOrderSelects);
        $outerSelectSql = implode(', ', $outerSelect);
        $primarySql = <<<SQL
SELECT {$outerSelectSql}
FROM `{$table}` p
JOIN (
    SELECT {$subquerySelect}
    FROM `{$table}`{$indexHintSql}
    {$candidateWhereSql}
) x ON x.`{$primaryKey}` = p.`{$primaryKey}`
{$whereSql}
ORDER BY {$orderSql}
LIMIT {$size} OFFSET {$offset}
SQL;
        $rows = $database->raw($primarySql, ...$candidateBuild['match'], ...$whereBuild['match'])->queryAll();
        $pagePrimaryKeys = [];
        foreach ($rows as $row) {
            if (!is_array($row) || !array_key_exists($primaryKey, $row) || $row[$primaryKey] === null || $row[$primaryKey] === '') {
                continue;
            }
            $pagePrimaryKeys[] = $row[$primaryKey];
        }
        return $this->buildListResultByPrimaryKeys($pagePrimaryKeys, $total, $format);
    }

    /**
     * 为候选集二段查询的最终 where 补齐主表别名，避免与候选子查询字段重名。
     *
     * 候选查询的外层会形成 `main p JOIN (subquery) x` 结构，像 `aweme_id` 这类最终筛选字段
     * 如果仍保持裸字段，就会同时命中 `p` 与 `x` 两边而出现 ambiguous column。这里统一把
     * WhereBuilder 生成的裸字段限定到主表别名上，同时保留已显式声明的 `x.foo` / `p.foo` 形式。
     *
     * @param string $sql WhereBuilder 构建出的 SQL 片段（不含 WHERE 关键字）
     * @param string $alias 主表别名
     * @return string
     */
    protected function qualifyCandidateWhereSql(string $sql, string $alias): string {
        return preg_replace_callback('/`([^`]+)`/', static function (array $matches) use ($alias) {
            $field = (string)$matches[1];
            if ($field === '') {
                return $matches[0];
            }
            if (str_contains($field, '.')) {
                return '`' . str_replace('.', '`.`', $field) . '`';
            }
            return "`{$alias}`.`{$field}`";
        }, $sql) ?: $sql;
    }

    /**
     * 合并候选条件和最终筛选条件，构造候选 count 使用的单表 WHERE 子句。
     *
     * `candidate` 的总数语义是“候选集 ∩ 最终筛选”的行数。由于候选条件和最终 where
     * 都只落在当前主表上，count 阶段无需再做同表自连接；直接把两段条件合并成单表
     * WHERE 即可，语义与 `JOIN subquery ON pk` 等价，但更稳定也更容易让数据库优化器处理。
     *
     * @param string $candidateWhereSql 已补主表别名的候选条件（不含 WHERE）
     * @param string $whereSql 已补主表别名的最终筛选条件（不含 WHERE）
     * @return string
     */
    protected function mergeQualifiedCandidateWhereSql(string $candidateWhereSql, string $whereSql): string {
        $parts = [];
        if ($candidateWhereSql !== '') {
            $parts[] = "({$candidateWhereSql})";
        }
        if ($whereSql !== '') {
            $parts[] = "({$whereSql})";
        }
        if (!$parts) {
            return '';
        }
        return 'WHERE ' . implode(' AND ', $parts);
    }

    /**
     * 根据候选集条件和排序字段，自动推断候选子查询应优先使用的联合索引。
     *
     * 这里的意图不是把索引名暴露给业务层，而是利用 ArCreator 生成的表映射元信息，
     * 在 Dao 内部优先寻找“候选等值字段 + 排序字段”匹配的联合索引。
     *
     * @param array $orderFields
     * @return string
     */
    protected function resolveCandidateIndexHintSql(array $orderFields): string {
        $candidateFields = $this->extractCandidateEqualityFields();
        if (!$candidateFields) {
            return '';
        }
        $tableConfig = Config::getDbTable($this->_dbName . '_' . $this->getCompleteTableName());
        $indexes = $tableConfig['index'] ?? [];
        if (!$indexes || !is_array($indexes)) {
            return '';
        }
        $expectedPrefix = array_values(array_unique(array_merge($candidateFields, $orderFields)));
        foreach ($indexes as $indexName => $index) {
            if (!is_array($index) || empty($index['content'])) {
                continue;
            }
            $indexFields = $this->parseIndexFieldsFromContent((string)$index['content']);
            if (!$indexFields) {
                continue;
            }
            if ($this->indexFieldsStartWith($indexFields, $expectedPrefix)) {
                return " USE INDEX (`{$indexName}`)";
            }
        }
        return '';
    }

    /**
     * 从候选集条件里提取可用于联合索引前缀匹配的简单等值字段。
     *
     * 这里只识别通过 `candidate(array)` 传入的简单 AND 等值条件；
     * 复杂 OR、范围和数组条件不参与自动索引推断，避免误判。
     *
     * @return array
     */
    protected function extractCandidateEqualityFields(): array {
        $fields = [];
        if (!$this->_candidateSource || !is_array($this->_candidateSource)) {
            return $fields;
        }
        foreach ($this->_candidateSource as $key => $value) {
            if (!is_string($key) || $key === '' || str_starts_with(strtoupper($key), '#OR') || str_starts_with(strtoupper($key), '#AND')) {
                continue;
            }
            if (is_array($value) || $value === null) {
                continue;
            }
            if (preg_match('/^([a-zA-Z0-9_\.]+)(\[(?<operator>[^\]]+)\])?$/', $key, $matches) !== 1) {
                continue;
            }
            $operator = $matches['operator'] ?? '=';
            if ($operator !== '=' && $operator !== '') {
                continue;
            }
            $field = $matches[1] ?? '';
            if ($field !== '') {
                $fields[] = $field;
            }
        }
        return array_values(array_unique($fields));
    }

    /**
     * 解析 ArCreator 索引映射中的字段顺序。
     *
     * @param string $content
     * @return array
     */
    protected function parseIndexFieldsFromContent(string $content): array {
        if (preg_match_all('/`([^`]+)`\s+ASC|`([^`]+)`\s+DESC|`([^`]+)`(?=[,\)])/i', $content, $matches) === 0) {
            return [];
        }
        $fields = [];
        foreach ($matches as $group) {
            foreach ($group as $field) {
                if ($field !== '') {
                    $fields[] = $field;
                }
            }
        }
        return array_values(array_unique($fields));
    }

    /**
     * 判断索引字段是否以给定前缀顺序开头。
     *
     * @param array $indexFields
     * @param array $expectedPrefix
     * @return bool
     */
    protected function indexFieldsStartWith(array $indexFields, array $expectedPrefix): bool {
        if (!$expectedPrefix || count($indexFields) < count($expectedPrefix)) {
            return false;
        }
        foreach ($expectedPrefix as $position => $field) {
            if (($indexFields[$position] ?? null) !== $field) {
                return false;
            }
        }
        return true;
    }

    /**
     * 将已登记的 join 结果批量回填到主表结果集中。
     *
     * @param array $rows 主表结果集
     * @param array $joins join 配置快照
     * @return array
     */
    protected function applyJoinsToRows(array $rows, array $joins): array {
        if (!$rows || !is_array(reset($rows))) {
            return $rows;
        }
        foreach ($joins as $join) {
            $rows = $this->applyJoinToRows($rows, $join);
        }
        return $rows;
    }

    /**
     * 对单个 join 配置执行批量查询并回填结果。
     *
     * @param array $rows
     * @param array $join
     * @return array
     */
    protected function applyJoinToRows(array $rows, array $join): array {
        $matchValues = [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            if (!array_key_exists($join['source_field'], $row)) {
                continue;
            }
            $value = $row[$join['source_field']];
            if ($value === null || $value === '') {
                continue;
            }
            $matchValues[(string)$value] = $value;
        }
        if (!$matchValues) {
            return $rows;
        }
        if (!empty($join['aggregate'])) {
            return $this->applyJoinAggregateToRows($rows, $join, $matchValues);
        }

        $joinDao = clone $join['dao'];
        [$queryFields, $removeTargetFieldAfterJoin] = $this->prepareJoinQueryFields($joinDao->_fields, $join['target_field']);
        if (!is_null($queryFields)) {
            $joinDao->setFields(...$queryFields);
        }
        $joinWhere = $joinDao->_where ? clone $joinDao->_where : WhereBuilder::create();
        $joinWhere->and([$join['target_field'] => array_values($matchValues)]);
        $joinedMap = [];
        $joinedRows = $joinDao->where($joinWhere)->all();
        if ($joinedRows) {
            foreach ($joinedRows as $joinedRow) {
                if (!is_array($joinedRow) || !array_key_exists($join['target_field'], $joinedRow)) {
                    continue;
                }
                $joinedKey = (string)$joinedRow[$join['target_field']];
                if ($removeTargetFieldAfterJoin) {
                    unset($joinedRow[$join['target_field']]);
                }
                if (!array_key_exists($joinedKey, $joinedMap)) {
                    $joinedMap[$joinedKey] = $joinedRow;
                }
            }
        }

        // 只在当前行具备匹配值时参与回填；命中失败时显式置空，便于后续 join 依据顺序覆盖。
        foreach ($rows as &$row) {
            if (!is_array($row) || !array_key_exists($join['source_field'], $row)) {
                continue;
            }
            $value = $row[$join['source_field']];
            if ($value === null || $value === '') {
                continue;
            }
            $joinedKey = (string)$value;
            $row[$join['assign_field']] = $joinedMap[$joinedKey] ?? null;
        }
        unset($row);

        return $rows;
    }

    /**
     * 对聚合 join 执行按匹配键分组聚合并回填统计值。
     *
     * @param array $rows
     * @param array $join
     * @param array $matchValues
     * @return array
     */
    protected function applyJoinAggregateToRows(array $rows, array $join, array $matchValues): array {
        $aggregate = $join['aggregate'] ?? null;
        if (!$aggregate) {
            return $rows;
        }
        $joinDao = clone $join['dao'];
        $joinDao->_fields = null;
        $joinDao->_group = null;
        $joinDao->_groupAggregateField = null;
        $joinDao->_groupAggregateFunction = 'min';

        $joinWhere = $joinDao->_where ? clone $joinDao->_where : WhereBuilder::create();
        $joinWhere->and([$join['target_field'] => array_values($matchValues)]);
        $joinDao->where($joinWhere);

        $connection = $joinDao->connection(resetParams: false);
        $connection->group($join['target_field']);
        $aggregateRows = match ($aggregate['type']) {
            'count' => $connection->count($aggregate['field']),
            'sum' => $connection->sum($aggregate['field']),
            'max', 'min' => $connection
                ->select($join['target_field'], strtoupper($aggregate['type']) . '(`' . $aggregate['field'] . '`)')
                ->get(),
            default => []
        };
        $aggregateMap = [];
        if (is_array($aggregateRows)) {
            foreach ($aggregateRows as $aggregateRow) {
                if (!is_array($aggregateRow) || !array_key_exists($join['target_field'], $aggregateRow)) {
                    continue;
                }
                $aggregateMap[(string)$aggregateRow[$join['target_field']]] = $this->normalizeJoinAggregateValue(
                    $aggregateRow[$aggregate['result_key']] ?? null,
                    $aggregate
                );
            }
        }

        foreach ($rows as &$row) {
            if (!is_array($row) || !array_key_exists($join['source_field'], $row)) {
                continue;
            }
            $value = $row[$join['source_field']];
            if ($value === null || $value === '') {
                continue;
            }
            $row[$join['assign_field']] = $aggregateMap[(string)$value] ?? $aggregate['default'];
        }
        unset($row);

        return $rows;
    }

    /**
     * 规范化 join 聚合配置。
     *
     * @param self $dao
     * @param string|array|null $aggregate
     * @return array{type: string, field: string, result_key: string, default: mixed}|null|false
     */
    protected function normalizeJoinAggregate(self $dao, string|array|null $aggregate): array|null|false {
        if ($aggregate === null) {
            return null;
        }
        if (is_string($aggregate)) {
            $aggregate = ['type' => strtolower($aggregate)];
        }
        $type = strtolower((string)($aggregate['type'] ?? ''));
        return match ($type) {
            'count' => [
                'type' => 'count',
                'field' => (string)($aggregate['field'] ?? $dao->getPrimaryKey()),
                'result_key' => 'count',
                'default' => (int)($aggregate['default'] ?? 0),
            ],
            'sum' => !empty($aggregate['field']) ? [
                'type' => 'sum',
                'field' => (string)$aggregate['field'],
                'result_key' => (string)$aggregate['field'],
                'default' => $this->normalizeJoinAggregateNumber($aggregate['default'] ?? 0),
            ] : false,
            'max', 'min' => !empty($aggregate['field']) ? [
                'type' => $type,
                'field' => (string)$aggregate['field'],
                'result_key' => (string)$aggregate['field'],
                'default' => $aggregate['default'] ?? null,
            ] : false,
            default => false,
        };
    }

    /**
     * 将聚合查询返回值标准化为可回填的数字。
     *
     * @param mixed $value
     * @param array{type: string, field: string, result_key: string, default: mixed} $aggregate
     * @return mixed
     */
    protected function normalizeJoinAggregateValue(mixed $value, array $aggregate): mixed {
        return match ($aggregate['type']) {
            'count' => (int)$value,
            'sum' => $this->normalizeJoinAggregateNumber($value),
            'max', 'min' => $this->normalizeJoinScalarValue($value),
            default => $value
        };
    }

    /**
     * 将数值聚合结果收敛成 int/float，避免把数据库返回的数字字符串直接泄漏到业务层。
     *
     * @param mixed $value
     * @return int|float
     */
    protected function normalizeJoinAggregateNumber(mixed $value): int|float {
        if (!is_numeric($value)) {
            return 0;
        }
        $stringValue = (string)$value;
        return str_contains($stringValue, '.') ? (float)$stringValue : (int)$stringValue;
    }

    /**
     * 将聚合返回的标量值标准化，数值转 int/float，其余类型原样返回。
     *
     * @param mixed $value
     * @return mixed
     */
    protected function normalizeJoinScalarValue(mixed $value): mixed {
        if (!is_numeric($value)) {
            return $value;
        }
        return $this->normalizeJoinAggregateNumber($value);
    }

    /**
     * 规范化 join 的字段映射配置。
     *
     * @param self $dao
     * @param string|array $match
     * @return array{0: string, 1: string}
     */
    protected function normalizeJoinMatch(self $dao, string|array $match): array {
        if (is_string($match)) {
            return [$match, $dao->getPrimaryKey()];
        }
        if (!$match) {
            return ['', ''];
        }
        $sourceField = array_key_first($match);
        $targetField = $sourceField ? (string)$match[$sourceField] : '';
        return [(string)$sourceField, $targetField];
    }

    /**
     * 为 join 查询准备字段列表，必要时补上目标匹配字段供内部建索引使用。
     *
     * @param array|null $selectedFields
     * @param string $targetField
     * @return array{0: ?array, 1: bool}
     */
    protected function prepareJoinQueryFields(?array $selectedFields, string $targetField): array {
        if (is_null($selectedFields)) {
            return [null, false];
        }
        $selectFields = array_values(array_filter($selectedFields, static fn($field) => is_string($field) && trim($field) !== ''));
        if (!$selectFields) {
            return [null, false];
        }
        $needAppendTargetField = !in_array($targetField, $selectFields, true);
        if ($needAppendTargetField) {
            $selectFields[] = $targetField;
        }
        return [$selectFields, $needAppendTargetField];
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
        $this->_candidateWhere = null;
        $this->_candidateSource = null;
        $this->_joins = [];
        $this->_group = null;
        $this->_groupAggregateField = null;
        $this->_groupAggregateFunction = 'min';
    }
}
