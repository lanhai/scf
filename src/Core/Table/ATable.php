<?php

namespace Scf\Core\Table;

use Scf\Core\Console;
use Scf\Helper\JsonHelper;
use Scf\Util\Arr;
use Swoole\Table;

/**
 * Swoole\Table 的统一抽象层。
 *
 * 责任边界：
 * 1. 统一封装行读写、计数、原子增减等基础能力；
 * 2. 在运行期提供容量保护，避免表满/冲突切片耗尽时 warning 直达全局错误处理；
 * 3. 作为各业务内存表的公共父类，保证调用方行为一致。
 *
 * 架构位置：
 * 位于 Core 层，为日志、请求计数、运行时状态等所有共享内存表提供底座。
 */
abstract class ATable {

    public static array $_instances;

    /**
     *  配置项
     * @var array
     */
    protected array $_config = [];
    /**
     * @var Table
     */
    protected Table $table;

    /**
     * 构造器
     * @param array|null $config 配置项
     */
    public function __construct(array $config = null) {
        if ($this->_config) {
            $this->_config = Arr::merge($this->_config, is_array($config) ? $config : []);
        }
        $table = new Table($this->_config['size'] ?? 1024);
        $colums = $this->_config['colums'] ?? ['_value' => ['type' => Table::TYPE_STRING, 'size' => 1024]];
        foreach ($colums as $name => $colum) {
            $table->column($name, $colum['type'], $colum['size'] ?? 0);
        }
        $table->create();
        $this->table = $table;
    }

    /**
     * 批量注册实例化
     * @param $tables
     * @return void
     */
    public static function register($tables = null): void {
        foreach ((array)$tables as $cls) {
            if (!isset(self::$_instances[$cls])) {
                self::$_instances[$cls] = new $cls();
            }
        }
//        if (is_array($tables)) {
//            foreach ($tables as $class) {
//                if (!isset(self::$_instances[$class])) {
//                    self::$_instances[$class] = new $class();
//                }
//            }
//        } else {
//            $class = static::class;
//            if (!isset(self::$_instances[$class])) {
//                self::$_instances[$class] = new $class();
//            }
//        }
    }

    /**
     * 返回已创建的内存表
     * @return array
     */
    public static function list(): array {
        if (!self::$_instances) {
            return [];
        }
        $list = [];
        foreach (self::$_instances as $k => $table) {
            $list[] = [
                'class' => $k,
                'size' => $table->size(),
                'count' => $table->count(),
                'status' => $table->stats(),
                'memory_size' => $table->memorySize()
            ];
        }
        return $list;
    }

    /**
     * 获取单例
     * @param array|null $conf
     * @return static
     */
    public static function instance(array $conf = null): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class($conf);
        }
        return self::$_instances[$class];
    }

    public function rows(): array {
        $rows = [];
        foreach ($this->table as $k => $row) {
            if (count($row) == 1 and isset($row['_value'])) {
                $rows[$k] = JsonHelper::is($row['_value']) ? JsonHelper::recover($row['_value']) : $row['_value'];
            } else {
                $rows[$k] = $row;
            }
        }
        return $rows;
    }

    /**
     * @return int
     */
    public function memorySize(): int {
        return $this->table->getMemorySize();
    }

    /**
     * @return int
     */
    public function size(): int {
        return $this->table->getSize();
    }

    /**
     * @return array|false
     */
    public function stats(): bool|array {
        return $this->table->stats();
    }

    /**
     * 取出数据
     * @param string $rowKey
     * @param string $field
     * @return false|mixed
     */
    public function get(string $rowKey, string $field = '_value'): mixed {
        if ($colums = $this->table->get($rowKey)) {
            foreach ($colums as &$value) {
                if (JsonHelper::is($value)) {
                    $value = JsonHelper::recover($value);
                }
            }
            return $colums[$field] ?? $colums;
        }
        return false;
    }

    /**
     * 设置数据
     * @param $rowKey
     * @param $datas
     * @return bool
     */
    public function set($rowKey, $datas): bool {
        if (!$this->exist($rowKey) && !$this->canInsertNewRow()) {
            Console::error("内存表:" . get_called_class() . "已满,请检查配置");
            return false;
        }
        if (isset($this->_config['colums']['_value']) && !isset($datas['_value'])) {
            if (Arr::isArray($datas)) {
                $datas = JsonHelper::toJson($datas);
            }
            // Swoole\Table 在内存不足时会抛 warning，这里使用 @ 抑制 warning，
            // 统一走返回值判定并记录结构化错误，避免业务请求被 warning 中断。
            $ok = @$this->table->set($rowKey, ['_value' => $datas]);
            if (!$ok) {
                Console::error("内存表写入失败:" . get_called_class() . ", key={$rowKey}, " . $this->tablePressureSummary());
            }
            return $ok;
        }
        foreach ($datas as &$value) {
            if (Arr::isArray($value)) {
                $value = JsonHelper::toJson($value);
            }
        }
        $ok = @$this->table->set($rowKey, $datas);
        if (!$ok) {
            Console::error("内存表写入失败:" . get_called_class() . ", key={$rowKey}, " . $this->tablePressureSummary());
        }
        return $ok;
    }

    /**
     * 原子自增
     * @param string $key
     * @param string $colum
     * @param int $incrby
     * @return int
     */
    public function incr(string $key, string $colum = '_value', int $incrby = 1): int {
        if (!$this->exist($key)) {
            if (!$this->canInsertNewRow()) {
                Console::error("内存表:" . get_called_class() . "已满,请检查配置");
                return 0;
            }
            // 先显式建行，再执行 incr，避免 Swoole 在 incr 的“隐式建行”路径触发 warning。
            $seedData = $colum === '_value' ? 0 : [$colum => 0];
            if (!$this->set($key, $seedData)) {
                return 0;
            }
        }
        $value = @$this->table->incr($key, $colum, $incrby);
        if ($value === false) {
            Console::error("内存表自增失败:" . get_called_class() . ", key={$key}, colum={$colum}, " . $this->tablePressureSummary());
            return 0;
        }
        return (int)$value;
    }

    /**
     * 原子自减
     * @param string $key
     * @param string $colum
     * @param int $decrby
     * @return int
     */
    public function decr(string $key, string $colum = '_value', int $decrby = 1): int {
        $value = @$this->table->decr($key, $colum, $decrby);
        if ($value === false) {
            Console::error("内存表自减失败:" . get_called_class() . ", key={$key}, colum={$colum}, " . $this->tablePressureSummary());
            return 0;
        }
        return (int)$value;
    }

    /**
     * 删除一行数据
     * @param $key
     * @return bool
     */
    public function delete($key): bool {
        return $this->table->del($key);
    }

    /**
     * 检查 table 中是否存在某一个 key。
     * @param $key
     * @return bool
     */
    public function exist($key): bool {
        return $this->table->exist($key);
    }

    /**
     * 统计内存表里有多少行数据
     * @return int
     */
    public function count(): int {
        return $this->table->count();
    }

    /**
     * 判断当前内存表是否还能安全插入新 key。
     *
     * 说明：
     * 1. 不能只看 count/size。Swoole\Table 在冲突切片耗尽时会提前“无法分配内存”；
     * 2. 因此优先读取 stats.available_slice_num，再结合 count/size 双重判断。
     *
     * @return bool
     */
    protected function canInsertNewRow(): bool {
        if ($this->count() >= $this->size()) {
            return false;
        }
        $stats = $this->stats();
        if (is_array($stats) && array_key_exists('available_slice_num', $stats)) {
            return ((int)$stats['available_slice_num']) > 0;
        }
        return true;
    }

    /**
     * 生成容量/冲突快照，便于线上定位表容量问题。
     *
     * @return string
     */
    protected function tablePressureSummary(): string {
        $stats = $this->stats();
        if (!is_array($stats)) {
            return "count=" . $this->count() . ", size=" . $this->size();
        }
        return "count=" . $this->count()
            . ", size=" . $this->size()
            . ", available_slice_num=" . ($stats['available_slice_num'] ?? '--')
            . ", total_slice_num=" . ($stats['total_slice_num'] ?? '--')
            . ", conflict_count=" . ($stats['conflict_count'] ?? '--')
            . ", conflict_max_level=" . ($stats['conflict_max_level'] ?? '--');
    }

}
