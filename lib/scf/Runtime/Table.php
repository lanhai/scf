<?php

namespace Scf\Runtime;

use Scf\Helper\JsonHelper;
use Scf\Util\Arr;

class Table {

    public static array $_instances;

    /**
     *  配置项
     * @var array
     */
    protected array $_config = [];
    /**
     * @var \Swoole\Table
     */
    protected \Swoole\Table $table;

    /**
     * 构造器
     * @param array|null $config 配置项
     */
    public function __construct(array $config = null) {
        if ($this->_config) {
            $this->_config = Arr::merge($this->_config, is_array($config) ? $config : []);
        }
        $table = new \Swoole\Table($this->_config['size'] ?? 1024);
        $colums = $this->_config['colums'] ?? ['_value' => ['type' => \Swoole\Table::TYPE_STRING, 'size' => 1024]];
        foreach ($colums as $name => $colum) {
            $table->column($name, $colum['type'], $colum['size']);
        }
        $table->create();
        $this->table = $table;
    }

    /**
     * 批量注册实例化
     * @param $tables
     * @return void
     */
    public static function register($tables = null) {
        if (is_array($tables)) {
            foreach ($tables as $class) {
                if (!isset(self::$_instances[$class])) {
                    self::$_instances[$class] = new $class();
                }
            }
        } else {
            $class = get_called_class();
            if (!isset(self::$_instances[$class])) {
                self::$_instances[$class] = new $class();
            }
        }
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
        $class = get_called_class();
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class($conf);
        }
        return self::$_instances[$class];
    }

    /**
     * @return int
     */
    public function memorySize(): int {
        return $this->table->getMemorySize();
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
        if (isset($this->_config['colums']['_value']) && !isset($datas['_value'])) {
            if (Arr::isArray($datas)) {
                $datas = JsonHelper::toJson($datas);
            }
            return $this->table->set($rowKey, ['_value' => $datas]);
        }
        foreach ($datas as &$value) {
            if (Arr::isArray($value)) {
                $value = JsonHelper::toJson($value);
            }
        }
        return $this->table->set($rowKey, $datas);
    }

    /**
     * 原子自增
     * @param string $key
     * @param string $colum
     * @param int $incrby
     * @return int
     */
    public function incr(string $key, string $colum = '_value', int $incrby = 1): int {
        return $this->table->incr($key, $colum, $incrby);
    }

    /**
     * 原子自减
     * @param string $key
     * @param string $colum
     * @param int $decrby
     * @return int
     */
    public function decr(string $key, string $colum = '_value', int $decrby = 1): int {
        return $this->table->decr($key, $colum, $decrby);
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
     * 统计内存表里有多少行数据
     * @return int
     */
    public function count(): int {
        return $this->table->count();
    }
}