<?php

namespace Scf\Core\Table;

use InvalidArgumentException;
use Swoole\Atomic\Long;
use Swoole\Atomic as SwooleAtomic;

abstract class Atomic {

    public static array $_instances;

    /**
     * @var Long|SwooleAtomic
     */
    protected Long|SwooleAtomic $table;

    protected int $byteLength = 32;


    /**
     * 构造器
     */
    public function __construct() {
        if ($this->byteLength == 32) {
            $table = new SwooleAtomic();
        } else {
            $table = new Long();
        }
        $this->table = $table;
    }

    /**
     * 批量注册实例化
     * @param string|array $classes
     * @return void
     */
    public static function register(string|array $classes): void {
        foreach ((array)$classes as $cls) {
            if (!is_string($cls)) {
                continue;
            }
            $cls = ltrim($cls, '\\');
            if ($cls === '') {
                continue; // 忽略空字符串
            }
            if (!class_exists($cls)) {
                throw new InvalidArgumentException("Class {$cls} not found");
            }
            if (!is_subclass_of($cls, self::class)) {
                throw new InvalidArgumentException("{$cls} must extend " . self::class);
            }
            if (!isset(self::$_instances[$cls])) {
                self::$_instances[$cls] = new $cls();
            }
        }
    }

    /**
     * 获取单例
     * @return Long|SwooleAtomic
     */
    public static function instance(): Long|SwooleAtomic {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            self::$_instances[$class] = new $class();
        }
        return self::$_instances[$class]->table();
    }

    private function table(): Long {
        return $this->table;
    }

    /**
     * 取出数据
     * @return int
     */
    public function get(): int {
        return $this->table->get();
    }

    /**
     * 设置数据
     * @param int $value
     * @return void
     */
    public function set(int $value): void {
        $this->table->set($value);
    }

    /**
     * 原子自增
     * @param $value
     * @return int
     */
    public function incr($value): int {
        return $this->table->add($value);
    }

    /**
     * 原子自减
     * @param int $value
     * @return int
     */
    public function decr(int $value): int {
        return $this->table->sub($value);
    }

    public function cmpset(int $cmp_value, int $set_value): bool {
        return $this->table->cmpset($cmp_value, $set_value);
    }

    public static function wait(float $timeout = 1): bool {
        return static::instance()->wait($timeout);
    }

    public static function wakeup(int $n = 1): bool {
        return static::instance()->wakeup($n);
    }

}