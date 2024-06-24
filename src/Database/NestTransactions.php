<?php

namespace Scf\Database;

use Scf\Core\Console;
use Scf\Core\Coroutine;
use Scf\Core\Traits\ProcessLifeSingleton;
use Scf\Mode\Web\Log;
use Scf\Util\Time;
use Throwable;

class NestTransactions {
    use ProcessLifeSingleton;

    protected bool $begin = false;
    protected bool $end = false;
    protected array $data = [
        'savepoints' => [],
        'connections' => [],
        'errors' => [],
        'started' => 0,
        'finished' => 0
    ];
    protected array $_result = [
        'commit' => 0,
        'points' => 0,
        'rollback' => 0,
        'errors' => []
    ];

    public function __construct($cid) {
        $this->ancestorCid = $cid;
        if ($this->ancestorCid !== -1) {
            Coroutine::defer(function () {
                $this->isBegin() and !$this->isEnd() and $this->finish();
            });
        }
    }

    public function __destruct() {
        $this->isBegin() and !$this->isEnd() and $this->finish();
    }

    public static function cancel(): void {
        static::instance()->_cancel();
    }

    public static function begin(): array {
        return static::instance()->_begin();
    }

    public static function finish(): static {
        $obj = static::instance();
        $obj->_finish();
        return $obj;
    }

    public static function status(): array {
        return static::instance()->_status();
    }

    public function hasError(): bool {
        return count($this->_result['errors']) > 0;
    }

    public function lastError(): array {
        $errors = $this->getErrors();
        return array_pop($errors);
    }

    public function getResult(): array {
        return $this->_result;
    }

    public function getErrors(): array {
        return $this->_result['errors'];
    }

    /**
     * @return bool
     */
    public function isBegin(): bool {
        return $this->begin;
    }

    /**
     * @return bool
     */
    public function isEnd(): bool {
        return $this->end;
    }

    /**
     * 开启嵌套事务,所有写入操作都将开启事务,谨慎使用
     */
    private function _begin(): array {
        !$this->begin and $this->data['created'] = Time::now();
        $this->begin = true;
        $this->end = false;
        return $this->data;
    }

    /**
     * 添加一个保存节点
     * @param $point
     * @return void
     */
    public function addPoint($point): void {
        $this->data['savepoints'][] = $point;
    }

    /**
     * 新增一个错误
     * @param $point
     * @param $error
     * @return void
     */
    public function addError($point, $error): void {
        $this->data['errors'][] = [
            'point' => $point,
            'message' => $error
        ];
    }

    /**
     * @return array|int[]
     */
    private function _finish(): array {
        $result = [
            'commit' => 0,
            'points' => 0,
            'rollback' => 0,
            'errors' => []
        ];
        if (!$this->begin || $this->end) {
            goto Reset;
        }
        if ($this->data['errors']) {
            $result = [
                'commit' => 0,
                'points' => count($this->data['savepoints']),
                'rollback' => $this->rollback(),
                'errors' => $this->data['errors']
            ];
        } else {
            $result = [
                'commit' => $this->commit(),
                'points' => count($this->data['savepoints']),
                'rollback' => 0,
                'errors' => []
            ];
        }
        Reset:
        $this->reset();
        $this->_result = $result;
        return $result;
    }

    /**
     * 取消进程事务后,回滚所有连接,后续的数据操作将退出事务,同时将事务强制设置为结束状态,重新开始之前commit将不再生效
     * @return void
     */
    private function _cancel(): void {
        $this->rollback();
        $this->reset();
    }

    private function reset(): void {
        $this->end = true;
        $this->data['finished'] = Time::now();
        $this->data['connections'] = [];
    }

    /**
     * 提交嵌套事务
     * @return int
     */
    private function commit(): int {
        $commitCount = 0;
        if (!$this->isBegin() || $this->isEnd()) {
            return $commitCount;
        }
        if ($this->data['connections']) {
            /** @var Transaction $transaction */
            foreach ($this->data['connections'] as $transaction) {
                try {
                    $transaction->commit();
                    $commitCount++;
                } catch (\PDOException $exception) {
                    Log::instance()->error("事务提交失败:" . $exception->getMessage());
                }
            }
        }
        return $commitCount;
    }

    /**
     * 回滚嵌套事务
     * @return int
     */
    private function rollback(): int {
        $rollbackCount = 0;
        if (!$this->isBegin() || $this->isEnd()) {
            return $rollbackCount;
        }
        if ($this->data['connections']) {
            /** @var Transaction $transaction */
            foreach ($this->data['connections'] as $transaction) {
                try {
                    $transaction->rollback();
                    $rollbackCount++;
                } catch (\PDOException $exception) {
                    Log::instance()->error("事务回滚失败:" . $exception->getMessage());
                }
            }
        }
        return $rollbackCount;
    }

    /**
     * 添加一个事务连接
     * @param $key
     * @param Transaction $transaction
     * @return void
     */
    public function addConnection($key, Transaction $transaction): void {
        $this->data['connections'][$key] = $transaction;
    }

    /**
     * @throws Throwable
     */
    public function getConnenction($key): ?Transaction {
        return $this->data['connections'][$key] ?? null;
    }

    private function _status(): array {
        return $this->data;
    }

}