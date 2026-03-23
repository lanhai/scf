<?php

namespace Scf\Server\Worker;

use Scf\Core\Context;
use Scf\Core\Env;
use Scf\Core\Traits\ProcessLifeSingleton;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Request;
use Scf\Util\Random;
use Scf\Util\Time;
use Swoole\Coroutine;

/**
 * 协程运行生命周期
 */
class ProcessLife {
    use ProcessLifeSingleton;

    protected const MAX_SQL_LOGS = 200;
    protected const MAX_REDIS_LOGS = 200;
    protected const MAX_LOG_LENGTH = 1024;

    public static function enabled(): bool {
        return Context::has(Request::CONTEXT_ACTIVE_KEY);
    }

    protected ?string $lifeId = null;
    protected int $count = 0;

    protected array $databaseLogs = [
        'write' => 0,
        'read' => 0,
        'sql' => []
    ];
    protected array $redisLogs = [
        'set' => 0,
        'get' => 0,
        'hit' => 0,
        'miss' => 0,
        'cmd' => []
    ];

    /**
     * 注意:在构造函数里不能进行任何会刷新缓冲期的操作,比如使用控制台打印(包含 flush)等操作,否则会导致当前实例自动销毁回收
     * @param $cid
     */
    public function __construct($cid) {
        $this->lifeId = Random::makeUUIDV4();
        $this->ancestorCid = $cid;
    }

    /**
     * @return string
     */
    public static function id(): string {
        return static::instance()->_id();
    }

    /**
     * 返回action name
     * @return string
     */
    public function _id(): string {
        if (is_null($this->lifeId)) {
            $this->lifeId = Random::makeUUIDV4();
        }
        $this->count++;
        //Env::isDev() and Console::info("ProcessLife 被引用次数:" . $this->count);
        return $this->lifeId;
    }

    /**
     * 记录redis操作记录
     * @param $cmd
     * @param float $cost
     * @return void
     */
    public function addRedis($cmd, float $cost): void {
        if (!self::enabled()) {
            return;
        }
        $record = "【" . date('H:i:s') . "." . substr(Time::millisecond(), -3) . " ⧖{$cost}ms】" . $cmd;
        if (str_starts_with(strtolower($cmd), 'get') || str_starts_with(strtolower($cmd), 'hget')) {
            $this->redisLogs['get'] += 1;
        } else {
            $this->redisLogs['set'] += 1;
        }
        if (Env::isDev()) {
            $this->appendLog($this->redisLogs['cmd'], $record, self::MAX_REDIS_LOGS);
        }
    }

    /**
     * 统计redis命中数量
     * @param bool $hit
     * @return void
     */
    public function hitRedis(bool $hit = true): void {
        if ($hit) {
            $this->redisLogs['hit'] += 1;
        } else {
            $this->redisLogs['miss'] += 1;
        }
    }

    /**
     * 记录sql
     * @param $sql
     * @param float $cost
     * @return void
     */
    public function addSql($sql, float $cost): void {
        if (!self::enabled()) {
            return;
        }
        $record = "【" . date('H:i:s') . "." . substr(Time::millisecond(), -3) . " ⧖{$cost}ms】" . $sql;
        if (str_starts_with(strtolower($sql), 'select')) {
            $this->databaseLogs['read'] += 1;
        } else {
            $this->databaseLogs['write'] += 1;
        }
        if (Env::isDev()) {
            $this->appendLog($this->databaseLogs['sql'], $record, self::MAX_SQL_LOGS);
        }
    }

    /**
     * 调试信息
     * @return array
     */
    public function requestDebugInfo(): array {
        return [
            'cid' => Coroutine::getCid(),
            'consume_ms' => App::instance()->consume(),
            'database' => $this->databaseLogs,
            'redis' => $this->redisLogs
        ];
    }

    protected function appendLog(array &$logs, string $record, int $max): void {
        if (mb_strlen($record) > self::MAX_LOG_LENGTH) {
            $record = mb_substr($record, 0, self::MAX_LOG_LENGTH) . '...';
        }
        $logs[] = $record;
        $count = count($logs);
        if ($count <= $max) {
            return;
        }
        array_splice($logs, 0, $count - $max);
    }


}
