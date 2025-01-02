<?php

namespace Scf\Server\Worker;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Core\Traits\ProcessLifeSingleton;
use Scf\Mode\Web\App;
use Scf\Util\Random;
use Scf\Util\Time;
use Swoole\Coroutine;

/**
 * 协程运行生命周期
 */
class ProcessLife {
    use ProcessLifeSingleton;

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
     * @return void
     */
    public function addRedis($cmd): void {
        if (defined('SERVER_MODE') && SERVER_MODE == MODE_CLI) {
            return;
        }
        $record = "【" . date('m-d H:i:s') . "." . substr(Time::millisecond(), -3) . "】" . $cmd;
        if (str_starts_with(strtolower($cmd), 'get') || str_starts_with(strtolower($cmd), 'hget')) {
            $this->redisLogs['get'] += 1;
        } else {
            $this->redisLogs['set'] += 1;
        }
        $this->redisLogs['cmd'][] = $record;
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
     * @return void
     */
    public function addSql($sql): void {
        if (defined('SERVER_MODE') && SERVER_MODE == MODE_CLI) {
            return;
        }
        $record = "【" . date('m-d H:i:s') . "." . substr(Time::millisecond(), -3) . "】" . $sql;
        if (str_starts_with(strtolower($sql), 'select')) {
            $this->databaseLogs['read'] += 1;
        } else {
            $this->databaseLogs['write'] += 1;
        }
        $this->databaseLogs['sql'][] = $record;
    }

    /**
     * 调试信息
     * @return array
     */
    #[ArrayShape(['cid' => "mixed", 'runtime' => "string", 'status' => "bool", 'database' => "array", 'redis' => "array"])] public function requestDebugInfo(): array {
        return [
            'cid' => Coroutine::getCid(),
            'runtime' => App::instance()->consume() . 'ms',
            'database' => $this->databaseLogs,
            'redis' => $this->redisLogs
        ];
    }


}