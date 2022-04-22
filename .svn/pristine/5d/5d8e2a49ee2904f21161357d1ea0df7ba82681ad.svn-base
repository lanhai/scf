<?php

namespace Scf\Mode\Web;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Util\Time;
use Swoole\Coroutine;

class Log extends \Scf\Core\Log {
    protected bool $debug = false;
    protected array $database = [
        'write' => 0,
        'read' => 0,
        'sql' => []
    ];
    protected array $redis = [
        'set' => 0,
        'get' => 0,
        'hit' => 0,
        'miss' => 0,
        'cmd' => []
    ];

    /**
     * 关闭调试
     * @return void
     */
    public function disableDebug() {
        $this->debug = false;
    }

    /**
     * 开启调试
     * @return void
     */
    public function enableDebug() {
        $this->debug = true;
    }

    /**
     * 检查调试信息是否开启
     * @return bool
     */
    public function isDebugEnable(): bool {
        return $this->debug;
    }

    /**
     * 记录redis操作记录
     * @param $cmd
     * @return void
     */
    public function addRedis($cmd) {
        $record = "【" . date('m-d H:i:s') . "." . substr(Time::millisecond(), -3) . "】" . $cmd;
        if (str_starts_with(strtolower($cmd), 'get') || str_starts_with(strtolower($cmd), 'hget')) {
            $this->redis['get'] += 1;
        } else {
            $this->redis['set'] += 1;
        }
        $this->redis['cmd'][] = $record;
    }

    /**
     * 统计redis命中数量
     * @param bool $hit
     * @return void
     */
    public function hitRedis(bool $hit = true) {
        if ($hit) {
            $this->redis['hit'] += 1;
        } else {
            $this->redis['miss'] += 1;
        }
    }

    /**
     * 记录sql
     * @param $sql
     * @return void
     */
    public function addSql($sql) {
        $record = "【" . date('m-d H:i:s') . "." . substr(Time::millisecond(), -3) . "】" . $sql;
        if (str_starts_with(strtolower($sql), 'select')) {
            $this->database['read'] += 1;
        } else {
            $this->database['write'] += 1;
        }
        $this->database['sql'][] = $record;
    }

    /**
     * 调试信息
     * @return array
     */
    #[ArrayShape(['cid' => "mixed", 'runtime' => "string", 'status' => "bool", 'database' => "array", 'redis' => "array"])] public function requestDebugInfo(): array {
        return [
            'cid' => Coroutine::getCid(),
            'runtime' => App::instance()->consume() . 'ms',
            'status' => $this->isDebugEnable(),
            'database' => $this->database,
            'redis' => $this->redis
        ];
    }
} 