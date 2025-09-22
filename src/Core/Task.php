<?php

namespace Scf\Core;

use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\CoroutineSingleton;
use Scf\Server\Http;
use Scf\Util\Time;
use Swoole\WebSocket\Server;

abstract class Task {
    use ComponentTrait, CoroutineSingleton;

    protected static array $tasks = [];
    protected int $taskId = -1;

    /**
     * @param $params
     * @return static
     */
    public static function add($params): static {
        $task = static::instance();
        $task->taskId = $task->server()->task([
            '_handler' => static::class,
            '_created' => Time::millisecond(),
            ...$params
        ]);
        //self::$tasks[] = $task->taskId;
        return $task;
    }


    /**
     * @return int
     */
    public function getTaskId(): int {
        return $this->taskId;
    }

    /**
     * 执行任务
     * @param \Swoole\Server\Task $task
     */
    abstract public function execute(\Swoole\Server\Task $task);

    /**
     * @param $data
     * @return void
     */
    public function finish($data): void {
        $this->server()->finish($data);
    }

    /**
     * @return ?Server
     */
    protected function server(): ?Server {
        return Http::server();
    }

}