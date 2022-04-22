<?php

namespace Scf\Service;

use JetBrains\PhpStorm\Pure;
use Scf\Command\Color;
use Scf\Core\ComponentTrait;
use Scf\Core\Console;
use Scf\Core\CoroutineInstance;
use Scf\Helper\JsonHelper;
use Scf\Server\Http;
use Swoole\WebSocket\Server;

abstract class Task {
    use ComponentTrait, CoroutineInstance;

    protected static array $tasks = [];
    protected int $taskId;

    /**
     * @param $params
     * @return static
     */
    public static function add($params): static {
        $task = static::instance();
        $task->taskId = $task->server()->task([
            'handler' => get_called_class(),
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
    public function finish($data) {
        $this->server()->finish($data);
    }

    /**
     * @return Server
     */
    #[Pure] protected function server(): Server {
        return Http::master();
    }

}