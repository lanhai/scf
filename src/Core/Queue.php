<?php

namespace Scf\Core;

use JetBrains\PhpStorm\Pure;
use Scf\Cache\Redis;
use Scf\Helper\JsonHelper;
use Scf\Service\Enum\QueueStatus;
use Scf\Service\Struct\QueueStruct;
use Scf\Util\Date;
use Scf\Util\Sn;
use Scf\Util\Time;
use Swoole\Timer;

abstract class Queue {

    protected QueueStruct $queue;
    protected mixed $data;

    /**
     * @param QueueStruct $queue
     */
    #[Pure]
    public function __construct(QueueStruct $queue) {
        $this->queue = $queue;
        $this->queue->start = Time::millisecond();
        $this->data = $queue->data;
    }

    /**
     * 执行队列任务
     * @param QueueStruct $queue
     * @return bool
     */
    public static function start(QueueStruct $queue): bool {
        $handler = new $queue->handler($queue);
        return $handler->end($handler->run());
    }

    /**
     * @return Result
     */
    abstract public function run(): Result;

    /**
     *
     * @param Result $result
     * @return bool
     */
    public function end(Result $result): bool {
        $this->queue->updated = time();
        $this->queue->try_times += 1;
        $this->queue->result = is_array($result->getData()) ? $result->getData() : ['result' => $result->getData()];
        $this->queue->end = Time::millisecond();
        $this->queue->duration = $this->queue->end - $this->queue->start;
        if ($result->hasError()) {
            $this->queue->remark = $result->getMessage();
            $this->queue->status = QueueStatus::FAILED->get();
            if ($this->queue->retry == STATUS_ON && $this->queue->try_times < $this->queue->try_limit) {
                //延迟加入
                $this->queue->next_try = time() + $this->queue->try_times * 60;
                Redis::pool()->lPush(QueueStatus::DELAY->key(), $this->queue->toArray());
                $timerId = Timer::after($this->queue->try_times * 60 * 1000, function () {
                    Redis::pool()->rPop(QueueStatus::DELAY->key());
                    if (!$this->reAdd()) {
                        Log::instance()->error('重新加入队列失败:' . JsonHelper::toJson($this->queue->toArray()));
                    }
                });
                return (bool)$timerId;
            }
        } else {
            $this->queue->remark = 'SUCCESS';
            $this->queue->finished = time();
            $this->queue->status = QueueStatus::FINISHED->get();
        }
        return (bool)Redis::pool()->lPush(QueueStatus::matchKey($this->queue->status) . '_' . Date::today('Y-m-d'), $this->queue->toArray());
    }

    /**
     * @return bool|int
     */
    protected function reAdd(): bool|int {
        return Redis::pool()->lPush(QueueStatus::IN->key(), $this->queue->toArray());
    }

    /**
     * @param $data
     * @param int $retry
     * @param int $tryLimit
     * @return int|bool
     */
    public static function add($data, int $retry = STATUS_ON, int $tryLimit = 3): int|bool {
        $queue = QueueStruct::factory();
        $queue->id = Sn::create_uuid();
        $queue->handler = static::class;
        $queue->data = $data;
        $queue->created = time();
        $queue->retry = $retry;
        $queue->try_limit = $retry == STATUS_OFF ? 1 : $tryLimit;
        $queue->try_times = 0;
        $queue->updated = 0;
        $queue->finished = 0;
        $queue->status = QueueStatus::IN->get();
        return Redis::pool()->lPush(QueueStatus::IN->key(), $queue->toArray());
    }


}