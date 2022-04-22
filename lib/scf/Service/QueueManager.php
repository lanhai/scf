<?php

namespace Scf\Service;

use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Instance;
use Scf\Database\Redis;
use Scf\Server\Table\Counter;
use Scf\Service\Enum\QueueStatus;
use Scf\Service\Struct\QueueStruct;
use Scf\Util\Date;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Timer;

class QueueManager {
    use Instance;

    protected int $managerId = 0;

    /**
     * 监听队列任务
     * @param int $mc
     * @return int
     */
    public function watch(int $mc = 512): int {
        //将待重试加入队列
        if ($retryCount = $this->count(2)) {
            for ($i = 0; $i < $retryCount; $i++) {
                Coroutine::create(function () {
                    $queue = Redis::pool()->rPop(QueueStatus::DELAY->key());
                    Redis::pool()->lPush(QueueStatus::IN->key(), $queue);
                });
            }
        }
        Counter::instance()->incr('_queue_manager_id');
        $this->managerId = Counter::instance()->get('_queue_manager_id');
        Coroutine::create(function () use ($mc) {
            //每一秒读取一次队列列表
            Timer::tick(1000, function ($tickerId) use ($mc) {
                $latestManagerId = Counter::instance()->get('_queue_manager_id');
                if ($this->managerId != $latestManagerId) {
                    Timer::clear($tickerId);
                }
                if ($count = $this->count()) {
                    $successed = 0;
                    Coroutine\parallel(min($count, $mc), function () use (&$successed) {
                        if ($this->pop()) {
                            $successed++;
                        }
                    });
                    Console::log('本次累计执行队列任务:' . min($count, $mc) . ',执行成功:' . $successed);
                }
            });
        });
        return $this->managerId;
//        Coroutine::create(function () use ($mc) {
//            while (true) {
//                if ($this->managerId != Counter::instance()->get('_queue_manager_id')) {
//                    break;
//                }
//                if ($count = $this->count()) {
//                    $channel = new Channel($count);
//                    for ($i = 0; $i < min($count, $mc); $i++) {
//                        Coroutine::create(function () use ($channel) {
//                            $channel->push($this->pop());
//                        });
//                    }
//                    $successed = 0;
//                    for ($i = 0; $i < min($count, $mc); $i++) {
//                        $successed += $channel->pop() ? 1 : 0;
//                    }
//                    Console::log('本次累计执行队列任务:' . $count . ',执行成功:' . $successed);
//                }
//                Coroutine::sleep(0.01);
//            }
//            Coroutine::defer(function () {
//                Console::log(Color::yellow('销毁队列监听孤儿协程:' . $this->managerId));
//            });
//        });
    }

    /**
     * 取出一个待执行任务并执行,待执行任务标识为:key+0
     * @return bool
     */
    public function pop(): bool {
        if ($queue = Redis::pool()->rPop(QueueStatus::IN->key())) {
            $queue = QueueStruct::factory($queue);
            return call_user_func('\\' . $queue->handler . '::start', $queue);
            //return $handler->handler::start($queue);
        }
        return false;
    }

    /**
     * 所有队列任务
     * @param int $status
     * @param string|null $day
     * @return bool|array
     */
    public function all(int $status = 0, string $day = null): bool|array {
        $day = $day ?: Date::today('Y-m-d');
        return Redis::pool()->lAll(QueueStatus::IN->is($status) || $status == QueueStatus::DELAY->is($status) ? QueueStatus::matchKey($status) : QueueStatus::matchKey($status) . '_' . $day);
    }

    /**
     * 取出队列任务
     * @description 已弃用,替代为redis原生rPop
     * @param int $start
     * @param int $end
     * @param int $status
     * @param string|null $day
     * @return bool|array
     */
    public function lRange(int $start = 0, int $end = -1, int $status = 0, string $day = null): bool|array {
        $day = $day ?: Date::today('Y-m-d');
        return Redis::pool()->lRange(QueueStatus::IN->is($status) || $status == QueueStatus::DELAY->is($status) ? QueueStatus::matchKey($status) : QueueStatus::matchKey($status) . '_' . $day, $start, $end);
    }

    /**
     * 统计队列任务
     * @param int $status
     * @param string|null $day
     * @return int
     */
    public function count(int $status = 0, string $day = null): int {
        $day = $day ?: Date::today('Y-m-d');
        return Redis::pool()->lLength(QueueStatus::IN->is($status) || $status == QueueStatus::DELAY->is($status) ? QueueStatus::matchKey($status) : QueueStatus::matchKey($status) . '_' . $day);
    }

    /**
     * 获取队列列表
     * @param int $length
     * @param int $status
     * @param string|null $day
     * @return bool|array
     */
    public function load(int $length = 300, int $status = 0, string $day = null): bool|array {
        $day = $day ?: Date::today('Y-m-d');
        return Redis::pool()->lRange(QueueStatus::IN->is($status) || $status == QueueStatus::DELAY->is($status) ? QueueStatus::matchKey($status) : QueueStatus::matchKey($status) . '_' . $day, 0 - $length);
    }


}