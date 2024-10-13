<?php

namespace Scf\Server\Task;

use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Traits\Singleton;
use Scf\Database\Exception\NullPool;
use Scf\Cache\Redis;
use Scf\Mode\Web\App;
use Scf\Command\Color;
use Scf\Server\Env;
use Scf\Server\Table\Counter;
use Scf\Service\Enum\QueueStatus;
use Scf\Service\Struct\QueueStruct;
use Scf\Util\Date;
use Scf\Util\File;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Process;
use Swoole\Timer;
use Throwable;

class RQueue {
    use Singleton;

    protected int $managerId = 0;

    public static function startProcess(): int {
        if (!App::isReady()) {
            return 0;
        }
        $process = new Process(function () {
            App::mount();
            $pool = Redis::pool();
            if ($pool instanceof NullPool) {
                Console::warning("【Redis Queue】Redis服务不可用(" . $pool->getError() . "),队列服务未启动");
            } else {
                $config = Config::server();
                self::instance()->watch($config['redis_queue_mc'] ?? 512);
                Event::wait();
            }
        });
        $pid = $process->start();
        File::write(SERVER_QUEUE_MANAGER_PID_FILE, $pid);
        return $pid;
    }

    public static function startByWorker(): void {
        $pool = Redis::pool();
        if ($pool instanceof NullPool) {
            Console::warning("【Redis Queue】Redis服务不可用,队列管理未启动");
        } else {
            $config = Config::server();
            self::instance()->watch($config['redis_queue_mc'] ?? 512);
        }
    }

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
        $this->managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
        Coroutine::create(function () use ($mc) {
            //每一秒读取一次队列列表
            Timer::tick(1000, function ($tickerId) use ($mc) {
                $latestManagerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
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
                    Env::isDev() and Console::log('【Redis Queue】本次累计执行队列任务:' . min($count, $mc) . ',执行成功:' . $successed);
                }
            });
        });
        return $this->managerId;
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
        try {
            return Redis::pool()->lLength(QueueStatus::IN->is($status) || $status == QueueStatus::DELAY->is($status) ? QueueStatus::matchKey($status) : QueueStatus::matchKey($status) . '_' . $day);
        } catch (Throwable $err) {
            Env::isDev() and Console::warning('【Redis Queue】查询队列任务错误:' . $err->getMessage());
            return 0;
        }

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