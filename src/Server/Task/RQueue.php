<?php

namespace Scf\Server\Task;

use Scf\Cache\Redis;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Core\Traits\Singleton;
use Scf\Database\Exception\NullPool;
use Scf\Service\Enum\QueueStatus;
use Scf\Service\Struct\QueueStruct;
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Throwable;

class RQueue {
    use Singleton;

    protected int $managerId = 0;

    public static function startProcess(): void {
        $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
        if (!App::isReady()) {
            sleep(1);
            return;
        }
        $process = new Process(function () use ($managerId) {
            App::mount();
            register_shutdown_function(function () use ($managerId) {
                $error = error_get_last();
                if ($error && in_array($error['type'], [E_ERROR, E_CORE_ERROR, E_COMPILE_ERROR, E_PARSE])) {
                    Console::error("【RedisQueue】#{$managerId} 致命错误: {$error['message']} ({$error['file']}:{$error['line']})");
                    // 确保所有定时器停止
                    Timer::clearAll();
                    // 主动退出子进程，让外部管理进程重新拉起
                    Process::kill(posix_getpid(), SIGTERM);
                }
            });
            $pool = Redis::pool();
            if ($pool instanceof NullPool) {
                Console::warning("【RedisQueue】#{$managerId}Redis服务不可用(" . $pool->getError() . "),队列服务未启动");
            } else {
                $config = Config::server();
                MemoryMonitor::start('redis:queue');
                self::instance()->watch($config['redis_queue_mc'] ?? 32);
            }
        }, false, 0, true);
        $pid = $process->start();
        Console::info("【RedisQueue】#{$managerId} 队列管理进程已创建,PID:{$pid}");
        File::write(SERVER_QUEUE_MANAGER_PID_FILE, $pid);
        Process::wait();
        MemoryMonitor::stop();
    }

    public static function startByWorker(): void {
        $pool = Redis::pool();
        if ($pool instanceof NullPool) {
            Console::warning("【RedisQueue】Redis服务不可用,队列管理未启动");
        } else {
            $config = Config::server();
            self::instance()->watch($config['redis_queue_mc'] ?? 32);
        }
    }

    /**
     * 监听队列任务
     * @param int $mc
     * @return int
     */
    public function watch(int $mc = 32): int {
        $mc = min($mc, 32);
        //将待重试加入队列
        if ($retryCount = $this->count(2)) {
            for ($i = 0; $i < $retryCount; $i++) {
                Coroutine::create(function () {
                    $queue = Redis::pool()->rPop(QueueStatus::DELAY->key());
                    Redis::pool()->lPush(QueueStatus::IN->key(), $queue);
                });
            }
        }
        $this->managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
        Coroutine::create(function () use ($mc) {
            $this->loop($mc);
        });
        return $this->managerId;
    }

    protected function loop($mc): void {
        //每一秒读取一次队列列表
        Timer::after(1000, function () use ($mc) {
            $latestManagerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
            if ($this->managerId != $latestManagerId) {
                Timer::clearAll();
            } else {
                if ($count = $this->count()) {
                    $successed = 0;
//                    for ($i = 0; $i <= min($count, $mc); $i++) {
//                        if ($this->pop()) {
//                            $successed++;
//                        }
//                    }
                    Coroutine\parallel(min($count, $mc), function () use (&$successed) {
                        if ($this->pop()) {
                            $successed++;
                        }
                    });
                    Env::isDev() and Console::log('【RedisQueue】本次累计执行队列任务:' . min($count, $mc) . ',执行完成:' . $successed);
                }
                $latestUsageUpdated = Runtime::instance()->get("redis:queue.memory.usage.updated") ?: 0;
                if (time() - $latestUsageUpdated >= 5) {
                    $processName = "redis:queue";
                    MemoryMonitor::updateUsage($processName);
                    Runtime::instance()->set("redis:queue.memory.usage.updated", time());
                }
                $this->loop($mc);
            }
        });
    }

    /**
     * 取出一个待执行任务并执行,待执行任务标识为:key+0
     * @return bool
     */
    public function pop(): bool {
        if ($queue = Redis::pool()->rPop(QueueStatus::IN->key())) {
            $queue = QueueStruct::factory($queue);
            try {
                return call_user_func('\\' . $queue->handler . '::start', $queue);
            } catch (Throwable $e) {
                Log::instance()->setModule('RQueue')->error($e->getMessage());
            }
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
            Env::isDev() and Console::warning('【RedisQueue】查询队列任务错误:' . $err->getMessage());
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