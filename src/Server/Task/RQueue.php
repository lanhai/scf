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
use Swoole\Coroutine\Channel;
use Swoole\Process;
use Swoole\Timer;
use Throwable;

class RQueue {
    use Singleton;

    protected int $managerId = 0;
    protected bool $shouldExit = false;
    protected ?Channel $exitChannel = null;

    /**
     * 为当前 RedisQueue 执行进程准备退出同步通道。
     *
     * 队列消费循环本身由 Timer 驱动，主协程不应该再靠 sleep 轮询 shouldExit。
     * 这里为每次 watch 生命周期创建一个单元素 Channel，用来把“该退出了”的
     * 信号从 timer/coroutine 分支传回主协程，避免 all coroutines asleep deadlock。
     *
     * @return void
     */
    protected function prepareExitChannel(): void {
        $this->shouldExit = false;
        $this->exitChannel = new Channel(1);
    }

    /**
     * 阻塞等待 RedisQueue 执行进程的退出信号。
     *
     * @return void
     */
    protected function waitForExitSignal(): void {
        if ($this->exitChannel instanceof Channel) {
            $this->exitChannel->pop();
        }
    }

    /**
     * 显式结束 RedisQueue 执行进程的协程运行时。
     *
     * 队列消费子进程是 enable_coroutine=true 的 Swoole\Process，内部依赖 Timer 和
     * Coroutine 驱动执行。若只把 shouldExit 置为 true 然后直接返回，Swoole 仍可能在
     * PHP rshutdown 阶段兜底执行 Event::wait()，从而打印 deprecated warning。
     *
     * @return void
     */
    protected function shutdownRuntime(): void {
        Timer::clearAll();
        $this->shouldExit = true;
        if ($this->exitChannel instanceof Channel) {
            $this->exitChannel->push(true, 0.001);
        }
    }

    /**
     * 启动 RedisQueue 真正执行队列消费的子进程。
     *
     * RedisQueue manager 负责生命周期编排，这里只负责把执行进程拉起并返回句柄，
     * 不在内部阻塞等待退出。这样 manager 进程在队列子进程存活期间仍然可以继续
     * 处理 upgrade / shutdown 指令，避免二次重启时因为内部 wait() 卡住整条控制链。
     *
     * @return Process|null 成功时返回已启动的队列子进程，应用未就绪或启动失败时返回 null
     */
    public static function startProcess(): ?Process {
        $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
        if (!App::isReady()) {
            sleep(1);
            return null;
        }
        $process = new Process(function () use ($managerId) {
            App::mount();
            $pool = Redis::pool();
            if ($pool instanceof NullPool) {
                Console::warning("【RedisQueue】#{$managerId}Redis服务不可用(" . $pool->getError() . "),队列服务未启动");
            } else {
                $config = Config::server();
                $memoryLimit = (int)($config['redis_queue_memory_limit'] ?? max((int)($config['worker_memory_limit'] ?? 256), 1024));
                @ini_set('memory_limit', $memoryLimit . 'M');
                MemoryMonitor::start('redis:queue');
                // Swoole 5.1+ 不再推荐依赖“创建协程后由 rshutdown 隐式 Event::wait() 收尾”。
                // RedisQueue 执行子进程在这里显式开启一次 coroutine runtime，让 Timer、Channel
                // 与队列消费协程都在同一个可控生命周期里结束，避免进程退出时刷 deprecated warning。
                Coroutine\run(function () use ($config): void {
                    self::instance()->prepareExitChannel();
                    self::instance()->watch($config['redis_queue_mc'] ?? 32);
                    self::instance()->waitForExitSignal();
                });
                MemoryMonitor::stop();
                return;
            }
        }, false, 0, false);
        $pid = $process->start();
        if ($pid <= 0) {
            Console::error("【RedisQueue】#{$managerId} 队列管理进程启动失败");
            return null;
        }
        Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_WORKER_PID, (int)$pid);
        if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
            Console::info("【RedisQueue】#{$managerId} 队列管理进程已创建,PID:{$pid}");
        }
        File::write(SERVER_QUEUE_MANAGER_PID_FILE, $pid);
        return $process;
    }

    public static function startByWorker(): void {
        $pool = Redis::pool();
        if ($pool instanceof NullPool) {
            Console::warning("【RedisQueue】Redis服务不可用,队列管理未启动");
        } else {
            $config = Config::server();
            $memoryLimit = (int)($config['redis_queue_memory_limit'] ?? max((int)($config['worker_memory_limit'] ?? 256), 1024));
            @ini_set('memory_limit', $memoryLimit . 'M');
            self::instance()->watch($config['redis_queue_mc'] ?? 32);
        }
    }

    /**
     * 监听队列任务
     * @param int $mc
     * @return int
     */
    public function watch(int $mc = 32): int {
        $this->shouldExit = false;
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
                if ((int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESSING) ?: 0) > 0) {
                    Timer::after(200, function () use ($mc) {
                        $this->loop($mc);
                    });
                    return;
                }
                $this->shutdownRuntime();
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

    public function shouldExit(): bool {
        return $this->shouldExit;
    }

    /**
     * 取出一个待执行任务并执行,待执行任务标识为:key+0
     * @return bool
     */
    public function pop(): bool {
        if ($queue = Redis::pool()->rPop(QueueStatus::IN->key())) {
            $queue = QueueStruct::factory($queue);
            Counter::instance()->incr(Key::COUNTER_REDIS_QUEUE_PROCESSING);
            try {
                return call_user_func('\\' . $queue->handler . '::start', $queue);
            } catch (Throwable $e) {
                Log::instance()->setModule('RQueue')->error($e->getMessage());
            } finally {
                Counter::instance()->decr(Key::COUNTER_REDIS_QUEUE_PROCESSING);
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
