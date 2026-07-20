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
use Scf\Util\Sn;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Process;
use Swoole\Timer;
use Throwable;

/**
 * RedisQueue 运行时与管理入口。
 *
 * 该类位于 RedisQueue 的执行层与 dashboard 运维层之间，一方面负责队列消费进程的
 * 生命周期编排，另一方面向管理面板提供列表读取、任务定位、人工重投等辅助能力。
 * 这里不会承接具体业务任务逻辑，只负责围绕 Redis 列表结构做统一访问。
 */
class RQueue {
    use Singleton;

    protected const WORKER_HEARTBEAT_INTERVAL_MS = 1000;
    protected const WORKER_START_PENDING_SECONDS = 5;

    /**
     * 手动重投时扫描历史队列的分片大小。
     */
    protected const FIND_SCAN_CHUNK_SIZE = 200;

    /**
     * 手动重投时并行扫描历史队列的最大协程数。
     */
    protected const FIND_SCAN_COROUTINE_LIMIT = 4;

    protected int $managerId = 0;
    protected string $managerGeneration = '';
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
    public static function startProcess(string $workerToken = '', string $managerGeneration = ''): ?Process {
        $managerId = Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS);
        $managerPid = (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0);
        $managerGeneration = trim($managerGeneration);
        if (!App::isReady()) {
            sleep(1);
            return null;
        }
        if (
            $managerPid <= 0
            || $managerGeneration === ''
            || !hash_equals(
                (string)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION) ?? ''),
                $managerGeneration
            )
        ) {
            return null;
        }
        $workerToken = $workerToken !== '' ? $workerToken : self::newWorkerToken();
        $lifecycleGuard = self::tryAcquireWorkerLifecycleGuard();
        if (!is_resource($lifecycleGuard)) {
            return null;
        }
        try {
            $workerLease = self::tryAcquireWorkerLeaseForStart($managerPid);
            if (!is_resource($workerLease)) {
                Console::warning("【RedisQueue】检测到现有消费进程仍持有租约，跳过重复拉起");
                return null;
            }
            // 先写入 starting 占位。另一个 manager 在 fork 与 PID 回填之间只能
            // 观察到 pending owner，不能创建第二个消费者。
            self::writeWorkerLockOwner($workerLease, 0, $workerToken, $managerPid);

            $process = new Process(function () use (
                $managerId,
                $managerGeneration,
                $workerToken,
                $workerLease
            ) {
                try {
                    if (!hash_equals(
                        (string)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION) ?? ''),
                        $managerGeneration
                    )) {
                        return;
                    }
                    App::mount();
                    $pool = Redis::pool();
                    if ($pool instanceof NullPool) {
                        Console::warning("【RedisQueue】#{$managerId}Redis服务不可用(" . $pool->getError() . "),队列服务未启动");
                    } else {
                        $config = Config::server();
                        $memoryLimit = (int)($config['redis_queue_memory_limit'] ?? max((int)($config['worker_memory_limit'] ?? 256), 1024));
                        @ini_set('memory_limit', $memoryLimit . 'M');
                        MemoryMonitor::start('redis:queue');
                        // 心跳由独立 Timer 驱动，不再借用内存采样时间判断 worker 身份。
                        // 正常的长队列任务即使尚未返回，也不会被 manager 误判为失效并重复拉起。
                        self::publishWorkerHeartbeat($workerToken, $managerGeneration);
                        Timer::tick(self::WORKER_HEARTBEAT_INTERVAL_MS, static function () use ($workerToken, $managerGeneration): void {
                            self::publishWorkerHeartbeat($workerToken, $managerGeneration);
                        });
                        Coroutine\run(function () use ($config, $managerGeneration): void {
                            self::instance()->prepareExitChannel();
                            self::instance()->watch(
                                (int)($config['redis_queue_mc'] ?? 32),
                                $managerGeneration
                            );
                            self::instance()->waitForExitSignal();
                        });
                        self::markWorkerHeartbeatStopped($workerToken, $managerGeneration);
                        MemoryMonitor::stop();
                    }
                } finally {
                    if (is_resource($workerLease)) {
                        // 只关闭当前进程的 lease fd。业务 handler 派生进程可能继续
                        // 继承旧 inode；manager 会在 owner PID 死亡后轮换固定路径，
                        // 不再被这个继承 fd 永久锁死。
                        @fclose($workerLease);
                    }
                }
            }, false, 0, false);
            $pid = (int)($process->start() ?: 0);
            if ($pid <= 0) {
                @flock($workerLease, LOCK_UN);
                @fclose($workerLease);
                Console::error("【RedisQueue】#{$managerId} 队列管理进程启动失败");
                return null;
            }
            self::writeWorkerLockOwner($workerLease, $pid, $workerToken, $managerPid);
            // fork 后父进程关闭自己的 fd；lease 由 worker（及可能的后代）持有。
            @fclose($workerLease);
            if (
                (int)(Runtime::instance()->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0) !== $managerPid
                || !hash_equals(
                    (string)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION) ?? ''),
                    $managerGeneration
                )
            ) {
                @Process::kill($pid, SIGTERM);
                Console::warning("【RedisQueue】manager 已换代，终止旧 manager 刚创建的消费进程:{$pid}");
                return null;
            }
            Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE, [
                'pid' => $pid,
                'token' => $workerToken,
                'legacy' => false,
                'manager_pid' => $managerPid,
                'manager_id' => (int)$managerId,
                'manager_generation' => $managerGeneration,
                'started_at' => time(),
                'heartbeat_at' => time(),
            ]);
            Runtime::instance()->set(Key::RUNTIME_REDIS_QUEUE_WORKER_PID, $pid);
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【RedisQueue】#{$managerId} 队列管理进程已创建,PID:{$pid}");
            }
            File::write(SERVER_QUEUE_MANAGER_PID_FILE, $pid);
            return $process;
        } finally {
            self::releaseWorkerLifecycleGuard($lifecycleGuard);
        }
    }

    /**
     * 在生命周期互斥锁内取得 worker lease。
     *
     * worker 与其 fork 后代可能共享同一个 open-file-description。若权威 owner
     * PID 已死亡但后代仍持锁，固定路径会被原子轮换到旧 inode；新 worker 在
     * 新 inode 上取得 lease，旧后代不再能永久阻塞恢复。
     *
     * 调用方必须已经持有 tryAcquireWorkerLifecycleGuard() 返回的互斥锁。
     *
     * @return resource|false
     */
    public static function tryAcquireWorkerLeaseForStart(int $managerPid) {
        $path = self::workerLockFile();
        $handle = @fopen($path, 'c+');
        if (!is_resource($handle)) {
            return false;
        }
        if (@flock($handle, LOCK_EX | LOCK_NB)) {
            @rewind($handle);
            @ftruncate($handle, 0);
            @fflush($handle);
            return $handle;
        }

        $owner = self::readWorkerLockOwnerFromHandle($handle);
        $ownerPid = max(0, (int)($owner['pid'] ?? 0));
        $ownerManagerPid = max(0, (int)($owner['manager_pid'] ?? 0));
        $startedAt = max(0, (int)($owner['started_at'] ?? 0));
        $ownerAlive = $ownerPid > 0 && @Process::kill($ownerPid, 0);
        $pending = $ownerPid <= 0
            && $startedAt > 0
            && (time() - $startedAt) <= self::WORKER_START_PENDING_SECONDS
            && (
                $ownerManagerPid <= 0
                || $ownerManagerPid === $managerPid
                || @Process::kill($ownerManagerPid, 0)
            );
        @fclose($handle);
        if ($ownerAlive || $pending) {
            return false;
        }

        $suffix = getmypid() . '.' . str_replace('.', '', uniqid('', true));
        $stalePath = $path . '.stale.' . $suffix;
        if (!@rename($path, $stalePath)) {
            return false;
        }
        // unlink 只移除旧 inode 的目录项；继承 fd 仍可自然关闭，但不再占用固定路径。
        @unlink($stalePath);
        $handle = @fopen($path, 'c+');
        if (!is_resource($handle)) {
            return false;
        }
        if (!@flock($handle, LOCK_EX | LOCK_NB)) {
            @fclose($handle);
            return false;
        }
        @rewind($handle);
        @ftruncate($handle, 0);
        @fflush($handle);
        return $handle;
    }

    protected static function readWorkerLockOwnerFromHandle($handle): array {
        if (!is_resource($handle)) {
            return [];
        }
        @rewind($handle);
        $payload = @fread($handle, 4096);
        $owner = is_string($payload) && $payload !== '' ? json_decode($payload, true) : null;
        return is_array($owner) ? $owner : [];
    }

    /**
     * 返回当前真正持有 worker 文件锁的身份。
     *
     * held=true 但 pid/token 为空表示锁正处在 fork/回填的极短窗口，或锁文件
     * 暂时不可读。调用方必须把它当作“已有 worker/正在启动”，禁止再次拉起。
     *
     * @return array{held?:bool,pid?:int,token?:string,manager_pid?:int,started_at?:int}
     */
    public static function workerLockOwner(): array {
        $handle = @fopen(self::workerLockFile(), 'c+');
        if (!is_resource($handle)) {
            // 无法验证时按“已有 worker”处理，安全地阻止重复消费者。
            return ['held' => true, 'pid' => 0, 'token' => ''];
        }
        $acquired = @flock($handle, LOCK_EX | LOCK_NB);
        if ($acquired) {
            @flock($handle, LOCK_UN);
            @fclose($handle);
            return [];
        }
        $owner = self::readWorkerLockOwnerFromHandle($handle);
        @fclose($handle);
        if (!$owner) {
            return ['held' => true, 'pid' => 0, 'token' => ''];
        }
        return [
            'held' => true,
            'pid' => max(0, (int)($owner['pid'] ?? 0)),
            'token' => (string)($owner['token'] ?? ''),
            'manager_pid' => max(0, (int)($owner['manager_pid'] ?? 0)),
            'started_at' => max(0, (int)($owner['started_at'] ?? 0)),
        ];
    }

    /**
     * 跨 manager 代际判断真实 worker 是否持有锁。
     *
     * 传入 pid/token 时必须与锁文件 owner 精确一致，不能再用“任意锁被占用”
     * 证明某个 Runtime PID 的身份，避免 PID 复用或旧状态误接管。
     */
    public static function workerLockIsHeld(int $expectedPid = 0, string $expectedToken = ''): bool {
        $owner = self::workerLockOwner();
        if (!(bool)($owner['held'] ?? false)) {
            return false;
        }
        if ($expectedPid > 0 && (int)($owner['pid'] ?? 0) !== $expectedPid) {
            return false;
        }
        if (
            $expectedToken !== ''
            && !hash_equals((string)($owner['token'] ?? ''), $expectedToken)
        ) {
            return false;
        }
        return true;
    }

    /**
     * 尝试取得 worker 生命周期互斥锁。
     *
     * 启动与 Runtime 清理都必须在同一把锁下完成，避免“刚确认无锁，另一个
     * manager 就启动 worker，而旧 manager 随后把新状态清掉”的 TOCTOU 竞态。
     *
     * @return resource|false
     */
    public static function tryAcquireWorkerLifecycleGuard() {
        $handle = @fopen(self::workerLifecycleLockFile(), 'c+');
        if (!is_resource($handle)) {
            return false;
        }
        if (!@flock($handle, LOCK_EX | LOCK_NB)) {
            @fclose($handle);
            return false;
        }
        return $handle;
    }

    /**
     * @param resource|false $handle
     */
    public static function releaseWorkerLifecycleGuard($handle): void {
        if (!is_resource($handle)) {
            return;
        }
        @flock($handle, LOCK_UN);
        @fclose($handle);
    }

    protected static function workerLockFile(): string {
        return SERVER_QUEUE_MANAGER_PID_FILE . '.worker.lock';
    }

    protected static function workerLifecycleLockFile(): string {
        return SERVER_QUEUE_MANAGER_PID_FILE . '.worker.lifecycle.lock';
    }

    protected static function newWorkerToken(): string {
        try {
            return bin2hex(random_bytes(16));
        } catch (Throwable) {
            return str_replace('.', '', uniqid('rq', true));
        }
    }

    /**
     * 将锁的持有者身份写入已经持锁的 fd。
     *
     * 元数据与锁使用同一个 fd 生命周期：worker 退出释放锁后，文件里即使还留有
     * 旧 JSON 也不会被采用，因为 workerLockOwner() 会先验证锁仍被真实持有。
     *
     * @param resource $handle
     */
    protected static function writeWorkerLockOwner(
        $handle,
        int $pid,
        string $workerToken,
        int $managerPid
    ): void {
        if (!is_resource($handle)) {
            return;
        }
        $payload = json_encode([
            'pid' => max(0, $pid),
            'token' => $workerToken,
            'manager_pid' => max(0, $managerPid),
            'started_at' => time(),
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if (!is_string($payload)) {
            return;
        }
        @rewind($handle);
        @ftruncate($handle, 0);
        @fwrite($handle, $payload);
        @fflush($handle);
    }

    protected static function publishWorkerHeartbeat(
        string $workerToken,
        string $managerGeneration
    ): void {
        $pid = getmypid() ?: 0;
        $runtime = Runtime::instance();
        $managerPid = (int)($runtime->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0);
        $state = (array)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE) ?? []);
        if (
            $pid <= 0
            || $managerPid <= 0
            || $managerGeneration === ''
            || !hash_equals(
                (string)($runtime->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION) ?? ''),
                $managerGeneration
            )
            || (int)($state['pid'] ?? 0) !== $pid
            || !hash_equals((string)($state['token'] ?? ''), $workerToken)
            || !hash_equals((string)($state['manager_generation'] ?? ''), $managerGeneration)
        ) {
            return;
        }
        $state['heartbeat_at'] = time();
        $state['manager_pid'] = $managerPid;
        $state['manager_id'] = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
        if ((int)($runtime->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0) === $managerPid) {
            $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE, $state);
        }
    }

    protected static function markWorkerHeartbeatStopped(
        string $workerToken,
        string $managerGeneration
    ): void {
        $pid = getmypid() ?: 0;
        $runtime = Runtime::instance();
        $managerPid = (int)($runtime->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0);
        $state = (array)($runtime->get(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE) ?? []);
        if (
            $managerPid <= 0
            || $managerGeneration === ''
            || (int)($state['pid'] ?? 0) !== $pid
            || !hash_equals((string)($state['token'] ?? ''), $workerToken)
            || !hash_equals((string)($state['manager_generation'] ?? ''), $managerGeneration)
        ) {
            return;
        }
        $state['heartbeat_at'] = 0;
        $state['manager_pid'] = $managerPid;
        $state['manager_id'] = (int)(Counter::instance()->get(Key::COUNTER_REDIS_QUEUE_PROCESS) ?: 0);
        if ((int)($runtime->get(Key::RUNTIME_REDIS_QUEUE_MANAGER_PID) ?? 0) === $managerPid) {
            $runtime->set(Key::RUNTIME_REDIS_QUEUE_WORKER_STATE, $state);
        }
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
    public function watch(int $mc = 32, string $managerGeneration = ''): int {
        $this->shouldExit = false;
        $mc = min($mc, 32);
        $this->managerGeneration = $managerGeneration;
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
            $latestManagerGeneration = (string)(
                Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION) ?? ''
            );
            $managerGenerationChanged = $this->managerGeneration !== ''
                && (
                    $latestManagerGeneration === ''
                    || !hash_equals($latestManagerGeneration, $this->managerGeneration)
                );
            if ($this->managerId != $latestManagerId || $managerGenerationChanged) {
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
        return Redis::pool()->lAll($this->key($status, $day));
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
        return Redis::pool()->lRange($this->key($status, $day), $start, $end);
    }

    /**
     * 统计队列任务
     * @param int $status
     * @param string|null $day
     * @return int
     */
    public function count(int $status = 0, string $day = null): int {
        try {
            return Redis::pool()->lLength($this->key($status, $day));
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
        return Redis::pool()->lRange($this->key($status, $day), 0 - $length);
    }

    /**
     * 解析指定状态在 Redis 中对应的实际列表 key。
     *
     * 运行中与待重试队列使用固定列表，完成/失败队列按日期分桶。
     * 管理端所有查询与重投都应通过这里拿 key，避免多处各自拼装。
     *
     * @param int $status 队列状态
     * @param string|null $day 完成/失败队列使用的日期分桶
     * @return string
     */
    public function key(int $status = 0, string $day = null): string {
        $day = $day ?: Date::today('Y-m-d');
        if (QueueStatus::IN->is($status) || QueueStatus::DELAY->is($status)) {
            return QueueStatus::matchKey($status);
        }
        return QueueStatus::matchKey($status) . '_' . $day;
    }

    /**
     * 在指定状态列表中定位单个队列任务。
     *
     * RedisQueue 历史记录并没有额外索引，dashboard 的手动重投需要先从
     * 状态列表中把原任务找出来，再复制为一条新的待执行任务。
     *
     * @param string $taskId 任务ID
     * @param int $status 当前任务所在状态
     * @param string|null $day 完成/失败队列使用的日期分桶
     * @return QueueStruct|null
     */
    public function find(string $taskId, int $status = 0, string $day = null): ?QueueStruct {
        $total = $this->count($status, $day);
        if ($total <= 0) {
            return null;
        }

        if (!$this->canUseCoroutineQuery() || $total <= self::FIND_SCAN_CHUNK_SIZE) {
            return $this->findSequentially($taskId, $status, $day);
        }

        $rangeQueue = new Channel((int)ceil($total / self::FIND_SCAN_CHUNK_SIZE));
        for ($start = 0; $start < $total; $start += self::FIND_SCAN_CHUNK_SIZE) {
            $rangeQueue->push([
                'start' => $start,
                'end' => min($total - 1, $start + self::FIND_SCAN_CHUNK_SIZE - 1),
            ]);
        }
        $rangeQueue->close();

        $found = null;
        $concurrency = min(self::FIND_SCAN_COROUTINE_LIMIT, (int)ceil($total / self::FIND_SCAN_CHUNK_SIZE));

        // 历史队列没有按 task_id 建索引，人工重投只能做列表扫描。
        // 在协程环境下按分片并发拉取 Redis 列表，可以显著缩短大列表定位耗时。
        Coroutine\parallel($concurrency, function () use ($rangeQueue, $taskId, $status, $day, &$found): void {
            while (true) {
                if ($found instanceof QueueStruct) {
                    return;
                }
                $range = $rangeQueue->pop();
                if ($range === false || !is_array($range)) {
                    return;
                }
                $items = $this->lRange((int)$range['start'], (int)$range['end'], $status, $day);
                foreach ((array)$items as $item) {
                    if (!is_array($item)) {
                        continue;
                    }
                    $queue = QueueStruct::factory($item);
                    if ($queue->id === $taskId) {
                        $found = $queue;
                        return;
                    }
                }
            }
        });

        return $found instanceof QueueStruct ? $found : null;
    }

    /**
     * 顺序扫描指定状态列表中的单个任务。
     *
     * 该路径既是普通 CLI 的兼容兜底，也覆盖小列表场景，避免协程调度开销大于收益。
     *
     * @param string $taskId 任务ID
     * @param int $status 当前任务所在状态
     * @param string|null $day 完成/失败队列使用的日期分桶
     * @return QueueStruct|null
     */
    protected function findSequentially(string $taskId, int $status = 0, string $day = null): ?QueueStruct {
        $items = $this->all($status, $day);
        if (!$items) {
            return null;
        }
        foreach ($items as $item) {
            if (!is_array($item)) {
                continue;
            }
            $queue = QueueStruct::factory($item);
            if ($queue->id === $taskId) {
                return $queue;
            }
        }
        return null;
    }

    /**
     * 当前是否处于适合做 Redis 列表并发扫描的协程上下文。
     *
     * @return bool
     */
    protected function canUseCoroutineQuery(): bool {
        return Coroutine::getCid() > 0;
    }

    /**
     * 将历史任务重新投递为一条新的待执行任务。
     *
     * 这里不会修改原来的完成/失败记录，而是复制任务基础参数并重置执行态字段，
     * 然后作为一条新的 IN 队列任务推回 Redis。这样既能人工补投，又能保留原始审计历史。
     *
     * @param string $taskId 需要重新投递的原任务ID
     * @param int $status 原任务所在状态，仅允许完成/失败队列
     * @param string|null $day 完成/失败队列使用的日期分桶
     * @return array
     * @throws \RuntimeException 任务不存在、状态不支持或重新入队失败时抛出
     */
    public function redeliver(string $taskId, int $status, string $day = null): array {
        if (!in_array($status, [QueueStatus::FINISHED->value, QueueStatus::FAILED->value], true)) {
            throw new \RuntimeException('当前状态不支持手动重新投递');
        }
        $queue = $this->find($taskId, $status, $day);
        if (!$queue) {
            throw new \RuntimeException('目标队列任务不存在或已被清理');
        }

        $newQueue = QueueStruct::factory($queue->toArray());
        $newQueue->id = Sn::create_uuid();
        $newQueue->created = time();
        $newQueue->updated = 0;
        $newQueue->finished = 0;
        $newQueue->status = QueueStatus::IN->value;
        $newQueue->try_times = 0;
        $newQueue->next_try = 0;
        $newQueue->start = 0;
        $newQueue->end = 0;
        $newQueue->duration = 0;
        $newQueue->remark = 'MANUAL_REDELIVERY';
        $newQueue->result = [];

        $count = Redis::pool()->lPush(QueueStatus::IN->key(), $newQueue->toArray());
        if ($count === false) {
            throw new \RuntimeException('重新投递失败，请检查 Redis 服务状态');
        }

        return [
            'source_id' => $taskId,
            'new_id' => $newQueue->id,
            'status' => QueueStatus::IN->value,
            'count' => $count,
        ];
    }
}
