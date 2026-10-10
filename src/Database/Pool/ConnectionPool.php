<?php

namespace Scf\Database\Pool;

use Mix\ObjectPool\AbstractObjectPool;
use Mix\ObjectPool\Exception\WaitTimeoutException;
use PDOException;
use Scf\Database\Driver;
use Scf\Database\Logger\PdoLogger;
use Swoole\Coroutine;
use Throwable;
use WeakMap;

/**
 * 为 PDO 与 Mix Redis 驱动管理独占借还，在借用边界淘汰过期连接。
 *
 * PDO 探测只发生在业务 SQL/事务开始前；失败最多替换一次，不重放业务操作。
 * 空闲回收只操作 Channel 中已归还的会话，调度及退出边界由 worker 管理。
 */
class ConnectionPool extends AbstractObjectPool {
    protected int $autoPingInterval = 0;
    protected int $idleIimeout = 0;
    protected int $id = 0;
    protected PdoLogger $logger;
    /** @var array<string, int> 与 Channel 的 FIFO 顺序一致，只保存归还时间。 */
    protected array $idleSince = [];
    /** @var ?WeakMap<object, bool> 关闭后交给等待者的容量标记，不依赖驱动将 instance 置空。 */
    private ?WeakMap $retired = null;


    /**
     * 借用连接，交付前处理最大寿命、空闲期限及到期的可用性探测。
     *
     * @return Driver|\Mix\Redis\Driver
     * @throws WaitTimeoutException
     * @throws PDOException 建连失败或替换后的连接仍无法通过探测。
     * @throws Throwable 驱动关闭或建连失败。
     */
    public function borrow(): object {
        //Console::info("连接池数量:" . $this->getTotalNumber() . ',最大数量:' . $this->maxOpen);
        /** @var Driver|\Mix\Redis\Driver $driver */
        if ($this->getIdleNumber() > 0 || ($this->maxOpen && $this->getTotalNumber() >= $this->maxOpen)) {
            // 队列有连接，从队列取
            // 达到最大连接数，从队列取
            // 等待失败必须返回调用方，不能绕过 maxOpen 创建未登记连接。
            $driver = $this->pop();
        } else {
            // 创建连接
            $driver = $this->createConnection();
        }
        // 登记, 队列中出来的也需要登记，因为有可能是 discard 中创建的新连接
        $id = spl_object_hash($driver);
        $this->actives[$id] = ''; // 不可保存外部连接的引用，否则导致外部连接不析构
        $now = time();
        $lifetimeExpired = $this->maxLifetime && $driver->createTime + $this->maxLifetime <= $now;
        $idleExpired = $this->idleIimeout > 0 && $driver->lastUseTime + $this->idleIimeout <= $now;
        $retired = $this->retired !== null && isset($this->retired[$driver]);
        if ($retired || $driver->instance() === null || $lifetimeExpired || $idleExpired) {
            $this->retireBorrowed($driver);
            // 直接用新连接替换本次借用，避免递归遍历整池的旧连接。
            $driver = $this->createConnection();
            $this->actives[spl_object_hash($driver)] = '';
        } elseif ($driver instanceof Driver && $this->autoPingInterval > 0 && $driver->lastPingTime + $this->autoPingInterval <= $now) {
            try {
                $driver->ping();
            } catch (PDOException $exception) {
                $this->retireBorrowed($driver);
                $driver = $this->createConnection();
                $this->actives[spl_object_hash($driver)] = '';
                try {
                    // 仅重试无业务副作用的探测；持续故障必须向上传递。
                    $driver->ping();
                } catch (Throwable $exception) {
                    $this->retireBorrowed($driver);
                    throw $exception;
                }
            } catch (Throwable $exception) {
                $this->retireBorrowed($driver);
                throw $exception;
            }
            $driver->lastPingTime = time();
            $driver->pingTimes++;
        }
        $driver->lastUseTime = time();
        // 返回
        return $driver;
    }

    /**
     * 归还连接，从实际使用结束时起计算空闲时间。
     *
     * @param object $connection 当前协程独占的 Driver。
     * @return bool 是否成功归还到池中。
     */
    public function return(object $connection): bool {
        if (!isset($this->actives[spl_object_hash($connection)])) {
            return false;
        }
        $connection->lastUseTime = time();
        return parent::return($connection);
    }

    /**
     * 无等待归还到 FIFO；队列已满时释放多余连接，避免归还阻塞五秒。
     *
     * @param object $connection 已结束业务使用的 Driver。
     * @return bool 是否留在池内（也可能立即交付等待协程）。
     */
    protected function push(object $connection): bool {
        if (Coroutine::getCid() < 0 || $this->queue->isFull()) {
            $this->actives[spl_object_hash($connection)] = '';
            $this->retireBorrowed($connection, true);
            return false;
        }
        $id = spl_object_hash($connection);
        // Channel::push 会立即唤醒消费者，必须在交接前登记，借出端负责删除。
        $this->idleSince[$id] = $connection->lastUseTime;
        $returned = $this->queue->push($connection, 0.001);
        if (!$returned) {
            unset($this->idleSince[$id]);
            $this->actives[$id] = '';
            $this->retireBorrowed($connection, true);
        }
        IdleConnectionReaper::watch($this);
        return $returned;
    }

    /**
     * 沿用原等待约束，在交付前移除空闲标记。
     * @return object 已从 FIFO 移出的 Driver。
     * @throws WaitTimeoutException
     */
    protected function pop(): object {
        $connection = parent::pop();
        unset($this->idleSince[spl_object_hash($connection)]);
        if (!$this->idleSince) {
            IdleConnectionReaper::forget($this);
        }
        return $connection;
    }

    /**
     * 返回队首连接的空闲期限，不扫描连接、不发 SQL。
     * @return ?int 最早到期秒数；关闭回收或空池时为 null。
     */
    public function nextIdleDeadline(): ?int {
        if ($this->idleIimeout <= 0 || !$this->idleSince) {
            return null;
        }
        return reset($this->idleSince) + $this->idleIimeout;
    }

    /**
     * 分批关闭到期且已归还的连接，保留在用会话及未到期队列顺序。
     *
     * @param int $now 本轮检查秒数。
     * @param int $limit 本 worker 剩余回收额度。
     * @param callable $mayRun 每次关闭前确认 worker 尚未退出/排空。
     * @return int 实际关闭的连接数。
     */
    public function reapIdleConnections(int $now, int $limit, callable $mayRun): int {
        $closed = 0;
        while ($closed < $limit && ($deadline = $this->nextIdleDeadline()) !== null && $deadline <= $now) {
            // 不和业务消费者抢连接。pop 的零超时在 Swoole 中仍会等待，不能当作 tryPop。
            $stats = $this->queue->stats();
            if ($stats['queue_num'] <= 0 || $stats['consumer_num'] > 0 || !$mayRun()) {
                break;
            }
            /** @var Driver|\Mix\Redis\Driver $driver */
            $driver = $this->pop();
            $this->actives[spl_object_hash($driver)] = '';
            $this->retireBorrowed($driver, true);
            $closed++;
        }
        return $closed;
    }

    /**
     * 关闭本次借用的旧会话并释放容量，不主动建立替代连接。
     *
     * @param Driver|\Mix\Redis\Driver $driver 已从空闲队列移出并登记为 active 的连接。
     * @param bool $wakeWaiters 归还/回收释放空槽时唤醒等待者；本次借用替换时保留槽位。
     * @return void
     * @throws Throwable 驱动关闭失败；容量仍会释放。
     */
    protected function retireBorrowed(object $driver, bool $wakeWaiters = false): void {
        try {
            // 关闭可能挂起；在完成前保留 active，避免其他协程抢占容量。
            // Redis close() 保留底层对象，已交接的关闭标记不能再次关闭。
            if (($this->retired === null || !isset($this->retired[$driver])) && $driver->instance() !== null) {
                $driver->close();
            }
        } finally {
            unset($this->actives[spl_object_hash($driver)]);
            if ($this->retired !== null) {
                unset($this->retired[$driver]);
            }
            // 关闭若挂起，期间可能有人因 maxOpen 等待。交付关闭标记让借出端
            // 按正常容量约束建连，不能让 wait_timeout=0 的消费者永久悬挂。
            if ($wakeWaiters && Coroutine::getCid() >= 0 && $this->queue->stats()['consumer_num'] > 0) {
                // Channel push 会立即恢复消费者，必须先标记；弱键不延长驱动存活期。
                $this->retired ??= new WeakMap();
                $this->retired[$driver] = true;
                $this->queue->push($driver, 0.001);
            }
        }
    }

    /**
     * 丢弃连接
     * @param object $connection
     * @return bool
     */
    public function discard(object $connection): bool {
        $id = spl_object_hash($connection);
        // 判断是否已丢弃
        if (!isset($this->actives[$id])) {
            return false;
        }
        // 移除登记
        unset($this->actives[$id]); // 注意：必须是先减 actives，否则会 maxActive - maxIdle <= 1 时会阻塞
        // 入列一个新连接替代丢弃的连接
        //Console::warning("#" . $this->id . " 连接:" . $id . "丢弃成功");
        return $this->push($this->createConnection());
    }

    /**
     * 兼容旧的维护入口，改为加入 worker 共用的到期调度。
     * @return int 固定为 0，连接池不再拥有可单独清除的定时器。
     * @deprecated 归还连接时自动登记，由 worker 生命周期管理维护。
     */
    public function idleCheck(): int {
        IdleConnectionReaper::watch($this);
        return 0;
    }

    /**
     * 创建连接
     * @return object
     */
    protected function createConnection(): object {
        //连接创建时会挂起当前协程，导致 actives 未增加，因此需先 actives++ 连接创建成功后 actives--
        $closure = function () {
            /** @var Driver|\Mix\Redis\Driver $connection */
            $connection = $this->dialer->dial();
            $connection->pool = $this;
            $connection->createTime = time();
            $connection->lastUseTime = time();
            $connection->lastPingTime = time();
            $connection->pingTimes = 0;
            $connection->id = spl_object_hash($connection);
            return $connection;
        };
        $id = spl_object_hash($closure);
        $this->actives[$id] = '';
        try {
            $connection = call_user_func($closure);
        } finally {
            unset($this->actives[$id]);
        }
        return $connection;
    }

    public function setPingInterval($sec): void {
        $this->autoPingInterval = $sec;
    }

    public function setId($id): void {
        $this->id = $id;
    }

    public function setIdleTimeout($sec): void {
        $this->idleIimeout = max(0, (int)$sec);
        IdleConnectionReaper::watch($this);
    }

    public function setLogger($logger): void {
        $this->logger = $logger;
    }
}
