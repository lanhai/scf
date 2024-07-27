<?php

namespace Scf\Database\Pool;

use Mix\ObjectPool\AbstractObjectPool;
use Mix\ObjectPool\Exception\WaitTimeoutException;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Database\Connection;
use Scf\Database\Driver;
use Scf\Database\DriverTrait;
use Scf\Database\Logger\PdoLogger;
use Swoole\Timer;
use Throwable;

/**
 * Class ConnectionPool
 * @package Mix\Database\Pool
 * @author liu,jian <coder.keda@gmail.com>
 */
class ConnectionPool extends AbstractObjectPool {
    protected int $autoPingInterval = 0;
    protected int $idleIimeout = 0;
    protected int $id = 0;
    protected PdoLogger $logger;
    protected int $idleCheckTimer = 0;


    /**
     * 借用连接
     * @return Driver
     * @throws WaitTimeoutException
     */
    public function borrow(): object {
        //Console::info("连接池数量:" . $this->getTotalNumber() . ',最大数量:' . $this->maxOpen);
        /** @var DriverTrait $driver */
        if ($this->getIdleNumber() > 0 || ($this->maxOpen && $this->getTotalNumber() >= $this->maxOpen)) {
            // 队列有连接，从队列取
            // 达到最大连接数，从队列取
            try {
                $driver = $this->pop();
            } catch (Throwable $exception) {
                Log::instance()->error("从连接池取出连接错误:" . $exception->getMessage());
                //创建一个新连接
                return $this->dialer->dial();
            }
        } else {
            // 创建连接
            $driver = $this->createConnection();
        }
        // 登记, 队列中出来的也需要登记，因为有可能是 discard 中创建的新连接
        $id = spl_object_hash($driver);
        $this->actives[$id] = ''; // 不可保存外部连接的引用，否则导致外部连接不析构
        // 检查最大生命周期
        if ($this->maxLifetime && $driver->createTime + $this->maxLifetime <= time()) {
            $this->discard($driver);
            return $this->borrow();
        }
        $driver->lastUseTime = time();
        // 返回
        return $driver;
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
     * 空闲连接回收
     * @return int
     */
    public function idleCheck(): int {
        return Timer::tick($this->autoPingInterval * 1000, function () {
            //Console::log('#' . $this->id . ' 闲置检查,当前连接池数量:' . PdoPoolTable::instance()->count());
            /** @var Driver $driver */
            $size = $this->getIdleNumber();
            while ($size > 0) {
                $size--;
                try {
                    $driver = $this->pop();
                    //Console::info("#" . $driver->id . " 第" . $driver->pingTimes . "次ping,创建时间:" . date('m-d H:i:s', $driver->createTime) . ",最近使用:" . (time() - $driver->lastUseTime) . "秒前@" . date('m-d H:i:s', $driver->lastUseTime));
                    if ($this->idleIimeout > 0 && time() - $driver->lastUseTime >= $this->idleIimeout) {
                        $this->actives[$driver->id] = '';
                        $this->discard($driver);
                    } else {
                        $driver->pingTimes++;
                        $driver->lastPingTime = time();
                        $this->actives[$driver->id] = '';
                        $conn = new Connection($driver, $this->logger);
                        $conn->exec("select 1");

                        //$this->push($driver);
                    }
                } catch (Throwable $exception) {
                    Console::error("从连接池取出连接错误:" . $exception->getMessage());
                }
            }
        });
    }

    /**
     * 销毁
     */
//    public function __destruct() {
//        if (PdoPoolTable::instance()->get($this->id, 'id')) {
//            $timerId = PdoPoolTable::instance()->get($this->id, 'timer_id');
//            Timer::clear($timerId);
//            PdoPoolTable::instance()->delete($this->id);
//            Counter::instance()->decr(Mysql::PDO_POOL_ID_KEY);
//            Console::error("#" . $this->id . " 连接池销毁,timerId:" . $timerId);
//        }
//    }

    /**
     * 创建连接
     * @return object
     */
    protected function createConnection(): object {
        //连接创建时会挂起当前协程，导致 actives 未增加，因此需先 actives++ 连接创建成功后 actives--
        $closure = function () {
            /** @var DriverTrait $connection */
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
        $this->idleIimeout = $sec;
    }

    public function setLogger($logger): void {
        $this->logger = $logger;
    }
}
