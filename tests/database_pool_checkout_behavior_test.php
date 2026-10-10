<?php
declare(strict_types=1);

namespace Scf\Core {
    // Isolate framework shared tables; still assert that every probe balances its I/O window.
    class InflightCounter {
        public static int $mysql = 0;
        public static function beginMysql(): void { self::$mysql++; }
        public static function endMysql(): void { self::$mysql--; }
    }
}

namespace {
    require dirname(__DIR__) . '/vendor/autoload.php';
    require dirname(__DIR__) . '/src/Database/DriverTrait.php';
    require dirname(__DIR__) . '/src/Database/Driver.php';
    require dirname(__DIR__) . '/src/Database/Pool/IdleConnectionReaper.php';
    require dirname(__DIR__) . '/src/Database/Pool/ConnectionPool.php';
    require dirname(__DIR__) . '/src/Database/Pool/Dialer.php';

    function checkPool(bool $condition, string $message): void {
        if (!$condition) throw new RuntimeException($message);
    }

    class ProbePdo extends PDO {
        public bool $disconnected = false;
        public int $probes = 0;
        public function __construct() { parent::__construct('sqlite::memory:'); }
        public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false {
            $this->probes++;
            if ($this->disconnected) throw new PDOException('SQLSTATE[HY000] [2006] MySQL server has gone away');
            return parent::query($query);
        }
    }

    class ProbeDriver extends \Scf\Database\Driver {
        public function __construct(public ProbePdo $handle) { $this->pdo = $handle; }
    }

    class ProbeDialer implements \Mix\ObjectPool\DialerInterface {
        public array $drivers = [];
        public bool $dead = false;
        public bool $failDial = false;
        public function dial(): object {
            if ($this->failDial) throw new PDOException('Connection refused');
            $pdo = new ProbePdo();
            $pdo->disconnected = $this->dead;
            return $this->drivers[] = new ProbeDriver($pdo);
        }
    }

    \Swoole\Coroutine\run(function () use ($argv): void {
        if (($argv[1] ?? '') === '--tcp') {
            $dialer = new \Scf\Database\Pool\Dialer('mysql:host=127.0.0.1;port=' . (int)$argv[2] . ';charset=utf8mb4', 'fixture', 'fixture');
            $pool = new \Scf\Database\Pool\ConnectionPool($dialer, 1, 1, 0, 0.02);
            $pool->setPingInterval(1);
            $old = $pool->borrow();
            $stmt = $old->instance()->query('SELECT 7');
            checkPool((int)$stmt->fetchColumn() === 7, 'Real PDO initial query failed');
            $stmt = null;
            $old->__return();
            $old->lastPingTime = time() - 2;
            $new = $pool->borrow();
            checkPool($new !== $old, 'Disconnected real PDO reached the business caller');
            checkPool($old->instance() === null, 'Disconnected real PDO was not closed');
            checkPool((int)$new->instance()->query('SELECT 99')->fetchColumn() === 99, 'Business query did not use the replacement connection');
            checkPool($pool->stats() === ['total' => 1, 'idle' => 0, 'active' => 1], 'TCP replacement leaked a pool slot');
            $new->__return();
            checkPool(\Scf\Core\InflightCounter::$mysql === 0, 'TCP probe leaked inflight');
            echo "PASS real PDO/MySQL protocol: server closed idle socket, stale handle replaced before business SQL\n";
            return;
        }

        $dialer = new ProbeDialer();
        $pool = new \Scf\Database\Pool\ConnectionPool($dialer, 1, 1, 0, 0.01);
        $pool->setPingInterval(1);
        $old = $pool->borrow();
        $old->__return();
        $old->handle->disconnected = true;
        $old->lastPingTime = time() - 2;
        $new = $pool->borrow();
        checkPool($new !== $old && $old->instance() === null, 'Dead reused PDO reached business SQL');
        checkPool(count($dialer->drivers) === 2 && $new->handle->probes === 1, 'Recovery must validate exactly one replacement');
        checkPool($pool->stats()['total'] === 1, 'Dead handle leaked pool capacity');
        $new->__return();
        $again = $pool->borrow();
        checkPool($again === $new && $again->handle->probes === 1, 'Recent successful probe should be reused');
        try {
            $pool->borrow();
            throw new RuntimeException('Pool wait timeout opened an untracked extra connection');
        } catch (\Mix\ObjectPool\Exception\WaitTimeoutException $expected) {}
        checkPool(count($dialer->drivers) === 2, 'Pool exceeded max_open');
        $again->lastUseTime = time() - 100;
        $again->__return();
        $pool->setIdleTimeout(30);
        checkPool($pool->borrow() === $again, 'Completed long operation must start a fresh idle interval');
        $again->__return();
        $again->lastUseTime = time() - 31;
        $replacement = $pool->borrow();
        checkPool($replacement !== $again && $again->instance() === null, 'Idle timeout did not retire the old PDO');
        $replacement->__return();

        $badDialer = new ProbeDialer();
        $badPool = new \Scf\Database\Pool\ConnectionPool($badDialer, 1, 1, 0, 0.01);
        $badPool->setPingInterval(1);
        $bad = $badPool->borrow();
        $bad->__return();
        $bad->handle->disconnected = true;
        $bad->lastPingTime = time() - 2;
        $badDialer->dead = true;
        try {
            $badPool->borrow();
            throw new RuntimeException('Continuing outage was swallowed');
        } catch (PDOException $expected) {}
        checkPool(count($badDialer->drivers) === 2, 'Persistent failure retried more than once');
        checkPool($badPool->stats()['total'] === 0, 'Persistent failure leaked active handles');
        $badDialer->dead = false;
        $badDialer->failDial = true;
        try { $badPool->borrow(); throw new RuntimeException('Dial failure swallowed'); } catch (PDOException $expected) {}
        checkPool($badPool->stats()['total'] === 0, 'Dial failure leaked its reservation');

        $ageDialer = new ProbeDialer();
        $agePool = new \Scf\Database\Pool\ConnectionPool($ageDialer, 2, 2, 60, 0.01);
        $expired = $agePool->borrow();
        $inUse = $agePool->borrow();
        $expired->__return();
        $expired->createTime = time() - 61;
        $fresh = $agePool->borrow();
        checkPool($fresh !== $expired && $expired->instance() === null, 'Lifetime expiry did not close the replaced PDO');
        checkPool($inUse->instance() !== null && $agePool->stats()['active'] === 2, 'Replacing an idle PDO disturbed an in-use connection');
        $fresh->__return();
        $inUse->__return();

        $legacyDialer = new ProbeDialer();
        $legacyPool = new \Scf\Database\Pool\ConnectionPool($legacyDialer, 1, 1, 0, 0.01);
        $legacy = $legacyPool->borrow();
        $legacy->__return();
        $legacy->lastPingTime = time() - 100;
        checkPool($legacyPool->borrow() === $legacy && $legacy->handle->probes === 0, 'Disabled probing changed legacy behavior');
        checkPool(\Scf\Core\InflightCounter::$mysql === 0, 'Failed probe leaked inflight');
        echo "PASS checkout recovery, idle/lifetime expiry, in-use isolation, probe interval, max_open, failed replacement/dial and balanced inflight\n";
    });
}
