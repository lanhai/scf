<?php
declare(strict_types=1);

namespace Scf\Core {
    // Keep the real Redis command wrapper; only isolate framework shared tables.
    class InflightCounter {
        public static int $redis = 0;
        public static function beginRedis(): void { self::$redis++; }
        public static function endRedis(): void { self::$redis--; }
    }
}

namespace {
    require dirname(__DIR__) . '/vendor/autoload.php';
    require dirname(__DIR__) . '/src/Database/Pool/IdleConnectionReaper.php';
    require dirname(__DIR__) . '/src/Database/Pool/ConnectionPool.php';
    require dirname(__DIR__) . '/src/Cache/Connection/InflightRedisConnection.php';
    require dirname(__DIR__) . '/src/Cache/Connection/RedisPool.php';

    use Mix\Redis\Driver;
    use Scf\Database\Pool\ConnectionPool;
    use Scf\Database\Pool\IdleConnectionReaper;
    use Swoole\Coroutine;

    function expectRedis(bool $ok, string $message): void {
        if (!$ok) throw new RuntimeException($message);
    }

    class InspectRedisPool extends \Scf\Cache\Connection\RedisPool {
        public function connection(): \Mix\Redis\Connection { return $this->borrow(); }
        public function objectPool(): ConnectionPool { return $this->pool; }
    }

    function driverOf(\Mix\Redis\Connection $connection): Driver {
        $property = new ReflectionProperty(\Mix\Redis\AbstractConnection::class, 'driver');
        $property->setAccessible(true);
        return $property->getValue($connection);
    }

    class ClosingRedisDriver extends Driver {
        public bool $yieldOnClose = false;
        public bool $failClose = false;
        public function close() {
            parent::close();
            if ($this->yieldOnClose) Coroutine::sleep(0.02);
            if ($this->failClose) throw new RedisException('Fixture close failed');
        }
    }

    class ClosingRedisDialer implements \Mix\ObjectPool\DialerInterface {
        public array $drivers = [];
        public function __construct(private int $port) {}
        public function dial(): object {
            return $this->drivers[] = new ClosingRedisDriver('127.0.0.1', $this->port, '', 0, 1, 0, 1);
        }
    }

    \Swoole\Coroutine\run(function () use ($argv): void {
        $port = (int)$argv[1];
        $client = new InspectRedisPool('127.0.0.1', $port, '', 0, 1, 0, 1);
        $client->start(1, 1, 600, 0.02);
        $loan = $client->connection();
        $old = driverOf($loan);
        expectRedis($loan->get('fixture') === 'value', 'Initial real Redis command failed');
        unset($loan);
        $old->createTime = time() - 601;
        try {
            expectRedis($client->get('fixture') === 'value', 'Lifetime replacement command failed');
        } catch (Throwable $error) {
            $stats = $client->objectPool()->stats();
            $blocked = false;
            try { $client->get('fixture'); }
            catch (\Mix\ObjectPool\Exception\WaitTimeoutException $timeout) { $blocked = true; }
            throw new RuntimeException($error->getMessage() . '; leaked stats=' . json_encode($stats)
                . '; subsequent checkout blocked=' . ($blocked ? 'yes' : 'no'), 0, $error);
        }
        expectRedis(!$old->instance()->isConnected(), 'Expired Redis connection remained open');
        expectRedis($client->objectPool()->stats() === ['total' => 1, 'idle' => 1, 'active' => 0],
            'Lifetime replacement leaked a slot');
        $loan = $client->connection();
        driverOf($loan)->lastPingTime = time() - 2;
        unset($loan);
        $client->objectPool()->setPingInterval(1);
        expectRedis($client->get('fixture') === 'value', 'PDO probe setting leaked into Redis command behavior');
        echo "PASS real RedisPool lifetime expiry, command result and capacity\n";

        $full = new InspectRedisPool('127.0.0.1', $port, '', 0, 1, 0, 1);
        $full->start(2, 1);
        $first = $full->connection();
        $second = $full->connection();
        $overflow = driverOf($second);
        unset($first);
        $started = microtime(true);
        unset($second); // Exercise the real Mix connection destructor -> shared pool return.
        expectRedis(microtime(true) - $started < 0.1, 'Full idle queue blocked Redis return');
        expectRedis(!$overflow->instance()->isConnected(), 'Overflow Redis was not closed');
        expectRedis($full->get('fixture') === 'value' && $full->objectPool()->stats()['active'] === 0,
            'Full queue lost capacity or damaged retained Redis');
        echo "PASS Redis destructor return to full idle queue\n";

        // Redis close() retains a non-null Redis object, unlike PDO close().
        // Force a yield while closing so a maxOpen waiter needs a capacity handoff.
        $dialer = new ClosingRedisDialer($port);
        $handoff = new ConnectionPool($dialer, 2, 1, 0, 0);
        $held = $handoff->borrow();
        $closing = $handoff->borrow();
        $held->__return();
        $closing->yieldOnClose = true;
        $done = new \Swoole\Coroutine\Channel(1);
        Coroutine::create(function () use ($closing, $done): void {
            $closing->__return();
            $done->push(true);
        });
        $held = $handoff->borrow();
        $replacement = $handoff->borrow();
        expectRedis($replacement !== $closing && $replacement->instance()->isConnected(),
            'Closed Redis handoff marker escaped as a usable driver');
        expectRedis($replacement->instance()->get('fixture') === 'value', 'Replacement Redis command failed');
        expectRedis(count($dialer->drivers) === 3 && $handoff->stats()['total'] === 2,
            'Handoff over-dialed or leaked maxOpen capacity');
        $done->pop(0.1);
        $replacement->__return();
        $held->__return();
        echo "PASS Redis wait_timeout=0 waiter receives a fresh driver after yielding close\n";

        $failure = new ConnectionPool(new ClosingRedisDialer($port), 1, 1, 600, 0.02);
        $broken = $failure->borrow();
        $broken->__return();
        $broken->createTime = time() - 601;
        $broken->failClose = true;
        $caught = false;
        try { $failure->borrow(); }
        catch (RedisException $expected) { $caught = $expected->getMessage() === 'Fixture close failed'; }
        expectRedis($caught && $failure->stats()['total'] === 0, 'Close exception retained an active reservation');
        $recovered = $failure->borrow();
        expectRedis($recovered->instance()->get('fixture') === 'value', 'Close exception permanently blocked the pool');
        $recovered->__return();
        echo "PASS Redis close exception releases capacity and later checkout recovers\n";

        $bad = $client->connection();
        try { $bad->__call('rawCommand', ['INVALID_FIXTURE_COMMAND']); }
        catch (RedisException $expected) { /* Real Mix exception/discard path. */ }
        unset($bad);
        expectRedis($client->get('fixture') === 'value' && $client->objectPool()->stats()['active'] === 0,
            'Redis command exception/discard lost the slot');
        expectRedis(\Scf\Core\InflightCounter::$redis === 0, 'Redis command inflight was not balanced');
        expectRedis(count(iterator_to_array(\Swoole\Timer::list())) === 0,
            'Default Redis pool acquired a PDO maintenance timer');
        IdleConnectionReaper::stop();
        echo "PASS Redis command exception/discard and balanced inflight; no maintenance timer\n";
    });
}
