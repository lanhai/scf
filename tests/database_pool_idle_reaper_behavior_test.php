<?php
declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';
require dirname(__DIR__) . '/src/Database/DriverTrait.php';
require dirname(__DIR__) . '/src/Database/Driver.php';
require dirname(__DIR__) . '/src/Database/Pool/IdleConnectionReaper.php';
require dirname(__DIR__) . '/src/Database/Pool/ConnectionPool.php';
require dirname(__DIR__) . '/src/Database/Pool/Dialer.php';

use Scf\Database\Driver;
use Scf\Database\Pool\ConnectionPool;
use Scf\Database\Pool\IdleConnectionReaper;
use Swoole\Coroutine;
use Swoole\Timer;

function expectIdle(bool $condition, string $message): void {
    if (!$condition) throw new RuntimeException($message);
}

class IdleTestDriver extends Driver {
    public int $queries = 0;
    public ?Closure $beforeClose = null;
    public function __construct() { $this->pdo = new PDO('sqlite::memory:'); }
    public function ping(): void { $this->queries++; }
    public function close(): void {
        if ($this->beforeClose !== null) ($this->beforeClose)();
        parent::close();
    }
}

class IdleTestDialer implements \Mix\ObjectPool\DialerInterface {
    public int $dials = 0;
    public function dial(): object { $this->dials++; return new IdleTestDriver(); }
}

class IdleTestPool extends ConnectionPool {
    public function age(Driver $driver, int $seconds): void {
        $driver->lastUseTime = time() - $seconds;
        $this->idleSince[spl_object_hash($driver)] = $driver->lastUseTime;
    }
}

function newIdlePool(int $maxOpen = 2, int $maxIdle = 2, int $timeout = 60): array {
    $dialer = new IdleTestDialer();
    $pool = new IdleTestPool($dialer, $maxOpen, $maxIdle, 0, 0);
    $pool->setIdleTimeout($timeout);
    return [$pool, $dialer];
}

\Swoole\Coroutine\run(function () use ($argv): void {
    if (($argv[1] ?? '') === '--tcp') {
        $pool = new ConnectionPool(new \Scf\Database\Pool\Dialer('mysql:host=127.0.0.1;port=' . (int)$argv[2], 'fixture', 'fixture'), 1, 1);
        $pool->setIdleTimeout(1);
        IdleConnectionReaper::start(static fn(): bool => true);
        $driver = $pool->borrow();
        expectIdle((int)$driver->instance()->query('SELECT 99')->fetchColumn() === 99, 'Initial real PDO query failed');
        $driver->__return();
        Coroutine::sleep(1.15);
        expectIdle($driver->instance() === null && $pool->stats()['total'] === 0, 'Real idle PDO was not closed');
        IdleConnectionReaper::stop();
        echo "PASS real PDO idle close without maintenance SQL or replacement dial\n";
        return;
    }
    // CLI / short-lived Coroutine::run must still finish with pooled idle connections.
    [$cli] = newIdlePool();
    $cli->borrow()->__return();
    expectIdle(count(iterator_to_array(Timer::list())) === 0, 'Unmanaged CLI unexpectedly acquired a maintenance timer');

    IdleConnectionReaper::start(static fn(): bool => true);
    [$disabled] = newIdlePool(1, 1, 0);
    $disabled->borrow()->__return();
    expectIdle(count(iterator_to_array(Timer::list())) === 0, 'idle_timeout=0 created a maintenance timer');

    [$hot, $hotDialer] = newIdlePool(1, 1, 60);
    $hot->borrow()->__return();
    $initialTimers = iterator_to_array(Timer::list());
    $started = microtime(true);
    for ($i = 0; $i < 10000; $i++) $hot->borrow()->__return();
    $hotElapsed = microtime(true) - $started;
    expectIdle(iterator_to_array(Timer::list()) === $initialTimers && count($initialTimers) === 1,
        'Hot checkout recreated timers or allocated more than one timer');
    expectIdle($hotDialer->dials === 1, 'Hot checkout created redundant connections');
    $weakPool = WeakReference::create($hot);
    // Empty the native Channel's existing driver->pool ownership cycle first.
    $released = $hot->borrow();
    unset($hot, $released);
    gc_collect_cycles();
    expectIdle($weakPool->get() === null, 'Maintenance retained a discarded pool');
    IdleConnectionReaper::stop();

    // Natural expiration, not merely a manually invoked sweep; active loan remains usable.
    IdleConnectionReaper::start(static fn(): bool => true);
    [$natural, $naturalDialer] = newIdlePool(2, 2, 1);
    $loan = $natural->borrow();
    $idle = $natural->borrow();
    $idle->__return();
    Coroutine::sleep(1.15);
    expectIdle($idle->instance() === null && $loan->instance() !== null, 'Natural sweep disturbed the active loan or retained expired idle PDO');
    expectIdle($naturalDialer->dials === 2 && $idle->queries === 0, 'Sweep probed or refilled the database');
    expectIdle($natural->stats() === ['total' => 1, 'idle' => 0, 'active' => 1], 'Sweep leaked a capacity reservation');
    IdleConnectionReaper::stop();
    $loan->__return();
    expectIdle(count(iterator_to_array(Timer::list())) === 0, 'Return after exit rearmed maintenance');

    // FIFO shared timer and per-worker batch cap across two pools.
    IdleConnectionReaper::start(static fn(): bool => true);
    $all = [];
    $pools = [];
    foreach ([40, 40] as $size) {
        [$pool, $dialer] = newIdlePool($size, $size);
        $loans = [];
        for ($i = 0; $i < $size; $i++) $loans[] = $pool->borrow();
        foreach ($loans as $driver) {
            $driver->__return();
            $pool->age($driver, 61);
            $all[] = $driver;
        }
        $pool->idleCheck();
        $pools[] = [$pool, $dialer];
    }
    expectIdle(count(iterator_to_array(Timer::list())) === 1, 'Multiple pools allocated multiple maintenance timers');
    Coroutine::sleep(0.15);
    $closedCount = static fn(): int => count(array_filter($all, static fn(Driver $driver): bool => $driver->instance() === null));
    expectIdle($closedCount() === 32, 'First callback exceeded the global batch cap');
    Coroutine::sleep(0.25);
    expectIdle($closedCount() === 80, 'Batched cleanup did not finish');
    foreach ($pools as [$pool, $dialer]) {
        expectIdle($pool->stats()['total'] === 0 && $dialer->dials === 40, 'Cleanup eagerly redialed or leaked slots');
    }
    expectIdle(count(iterator_to_array(Timer::list())) === 0, 'Empty pools retained recurring maintenance');
    IdleConnectionReaper::stop();

    // A waiting consumer must wake even if PDO close suspends at maxOpen=1.
    IdleConnectionReaper::start(static fn(): bool => true);
    [$single, $singleDialer] = newIdlePool(1, 1);
    $old = $single->borrow();
    $old->__return();
    $single->age($old, 61);
    $received = null;
    $old->beforeClose = static function () use ($single, &$received): void {
        expectIdle($single->stats()['active'] === 1, 'Closing connection released its maxOpen reservation too early');
        Coroutine::create(static function () use ($single, &$received): void {
            $received = $single->borrow();
            $received->__return();
        });
        Coroutine::sleep(0.02);
    };
    $single->idleCheck();
    Coroutine::sleep(0.2);
    expectIdle($received !== null && $received !== $old && $received->instance() !== null,
        'Waiting borrower was stranded or received the closed PDO');
    expectIdle($singleDialer->dials === 2 && $single->stats()['total'] === 1, 'Closing handoff broke maxOpen accounting');
    IdleConnectionReaper::stop();

    // A full maxIdle queue must not stall return for the old five-second push timeout.
    [$full, $fullDialer] = newIdlePool(2, 1);
    $kept = $full->borrow();
    $overflow = $full->borrow();
    $kept->__return();
    $started = microtime(true);
    expectIdle(!$overflow->__return(), 'Full queue accepted an extra idle connection');
    expectIdle(microtime(true) - $started < 0.1 && $overflow->instance() === null && $kept->instance() !== null,
        'Full queue blocked return or closed the wrong connection');
    expectIdle($full->stats()['total'] === 1 && $fullDialer->dials === 2, 'Full queue created a replacement');

    // Overflow close can also suspend while the queued connection is borrowed elsewhere.
    [$overflowPool, $overflowDialer] = newIdlePool(2, 1);
    $queued = $overflowPool->borrow();
    $closing = $overflowPool->borrow();
    $queued->__return();
    $waitingResult = null;
    $closing->beforeClose = static function () use ($overflowPool, &$waitingResult): void {
        Coroutine::create(static function () use ($overflowPool): void {
            $held = $overflowPool->borrow();
            Coroutine::sleep(0.2);
            $held->__return();
        });
        Coroutine::create(static function () use ($overflowPool, &$waitingResult): void {
            $waitingResult = $overflowPool->borrow();
            $waitingResult->__return();
        });
        Coroutine::sleep(0.02);
    };
    $closing->__return();
    expectIdle($waitingResult !== null && $waitingResult !== $closing && $queued->instance() !== null,
        'Overflow close stranded a borrower until an unrelated long operation returned');
    expectIdle($overflowDialer->dials === 3 && $overflowPool->stats()['total'] === 2, 'Overflow handoff exceeded capacity');
    Coroutine::sleep(0.25);

    // Stop between closes; no second close and no resurrection of the timer.
    IdleConnectionReaper::start(static fn(): bool => true);
    [$stopping] = newIdlePool();
    $first = $stopping->borrow();
    $second = $stopping->borrow();
    foreach ([$first, $second] as $driver) { $driver->__return(); $stopping->age($driver, 61); }
    $first->beforeClose = static fn() => IdleConnectionReaper::stop();
    $stopping->idleCheck();
    Coroutine::sleep(0.2);
    expectIdle($first->instance() === null && $second->instance() !== null && count(iterator_to_array(Timer::list())) === 0,
        'Exit during cleanup closed more PDOs or rearmed maintenance');

    $draining = false;
    IdleConnectionReaper::start(static function () use (&$draining): bool { return !$draining; });
    [$paused] = newIdlePool(1, 1);
    $untouched = $paused->borrow();
    $untouched->__return();
    $paused->age($untouched, 61);
    $paused->idleCheck();
    $draining = true;
    Coroutine::sleep(0.2);
    expectIdle($untouched->instance() !== null && count(iterator_to_array(Timer::list())) === 0, 'Drain guard allowed maintenance I/O');
    IdleConnectionReaper::stop();

    // An application can disable coroutine timer callbacks independently of server settings.
    IdleConnectionReaper::start(static fn(): bool => true);
    [$nonCoroutine] = newIdlePool(1, 1);
    $untouched = $nonCoroutine->borrow();
    $untouched->__return();
    $nonCoroutine->age($untouched, 61);
    $nonCoroutine->idleCheck();
    // Retained but deprecated in 5.1.8; exercise legacy configuration without deprecation noise.
    @Timer::set(['enable_coroutine' => false]);
    Coroutine::sleep(0.2);
    @Timer::set(['enable_coroutine' => true]);
    expectIdle($untouched->instance() !== null && count(iterator_to_array(Timer::list())) === 0,
        'Non-coroutine timer attempted Channel I/O');
    IdleConnectionReaper::stop();

    printf("PASS idle expiry, active isolation, zero maintenance probes/redials, shared timer, 32-connection batches, weak ownership, blocked borrower handoff, nonblocking return, drain/exit and CLI\n10000 hot checkouts: %.2f ms, 1 dial, 1 unchanged timer\n", $hotElapsed * 1000);
});
