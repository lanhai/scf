<?php
declare(strict_types=1);
/** Run the real Timer-driven consumer under Swoole; only Redis/task storage and lifecycle exit are fixtures. */
namespace Scf\Core\Traits { trait Singleton {} }
namespace Scf\Core {
    class Key { const COUNTER_REDIS_QUEUE_PROCESS = 'manager'; const COUNTER_REDIS_QUEUE_PROCESSING = 'processing'; const RUNTIME_SUBPROCESS_MANAGER_GENERATION = 'generation'; }
    class Env { public static function isDev(): bool { return false; } }
    class Console { public static function log(string $message): void {} }
    class Log { public static array $errors = []; public static function instance(): self { return new self; } public function setModule(string $name): self { return $this; } public function error(string $message): void { self::$errors[] = $message; } }
}
namespace Scf\Core\Table {
    class Counter {
        public static array $values = ['manager' => 1, 'processing' => 0];
        public static function instance(): self { return new self; }
        public function get(string $key): int { return self::$values[$key] ?? 0; }
        public function incr(string $key): void { self::$values[$key] = $this->get($key) + 1; }
        public function decr(string $key): void { self::$values[$key] = $this->get($key) - 1; }
    }
    class Runtime {
        public static array $values = ['generation' => 'current'];
        public static function instance(): self { return new self; }
        public function get(string $key): mixed { return self::$values[$key] ?? null; }
        public function set(string $key, mixed $value): void { self::$values[$key] = $value; }
    }
}
namespace Scf\Util { class MemoryMonitor { public static function updateUsage(string $name): void {} } }
namespace {
    require dirname(__DIR__) . '/src/Server/Task/RQueue.php';
    use Swoole\Coroutine;
    use Scf\Core\Table\Counter;
    use Scf\Core\Table\Runtime;
    class QueueCapacityFixture extends \Scf\Server\Task\RQueue {
        public array $pending = [], $started = [], $ended = [];
        public int $active = 0, $peak = 0;
        public bool $stopped = false;
        public float $stopTime = 0;
        public float $fetchDelay = 0.01;
        public function begin(int $capacity): void { $this->managerId = 1; $this->managerGeneration = 'current'; $this->loop($capacity); }
        public function count(int $status = 0, ?string $day = null): int { return count($this->pending); }
        public function pop(): bool {
            if (!$this->pending) return false;
            [$id, $duration, $fail] = array_shift($this->pending);
            $this->peak = max($this->peak, ++$this->active);
            // Include time spent borrowing a Redis connection in the reservation bound.
            Coroutine::sleep($this->fetchDelay);
            Counter::instance()->incr('processing');
            $this->started[$id] = microtime(true);
            try { Coroutine::sleep($duration); if ($fail) throw new \RuntimeException('fixture task failure'); return true; }
            finally { $this->ended[$id] = microtime(true); $this->active--; Counter::instance()->decr('processing'); }
        }
        protected function shutdownRuntime(): void { $this->stopped = true; $this->stopTime = microtime(true); }
    }
    $checks = [];
    \Swoole\Coroutine\run(function () use (&$checks): void {
        $queue = new QueueCapacityFixture;
        $queue->pending = [['slow-a', 2.4, false], ['slow-b', 2.4, false]];
        $start = microtime(true); $queue->begin(3);
        Coroutine::sleep(1.15);
        $queue->pending[] = ['fast-later', 0.03, false];
        Coroutine::sleep(1.12);
        $checks['idle slot accepts new task while earlier batch is still running'] = isset($queue->ended['fast-later']) && !isset($queue->ended['slow-a']);
        $checks['consumer remains within configured concurrency'] = $queue->peak <= 3;
        Runtime::$values['generation'] = 'next';
        $queue->pending[] = ['must-not-run-after-retirement', 0.01, false];
        Coroutine::sleep(2.5);
        $checks['retired worker stops fetching and drains existing tasks'] = !isset($queue->started['must-not-run-after-retirement']) && $queue->stopped && $queue->active === 0 && $queue->stopTime >= max($queue->ended);
        $checks['short task starts within next polling interval'] = isset($queue->started['fast-later']) && $queue->started['fast-later'] - $start < 2.25;
        Runtime::$values['generation'] = 'current';
        $full = new QueueCapacityFixture;
        $full->pending = [['fails', 1.6, true], ['succeeds', 1.6, false], ['after-failure', 0.02, false]];
        $full->begin(2);
        Coroutine::sleep(2.2);
        $checks['full capacity does not overfetch'] = !isset($full->started['after-failure']) && $full->peak === 2;
        Coroutine::sleep(1.1);
        $checks['exception releases slot for subsequent tasks'] = isset($full->ended['after-failure']) && $full->active === 0 && in_array('fixture task failure', \Scf\Core\Log::$errors, true);
        Runtime::$values['generation'] = 'next';
        Coroutine::sleep(1.2);
        $checks['exception path still shuts down cleanly'] = $full->stopped && Counter::$values['processing'] === 0;

        Runtime::$values['generation'] = 'current';
        $fetch = new QueueCapacityFixture;
        $fetch->fetchDelay = 1.4;
        $fetch->pending = [['waiting-redis-a', 0.02, false], ['waiting-redis-b', 0.02, false], ['not-dequeued', 0.01, false]];
        $fetch->begin(2);
        Coroutine::sleep(1.2);
        Runtime::$values['generation'] = 'next';
        Coroutine::sleep(0.95);
        $checks['retirement drains reservations before Redis returns'] = !$fetch->stopped && $fetch->active === 2 && Counter::$values['processing'] === 0;
        Coroutine::sleep(1.5);
        $checks['reserved slots are bounded and released after drain'] = $fetch->peak === 2 && $fetch->stopped && !isset($fetch->started['not-dequeued']) && $fetch->active === 0;

    });
    foreach ($checks as $label => $ok) if (!$ok) throw new \RuntimeException($label);
    echo 'PASS native queue capacity refill and generation drain: ' . count($checks) . " checks\n";
}
