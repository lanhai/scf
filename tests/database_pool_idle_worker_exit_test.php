<?php
declare(strict_types=1);

namespace Scf\Core {
    class InflightCounter {
        public static function beginRedis(): void {}
        public static function endRedis(): void {}
        public static function cleanupProcessInflightByPid(int $pid): array { return ['total' => 0]; }
    }
}

namespace {
    require dirname(__DIR__) . '/vendor/autoload.php';
    foreach (['Database/DriverTrait', 'Database/Driver', 'Database/Pool/IdleConnectionReaper',
        'Database/Pool/ConnectionPool', 'Cache/Connection/InflightRedisConnection',
        'Server/Listener/Listener', 'Server/Listener/WorkerListener'] as $file) {
        require dirname(__DIR__) . '/src/' . $file . '.php';
    }

    use Scf\Database\Driver;
    use Scf\Database\Pool\ConnectionPool;
    use Scf\Database\Pool\IdleConnectionReaper;
    use Swoole\Coroutine;
    use Swoole\Table;
    use Swoole\Timer;
    use Swoole\WebSocket\Server;

    class ExitTestDriver extends Driver {
        public function __construct() { $this->pdo = new PDO('sqlite::memory:'); }
    }
    class ExitTestDialer implements \Mix\ObjectPool\DialerInterface {
        public function dial(): object { return new ExitTestDriver(); }
    }
    class ExitTestListener extends \Scf\Server\Listener\WorkerListener {
        public function exitWorker(Server $server, int $id): void { parent::onWorkerExit($server, $id); }
        public function stopWorker(Server $server, int $id): void { parent::onWorkerStop($server, $id); }
    }

    $taskMode = ($argv[1] ?? '') === '--task';
    $redisOption = array_search('--redis', $argv, true);
    $redisPort = $redisOption === false ? 0 : (int)$argv[$redisOption + 1];
    $records = new Table(16);
    foreach (['starts', 'first_pid', 'second_pid', 'exits', 'stops', 'errors', 'lost_active', 'late_timer',
        'work_done', 'result_done'] as $key) $records->column($key, Table::TYPE_INT);
    $records->column('reload_at', Table::TYPE_FLOAT);
    $records->column('stop_delay', Table::TYPE_FLOAT);
    $records->create();
    $records->set('run', []);

    $server = new Server('127.0.0.1', 0, SWOOLE_PROCESS);
    $server->set(['worker_num' => 1, 'task_worker_num' => $taskMode ? 1 : 0,
        'task_enable_coroutine' => true, 'reload_async' => true, 'max_wait_time' => 2, 'log_file' => '/dev/null']);
    $listener = new ExitTestListener($server);
    $pool = null;
    $timerId = new ReflectionProperty(IdleConnectionReaper::class, 'timerId');
    $runBusiness = static function () use (&$pool, $records, $timerId, $redisPort): void {
        $active = $pool->borrow();
        if ($redisPort > 0) {
            $connection = new \Scf\Cache\Connection\InflightRedisConnection($active, null);
            if ($connection->get('fixture') !== 'value') $records->incr('run', 'lost_active');
            $pool->borrow()->__return();
            Coroutine::sleep(0.3);
            if (!$active->instance()->isConnected() || $connection->get('fixture') !== 'value') {
                $records->incr('run', 'lost_active');
            }
            unset($connection);
            if ($timerId->getValue() !== 0) $records->incr('run', 'late_timer');
            $records->incr('run', 'work_done');
            return;
        }
        $active->instance()->exec('CREATE TABLE fixture (value INTEGER)');
        $active->instance()->beginTransaction();
        $active->instance()->exec('INSERT INTO fixture VALUES (7)');
        $pool->borrow()->__return(); // Leave a 60-second maintenance timer alongside the active operation.
        Coroutine::sleep(0.3);
        if ($active->instance() === null || !$active->instance()->inTransaction()) {
            $records->incr('run', 'lost_active');
        } else {
            $active->instance()->commit();
            if ((int)$active->instance()->query('SELECT value FROM fixture')->fetchColumn() !== 7) {
                $records->incr('run', 'lost_active');
            }
        }
        $active->__return();
        if ($timerId->getValue() !== 0) $records->incr('run', 'late_timer');
        $records->incr('run', 'work_done');
    };
    $server->on('Request', static function ($request, $response) use ($runBusiness): void {
        $runBusiness();
        $response->end('completed');
    });
    $server->on('Message', static function (): void {});
    if ($taskMode) {
        $server->on('Task', static function (Server $server, $task) use ($runBusiness): void {
            $runBusiness();
            $task->finish('completed');
        });
        $server->on('Finish', static function () use ($records): void { $records->incr('run', 'result_done'); });
    }
    $server->on('WorkerStart', static function (Server $server, int $id) use (&$pool, $records, $taskMode, $redisPort): void {
        IdleConnectionReaper::start(static fn(): bool => true);
        $dialer = $redisPort > 0
            ? new \Mix\Redis\Pool\Dialer('127.0.0.1', $redisPort, '', 0, 1, 0, 1)
            : new ExitTestDialer();
        $pool = new ConnectionPool($dialer, 2, 2, $redisPort > 0 ? 600 : 0, 0);
        $pool->setIdleTimeout(60);
        Coroutine::create(static function () use ($pool, $redisPort): void {
            $idle = $pool->borrow();
            $idle->__return();
            if ($redisPort > 0) $idle->createTime = time() - 601;
        });
        $target = $taskMode ? $server->taskworker : !$server->taskworker;
        if ($target) {
            $generation = $records->incr('run', 'starts');
            $records->set('run', [$generation === 1 ? 'first_pid' : 'second_pid' => getmypid()]);
            if ($generation > 1) {
                // Swoole may start the successor before the old worker has finished draining.
                $watchdog = Timer::after(1800, static fn() => $server->shutdown());
                Timer::tick(20, static function (int $id) use ($server, $records, $watchdog): void {
                    if ($records->get('run', 'stop_delay') > 0 && $records->get('run', 'result_done') === 1) {
                        Timer::clear($id);
                        Timer::clear($watchdog);
                        $server->shutdown();
                    }
                });
            }
        }
        if (!$server->taskworker && $records->get('run', 'reload_at') === 0.0) {
            Timer::after(30, static function () use ($server, $taskMode, $records): void {
                if ($taskMode) {
                    $server->task('hold');
                    return;
                }
                $client = new \Swoole\Coroutine\Http\Client('127.0.0.1', $server->port);
                $client->set(['timeout' => 1.5]);
                if ($client->get('/hold') && $client->body === 'completed') $records->incr('run', 'result_done');
                $client->close();
            });
            Timer::after(120, static function () use ($server, $taskMode, $records): void {
                $records->set('run', ['reload_at' => microtime(true)]);
                $server->reload($taskMode);
            });
        }
    });
    $server->on('WorkerExit', static function (Server $server, int $id) use ($listener, $records): void {
        $listener->exitWorker($server, $id);
        $records->incr('run', 'exits');
    });
    $server->on('WorkerStop', static function (Server $server, int $id) use ($listener, $records, $timerId): void {
        $listener->stopWorker($server, $id);
        if ($timerId->getValue() !== 0) $records->incr('run', 'late_timer');
        if (getmypid() === $records->get('run', 'first_pid')) {
            $records->set('run', ['stop_delay' => microtime(true) - $records->get('run', 'reload_at')]);
        }
        $records->incr('run', 'stops');
    });
    $server->on('WorkerError', static function () use ($records): void { $records->incr('run', 'errors'); });
    $server->start();
    $result = $records->get('run');
    if ($result['starts'] !== 2 || $result['first_pid'] === $result['second_pid'] || $result['errors'] !== 0
        || $result['lost_active'] !== 0 || $result['late_timer'] !== 0 || $result['work_done'] !== 1
        || $result['result_done'] !== 1 || $result['stop_delay'] <= 0 || $result['stop_delay'] >= 1.2
        || $result['exits'] < 1 || $result['stops'] < 2) {
        throw new RuntimeException('Graceful idle maintenance reload failed: ' . json_encode($result));
    }
    printf("PASS %s %s worker reload: active operation completed, 60s maintenance timer cleared, old PID retired in %.3fs, no forced worker exit\n",
        $redisPort > 0 ? 'Redis' : 'PDO', $taskMode ? 'task' : 'HTTP', $result['stop_delay']);
}
