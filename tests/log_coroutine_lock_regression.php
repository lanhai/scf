<?php
declare(strict_types=1);

/** Real Log + File paths; isolated counters and temp files, no running server required. */
namespace Scf\Core { class Component {} }
namespace Monolog { class Logger { public const NOTICE = 250; } }
namespace Scf\Core\Table {
    class Counter {
        private array $values = [];
        public static function instance(): self { static $instance; return $instance ??= new self(); }
        public function get(string $key): mixed { return $this->values[$key] ?? false; }
        public function set(string $key, mixed $value): void { $this->values[$key] = $value; }
        public function exist(string $key): bool { return array_key_exists($key, $this->values); }
        public function incr(string $key, string $field, int $value): void { $this->values[$key] += $value; }
    }
}
namespace {
    require dirname(__DIR__) . '/src/Util/File.php';
    require dirname(__DIR__) . '/src/Core/Log.php';
    class ConcurrentLogProbe extends \Scf\Core\Log {
        public function append(string $message): bool { return $this->saveToFile('error', ['day'=>date('Y-m-d'),'log'=>$message]); }
        public function locked(string $file, callable $callback): mixed { return $this->withLogCounterLock($file, $callback); }
        protected function logFileSignature(string $fileName): array {
            // File hooks can yield while holding the lock. Force that scheduling point deterministically.
            if (\Swoole\Coroutine::getCid() >= 0) \Swoole\Coroutine::sleep(0.001);
            return parent::logFileSignature($fileName);
        }
    }
    $dir = sys_get_temp_dir() . '/scf-log-lock-' . getmypid();
    define('APP_LOG_PATH', $dir);
    mkdir($dir . '/error', 0700, true);
    $logger = new ConcurrentLogProbe();
    try {
        \Swoole\Coroutine::set(['hook_flags'=>SWOOLE_HOOK_ALL & ~SWOOLE_HOOK_FILE & ~SWOOLE_HOOK_STDIO & ~SWOOLE_HOOK_STREAM_FUNCTION]);
        // The HTTP error handler logs even suppressed stat warnings. A new day has no log file yet.
        set_error_handler(function (int $level, string $message) use ($logger): bool {
            $logger->append('handler: ' . $message);
            return true;
        });
        $logger->append('cold-start');
        restore_error_handler();
        $ticks = 0; $done = 0;
        \Swoole\Coroutine\run(function () use ($logger, &$ticks, &$done): void {
            for ($i=0; $i<12; $i++) \Swoole\Coroutine::create(function () use ($logger, &$done, $i): void {
                if (!$logger->append('entry-' . $i)) throw new \RuntimeException('append failed');
                $done++;
            });
            while ($done < 12) { $ticks++; \Swoole\Coroutine::sleep(0.002); }
        });
        $file = $dir . '/error/' . date('Y-m-d') . '.log';
        if ($done !== 12 || $ticks < 2 || $logger->count('error',date('Y-m-d')) !== 13) throw new \RuntimeException('Concurrent writes or event loop stalled');
        if (count(array_unique(file($file, FILE_IGNORE_NEW_LINES))) !== 13) throw new \RuntimeException('Missing or duplicate entries');
        try { $logger->locked($file, static fn() => throw new \RuntimeException('callback failed')); }
        catch (\RuntimeException $error) { if ($error->getMessage() !== 'callback failed') throw $error; }
        if (!$logger->append('after-exception') || $logger->count('error',date('Y-m-d')) !== 14) throw new \RuntimeException('Lock not released after exception');
        echo "PASS concurrent actual log writes, live coroutine heartbeat, exact counts, synchronous writer and exception release\n";
    } finally {
        foreach (glob($dir . '/error/*') as $file) unlink($file);
        rmdir($dir . '/error'); rmdir($dir);
    }
}
