<?php
declare(strict_types=1);

/** Exercise the real lock method with a Redis command boundary that can fail after SETNX. */
namespace Scf\Core { class Cache {} }
namespace Mix\Redis {
    class Connection {
        public array $values = [];
        public array $ttls = [];
        public array $calls = [];
        public bool $failExpire = false;
        public bool $failSet = false;
        public function get(string $key): mixed { $this->calls[] = 'get'; return $this->values[$key] ?? false; }
        public function setNX(string $key, mixed $value): bool {
            $this->calls[] = 'setnx';
            if (array_key_exists($key, $this->values)) return false;
            $this->values[$key] = $value;
            return true;
        }
        public function expire(string $key, int $ttl): bool {
            $this->calls[] = 'expire';
            if ($this->failExpire) throw new \RuntimeException('Interrupted between SETNX and EXPIRE');
            $this->ttls[$key] = $ttl;
            return true;
        }
        public function set(string $key, mixed $value, array $options): bool {
            $this->calls[] = 'set';
            if ($this->failSet) throw new \RuntimeException('Connection unavailable');
            if (!in_array('nx', $options, true) || (int)($options['ex'] ?? 0) <= 0) {
                throw new \RuntimeException('Expected an atomic NX/EX command');
            }
            if (array_key_exists($key, $this->values)) return false;
            $this->values[$key] = $value;
            $this->ttls[$key] = $options['ex'];
            return true;
        }
    }
}
namespace {
    define('REDIS_IGNORE_KEY_PREFIX', '@');
    require dirname(__DIR__) . '/src/Cache/Redis.php';
    class LockClient extends \Scf\Cache\Redis {
        public array $errors = [];
        public function __construct(\Mix\Redis\Connection $connection) { $this->connection = $connection; $this->keyPrefix = 'test'; }
        protected function onExecuteError(\Throwable $exception): void { $this->errors[] = $exception->getMessage(); }
    }
    function check(bool $ok, string $message): void { if (!$ok) throw new \RuntimeException($message); }
    $connection = new \Mix\Redis\Connection();
    $client = new LockClient($connection);
    $connection->failExpire = true;
    $client->lock('account', 600);
    check(!isset($connection->values['test:account']) || isset($connection->ttls['test:account']), 'Interrupted acquisition must never leave a permanent lock');
    check($connection->ttls['test:account'] === 600, 'Successful acquisition retains requested expiry');
    check($connection->calls === ['set'], 'Acquisition uses one atomic Redis command');
    $before = [$connection->values, $connection->ttls];
    check(!$client->lock('account', 900), 'Contending acquisition fails');
    check([$connection->values, $connection->ttls] === $before, 'Contention must not change the owner or renew the TTL');
    check($client->lock('default'), 'Default timeout acquisition succeeds');
    check($connection->ttls['test:default'] === 5, 'Default timeout stays five seconds');
    check($client->lock('@raw', 10) && isset($connection->ttls['raw']), 'Explicit prefix bypass remains supported');
    $connection->failSet = true;
    check(!$client->lock('unavailable') && !isset($connection->values['test:unavailable']), 'Redis failures fail closed without a key');
    check(count($client->errors) === 1, 'Redis errors use existing error handling');
    echo "PASS atomic lock acquisition, interrupted command boundary, contention, TTL, prefix and Redis failure\n";
}
