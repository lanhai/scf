<?php
declare(strict_types=1);

namespace Scf\Core {
    class Config {
        public static function server(): array { return []; }
    }
}

namespace {
    spl_autoload_register(static function (string $class): void {
        if (str_starts_with($class, 'Scf\\')) {
            $file = dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
            if (is_file($file)) require_once $file;
        }
    });

    use Scf\Core\Key;
    use Scf\Core\Table\MemoryMonitorTable;
    use Scf\Core\Table\Runtime;
    use Scf\Core\Table\SocketConnectionTable;
    use Scf\Util\MemoryMonitor;

    function checkMemory(bool $condition, string $message): void {
        if (!$condition) throw new \RuntimeException($message);
    }

    /** Exercise the production IPC handler without starting an application server. */
    class MemoryRowsUpstream extends \Scf\Server\Http {
        public function __construct() {}

        public function memoryResponse(): array {
            return $this->handleLocalIpcRequest(['action' => 'upstream.memory_rows']);
        }
    }

    /** Exercise production aggregation; only the IPC transport is replaced. */
    class MemoryRowsGateway {
        use \Scf\Server\Gateway\GatewayTelemetryTrait;

        public array $snapshotRows = [];

        protected function fetchUpstreamMemoryRows(array $instance): array {
            return $this->snapshotRows;
        }

        public function aggregate(array $fallback): array {
            return $this->buildGatewayMemoryUsage([
                'rows' => [[
                    'name' => 'MemoryMonitor',
                    'pid' => getmypid(),
                    'os_actual' => '16.00 MB',
                    'rss' => '16.00 MB',
                    'status' => '正常',
                ]],
            ], [['online' => true, 'memory_usage' => $fallback]]);
        }
    }

    // Real Swoole tables retain the zero-valued fields written by start().
    $runtime = Runtime::instance(['size' => 16]);
    $table = MemoryMonitorTable::instance(['size' => 16]);
    SocketConnectionTable::instance(['size' => 16]);
    MemoryMonitor::start('worker:1');
    $pid = getmypid();
    $upstream = new MemoryRowsUpstream();
    $gateway = new MemoryRowsGateway();
    $runtime->set(Key::RUNTIME_MEMORY_MONITOR_PID, $pid);

    function sampleMemory(?int $pssKb, ?int $rssKb): void {
        Runtime::instance()->set(Key::RUNTIME_PROCESS_MEMORY_SNAPSHOT, [
            'updated_at' => time(),
            'values' => [getmypid() => ['pss_kb' => $pssKb, 'rss_kb' => $rssKb]],
        ]);
    }

    // Darwin supplies RSS but no PSS. The table's initial PSS=0 is not a sample.
    sampleMemory(null, 65536);
    $response = $upstream->memoryResponse();
    checkMemory($response['status'] === 200, 'Memory IPC must succeed');
    $row = $response['data'][0];
    checkMemory($row['rss_mb'] === 64.0, 'Batch RSS must reach memory IPC');
    checkMemory($row['pss_mb'] === null, 'Uncollected PSS must remain unavailable');
    checkMemory($row['os_actual'] === 64.0, 'Unavailable PSS must use actual RSS');
    $fallback = MemoryMonitor::sum();
    $gateway->snapshotRows = $response['data'];
    $aggregate = $gateway->aggregate($fallback);
    $byName = array_column($aggregate['rows'], null, 'name');
    checkMemory($byName['worker:1']['os_actual'] === '64.00 MB', 'Gateway must retain the worker RSS');
    checkMemory($aggregate['os_actual_total_mb'] === 80.0, 'Dashboard total must include worker RSS');

    // Linux's valid PSS must still take precedence over RSS.
    sampleMemory(24576, 65536);
    $response = $upstream->memoryResponse();
    checkMemory($response['data'][0]['os_actual'] === 24.0, 'Valid PSS must take precedence');
    $gateway->snapshotRows = $response['data'];
    checkMemory($gateway->aggregate(MemoryMonitor::sum())['os_actual_total_mb'] === 40.0, 'Gateway must sum valid PSS');

    // A real zero sample is distinguishable from the table's uncollected zero.
    sampleMemory(0, 65536);
    checkMemory($upstream->memoryResponse()['data'][0]['os_actual'] === 0.0, 'An explicit sampled PSS=0 must be retained');

    sampleMemory(null, null);
    $row = $upstream->memoryResponse()['data'][0];
    checkMemory($row['rss_mb'] === null && $row['pss_mb'] === null && $row['os_actual'] === null,
        'Missing OS samples must not be fabricated as zero');
    $table->set('worker:1', ['rss_mb' => 42, 'pss_mb' => 0]);
    checkMemory($upstream->memoryResponse()['data'][0]['os_actual'] === 42.0, 'Last-good RSS must survive absent PSS');
    $table->set('worker:1', ['pss_mb' => 21]);
    checkMemory($upstream->memoryResponse()['data'][0]['os_actual'] === 21.0, 'Last-good PSS must retain priority');

    // Also run the real native sampler and pass its data through IPC + Gateway.
    MemoryMonitor::start('worker:1');
    $native = MemoryMonitor::getPssRssByPids([$pid], true)[$pid];
    checkMemory(($native['rss_kb'] ?? 0) > 0, 'Native OS sampler must find this PHP process');
    $expected = round(($native['pss_kb'] ?? $native['rss_kb']) / 1024, 1);
    $response = $upstream->memoryResponse();
    checkMemory($response['data'][0]['os_actual'] === $expected, 'Real OS memory must reach IPC');
    $gateway->snapshotRows = $response['data'];
    $aggregate = $gateway->aggregate(MemoryMonitor::sum());
    checkMemory($aggregate['os_actual_total_mb'] === round(16 + $expected, 2), 'Real OS memory must reach dashboard totals');

    fwrite(STDOUT, "Upstream memory rows behavior checks passed (native actual={$expected} MB).\n");
}
