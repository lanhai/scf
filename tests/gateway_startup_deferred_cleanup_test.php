<?php
declare(strict_types=1);

namespace Scf\Core {
    class Config { public static function server(): array { return ['port' => 30100]; } }
    class Console {
        public static function timestamp(): string { return ''; }
        public static function warning(...$args): void {}
        public static function success(...$args): void {}
    }
}
namespace {
    spl_autoload_register(static function (string $class): void {
        if (str_starts_with($class, 'Scf\\')) {
            $path = dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
            if (is_file($path)) require_once $path;
        }
    });
    define('APP_DIR_NAME', 'startup_fixture');
    define('SERVER_ROLE', 'master');
    define('SERVER_RUN_ENV', 'production');
    define('APP_SRC_TYPE', 'dir');
    function checkDeferred(bool $ok, string $message): void {
        if (!$ok) throw new RuntimeException($message);
    }
    class DeferredLauncher extends \Scf\Server\Gateway\AppServerLauncher {
        public int $stops = 0;
        public function stop(array $instance, int $graceSeconds = 1800): void { $this->stops++; }
        public function isListening(string $host, int $port, float $timeout = 0.5): bool { return $port === 30101; }
        public function findAvailablePortAvoiding(string $host, int $startPort, array $reservedPorts = [], int $maxAttempts = 2000): int {
            while ($startPort === 30101 || in_array($startPort, $reservedPorts, true)) $startPort++;
            return $startPort;
        }
    }
    class DeferredBootstrap extends \Scf\Server\Gateway\CliBootstrap {
        public static int $scans = 0;
        public static function context(array $opts): array {
            return parent::buildGatewayStartupContext($opts, new \Scf\Server\Gateway\AppServerLauncher());
        }
        public static function prepare(array $startup, \Scf\Server\Gateway\AppInstanceManager $manager, DeferredLauncher $launcher): array {
            return parent::prepareGatewayUpstreams([], $startup, $manager, $launcher);
        }
        protected static function reconcileRegistry($manager, $launcher, string $host, int $port): void { self::$scans++; }
        protected static function restartHandoffPorts(array $startup, $manager): array { return []; }
        protected static function cleanupOrphanManagedUpstreams($launcher, int $port, array $keepPorts = []): void { self::$scans++; }
    }
    class DeferredGateway extends \Scf\Server\Gateway\GatewayServer {
        public function waitForPorts(): void { $this->waitForGatewayPortsReleased(2); }
        public function bindControl(): void { $this->createGatewaySocketServer(2); }
    }
    $root = sys_get_temp_dir() . '/scf-deferred-start-' . bin2hex(random_bytes(5));
    $manager = new \Scf\Server\Gateway\AppInstanceManager(new \Scf\Server\Gateway\UpstreamRegistry($root));
    $manager->bootstrap('same-version', '127.0.0.1', 30101, true, 100, ['managed' => true, 'pid' => 19018, 'owner_epoch' => 92]);
    $manager->registerUpstream('stopped-listener', '127.0.0.1', 30102, 100, ['managed' => true, 'pid' => 19019, 'owner_epoch' => 91, 'rpc_port' => 30103]);
    $launcher = new DeferredLauncher();
    $startup = ['bind_port' => 30100, 'bind_host' => '127.0.0.1', 'rpc_bind_port' => 0, 'control_bind_port' => 31100,
        'traffic_mode' => 'nginx', 'spawn_upstream' => true, 'reuse_existing_upstream' => false,
        'version' => 'same-version', 'server_config' => [], 'upstream_host' => '127.0.0.1', 'bootstrap_active' => true, 'weight' => 100];
    try {
        $socket = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        checkDeferred(is_resource($socket), 'Fixture must bind a private port');
        try {
            $occupied = (int)substr(strrchr(stream_socket_get_name($socket, false), ':'), 1);
            $options = [
                'host' => '127.0.0.1',
                'port' => 30100,
                'control_port' => $occupied,
                'rpc_port' => $occupied + 1,
                'upstream_port' => $occupied + 2,
                'upstream_rpc_port' => $occupied + 3,
                'upstream_version' => 'fixture',
                'state_file' => $root,
            ];
            $nginx = DeferredBootstrap::context($options + ['traffic_mode' => 'nginx']);
            $tcp = DeferredBootstrap::context($options + ['traffic_mode' => 'tcp']);
            checkDeferred(!in_array($nginx['control_bind_port'], array_values($options), true),
                'Nginx control listener must avoid the occupied, public and reserved ports');
            checkDeferred($tcp['control_bind_port'] === $occupied, 'TCP mode must retain its configured control port');
            checkDeferred(\Scf\Core\Server::isListeningPortInUse($occupied), 'Existing listener must survive');
            $gateway = new DeferredGateway($manager, '127.0.0.1', 30100, controlBindPort: $occupied, configuredTrafficMode: 'nginx');
            $startedAt = microtime(true);
            $gateway->waitForPorts();
            try {
                $gateway->bindControl();
                throw new LogicException('The occupied listener must reject the bind');
            } catch (RuntimeException $exception) {
                checkDeferred(str_contains($exception->getMessage(), '端口仍被占用'), 'Expose a bind conflict without retries');
            }
            checkDeferred(microtime(true) - $startedAt < 1.0, 'Nginx must not wait even if a port is claimed after planning');
        } finally {
            fclose($socket);
        }
        $result = DeferredBootstrap::prepare($startup, $manager, $launcher);
        checkDeferred($launcher->stops === 0, 'New upstream startup must not synchronously stop historical instances');
        checkDeferred(DeferredBootstrap::$scans === 0, 'Historical reconciliation/orphan scanning must not precede startup');
        $plan = $result['managed_upstream_plans'][0];
        checkDeferred(!in_array($plan['port'], [30101, 30102, 30103], true), 'New upstream must avoid live and still-registered HTTP/RPC endpoints');
        checkDeferred($plan['version'] !== 'same-version', 'Same-version cold recovery needs a separate generation');
        checkDeferred($manager->state()['active_version'] === 'same-version', 'Old routing must survive preparation');
        checkDeferred(count($manager->managedInstances()) === 2, 'Old owner metadata must remain for asynchronous recycling');
        echo "PASS deferred startup: no stop or scan, free port, distinct generation, preserved old state\n";
    } finally { if (is_file($root)) unlink($root); }
}
