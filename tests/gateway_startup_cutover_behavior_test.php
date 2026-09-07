<?php
declare(strict_types=1);

namespace Scf\Core {
    class Console { public static function warning(...$args): void {} public static function info(...$args): void {} }
}
namespace Scf\Core\Table {
    class Runtime {
        private static ?self $instance = null;
        private array $data = [];
        public static function instance(): self { return self::$instance ??= new self(); }
        public function get(string $key): mixed { return $this->data[$key] ?? null; }
        public function set(string $key, mixed $value): bool { $this->data[$key] = $value; return true; }
        public function serverIsAlive(?bool $value = null): bool { return true; }
        public function serverIsReady(?bool $value = null): bool { return false; }
        public function serverIsDraining(?bool $value = null): bool { return false; }
    }
}
namespace {
    spl_autoload_register(static function (string $class): void {
        if (str_starts_with($class, 'Scf\\')) {
            $file = dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
            if (is_file($file)) require_once $file;
        }
    });
    define('APP_DIR_NAME', 'startup_fixture');
    define('SERVER_ROLE', 'master');
    define('APP_SRC_TYPE', 'dir');
    function checkStartup(bool $ok, string $message): void { if (!$ok) throw new \RuntimeException($message); }
    class CutoverFixtureGateway extends \Scf\Server\Gateway\GatewayServer {
        public bool $healthy = true;
        public bool $nginxOk = true;
        public bool $ingressOk = true;
        public int $readyCount = 0;
        public int $detaches = 0;
        public int $stops = 0;
        public int $quiesced = 0;
        public function __construct(\Scf\Server\Gateway\AppInstanceManager $manager) { $this->instanceManager = $manager; }
        public function nginxProxyModeEnabled(): bool { return true; }
        public function serverConfig(): array { return []; }
        public function complete(array $plan): bool { return $this->completeManagedStartupCutover($plan); }
        protected function probeManagedUpstreamHealth(array $plan): array { return ['healthy' => $this->healthy]; }
        protected function syncNginxProxyTargets(?string $reason = null, array $summaryContext = [], bool $deferSummary = false): bool { return $this->nginxOk; }
        protected function waitForManagedGenerationCutover(string $generationVersion, array $plans, string $stage): bool { return $this->ingressOk; }
        protected function notifyManagedUpstreamGenerationIterated(string $version): void {}
        protected function recordStartupReadyInstance(array $plan): void { $this->readyCount++; }
        protected function describePlan(array $plan): string { return 'fixture'; }
        protected function quiesceManagedPlanBusinessPlane(array $plan): void { $this->quiesced++; }
        public function prepare(bool $preserve): void { $this->preserveManagedUpstreamsOnShutdown = $preserve; $this->prepareGatewayShutdown(); }
        protected function stopGatewayLeaseRenewTimer(): void {}
        protected function renewGatewayLease(string $state = 'running', bool $force = false): void {}
        protected function clearPendingManagedRecycleWatchers(): void {}
        protected function detachUpstreamSupervisor(): void { $this->detaches++; }
        protected function shutdownManagedUpstreams(): void { $this->stops++; }
    }
    $root = sys_get_temp_dir() . '/scf-startup-cutover-' . bin2hex(random_bytes(5));
    $manager = new \Scf\Server\Gateway\AppInstanceManager(new \Scf\Server\Gateway\UpstreamRegistry($root));
    $manager->bootstrap('old', '127.0.0.1', 30101, true, 100, ['managed' => true]);
    $manager->registerUpstream('new', '127.0.0.1', 30102);
    $plan = ['version' => 'new', 'host' => '127.0.0.1', 'port' => 30102];
    $gateway = new CutoverFixtureGateway($manager);
    try {
        $gateway->healthy = false;
        checkStartup(!$gateway->complete($plan), 'Listening without business health must not activate');
        checkStartup($manager->state()['active_version'] === 'old', 'Unhealthy startup must preserve old routing');
        $gateway->healthy = true;
        $gateway->nginxOk = false;
        checkStartup(!$gateway->complete($plan), 'nginx failure must fail startup');
        checkStartup($manager->state()['active_version'] === 'old', 'nginx failure restores old generation');
        checkStartup($manager->state()['generations']['new']['status'] === 'prepared', 'Candidate remains retryable without respawn');
        $gateway->nginxOk = true;
        $gateway->ingressOk = false;
        checkStartup(!$gateway->complete($plan), 'Wrong ingress target must fail startup');
        checkStartup($gateway->readyCount === 0, 'No startup-completed report before real cutover');
        checkStartup($gateway->quiesced === 0, 'Do not quiesce old business before cutover succeeds');
        $gateway->ingressOk = true;
        checkStartup($gateway->complete($plan), 'Recovery must activate the same candidate');
        checkStartup($manager->state()['active_version'] === 'new', 'New generation active after verification');
        checkStartup($gateway->quiesced === 1, 'Verified startup must use the normal business drain workflow');
        checkStartup($manager->state()['generations']['old']['status'] === 'draining', 'Old generation enters drain only after verified cutover');
        checkStartup(count($manager->state()['generations']['old']['instances']) === 1, 'Old endpoint stays tracked during drain');
        $worker = new CutoverFixtureGateway($manager);
        $worker->prepare(true);
        $master = new CutoverFixtureGateway($manager);
        $master->prepare(false);
        checkStartup($worker->detaches === 1 && $master->detaches === 1 && $master->stops === 0,
            'Master shutdown callback must honor worker handoff through shared state');
        echo "PASS startup: health gate, nginx failure, wrong target, retry, old drain, worker/master handoff\n";
    } finally { if (is_file($root)) unlink($root); }
}
