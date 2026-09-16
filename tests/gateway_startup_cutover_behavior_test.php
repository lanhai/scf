<?php
declare(strict_types=1);

namespace Scf\Core {
    class Console { public static function warning(...$args): void {} public static function info(...$args): void {} }
    class App { public static function isReady(): bool { return true; } }
    class Config { public static function server(): array { return []; } }
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
    define('SERVER_RUN_ENV', 'dev');
    function checkStartup(bool $ok, string $message): void { if (!$ok) throw new \RuntimeException($message); }
    class CutoverFixtureGateway extends \Scf\Server\Gateway\GatewayServer {
        public bool $healthy = true;
        public bool $nginxOk = true;
        public bool $ingressOk = true;
        public int $readyCount = 0;
        public int $detaches = 0;
        public int $stops = 0;
        public int $quiesced = 0;
        public array $events = [];
        public array $orphans = [];
        public bool $finishBootstrap = false;
        public function __construct(\Scf\Server\Gateway\AppInstanceManager $manager) {
            $this->instanceManager = $manager;
            $this->port = 30100;
            $this->managedUpstreamPlans = [['version' => 'new', 'host' => '127.0.0.1', 'port' => 30102]];
        }
        public function retire(): void { $this->retireStartupHistoricalInstances(); }
        public function coordinatorTick(): void { $this->runGatewayBusinessCoordinatorTick(); }
        protected function discoverStartupOrphanInstances(array $knownPorts): array {
            checkStartup(in_array(30102, $knownPorts, true), 'Orphan scan must exclude the new endpoint');
            return $this->orphans;
        }
        protected function consumeFallbackBusinessReloadSignal(): bool { return false; }
        protected function observeUpstreamSupervisorProcess(): void {}
        protected function bootstrapManagedUpstreams(): void {
            $this->events[] = 'bootstrap';
            if ($this->finishBootstrap) \Scf\Core\Table\Runtime::instance()->set('gateway_startup_ingress_ready', true);
        }
        protected function syncUpstreamSupervisorState(): void { $this->events[] = 'sync'; }
        protected function refreshManagedUpstreamRuntimeStates(): void { $this->events[] = 'refresh'; }
        protected function pollPendingManagedRecycles(): void {}
        protected function drainQuarantinedManagedRecycles(): void {}
        protected function warnStuckDrainingManagedUpstreams(): void {}
        protected function cleanupOfflineManagedUpstreams(): void { $this->events[] = 'recycle'; }
        protected function refreshGatewayInstallTakeover(): void {}
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
    // Real Swoole timers drive the production pending-recycle state machine;
    // only upstream IPC/probes are simulated so no application or production process is touched.
    class DelayedExitLauncher extends \Scf\Server\Gateway\AppServerLauncher {
        public bool $oldAlive = true;
        public int $shutdownRequests = 0;
        public function requestManagedGracefulShutdown(array $instance, float $timeoutSeconds = 1.0): bool {
            $this->shutdownRequests++;
            return true;
        }
        public function isListening(string $host, int $port, float $timeoutSeconds = 0.3): bool {
            return $port === 30102 || ($port === 30101 && $this->oldAlive);
        }
        public function isProcessAlive(int $pid): bool { return $this->oldAlive; }
        public function forceStopManagedInstance(array $instance, bool $hard = false): void {
            throw new \RuntimeException('A delayed graceful exit must not trigger an early kill');
        }
    }
    class TimerRecycleGateway extends CutoverFixtureGateway {
        public function setLauncher(DelayedExitLauncher $launcher): void { $this->launcher = $launcher; }
        protected function quiesceManagedPlanBusinessPlane(array $plan): void { $this->stopManagedPlan($plan); }
        protected function fetchUpstreamRuntimeStatus(array $instance): array { return ['http_request_processing' => 1]; }
        protected function logOldInstanceLifecycle(string $message, int $port = 0): void {}
        public function pendingCount(): int { return count($this->pendingManagedRecycles); }
        public function clearWatchers(): void { $this->clearPendingManagedRecycleWatchers(); }
        protected function clearPendingManagedRecycleWatchers(): void {
            foreach ($this->pendingManagedRecycleWatchers as $id) \Swoole\Timer::clear($id);
            $this->pendingManagedRecycleWatchers = [];
        }
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
        checkStartup($gateway->quiesced === 0, 'Startup readiness must not wait on old-instance IPC');
        checkStartup($manager->state()['generations']['old']['status'] === 'draining', 'Old generation enters drain only after verified cutover');
        checkStartup(count($manager->state()['generations']['old']['instances']) === 1, 'Old endpoint stays tracked during drain');
        $manager->registerUpstream('abandoned', '127.0.0.1', 30103, 100, ['managed' => true, 'owner_epoch' => 7]);
        $gateway->orphans = [['host' => '127.0.0.1', 'port' => 30104, 'metadata' => ['managed' => true, 'pid' => 123, 'owner_epoch' => 6]]];
        $gateway->retire();
        checkStartup($gateway->quiesced === 3, 'Background retirement covers old active, abandoned prepared and orphan instances');
        checkStartup($manager->state()['generations']['abandoned']['status'] === 'draining', 'Abandoned candidates enter the existing drain lifecycle');
        checkStartup($manager->state()['generations']['startup-orphan-6-30104']['instances'][0]['metadata']['owner_epoch'] === 6,
            'Orphan reclamation must retain its original owner epoch');
        $gateway->retire();
        checkStartup($gateway->quiesced === 3, 'Background retirement is idempotent');
        $priority = new CutoverFixtureGateway($manager);
        \Scf\Core\Table\Runtime::instance()->set('gateway_startup_ingress_ready', false);
        $priority->coordinatorTick();
        checkStartup($priority->events === ['bootstrap'] && $priority->quiesced === 0, 'Failed startup must skip historical scans and cleanup');
        $priority->finishBootstrap = true;
        $priority->coordinatorTick();
        checkStartup($priority->events === ['bootstrap', 'bootstrap'] && $priority->quiesced === 0,
            'Successful startup must return before old-instance work');
        $priority->coordinatorTick();
        checkStartup(in_array('recycle', $priority->events, true) && $priority->quiesced === 3,
            'Next coordinator tick resumes existing asynchronous recycling');
        $worker = new CutoverFixtureGateway($manager);
        $worker->prepare(true);
        $master = new CutoverFixtureGateway($manager);
        $master->prepare(false);
        checkStartup($worker->detaches === 1 && $master->detaches === 1 && $master->stops === 0,
            'Master shutdown callback must honor worker handoff through shared state');
        echo "PASS startup: health gate, nginx failure, retry, deferred old/orphan drain, coordinator priority, handoff\n";
        if (extension_loaded('swoole')) {
            $timerPath = $root . '-timer';
            $timerManager = new \Scf\Server\Gateway\AppInstanceManager(new \Scf\Server\Gateway\UpstreamRegistry($timerPath));
            $timerManager->bootstrap('old', '127.0.0.1', 30101, true, 100, ['managed' => true, 'pid' => 123]);
            $timerManager->registerUpstream('new', '127.0.0.1', 30102, 100, ['managed' => true]);
            $delayed = new DelayedExitLauncher();
            $timerGateway = new TimerRecycleGateway($timerManager);
            $timerGateway->setLauncher($delayed);
            $beats = 0;
            try {
                \Swoole\Coroutine\run(function () use ($timerGateway, $timerManager, $delayed, $plan, &$beats): void {
                    checkStartup($timerGateway->complete($plan), 'New ingress is ready while the old process remains alive');
                    $timerGateway->retire();
                    checkStartup($timerGateway->pendingCount() === 1 && $delayed->oldAlive, 'Old exit must remain pending after retirement returns');
                    $heartbeat = \Swoole\Timer::tick(50, function () use (&$beats): void { $beats++; });
                    \Swoole\Timer::after(1200, function () use ($delayed): void { $delayed->oldAlive = false; });
                    $deadline = microtime(true) + 4;
                    while ($timerGateway->pendingCount() && microtime(true) < $deadline) \Swoole\Coroutine::sleep(0.05);
                    \Swoole\Timer::clear($heartbeat);
                    checkStartup($timerGateway->pendingCount() === 0 && $beats >= 15, 'Native timers must reclaim the old instance without blocking other work');
                    checkStartup($delayed->shutdownRequests === 1 && $timerManager->state()['active_version'] === 'new', 'Recycle only the old instance once');
                    checkStartup(!isset($timerManager->state()['generations']['old']), 'Completed recycle removes the historical generation');
                });
                echo "PASS Swoole timer recycle: delayed old exit, concurrent timer progress, preserved new generation\n";
            } finally {
                $timerGateway->clearWatchers();
                if (is_file($timerPath)) unlink($timerPath);
            }
        }
    } finally { if (is_file($root)) unlink($root); }
}
