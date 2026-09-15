<?php
declare(strict_types=1);

// Private Docker fixture: no app, DB, host config, published port, or network dependency.
// Run as PID 1, restart the container, then inspect its READY lines and / entry internally.
if (PHP_OS_FAMILY !== 'Linux' || getmypid() !== 1) {
    throw new RuntimeException('Run this fixture only as PID 1 in an isolated Linux container');
}
if (!class_exists(\Swoole\Coroutine::class)) {
    class RestartFixtureCliContext { public static function getCid(): int { return -1; } }
    class_alias(RestartFixtureCliContext::class, \Swoole\Coroutine::class);
}
spl_autoload_register(static function (string $class): void {
    if (str_starts_with($class, 'Scf\\')) {
        $file = dirname(__DIR__, 2) . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
        if (is_file($file)) require_once $file;
    }
});
define('APP_DIR_NAME', 'container_fixture');
define('SERVER_ROLE', 'master');
define('SERVER_RUN_ENV', 'dev');
define('APP_SRC_TYPE', 'dir');
$root = '/tmp/scf-nginx-container-restart';
if (!is_dir($root . '/servers')) mkdir($root . '/servers', 0700, true);
define('APP_UPDATE_DIR', $root . '/update');

class RestartFixtureGateway extends \Scf\Server\Gateway\GatewayServer {
    public function __construct() {}
    public function nginxProxyModeEnabled(): bool { return true; }
    public function businessPort(): int { return 18880; }
    public function serverConfig(): array { return []; }
    public function resolvedGatewayIngressProbeHost(?int $port = null): string { return 'localhost'; }
}
class RestartFixtureNginx extends \Scf\Server\Gateway\GatewayNginxProxyHandler {
    public function __construct(string $root, private int $bootCount) {
        parent::__construct(new RestartFixtureGateway(), new \Scf\Server\Gateway\AppInstanceManager(
            new \Scf\Server\Gateway\UpstreamRegistry($root . '/registry.json')));
        $this->nginxRuntimeMeta = ['bin' => '/usr/sbin/nginx', 'conf_path' => $root . '/nginx.conf',
            'conf_dir' => $root . '/servers', 'pid_path' => $root . '/nginx.pid'];
    }
    protected function renderGlobalConfig(): string { return "# restart fixture\n"; }
    protected function renderUpstreamConfig(): string { return "# no app upstream\n"; }
    protected function renderServerConfig(): string {
        return 'server { listen 127.0.0.1:18880; location / { return 200 "boot-' . $this->bootCount . '"; }}';
    }
    protected function activeInstances(): array { return []; }
    protected function nginxServerFileExists(): bool { return true; }
}

$count = is_file($root . '/count') ? (int)file_get_contents($root . '/count') + 1 : 1;
file_put_contents($root . '/nginx.conf', 'worker_processes 1; pid ' . $root . '/nginx.pid; error_log '
    . $root . '/error.log notice; events {} http { access_log off; include ' . $root . '/servers/*; }');
// Force the stale-file collision on every boot; never send a signal to the reused PHP/PID 1.
file_put_contents($root . '/nginx.pid', (string)getmypid());
$handler = new RestartFixtureNginx($root, $count);
$handler->sync('register_managed_plan_activate');
$handler->sync('rolling_restart_activate');
if (file_get_contents('http://127.0.0.1:18880/') !== 'boot-' . $count) {
    throw new RuntimeException('The current container generation did not reach nginx');
}
file_put_contents($root . '/count', (string)$count);
echo 'READY boot=', $count, ' php_pid=', getmypid(), ' nginx_pid=', trim(file_get_contents($root . '/nginx.pid')), PHP_EOL;
flush();
while (true) sleep(1);
