<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/src/Command/Bootstrap/Support.php';
require_once dirname(__DIR__) . '/src/Command/Bootstrap/ServerRuntimeMain.php';
require_once dirname(__DIR__) . '/src/Command/Bootstrap/ServerRuntimePorts.php';

function checkHandoffRecovery(bool $ok, string $message): void {
    if (!$ok) throw new RuntimeException($message);
}

$root = sys_get_temp_dir() . '/scf-handoff-recovery-' . bin2hex(random_bytes(5));
mkdir($root . '/scf', 0700, true);
mkdir($root . '/apps/startup_fixture/update', 0700, true);
mkdir($root . '/var', 0700, true);
define('SCF_ROOT', $root . '/scf');
define('SCF_APPS_ROOT', $root . '/apps');
$bootstrapDir = dirname(__DIR__) . '/src/Command/Bootstrap/';
file_put_contents($root . '/guard.php', '<?php ' . "\n"
    . 'define("SCF_ROOT", ' . var_export(SCF_ROOT, true) . ");\n"
    . 'define("SCF_APPS_ROOT", ' . var_export(SCF_APPS_ROOT, true) . ");\n"
    . "define('IS_SERVER_PROCESS_START', true);\n"
    . 'require ' . var_export($bootstrapDir . 'Support.php', true) . ";\n"
    . 'require ' . var_export($bootstrapDir . 'ServerRuntimeMain.php', true) . ";\n"
    . 'require ' . var_export($bootstrapDir . 'ServerRuntimePorts.php', true) . ";\n"
    . 'scf_run_server_process_loop($argv);');
// A real, owned PHP listener reproduces a Swoole process retaining its control FD
// after workers stop responding. Each fixture owns only dynamically allocated ports.
file_put_contents($root . '/boot', <<<'PHP'
<?php
$opts = [];
foreach ($argv as $arg) if (preg_match('/^-([^=]+)=(.*)$/', $arg, $m)) $opts[$m[1]] = $m[2];
$mode = $opts['fixture'];
if ($mode === 'respawning') {
    while (true) {
        $child = pcntl_fork();
        if ($child === 0) break;
        pcntl_waitpid($child, $status);
        sleep(2);
    }
}
$socket = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
$port = (int)substr(strrchr(stream_socket_get_name($socket, false), ':'), 1);
$leaseFile = __DIR__ . '/apps/startup_fixture/update/gateway_lease_startup_fixture_dev_master_' . $opts['port'] . '.json';
$lease = ['app' => 'startup_fixture', 'env' => 'dev', 'role' => 'master', 'gateway_port' => (int)$opts['port'],
    'epoch' => 1, 'state' => $mode === 'stopped' ? 'stopped' : 'running', 'expires_at' => time() + 60,
    'meta' => ['gateway_pid' => getmypid(), 'grace_seconds' => 20, 'restart_grace_seconds' => 120]];
if ($mode === 'expired') $lease['expires_at'] = time() - 200;
file_put_contents($leaseFile, json_encode($lease));
echo $port, PHP_EOL;
fflush(STDOUT);
$client = stream_socket_accept($socket, 10);
if (!$client) exit(2);
stream_set_timeout($client, 2);
while (($line = fgets($client)) !== false && trim($line) !== '') {}
if (in_array($mode, ['lost_ack', 'shutting_down'], true)) {
    $lease['state'] = 'restarting';
    file_put_contents($leaseFile, json_encode($lease));
    if ($mode === 'shutting_down') {
        $body = json_encode(['accepted' => false, 'message' => 'Gateway 已在关闭中']);
        fwrite($client, "HTTP/1.1 409 Conflict\r\nContent-Length: " . strlen($body) . "\r\nConnection: close\r\n\r\n" . $body);
    }
    fclose($client);
    usleep(600000);
    exit(0);
}
if (in_array($mode, ['disappeared', 'respawning'], true)) { fclose($client); exit(0); }
if ($mode === 'rejected') {
    $body = json_encode(['accepted' => false, 'message' => 'fixture identity rejected']);
    fwrite($client, "HTTP/1.1 409 Conflict\r\nContent-Length: " . strlen($body) . "\r\nConnection: close\r\n\r\n" . $body);
}
fclose($client);
while (true) usleep(100000);
PHP);

$process = null;
$pipes = [];
try {
    foreach (['stopped', 'expired', 'lost_ack', 'shutting_down', 'disappeared', 'respawning', 'rejected', 'active'] as $mode) {
        $reserved = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        $businessPort = (int)substr(strrchr(stream_socket_get_name($reserved, false), ':'), 1);
        fclose($reserved);
        $args = [$root . '/boot', 'gateway', 'start', '-app=startup_fixture', '-role=master', '-port=' . $businessPort, '-dev'];
        $process = proc_open([PHP_BINARY, ...$args, '-fixture=' . $mode], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        stream_set_timeout($pipes[1], 3);
        $controlPort = (int)trim((string)fgets($pipes[1]));
        checkHandoffRecovery($controlPort > 0, 'Fixture must listen: ' . $mode);
        $args[] = '-control_port=' . $controlPort;
        if ($mode === 'active') {
            $guard = proc_open([PHP_BINARY, $root . '/guard.php', ...array_slice($args, 1)],
                [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $guardPipes);
            $stdout = stream_get_contents($guardPipes[1]);
            $stderr = stream_get_contents($guardPipes[2]);
            foreach ($guardPipes as $pipe) fclose($pipe);
            checkHandoffRecovery(proc_close($guard) === 1, 'Unconfirmed active owner must produce a controlled nonzero exit');
            checkHandoffRecovery(!str_contains($stdout . $stderr, 'Fatal error') && str_contains($stderr, 'lease=running'),
                'Report actionable lease state without an uncaught PHP fatal');
            checkHandoffRecovery(proc_get_status($process)['running'], 'A valid lease must prevent timeout-based killing');
        } elseif ($mode === 'rejected') {
            try {
                scf_prepare_command_ports_for_start($args);
                throw new RuntimeException('An explicit rejection must stop this startup');
            } catch (RuntimeException $e) {
                checkHandoffRecovery(str_contains($e->getMessage(), 'fixture identity rejected'), 'Report the rejection reason');
            }
            checkHandoffRecovery(proc_get_status($process)['running'], 'Do not signal an explicitly rejecting live owner');
        } else {
            scf_prepare_command_ports_for_start($args);
            checkHandoffRecovery(!scf_is_port_listening('127.0.0.1', $controlPort), 'Release the old control FD: ' . $mode);
            if ($mode === 'respawning') {
                usleep(100000);
                checkHandoffRecovery(!proc_get_status($process)['running'], 'A lost response must not leave the old boot respawning');
            }
        }
        echo "PASS handoff recovery: {$mode}\n";
        if (proc_get_status($process)['running']) proc_terminate($process, SIGKILL);
        foreach ($pipes as $pipe) fclose($pipe);
        proc_close($process);
        $process = null;
        $pipes = [];
    }
} finally {
    if (is_resource($process)) {
        if (proc_get_status($process)['running']) proc_terminate($process, SIGKILL);
        foreach ($pipes as $pipe) fclose($pipe);
        proc_close($process);
    }
    foreach (glob($root . '/apps/startup_fixture/update/*') ?: [] as $file) unlink($file);
    unlink($root . '/boot');
    unlink($root . '/guard.php');
    foreach (['/apps/startup_fixture/update', '/apps/startup_fixture', '/apps', '/var', '/scf', ''] as $dir) rmdir($root . $dir);
}
