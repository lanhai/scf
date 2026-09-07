<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/src/Command/Bootstrap/Support.php';
require_once dirname(__DIR__) . '/src/Command/Bootstrap/ServerRuntimeMain.php';
require_once dirname(__DIR__) . '/src/Command/Bootstrap/ServerRuntimePorts.php';

function checkBootHandoff(bool $ok, string $message): void { if (!$ok) throw new RuntimeException($message); }
$root = sys_get_temp_dir() . '/scf-boot-handoff-' . bin2hex(random_bytes(5));
mkdir($root . '/scf', 0700, true);
mkdir($root . '/var', 0700, true);
define('SCF_ROOT', $root . '/scf');
$socket = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
$port = (int)substr(strrchr(stream_socket_get_name($socket, false), ':'), 1);
fclose($socket);
$argvFixture = ['boot', 'gateway', 'start', '-app=startup_fixture', '-role=master', '-port=' . $port];
file_put_contents($root . '/boot', <<<'PHP'
<?php
$port = (int)substr($argv[5], strlen('-port='));
while (true) {
    $pid = pcntl_fork();
    if ($pid === 0) {
        $socket = stream_socket_server('tcp://127.0.0.1:' . $port, $errno, $error);
        if (!$socket) exit(1);
        echo getmypid(), PHP_EOL;
        fflush(STDOUT);
        while (true) usleep(100000);
    }
    pcntl_waitpid($pid, $status);
}
PHP);
$process = proc_open([PHP_BINARY, $root . '/boot', 'gateway', 'start', '-app=startup_fixture', '-role=master', '-port=' . $port],
    [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
$parent = (int)proc_get_status($process)['pid'];
stream_set_timeout($pipes[1], 3);
$child = (int)trim((string)fgets($pipes[1]));
try {
    checkBootHandoff($child > 0, 'Legacy fixture child must start');
    $ancestors = scf_conflicting_boot_ancestor_pids($argvFixture, [$child]);
    checkBootHandoff(in_array($parent, $ancestors, true), 'Discover the respawning boot above the listener');
    $other = $argvFixture;
    $other[3] = '-app=startup_fixture2';
    checkBootHandoff(scf_conflicting_boot_ancestor_pids($other, [$child]) === [], 'Do not match app prefixes');
    $other = $argvFixture;
    $other[5] = '-port=' . ($port + 1);
    checkBootHandoff(scf_conflicting_boot_ancestor_pids($other, [$child]) === [], 'Do not stop a different port owner');
    foreach (['-role=slave', '-dev', '-port=' . ($port + 1)] as $option) {
        $other = [...$argvFixture, $option];
        checkBootHandoff(scf_conflicting_listener_pids($other, [$port]) === [], 'Do not reclaim a different command identity: ' . $option);
    }
    scf_prepare_command_ports_for_start($argvFixture);
    usleep(250000);
    checkBootHandoff(!proc_get_status($process)['running'], 'Legacy boot must not respawn after listener recovery');
    checkBootHandoff(!scf_is_port_listening('127.0.0.1', $port), 'The reclaimed port must stay free');
    $ownFlag = scf_process_control_flag_path($argvFixture, 'stop.' . getmypid());
    $otherFlag = scf_process_control_flag_path($argvFixture, 'stop.' . (getmypid() + 100000));
    file_put_contents($otherFlag, 'handoff');
    checkBootHandoff(!scf_should_stop_server_process_loop($argvFixture), 'Do not consume another boot handoff');
    file_put_contents($ownFlag, 'handoff');
    checkBootHandoff(scf_should_stop_server_process_loop($argvFixture), 'The old boot must honor its targeted stop');
    checkBootHandoff(is_file($otherFlag), 'Other boot flag remains intact');
    unlink($otherFlag);
    echo "PASS boot: legacy respawn prevention, exact ownership, targeted handoff stop\n";
} finally {
    if (proc_get_status($process)['running']) proc_terminate($process, SIGKILL);
    if ($child > 0 && posix_kill($child, 0)) posix_kill($child, SIGKILL);
    foreach ($pipes as $pipe) fclose($pipe);
    proc_close($process);
    foreach (glob($root . '/var/*') ?: [] as $path) unlink($path);
    unlink($root . '/boot');
    rmdir($root . '/var');
    rmdir($root . '/scf');
    rmdir($root);
}
