<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/src/Util/ProcessInspector.php';
require_once dirname(__DIR__) . '/src/Util/ProcessCommandLine.php';

use Scf\Util\ProcessInspector;
use Scf\Util\ProcessCommandLine;

function checkProcfs(bool $ok, string $message): void {
    if (!$ok) throw new RuntimeException($message);
}

$parse = new ReflectionMethod(ProcessInspector::class, 'parseLinuxStat');
$fields = array_fill(0, 20, '0');
$fields[0] = 'S';
$fields[1] = '7';
$fields[19] = '123456789';
$stat = '42 (worker (name) with ) space) ' . implode(' ', $fields);
$parsed = $parse->invoke(null, $stat, 42);
checkProcfs($parsed === ['ppid' => 7, 'state' => 'S', 'name' => 'worker (name) with ) space', 'start_time' => '123456789'],
    'stat fields must survive spaces and parentheses in comm');
checkProcfs($parse->invoke(null, $stat, 43) === null, 'Reject a different PID');
checkProcfs($parse->invoke(null, '42 (truncated) S 1', 42) === null, 'Reject an incomplete stat record');

if (PHP_OS_FAMILY !== 'Linux') {
    echo "PASS procfs stat parser (live Linux checks require a Linux container)\n";
    return;
}

$code = 'echo getmypid(), PHP_EOL; fflush(STDOUT); fgets(STDIN);';
$process = proc_open([PHP_BINARY, '-r', $code, '--', '-app=procfs-fixture', '-port=39821'],
    [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
checkProcfs(is_resource($process), 'Start the private child');
try {
    stream_set_timeout($pipes[1], 5);
    $pid = (int)trim((string)fgets($pipes[1]));
    checkProcfs($pid > 0, 'Child must publish its PID');
    // No BoundedProcessRunner is loaded: these operations must not require ps or procps.
    $record = ProcessInspector::find($pid, true);
    checkProcfs(($record['ppid'] ?? 0) === getmypid(), 'Read the real parent relationship');
    checkProcfs(ProcessCommandLine::hasOptionValue($record['command'] ?? '', 'app', 'procfs-fixture')
        && ProcessCommandLine::hasOptionValue($record['command'] ?? '', 'port', '39821'), 'Retain exact CLI ownership options');
    checkProcfs(!str_contains($record['command'], "\0"), 'Normalize argv separators for existing matchers');
    $snapshot = ProcessInspector::snapshot(true);
    checkProcfs(isset($snapshot[$pid], $snapshot[getmypid()]), 'Enumerate the current PID namespace without ps');
    fwrite($pipes[0], "stop\n");
    foreach ($pipes as $pipe) fclose($pipe);
    $pipes = [];
    checkProcfs(proc_close($process) === 0, 'Reap the private child');
    $process = null;
    checkProcfs(ProcessInspector::find($pid, true) === null, 'Never reuse a cached identity after process exit');
    checkProcfs(ProcessInspector::find(0, true) === null, 'Reject invalid PIDs');
    echo "PASS procfs: stat parsing, live identity, parent tree, exact CLI options, refresh after exit, no ps dependency\n";
} finally {
    if (is_resource($process)) {
        if (proc_get_status($process)['running']) proc_terminate($process, SIGKILL);
        foreach ($pipes as $pipe) fclose($pipe);
        proc_close($process);
    }
}
