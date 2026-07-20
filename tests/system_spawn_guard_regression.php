<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../src/Command/Bootstrap/ServerRuntimeMain.php';
require_once __DIR__ . '/../src/Command/Bootstrap/ServerRuntimePorts.php';
spl_autoload_register(static function (string $class): void {
    if (!str_starts_with($class, 'Scf\\')) {
        return;
    }
    $path = __DIR__ . '/../src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
    if (is_file($path)) {
        require_once $path;
    }
});

use Scf\Core\Log;
use Scf\Core\Server;
use Scf\Server\Gateway\AppServerLauncher;
use Scf\Server\Task\RQueue;
use Scf\Util\BoundedProcessRunner;
use Scf\Util\ProcessCommandLine;
use Scf\Util\ReverseFileReader;

function assertSpawnGuard(bool $condition, string $message): void {
    if (!$condition) {
        fwrite(STDERR, "FAILED: {$message}\n");
        exit(1);
    }
}

function sourceFile(string $relativePath): string {
    $source = file_get_contents(__DIR__ . '/../' . $relativePath);
    assertSpawnGuard(is_string($source), "cannot read {$relativePath}");
    return $source;
}

$coreServer = sourceFile('src/Core/Server.php');
$memoryMonitor = sourceFile('src/Util/MemoryMonitor.php');
$log = sourceFile('src/Core/Log.php');
$masterDb = sourceFile('src/Server/MasterDB.php');
$gatewayServer = sourceFile('src/Server/Gateway/GatewayServer.php');
$gatewayTelemetry = sourceFile('src/Server/Gateway/GatewayTelemetryTrait.php');
$managedLifecycle = sourceFile('src/Server/Gateway/GatewayManagedUpstreamLifecycleTrait.php');
$appServerLauncher = sourceFile('src/Server/Gateway/AppServerLauncher.php');
$runtimePorts = sourceFile('src/Command/Bootstrap/ServerRuntimePorts.php');
$subProcessManager = sourceFile('src/Server/SubProcessManager.php');
$redisQueue = sourceFile('src/Server/SubProcess/RedisQueueProcess.php');
$queueRuntime = sourceFile('src/Server/Task/RQueue.php');
$boundedRunner = sourceFile('src/Util/BoundedProcessRunner.php');
$processInspector = sourceFile('src/Util/ProcessInspector.php');
$processCommandLine = sourceFile('src/Util/ProcessCommandLine.php');
$constants = sourceFile('src/Const.php');
$gatewayCommand = sourceFile('src/Command/DefaultCommand/Gateway.php');
$gatewayCliBootstrap = sourceFile('src/Server/Gateway/CliBootstrap.php');
$nginxProxyHandler = sourceFile('src/Server/Gateway/GatewayNginxProxyHandler.php');
$linuxCrontabManager = sourceFile('src/Server/LinuxCrontab/LinuxCrontabManager.php');
$defaultCrontabCommand = sourceFile('src/Command/DefaultCommand/Crontab.php');
$directoryUtility = sourceFile('src/Util/Dir.php');
$crontabManager = sourceFile('src/Server/Task/CrontabManager.php');
$crontabManagerProcess = sourceFile('src/Server/SubProcess/CrontabManagerProcess.php');
$rootProcessGuard = sourceFile('src/Server/RootProcessRespawnGuard.php');
$runtimeProcessBase = sourceFile('src/Server/SubProcess/AbstractRuntimeProcess.php');
$managedRuntimeProcesses = [
    'GatewayClusterCoordinator' => sourceFile('src/Server/SubProcess/GatewayClusterCoordinatorProcess.php'),
    'GatewayBusinessCoordinator' => sourceFile('src/Server/SubProcess/GatewayBusinessCoordinatorProcess.php'),
    'GatewayHealthMonitor' => sourceFile('src/Server/SubProcess/GatewayHealthMonitorProcess.php'),
    'MemoryUsageCount' => sourceFile('src/Server/SubProcess/MemoryUsageCountProcess.php'),
    'Heartbeat' => sourceFile('src/Server/SubProcess/HeartbeatProcess.php'),
    'LogBackup' => sourceFile('src/Server/SubProcess/LogBackupProcess.php'),
    'CrontabManager' => $crontabManagerProcess,
    'RedisQueue' => $redisQueue,
    'FileWatch' => sourceFile('src/Server/SubProcess/FileWatchProcess.php'),
];

assertSpawnGuard(!str_contains($coreServer, 'command -v'), 'Core Server must not resolve tools through a shell');
assertSpawnGuard(!str_contains($coreServer, 'shell_exec('), 'Core Server port helpers must not use shell_exec');
assertSpawnGuard(!str_contains($memoryMonitor, 'shell_exec('), 'memory sampler must not use shell_exec');
assertSpawnGuard(str_contains($memoryMonitor, 'BoundedProcessRunner::output'), 'memory sampler must retain a hard command deadline');
assertSpawnGuard(str_contains($memoryMonitor, '$localSnapshots'), 'memory sampler must retain a process-local last-good fallback');
assertSpawnGuard(substr_count($memoryMonitor, 'getPssRssByPid(') === 1, 'single-PID memory sampling must remain a wrapper only');
assertSpawnGuard(!str_contains($gatewayTelemetry, 'getPssRssByPid('), 'Gateway telemetry must consume the batch memory snapshot');
assertSpawnGuard(!str_contains($log, 'wc -l'), 'Log line counter must stay in-process');
assertSpawnGuard(!str_contains($log, 'tac '), 'Log pagination must stay in-process');
assertSpawnGuard(!str_contains($masterDb, 'wc -l'), 'MasterDB line counter must stay in-process');
assertSpawnGuard(!str_contains($masterDb, 'tac '), 'MasterDB log pagination must stay in-process');
assertSpawnGuard(str_contains($gatewayServer, 'gatewayClusterTickInFlight'), 'Gateway tick must retain local single-flight protection');
assertSpawnGuard(str_contains($runtimePorts, 'scf_collect_occupied_listening_ports($ports, false)'), 'startup polling must use the native probe');
assertSpawnGuard(str_contains($subProcessManager, 'ProcessRespawnBackoff'), 'managed subprocesses must keep crash-loop backoff');
assertSpawnGuard(str_contains($redisQueue, 'ProcessRespawnBackoff'), 'RedisQueue worker must keep crash-loop backoff');
assertSpawnGuard(str_contains($managedLifecycle, '$lastForceAt'), 'managed recycle must read its force retry timestamp');
assertSpawnGuard(str_contains($managedLifecycle, 'managedRecycleForceRetryIntervalSeconds'), 'managed recycle must retain force retry backoff');
assertSpawnGuard(str_contains($managedLifecycle, 'elseif (!$shouldForceKill && $windowDue)'), 'deadline retry wait must not emit a log every tick');
assertSpawnGuard(substr_count($appServerLauncher, '$snapshot = $this->loadProcessSnapshot();') === 1, 'force-stop path must reuse one process snapshot');
assertSpawnGuard(!str_contains($appServerLauncher, "shell_exec('ps -o command="), 'force-stop ownership must reuse process snapshot commands');
assertSpawnGuard(str_contains($appServerLauncher, 'commandMatchesManagedOwnership($masterCommand, $instance)'), 'metadata master PID must be owner-verified');
assertSpawnGuard(!str_contains($appServerLauncher, '$targetRootPids[$pid] = $pid;'), 'unverified metadata PID must never be killed directly');
assertSpawnGuard(str_contains($runtimePorts, 'stream_set_blocking'), 'bootstrap command pipes must be non-blocking');
assertSpawnGuard(str_contains($runtimePorts, 'proc_terminate'), 'bootstrap command runner must terminate timed-out children');
assertSpawnGuard(str_contains($runtimePorts, '__SCF_BOOTSTRAP_DEFERRED_PROCESSES'), 'bootstrap runner must retain its deferred-process circuit breaker');
assertSpawnGuard(str_contains($boundedRunner, 'KILL_GRACE_SECONDS'), 'shared process runner must retain TERM/KILL timeout enforcement');
assertSpawnGuard(str_contains($boundedRunner, 'system probe circuit open'), 'shared process runner must refuse probes while an unkillable child remains');
assertSpawnGuard(str_contains($boundedRunner, "'truncated' =>"), 'shared process runner must expose output truncation');
assertSpawnGuard(str_contains($processInspector, 'FAILURE_CACHE_TTL_SECONDS'), 'failed ps samples must be negatively cached');
assertSpawnGuard(str_contains($processInspector, '$sampleInFlight'), 'shared ps snapshot must retain single-flight protection');
assertSpawnGuard(str_contains($processInspector, "\$result['truncated']"), 'process snapshots must reject truncated ps output');
assertSpawnGuard(str_contains($processCommandLine, 'hash_equals'), 'process owner options must use exact value matching');
assertSpawnGuard(!str_contains($constants, 'shell_exec('), 'loading SCF constants must never launch Composer');
assertSpawnGuard(!str_contains($constants, 'composer show'), 'loading SCF constants must read local Composer metadata');
assertSpawnGuard(!str_contains($gatewayCommand, 'shell_exec('), 'gateway CLI process discovery must use bounded shared probes');
assertSpawnGuard(
    !str_contains($gatewayCommand, "str_contains(\$command, '-role=' . \$role)")
    && !str_contains($gatewayCommand, "str_contains(\$command, \$portFlag)"),
    'destructive Gateway process discovery must not use prefix-prone option matching'
);
assertSpawnGuard(
    str_contains($gatewayCommand, 'return ProcessInspector::snapshot(true);'),
    'destructive Gateway process discovery must use a fresh process snapshot'
);
assertSpawnGuard(!str_contains($gatewayCliBootstrap, 'shell_exec('), 'gateway bootstrap orphan discovery must use the shared process snapshot');
assertSpawnGuard(!str_contains($redisQueue, "(time() - (int)(\$queueWorkerRow['usage_updated'] ?? 0)) <= 15"), 'queue worker identity must not depend on memory sample freshness');
assertSpawnGuard(str_contains($redisQueue, 'clearQueueWorkerRuntimeIfCurrent'), 'queue runtime cleanup must be owner-aware');
assertSpawnGuard(str_contains($queueRuntime, 'LOCK_EX | LOCK_NB'), 'queue worker must retain a cross-generation singleton lock');
assertSpawnGuard(str_contains($queueRuntime, 'RUNTIME_REDIS_QUEUE_WORKER_STATE'), 'queue worker must publish token-bound state');
assertSpawnGuard(str_contains($queueRuntime, 'workerLockOwner'), 'queue worker lock must expose exact owner identity');
assertSpawnGuard(str_contains($redisQueue, 'expectedStateManagerPid'), 'queue runtime cleanup must compare the complete owner tuple');
assertSpawnGuard(str_contains($appServerLauncher, 'isTrustedLaunchedProcess'), 'launcher must retain lock-bound startup cleanup');
assertSpawnGuard(str_contains($appServerLauncher, 'launch_lock_file'), 'launcher must publish its startup identity lock');
assertSpawnGuard(!str_contains($nginxProxyHandler, 'shell_exec('), 'nginx control must not use shell_exec');
assertSpawnGuard(!preg_match('/(?<!function )\bexec\s*\(/', $nginxProxyHandler), 'nginx control must not use unbounded exec');
assertSpawnGuard(str_contains($nginxProxyHandler, 'BoundedProcessRunner::run'), 'nginx control must retain hard deadlines');
assertSpawnGuard(!str_contains($linuxCrontabManager, 'shell_exec('), 'Linux crontab manager must not resolve tools through a shell');
assertSpawnGuard(!preg_match('/(?<!function )\bexec\s*\(/', $linuxCrontabManager), 'Linux crontab manager must not use unbounded exec');
assertSpawnGuard(str_contains($linuxCrontabManager, 'BoundedProcessRunner::run'), 'Linux crontab mutations must retain hard deadlines');
assertSpawnGuard(
    str_contains($linuxCrontabManager, '$current = $this->readSystemCrontab(true);'),
    'Linux crontab sync must abort when the current user crontab cannot be read safely'
);
assertSpawnGuard(!str_contains($defaultCrontabCommand, 'shell_exec('), 'default crontab command must reuse process snapshots');
assertSpawnGuard(!preg_match('/(?<!function )\bexec\s*\(/', $directoryUtility), 'directory scanning must stay in-process');
assertSpawnGuard(str_contains($crontabManager, 'ProcessRespawnBackoff'), 'crontab tasks must retain per-task crash-loop backoff');
assertSpawnGuard(str_contains($crontabManager, 'TASK_RESPAWN_STATE_KEY_PREFIX'), 'crontab task backoff must survive manager replacement');
assertSpawnGuard(str_contains($crontabManagerProcess, '$taskDiscoveryBackoff'), 'empty crontab discovery must remain rate-limited');
assertSpawnGuard(str_contains($rootProcessGuard, 'ProcessRespawnBackoff'), 'root custom processes must retain cross-generation backoff');
assertSpawnGuard(str_contains($rootProcessGuard, 'finish(bool $intentional)'), 'intentional root process exits must bypass crash penalties');
assertSpawnGuard(
    str_contains($runtimeProcessBase, 'claimRuntimeOwnership')
    && str_contains($runtimeProcessBase, 'ownsRuntimeProcess')
    && str_contains($runtimeProcessBase, 'clearRuntimeOwnershipIfCurrent'),
    'managed subprocess generation/PID ownership helpers must remain centralized'
);
foreach ($managedRuntimeProcesses as $managedName => $managedSource) {
    assertSpawnGuard(
        str_contains($managedSource, 'captureManagerGeneration'),
        "{$managedName} must capture its SubProcessManager generation"
    );
    assertSpawnGuard(
        str_contains($managedSource, 'claimRuntimeOwnership'),
        "{$managedName} must claim a generation-bound PID slot"
    );
    assertSpawnGuard(
        str_contains($managedSource, 'clearRuntimeOwnershipIfCurrent'),
        "{$managedName} cleanup must compare generation and PID ownership"
    );
}
assertSpawnGuard(
    str_contains($redisQueue, 'RQueue::startProcess($queueWorkerToken, $managerGeneration)'),
    'RedisQueue manager must pass its generation to worker startup'
);
assertSpawnGuard(
    str_contains($redisQueue, "\$result['stale_lock']")
    && str_contains($queueRuntime, "rename(\$path, \$stalePath)"),
    'dead RedisQueue lease owners must rotate inherited stale lock inodes'
);
assertSpawnGuard(
    !str_contains($processInspector, 'FORCE_COALESCE_SECONDS')
    && str_contains($processInspector, 'if (!$forceRefresh && self::$sampledAt > 0'),
    'destructive process snapshots must never reuse a prior sample'
);
assertSpawnGuard(
    !str_contains($coreServer, 'PORT_PID_FORCE_COALESCE_SECONDS')
    && str_contains($coreServer, 'return $forceRefresh ? []'),
    'destructive port-owner snapshots must never reuse prior or in-flight samples'
);

$deadlineStartedAt = microtime(true);
$deadlineResult = BoundedProcessRunner::run(['/bin/sleep', '2'], 0.10);
$deadlineElapsed = microtime(true) - $deadlineStartedAt;
assertSpawnGuard($deadlineResult['timed_out'] === true, 'bounded process runner did not report timeout');
assertSpawnGuard($deadlineElapsed < 1.0, 'bounded process runner exceeded its hard deadline');

$echoResult = BoundedProcessRunner::run(['/bin/echo', 'spawn-guard-ok'], 1.0);
assertSpawnGuard(
    $echoResult['timed_out'] === false && trim($echoResult['output']) === 'spawn-guard-ok',
    'bounded process runner changed successful command output'
);

$largeOutputResult = BoundedProcessRunner::run([
    PHP_BINARY,
    '-r',
    'echo str_repeat("x", 2048);',
], 1.0, 1024);
assertSpawnGuard(
    $largeOutputResult['truncated'] === true && strlen($largeOutputResult['output']) === 1024,
    'bounded process runner did not expose output truncation'
);

if (function_exists('pcntl_signal')) {
    $ignoreTermCode = <<<'PHP'
pcntl_signal(SIGTERM, SIG_IGN);
echo getmypid(), PHP_EOL;
fflush(STDOUT);
while (true) {
    usleep(10000);
}
PHP;
    $killStartedAt = microtime(true);
    $killResult = BoundedProcessRunner::run([PHP_BINARY, '-r', $ignoreTermCode], 0.10);
    $killElapsed = microtime(true) - $killStartedAt;
    $ignoredTermPid = (int)trim($killResult['output']);
    assertSpawnGuard($killResult['timed_out'] === true, 'SIGTERM-ignoring child did not time out');
    assertSpawnGuard($killElapsed < 1.0, 'SIGTERM-ignoring child exceeded the SIGKILL deadline');
    assertSpawnGuard(
        $ignoredTermPid > 0 && !@\Swoole\Process::kill($ignoredTermPid, 0),
        'shared runner left the SIGTERM-ignoring child alive'
    );

    $bootstrapPidFile = tempnam(sys_get_temp_dir(), 'scf-bootstrap-pid-');
    assertSpawnGuard(is_string($bootstrapPidFile), 'cannot allocate bootstrap child pid file');
    try {
        $bootstrapCode = 'file_put_contents(' . var_export($bootstrapPidFile, true) . ', (string)getmypid());'
            . 'pcntl_signal(SIGTERM, SIG_IGN);'
            . 'while (true) { usleep(10000); }';
        $bootstrapStartedAt = microtime(true);
        $bootstrapOutput = scf_process_output([PHP_BINARY, '-r', $bootstrapCode], 0.10);
        $bootstrapElapsed = microtime(true) - $bootstrapStartedAt;
        $bootstrapChildPid = (int)trim((string)file_get_contents($bootstrapPidFile));
        assertSpawnGuard($bootstrapOutput === '', 'bootstrap runner leaked timed-out output');
        assertSpawnGuard($bootstrapElapsed < 1.0, 'bootstrap runner exceeded the SIGKILL deadline');
        assertSpawnGuard(
            $bootstrapChildPid > 0 && !@\Swoole\Process::kill($bootstrapChildPid, 0),
            'bootstrap runner left the SIGTERM-ignoring child alive'
        );
        assertSpawnGuard(
            trim(scf_process_output(['/bin/echo', 'bootstrap-recovered'], 1.0)) === 'bootstrap-recovered',
            'bootstrap runner did not recover after a timed-out child'
        );
    } finally {
        @unlink($bootstrapPidFile);
    }
}

$outerAttempts = 0;
foreach ([2, 4, 8, 16, 32, 60] as $expectedDelay) {
    $actualDelay = scf_server_process_restart_delay_seconds(1.0, 1, 0, $outerAttempts);
    assertSpawnGuard($actualDelay === $expectedDelay, "outer loop expected {$expectedDelay}s, got {$actualDelay}s");
}
assertSpawnGuard(
    scf_server_process_restart_delay_seconds(30.0, 1, 0, $outerAttempts) === 2 && $outerAttempts === 0,
    'stable outer server must reset crash-loop backoff'
);

$logReflection = new ReflectionClass(Log::class);
$logObject = $logReflection->newInstanceWithoutConstructor();
$lineCounter = $logReflection->getMethod('countFileLines');
$lineCounter->setAccessible(true);
$tmp = tempnam(sys_get_temp_dir(), 'scf-log-counter-');
assertSpawnGuard(is_string($tmp), 'cannot allocate temporary log file');
try {
    file_put_contents($tmp, '');
    assertSpawnGuard($lineCounter->invoke($logObject, $tmp) === 0, 'empty log count changed');
    file_put_contents($tmp, "first\nsecond\n");
    assertSpawnGuard($lineCounter->invoke($logObject, $tmp) === 2, 'terminated log line count changed');
    file_put_contents($tmp, "first\nsecond");
    assertSpawnGuard($lineCounter->invoke($logObject, $tmp) === 2, 'unterminated log line count changed');
    file_put_contents($tmp, "first\nsecond\nthird\nfourth\n");
    assertSpawnGuard(
        ReverseFileReader::page($tmp, 0, 3) === ['fourth', 'third', 'second'],
        'reverse log first page changed'
    );
    assertSpawnGuard(
        ReverseFileReader::page($tmp, 1, 2) === ['third', 'second'],
        'reverse log offset page changed'
    );
    file_put_contents($tmp, "first\nsecond\nthird");
    assertSpawnGuard(
        ReverseFileReader::page($tmp, 0, 3) === ['third', 'second', 'first'],
        'reverse log unterminated final line changed'
    );
    $longLine = str_repeat('x', 5000);
    file_put_contents($tmp, "first\r\n\r\nthird\r\n{$longLine}\r\n");
    assertSpawnGuard(
        ReverseFileReader::page($tmp, 0, 4) === [$longLine, 'third', '', 'first'],
        'reverse log CRLF, blank-line, or cross-chunk handling changed'
    );
    assertSpawnGuard(
        ReverseFileReader::page($tmp, 99, 2) === [],
        'reverse log offset beyond EOF must stay empty'
    );
} finally {
    @unlink($tmp);
}

defined('APP_DIR_NAME') || define('APP_DIR_NAME', 'spawn-guard-app');
$launcherProbe = new class extends AppServerLauncher {
    public function resolveOwned(int $pid, array $snapshot, array $instance): int {
        return $this->resolveOwnedManagedRootPid($pid, $snapshot, $instance);
    }
};
$ownedInstance = [
    'port' => 10680,
    'rpc_port' => 11680,
    'metadata' => [
        'master_pid' => 4242,
        'gateway_port' => 9580,
        'rpc_port' => 11680,
        'owner_epoch' => 7,
    ],
];
assertSpawnGuard(
    $launcherProbe->resolveOwned(4242, [
        4242 => ['ppid' => 1, 'command' => '/usr/bin/unrelated-service'],
    ], $ownedInstance) === 0,
    'reused metadata master PID bypassed owner validation'
);
assertSpawnGuard(
    $launcherProbe->resolveOwned(4242, [
        4242 => [
            'ppid' => 1,
            'command' => 'php boot gateway_upstream start -app=spawn-guard-app -port=10680 -rport=11680 -gateway_port=9580 -gateway_epoch=7',
        ],
    ], $ownedInstance) === 4242,
    'valid managed master PID was no longer recognized'
);
foreach ([
    'php boot gateway_upstream start -app=spawn-guard-app2 -port=10680 -rport=11680 -gateway_port=9580 -gateway_epoch=7',
    'php boot gateway_upstream start -app=spawn-guard-app -port=106800 -rport=11680 -gateway_port=9580 -gateway_epoch=7',
    'php boot gateway_upstream start -app=spawn-guard-app -port=10680 -rport=116800 -gateway_port=9580 -gateway_epoch=7',
    'php boot gateway_upstream start -app=spawn-guard-app -port=10680 -rport=11680 -gateway_port=95800 -gateway_epoch=7',
] as $nearMatchCommand) {
    assertSpawnGuard(
        $launcherProbe->resolveOwned(4242, [
            4242 => ['ppid' => 1, 'command' => $nearMatchCommand],
        ], $ownedInstance) === 0,
        'near-match process option bypassed owner validation'
    );
}
assertSpawnGuard(
    ProcessCommandLine::hasOptionValue('-app=foo -port=9680', 'app', 'foo')
    && !ProcessCommandLine::hasOptionValue('-app=foo2 -port=96800', 'app', 'foo')
    && !ProcessCommandLine::hasOptionValue('-app=foo2 -port=96800', 'port', 9680)
    && ProcessCommandLine::hasOptionValue('-role=master -port=9680', 'role', 'master')
    && !ProcessCommandLine::hasOptionValue('-role=master2 -port=96800', 'role', 'master'),
    'process command option boundary matching changed'
);

$crontabReadProbe = new class extends \Scf\Server\LinuxCrontab\LinuxCrontabManager {
    /** @var array{output:string,error:string,exit_code:int,timed_out:bool,started:bool,truncated:bool} */
    public array $result = [];

    protected function resolveSystemCrontabCommand(): string {
        return '/usr/bin/crontab';
    }

    protected function runSystemCrontabListCommand(string $command): array {
        unset($command);
        return $this->result;
    }

    public function readForTest(bool $strict): string {
        return $this->readSystemCrontab($strict);
    }
};
$crontabReadProbe->result = [
    'output' => "# user entry\n",
    'error' => '',
    'exit_code' => 0,
    'timed_out' => false,
    'started' => true,
    'truncated' => false,
];
assertSpawnGuard(
    $crontabReadProbe->readForTest(true) === '# user entry',
    'successful crontab reads changed content semantics'
);
$crontabReadProbe->result = [
    'output' => '',
    'error' => 'no crontab for spawn-guard',
    'exit_code' => 1,
    'timed_out' => false,
    'started' => true,
    'truncated' => false,
];
assertSpawnGuard(
    $crontabReadProbe->readForTest(true) === '',
    'legitimate empty user crontab must remain a valid empty state'
);
foreach ([
    [
        'output' => '',
        'error' => 'system probe circuit open',
        'exit_code' => -1,
        'timed_out' => true,
        'started' => false,
        'truncated' => false,
    ],
    [
        'output' => '# partial user entry',
        'error' => '',
        'exit_code' => 0,
        'timed_out' => false,
        'started' => true,
        'truncated' => true,
    ],
] as $unsafeCrontabRead) {
    $crontabReadProbe->result = $unsafeCrontabRead;
    $readFailedClosed = false;
    try {
        $crontabReadProbe->readForTest(true);
    } catch (Throwable) {
        $readFailedClosed = true;
    }
    assertSpawnGuard($readFailedClosed, 'unsafe crontab read did not abort destructive sync');
}

$trustedLauncherProbe = new class extends AppServerLauncher {
    protected function buildCommand(
        string $app,
        string $env,
        string $role,
        int $port,
        int $rpcPort,
        string $src,
        array $extra
    ): array {
        return [PHP_BINARY, '-r', 'sleep(30);'];
    }

    protected function loadProcessSnapshot(): array {
        // 强制模拟 ps 本身超时，验证回收只依赖 launcher 的唯一身份锁。
        return [];
    }

    public function trusted(int $pid, array $metadata): bool {
        return $this->isTrustedLaunchedProcess($pid, $metadata);
    }
};
$trustedSpec = $trustedLauncherProbe->launch(['app' => 'spawn-guard', 'port' => 19001]);
usleep(250000);
$trustedMetadata = [
    'managed' => true,
    'pid' => (int)$trustedSpec['pid'],
    'launch_pid' => (int)$trustedSpec['launch_pid'],
    'launch_token' => (string)$trustedSpec['launch_token'],
    'launch_lock_file' => (string)$trustedSpec['launch_lock_file'],
];
$trustedLockPresent = is_file($trustedMetadata['launch_lock_file']);
$trustedIdentityMatched = $trustedLauncherProbe->trusted((int)$trustedSpec['pid'], $trustedMetadata);
$wrongTrustedMetadata = $trustedMetadata;
$wrongTrustedMetadata['launch_token'] .= '-wrong';
$wrongIdentityRejected = !$trustedLauncherProbe->trusted((int)$trustedSpec['pid'], $wrongTrustedMetadata);
$trustedLauncherProbe->forceStopManagedInstance([
    'host' => '127.0.0.1',
    'port' => 0,
    'metadata' => $trustedMetadata,
], true);
$trustedStopDeadline = microtime(true) + 1.0;
while (
    @\Swoole\Process::kill((int)$trustedSpec['pid'], 0)
    && microtime(true) < $trustedStopDeadline
) {
    @\Swoole\Process::wait(false);
    usleep(10000);
}
@\Swoole\Process::wait(false);
$trustedChildStopped = !@\Swoole\Process::kill((int)$trustedSpec['pid'], 0);
@unlink($trustedMetadata['launch_lock_file']);
assertSpawnGuard($trustedLockPresent, 'launcher identity lock was not preserved across exec');
assertSpawnGuard($trustedIdentityMatched, 'launcher rejected its exact lock-bound child identity');
assertSpawnGuard($wrongIdentityRejected, 'launcher accepted a wrong startup token');
assertSpawnGuard($trustedChildStopped, 'launcher failed to stop its child when ps snapshot was unavailable');

$queuePidBase = sys_get_temp_dir() . '/scf-rqueue-lock-' . getmypid();
defined('SERVER_QUEUE_MANAGER_PID_FILE') || define('SERVER_QUEUE_MANAGER_PID_FILE', $queuePidBase);
$queueLock = fopen($queuePidBase . '.worker.lock', 'c+');
assertSpawnGuard(is_resource($queueLock), 'cannot create queue singleton lock');
try {
    assertSpawnGuard(flock($queueLock, LOCK_EX | LOCK_NB), 'cannot acquire queue singleton test lock');
    $queueToken = 'queue-lock-token-' . getmypid();
    rewind($queueLock);
    ftruncate($queueLock, 0);
    fwrite($queueLock, json_encode([
        'pid' => getmypid(),
        'token' => $queueToken,
        'manager_pid' => getmypid(),
        'started_at' => time(),
    ], JSON_UNESCAPED_SLASHES));
    fflush($queueLock);
    $queueOwner = RQueue::workerLockOwner();
    assertSpawnGuard(RQueue::workerLockIsHeld(), 'queue singleton lock failed to expose a live worker');
    assertSpawnGuard(
        (int)($queueOwner['pid'] ?? 0) === getmypid()
        && hash_equals($queueToken, (string)($queueOwner['token'] ?? '')),
        'queue singleton lock did not expose its exact owner'
    );
    assertSpawnGuard(
        RQueue::workerLockIsHeld(getmypid(), $queueToken),
        'queue singleton lock rejected the exact pid/token'
    );
    assertSpawnGuard(
        !RQueue::workerLockIsHeld(getmypid() + 1, $queueToken)
        && !RQueue::workerLockIsHeld(getmypid(), $queueToken . '-wrong'),
        'queue singleton lock accepted a wrong pid or token'
    );
    flock($queueLock, LOCK_UN);
    assertSpawnGuard(!RQueue::workerLockIsHeld(), 'queue singleton lock remained held after release');
} finally {
    is_resource($queueLock) and fclose($queueLock);
    @unlink($queuePidBase . '.worker.lock');
}

if (function_exists('pcntl_fork') && function_exists('pcntl_waitpid')) {
    $inheritedLease = fopen($queuePidBase . '.worker.lock', 'c+');
    assertSpawnGuard(is_resource($inheritedLease), 'cannot create inherited queue lease test lock');
    assertSpawnGuard(flock($inheritedLease, LOCK_EX | LOCK_NB), 'cannot acquire inherited queue lease test lock');
    $pidChannel = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    assertSpawnGuard(is_array($pidChannel), 'cannot create inherited lease pid channel');
    $leaseManagerPid = getmypid();
    $leaseOwnerPid = pcntl_fork();
    assertSpawnGuard($leaseOwnerPid >= 0, 'cannot fork inherited lease owner');
    if ($leaseOwnerPid === 0) {
        fclose($pidChannel[0]);
        rewind($inheritedLease);
        ftruncate($inheritedLease, 0);
        fwrite($inheritedLease, json_encode([
            'pid' => getmypid(),
            'token' => 'inherited-lease-owner',
            'manager_pid' => $leaseManagerPid,
            'started_at' => time(),
        ], JSON_UNESCAPED_SLASHES));
        fflush($inheritedLease);
        $leaseDescendantPid = pcntl_fork();
        if ($leaseDescendantPid === 0) {
            fclose($pidChannel[1]);
            sleep(10);
            exit(0);
        }
        fwrite($pidChannel[1], (string)$leaseDescendantPid . "\n");
        fflush($pidChannel[1]);
        fclose($pidChannel[1]);
        exit(0);
    }
    fclose($pidChannel[1]);
    fclose($inheritedLease);
    $leaseDescendantPid = (int)trim((string)fgets($pidChannel[0]));
    fclose($pidChannel[0]);
    pcntl_waitpid($leaseOwnerPid, $leaseOwnerStatus);
    assertSpawnGuard($leaseDescendantPid > 0, 'inherited lease descendant PID was not reported');
    assertSpawnGuard(
        @\Swoole\Process::kill($leaseDescendantPid, 0),
        'inherited lease descendant exited before stale-inode rotation test'
    );
    $rotatedLease = RQueue::tryAcquireWorkerLeaseForStart(getmypid());
    assertSpawnGuard(
        is_resource($rotatedLease),
        'dead queue owner with inherited lease fd blocked fixed-path lock rotation'
    );
    if (is_resource($rotatedLease)) {
        flock($rotatedLease, LOCK_UN);
        fclose($rotatedLease);
    }
    @\Swoole\Process::kill($leaseDescendantPid, SIGKILL);
    @unlink($queuePidBase . '.worker.lock');
}

$listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
assertSpawnGuard(is_resource($listener), "cannot create test listener: {$errstr}");
$listenerAddress = stream_socket_get_name($listener, false);
$listenerPort = (int)substr((string)$listenerAddress, (int)strrpos((string)$listenerAddress, ':') + 1);
assertSpawnGuard(Server::isListeningPortInUse($listenerPort), 'native port probe missed a live listener');
fclose($listener);

$childReservation = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
assertSpawnGuard(is_resource($childReservation), "cannot reserve child-listener port: {$errstr}");
$childReservationAddress = stream_socket_get_name($childReservation, false);
$childListenerPort = (int)substr(
    (string)$childReservationAddress,
    (int)strrpos((string)$childReservationAddress, ':') + 1
);
fclose($childReservation);
$listenerProcess = new \Swoole\Process(static function () use ($childListenerPort): void {
    $socket = @stream_socket_server("tcp://127.0.0.1:{$childListenerPort}", $errno, $errstr);
    if (!is_resource($socket)) {
        exit(2);
    }
    sleep(10);
    fclose($socket);
}, false, SOCK_DGRAM, false);
$listenerChildPid = $listenerProcess->start();
assertSpawnGuard($listenerChildPid > 0, 'cannot start child listener process');
$listenerReadyDeadline = microtime(true) + 1.0;
while (
    !Server::isListeningPortInUse($childListenerPort)
    && microtime(true) < $listenerReadyDeadline
) {
    usleep(10000);
}
$listenerPids = Server::findPidsByPort($childListenerPort, true);
@\Swoole\Process::kill($listenerChildPid, SIGKILL);
$listenerStopDeadline = microtime(true) + 1.0;
while (@\Swoole\Process::kill($listenerChildPid, 0) && microtime(true) < $listenerStopDeadline) {
    @\Swoole\Process::wait(false);
    usleep(10000);
}
@\Swoole\Process::wait(false);
assertSpawnGuard(
    in_array($listenerChildPid, $listenerPids, true),
    'port owner discovery missed a real child listener PID'
);

$released = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
assertSpawnGuard(is_resource($released), "cannot reserve release-test port: {$errstr}");
$releasedAddress = stream_socket_get_name($released, false);
$releasedPort = (int)substr((string)$releasedAddress, (int)strrpos((string)$releasedAddress, ':') + 1);
fclose($released);
assertSpawnGuard(!Server::isListeningPortInUse($releasedPort), 'native port probe marked a released listener as occupied');

fwrite(STDOUT, "System spawn guard regression checks passed.\n");
