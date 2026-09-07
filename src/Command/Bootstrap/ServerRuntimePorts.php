<?php
declare(strict_types=1);

/**
 * boot 端口探测函数。
 */

/**
 * 在真正拉起新的 server/gateway 之前，尝试回收占着目标监听端口的旧同类实例。
 *
 * 这个动作必须放在 bootstrap 层而不是 pack 内部命令类里，因为 `-pack` 场景下
 * 当前要加载的 pack 本身可能就是旧版本；如果等到 pack 里的 `Gateway::start`
 * 才处理端口冲突，就已经来不及了。
 *
 * @param array $argv 原始 CLI 参数
 * @return void
 */
function scf_prepare_command_ports_for_start(array $argv): void {
    if (!scf_has_arg($argv, 'start')) {
        return;
    }

    $ports = scf_command_guard_ports($argv);
    if (!$ports) {
        return;
    }

    $pids = scf_conflicting_listener_pids($argv, $ports);
    if (!$pids) {
        return;
    }

    if (scf_request_gateway_start_handoff($argv, $ports)) {
        scf_stdout('【Boot】旧 Gateway 交接/退出状态已确认，等待控制面释放');
        $deadline = microtime(true) + 40;
        do {
            if (!scf_conflicting_listener_pids($argv, $ports)) return;
            usleep(200000);
        } while (microtime(true) < $deadline);
        // 已进入交接就不能再发送 SIGKILL：旧控制面仍可能正在确认 supervisor detach。
        throw new RuntimeException('Gateway 平滑交接未完成，保留现有实例，请检查关停日志');
    }

    // HTTP 往返期间监听者可能已退出；仅回收本次开始时确认的旧 PID，不能追杀新一代。
    $pids = array_values(array_intersect($pids, scf_conflicting_listener_pids($argv, $ports)));
    if (!$pids) return;
    // 旧版本或已失效的控制面：先停同一命令的外层 boot，防止回收 child 时再次抢占端口。
    scf_signal_processes(scf_conflicting_boot_ancestor_pids($argv, $pids), SIGTERM);
    scf_stdout('【Boot】发现旧命令监听占用，开始优雅回收: ports=' . implode(', ', $ports) . '; pids=' . implode(', ', $pids));
    scf_signal_processes($pids, SIGTERM);
    if (scf_wait_owned_command_listeners_released($argv, $ports, 8)) {
        scf_stdout('【Boot】命令监听端口已释放');
        return;
    }

    $remaining = scf_conflicting_listener_pids($argv, $ports);
    if (!$remaining) {
        return;
    }
    if (array_diff($remaining, $pids)) {
        throw new RuntimeException('监听已被另一代 Gateway 接管，停止本次回收');
    }

    scf_stderr('【Boot】SIGTERM 后仍有监听存活，开始强制回收: pids=' . implode(', ', $remaining));
    scf_signal_processes($remaining, SIGKILL);
    if (!scf_wait_owned_command_listeners_released($argv, $ports, 5)) {
        throw new RuntimeException('旧命令监听未释放，停止本次启动');
    }
}

/**
 * 从已确认的监听者向上追踪同一 PHP boot 命令，找出负责自动重拉的非监听父进程。
 * 不向下扩大回收范围；命令、app、role、port 必须一致，遇到 shell 或无关父进程立即停止。
 * @param array $argv 当前启动命令。
 * @param array<int,int> $listeners 已确认的旧监听者。
 * @return array<int,int> 非监听 boot 祖先进程。
 */
function scf_conflicting_boot_ancestor_pids(array $argv, array $listeners): array {
    $output = scf_process_output(['/bin/ps', '-axo', 'pid=,ppid=,command=']);
    $processes = [];
    foreach (preg_split('/\r?\n/', trim($output)) ?: [] as $line) {
        if (preg_match('/^\s*(\d+)\s+(\d+)\s+(.+)$/', $line, $match)) {
            $processes[(int)$match[1]] = ['ppid' => (int)$match[2], 'command' => $match[3]];
        }
    }
    $ancestors = [];
    foreach ($listeners as $pid) {
        $seen = [];
        while (($parent = (int)($processes[$pid]['ppid'] ?? 0)) > 1 && !isset($seen[$parent])) {
            $seen[$parent] = true;
            if ($parent === getmypid()) break;
            if (!scf_boot_command_matches($argv, (string)($processes[$parent]['command'] ?? ''))) break;
            if (!in_array($parent, $listeners, true)) $ancestors[$parent] = $parent;
            $pid = $parent;
        }
    }
    return array_values($ancestors);
}

/**
 * 核实可回收进程的完整命令身份；监听端口相交或 app 名相同不足以授权发送信号。
 * @param array $argv 当前启动命令。
 * @param string $processCommand ps 返回的目标命令行。
 * @return bool 是否属于相同 app、环境、角色及业务端口的 PHP boot start。
 */
function scf_boot_command_matches(array $argv, string $processCommand): bool {
    $parts = preg_split('/\s+/', trim($processCommand)) ?: [];
    if (!preg_match('/^php[0-9.]*$/', basename($parts[0] ?? ''))
        || basename($parts[1] ?? '') !== 'boot'
        || ($parts[2] ?? '') !== ($argv[1] ?? '') || ($parts[3] ?? '') !== 'start') return false;
    $expected = scf_parse_opts($argv);
    $options = scf_parse_opts($parts);
    foreach (['app' => getenv('APP_DIR') ?: 'app', 'role' => getenv('SERVER_ROLE') ?: 'master', 'port' => 9580] as $name => $default) {
        if ((string)($options[$name] ?? $default) !== (string)($expected[$name] ?? $default)) return false;
    }
    return scf_boot_command_environment($argv) === scf_boot_command_environment($parts);
}

/**
 * 遵守 scf_define_runtime_constants 的环境解析规则，供 fork 前的身份与租约核验使用。
 * @param array $argv 命令参数。
 * @return string dev 或 prod。
 */
function scf_boot_command_environment(array $argv): string {
    return getenv('APP_ENV') === 'dev' || strtolower((string)(scf_option_value($argv, 'env') ?? '')) === 'dev'
        || scf_has_arg($argv, '-dev') ? 'dev' : 'prod';
}

/**
 * 通过已确认属于本命令的本机控制面请求交接；响应丢失时核对租约和旧监听者。
 * @param array $argv 启动参数。
 * @param array<int,int> $ports 本命令端口。
 * @return bool true 表示交接已确认或旧监听已退出，false 表示允许兼容回收。
 * @throws RuntimeException 现有服务明确拒绝交接，或无法确认安全恢复条件。
 */
function scf_request_gateway_start_handoff(array $argv, array $ports): bool {
    if (($argv[1] ?? '') !== 'gateway') return false;
    $opts = scf_parse_opts($argv);
    $port = (int)($opts['port'] ?? 9580);
    $controlPort = (int)($opts['control_port'] ?? ($opts['gateway_control_port'] ?? ($port + 1000)));
    $listeners = scf_conflicting_listener_pids($argv, [$controlPort]);
    if (!in_array($controlPort, $ports, true) || !$listeners) return false;
    $bootAncestors = scf_conflicting_boot_ancestor_pids($argv, $listeners);
    $payload = json_encode(['command' => 'handoff', 'params' => [
        'app' => $opts['app'] ?? (getenv('APP_DIR') ?: 'app'),
        'role' => $opts['role'] ?? (getenv('SERVER_ROLE') ?: 'master'),
        'port' => $port,
    ]]);
    $context = stream_context_create(['http' => ['method' => 'POST', 'timeout' => 3,
        'ignore_errors' => true, 'header' => "Content-Type: application/json\r\nConnection: close\r\n",
        'content' => $payload]]);
    error_clear_last();
    $body = @file_get_contents('http://127.0.0.1:' . $controlPort . '/_gateway/internal/command', false, $context);
    $transportError = error_get_last()['message'] ?? '空响应或非 JSON 响应';
    $result = is_string($body) ? json_decode($body, true) : null;
    if (is_array($result) && ($result['accepted'] ?? false) === true) return true;
    if (is_array($result) && str_contains((string)($result['message'] ?? ''), '暂不支持的命令:handoff')) return false;
    if (is_array($result) && isset($result['message']) && $result['message'] !== 'Gateway 已在关闭中') {
        throw new RuntimeException('Gateway 拒绝启动交接，保留当前服务: ' . (string)$result['message']);
    }

    scf_stdout('【Boot】未收到交接回执，核对旧控制面租约与退出状态: control_port=' . $controlPort);
    $deadline = microtime(true) + 40;
    $lease = null;
    do {
        $remaining = scf_conflicting_listener_pids($argv, [$controlPort]);
        if (!$remaining) {
            // 回执丢失也可能是 child 异常退出，外层 boot 尚在 2 秒重拉窗口。
            // 旧控制监听已全部退出后，停止请求前记录的 boot 祖先，避免随后重拉抢占。
            foreach ($bootAncestors as $pid) {
                if (scf_boot_command_matches($argv, scf_read_process_command($pid))) {
                    scf_signal_processes([$pid], SIGTERM);
                }
            }
            return true;
        }
        if (array_diff($remaining, $listeners)) {
            throw new RuntimeException('交接等待期间另一代 Gateway 已启动，保留现有服务');
        }
        $lease = scf_gateway_startup_lease($argv);
        if ($lease !== null) {
            $state = (string)($lease['state'] ?? '');
            $ownerPid = (int)($lease['meta']['gateway_pid'] ?? 0);
            $ownerMatches = in_array($ownerPid, $listeners, true)
                || ($ownerPid > 0 && function_exists('posix_kill') && !@posix_kill($ownerPid, 0));
            $grace = $state === 'restarting'
                ? max(0, (int)($lease['meta']['restart_grace_seconds'] ?? 120))
                : max(0, (int)($lease['meta']['grace_seconds'] ?? 20));
            $expiresAt = (int)($lease['expires_at'] ?? 0);
            // stopped 已明确撤销服务；其它状态必须超过 upstream 同样遵守的租约宽限期。
            // 有效 restarting 仍可能在等待 supervisor detach，不能因回执丢失强杀它。
            if ($ownerMatches && ($state === 'stopped'
                || ($expiresAt > 0 && time() > $expiresAt + $grace))) {
                scf_stdout('【Boot】旧控制面租约已失效，恢复残留监听: state=' . $state
                    . ', epoch=' . (int)($lease['epoch'] ?? 0) . ', pids=' . implode(',', $remaining));
                return false;
            }
        }
        // 此处是 fork 前的 CLI boot，不在 Swoole worker/协程中；等待不阻塞现有业务进程。
        usleep(200000);
    } while (microtime(true) < $deadline);
    throw new RuntimeException('Gateway 交接未完成，保留当前服务: control_port=' . $controlPort
        . ', pids=' . implode(',', $listeners) . ', lease=' . (string)($lease['state'] ?? 'unknown')
        . ', epoch=' . (int)($lease['epoch'] ?? 0) . ', transport=' . $transportError);
}

/**
 * 在 framework/App 尚未初始化的 boot 层只读租约；内容身份必须与本次命令完全匹配。
 * 文件名遵守 GatewayLease::leaseStateFile 的持久化协议，不加载可能仍为旧版的 pack 类。
 * @param array $argv 原始启动参数。
 * @return array<string,mixed>|null 完整租约，未知或不匹配时不授权回收。
 */
function scf_gateway_startup_lease(array $argv): ?array {
    $opts = scf_parse_opts($argv);
    $app = (string)($opts['app'] ?? (getenv('APP_DIR') ?: 'app'));
    $role = (string)($opts['role'] ?? (getenv('SERVER_ROLE') ?: 'master'));
    $env = scf_boot_command_environment($argv);
    $port = (int)($opts['port'] ?? 9580);
    $safe = array_map(static fn(string $value): string => preg_replace('/[^a-zA-Z0-9_-]+/', '_', $value), [$app, $env, $role]);
    $appsRoot = defined('SCF_APPS_ROOT') ? SCF_APPS_ROOT : dirname(SCF_ROOT) . '/apps';
    $file = $appsRoot . '/' . $app . '/update/gateway_lease_' . implode('_', $safe) . '_' . $port . '.json';
    $body = @file_get_contents($file);
    $lease = is_string($body) ? json_decode($body, true) : null;
    if (!is_array($lease) || ($lease['app'] ?? '') !== $app || ($lease['env'] ?? '') !== $env
        || ($lease['role'] ?? '') !== $role || (int)($lease['gateway_port'] ?? 0) !== $port
        || (int)($lease['epoch'] ?? 0) <= 0
        || !in_array($lease['state'] ?? '', ['running', 'restarting', 'stopped'], true)) return null;
    return $lease;
}

/**
 * 等待本命令的监听者退出；nginx 持有业务端口是正常状态。
 * @param array $argv 启动参数。
 * @param array<int,int> $ports 监听端口。
 * @param int $seconds 等待上限。
 * @return bool 是否释放。
 */
function scf_wait_owned_command_listeners_released(array $argv, array $ports, int $seconds): bool {
    $deadline = microtime(true) + $seconds;
    do {
        if (!scf_conflicting_listener_pids($argv, $ports)) return true;
        usleep(200000);
    } while (microtime(true) < $deadline);
    return false;
}

function scf_wait_command_ports_released(array $argv, int $timeoutSeconds = 20, int $intervalMs = 200): void {
    $ports = scf_command_listen_ports($argv);
    if (!$ports) {
        return;
    }

    $timeoutSeconds = max(1, $timeoutSeconds);
    $startedAt = microtime(true);
    $deadline = $startedAt + $timeoutSeconds;
    $nextProgressLogAt = $startedAt;
    // PID ownership 只在进入等待和已知 PID 消失时刷新；200ms 热循环仅做原生 TCP 探测。
    $conflictingPids = scf_conflicting_listener_pids($argv, $ports);
    if (!$conflictingPids) {
        return;
    }
    scf_stdout('【Boot】等待旧监听端口释放后再重拉: ports=' . implode(', ', $ports) . ", timeout={$timeoutSeconds}s");

    while (microtime(true) < $deadline) {
        $occupied = scf_collect_occupied_listening_ports($ports, false);
        if (!$occupied) {
            $elapsed = max(0, (int)round(microtime(true) - $startedAt));
            scf_stdout("【Boot】旧监听端口已释放，准备重拉: elapsed={$elapsed}s");
            return;
        }
        $knownOwnerAlive = false;
        foreach ($conflictingPids as $pid) {
            if ($pid > 0 && function_exists('posix_kill') && @posix_kill($pid, 0)) {
                $knownOwnerAlive = true;
                break;
            }
        }
        if (!$knownOwnerAlive) {
            $conflictingPids = scf_conflicting_listener_pids($argv, $ports);
            if (!$conflictingPids) {
                $elapsed = max(0, (int)round(microtime(true) - $startedAt));
                scf_stdout(
                    "【Boot】旧命令监听已释放，端口占用来自外部服务，跳过等待: elapsed={$elapsed}s, occupied="
                    . scf_format_occupied_listening_ports($occupied)
                );
                return;
            }
        }

        $now = microtime(true);
        if ($now >= $nextProgressLogAt) {
            $elapsed = max(0, (int)floor($now - $startedAt));
            scf_stdout(
                "【Boot】旧监听端口仍占用，继续等待: elapsed={$elapsed}s, occupied="
                . scf_format_occupied_listening_ports($occupied)
                . ", conflicting_pids=" . implode('|', array_values(array_unique(array_map('intval', $conflictingPids))))
            );
            $nextProgressLogAt = $now + 2.0;
        }
        usleep(max(50, $intervalMs) * 1000);
    }

    $occupied = scf_collect_occupied_listening_ports($ports);
    $conflictingPids = scf_conflicting_listener_pids($argv, $ports);
    if (!$conflictingPids) {
        scf_stdout(
            "【Boot】旧命令监听已释放，端口占用来自外部服务，跳过等待: occupied="
            . scf_format_occupied_listening_ports($occupied)
        );
        return;
    }
    scf_stderr(
        "【Boot】等待旧监听端口释放超时，继续重拉: timeout={$timeoutSeconds}s, occupied="
        . scf_format_occupied_listening_ports($occupied)
        . ", conflicting_pids=" . implode('|', array_values(array_unique(array_map('intval', $conflictingPids))))
    );
}

/**
 * 汇总当前仍处于监听态的端口和对应 PID。
 *
 * @param array<int, int> $ports 需要探测的端口列表
 * @return array<int, array<int, int>> [port => [pid...]]
 */
function scf_collect_occupied_listening_ports(array $ports, bool $includePids = true): array {
    $occupied = [];
    foreach ($ports as $port) {
        $port = (int)$port;
        if ($port <= 0) {
            continue;
        }
        if (!scf_is_port_listening('127.0.0.1', $port)) {
            continue;
        }
        $occupied[$port] = $includePids ? scf_listening_pids_by_port($port) : [];
    }

    ksort($occupied);
    return $occupied;
}

/**
 * 将监听占用信息格式化为可读日志片段。
 *
 * @param array<int, array<int, int>> $occupied [port => [pid...]]
 * @return string
 */
function scf_format_occupied_listening_ports(array $occupied): string {
    if (!$occupied) {
        return 'none';
    }

    $segments = [];
    foreach ($occupied as $port => $pids) {
        $pidList = array_slice(array_values(array_filter(array_map('intval', (array)$pids), static fn(int $pid) => $pid > 0)), 0, 8);
        $pidText = $pidList ? implode('|', $pidList) : 'unknown';
        $segments[] = $port . '(pids=' . $pidText . ')';
    }

    return implode(', ', $segments);
}

/**
 * 解析启动期需要守护的关键端口。
 *
 * 对 gateway 来说，真正会把新实例挡在门外的是“控制面端口”，而不是业务入口。
 * 因此这里除了业务端口，还会补上控制面端口；只要控制面监听者被回收，
 * 对应的整组 gateway 进程就会一起退出。
 *
 * @param array $argv 原始 CLI 参数
 * @return array<int, int>
 */
function scf_command_guard_ports(array $argv): array {
    $command = $argv[1] ?? '';
    $opts = scf_parse_opts($argv);
    $ports = scf_command_listen_ports($argv);

    if ($command === 'gateway') {
        $bindPort = (int)($opts['port'] ?? 9580);
        $controlPort = 0;
        if (array_key_exists('control_port', $opts) || array_key_exists('gateway_control_port', $opts)) {
            $controlPort = (int)($opts['control_port'] ?? ($opts['gateway_control_port'] ?? 0));
        }
        if ($controlPort <= 0) {
            $controlPort = $bindPort + 1000;
        }
        if ($controlPort > 0) {
            $ports[] = $controlPort;
        }
    }

    return array_values(array_unique(array_filter(array_map('intval', $ports), static fn(int $port) => $port > 0)));
}

function scf_command_listen_ports(array $argv): array {
    $command = $argv[1] ?? '';
    $opts = scf_parse_opts($argv);
    $ports = [];

    if ($command === 'gateway') {
        $ports[] = (int)($opts['port'] ?? 9580);
        $rpcPort = (int)($opts['rpc_port'] ?? ($opts['rport'] ?? 0));
        if ($rpcPort > 0) {
            $ports[] = $rpcPort;
        }
    } elseif ($command === 'server') {
        $ports[] = (int)($opts['port'] ?? 9580);
        $rpcPort = (int)($opts['rport'] ?? 0);
        if ($rpcPort > 0) {
            $ports[] = $rpcPort;
        }
    }

    return array_values(array_unique(array_filter(array_map('intval', $ports), static fn(int $port) => $port > 0)));
}

/**
 * 发现当前命令目标端口上的旧同类监听进程。
 *
 * 这里不会粗暴清理任何占端口的进程，只会回收命令行上能明确识别为
 * 同一条 `boot gateway start` / `boot server start` 链路的进程。
 *
 * @param array $argv 原始 CLI 参数
 * @param array<int, int> $ports 目标端口集合
 * @return array<int, int>
 */
function scf_conflicting_listener_pids(array $argv, array $ports): array {
    $command = (string)($argv[1] ?? '');
    if (!in_array($command, ['gateway', 'server'], true)) {
        return [];
    }

    $selfPid = getmypid() ?: 0;
    $pids = [];
    foreach ($ports as $port) {
        foreach (scf_listening_pids_by_port((int)$port) as $pid) {
            if ($pid <= 0 || $pid === $selfPid) {
                continue;
            }
            $processCommand = scf_read_process_command($pid);
            if (scf_boot_command_matches($argv, $processCommand)) {
                $pids[$pid] = $pid;
            }
        }
    }

    ksort($pids);
    return array_values($pids);
}

/**
 * 读取端口上的监听 PID 列表。
 *
 * @param int $port 目标端口
 * @return array<int, int>
 */
function scf_listening_pids_by_port(int $port): array {
    if ($port <= 0) {
        return [];
    }

    $lsof = '';
    foreach (['/usr/sbin/lsof', '/usr/bin/lsof', '/opt/homebrew/sbin/lsof'] as $candidate) {
        if (is_executable($candidate)) {
            $lsof = $candidate;
            break;
        }
    }
    if ($lsof === '') {
        return [];
    }
    $output = scf_process_output([$lsof, '-nP', '-t', '-iTCP:' . $port, '-sTCP:LISTEN']);
    if (!is_string($output) || trim($output) === '') {
        return [];
    }

    $pids = [];
    foreach (preg_split('/\r?\n/', trim($output)) as $line) {
        $pid = (int)trim((string)$line);
        if ($pid > 0) {
            $pids[$pid] = $pid;
        }
    }

    return array_values($pids);
}

/**
 * 读取指定 PID 的命令行。
 *
 * @param int $pid 进程 ID
 * @return string
 */
function scf_read_process_command(int $pid): string {
    if ($pid <= 0) {
        return '';
    }

    $ps = is_executable('/bin/ps') ? '/bin/ps' : '/usr/bin/ps';
    $output = scf_process_output([$ps, '-p', (string)$pid, '-o', 'command=']);
    return trim((string)$output);
}

/**
 * 不经过 /bin/sh 执行一个低频状态转换命令。
 *
 * @param array<int, string> $command
 */
function scf_process_output(array $command, float $timeoutSeconds = 2.0): string {
    $deferred = $GLOBALS['__SCF_BOOTSTRAP_DEFERRED_PROCESSES'] ?? [];
    if (!is_array($deferred)) {
        $deferred = [];
    }
    $liveDeferred = [];
    foreach ($deferred as $process) {
        if (!is_resource($process)) {
            continue;
        }
        $status = @proc_get_status($process);
        if (is_array($status) && ($status['running'] ?? false)) {
            $liveDeferred[] = $process;
            continue;
        }
        @proc_close($process);
    }
    $GLOBALS['__SCF_BOOTSTRAP_DEFERRED_PROCESSES'] = $liveDeferred;

    // 启动链中只要已有一个无法回收的系统探针，就打开熔断，避免每次
    // restart/reload 再派生一个新的 _dyld_start 僵持进程。
    if (
        $liveDeferred
        || (bool)($GLOBALS['__SCF_BOOTSTRAP_PROCESS_OUTPUT_IN_FLIGHT'] ?? false)
    ) {
        return '';
    }

    $GLOBALS['__SCF_BOOTSTRAP_PROCESS_OUTPUT_IN_FLIGHT'] = true;
    try {
    $process = @proc_open($command, [
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ], $pipes, null, null, ['bypass_shell' => true]);
    if (!is_resource($process)) {
        return '';
    }
    foreach ([1, 2] as $index) {
        if (is_resource($pipes[$index] ?? null)) {
            @stream_set_blocking($pipes[$index], false);
        }
    }

    $output = '';
    $deadline = microtime(true) + max(0.05, min(10.0, $timeoutSeconds));
    $timedOut = false;
    $truncated = false;
    while (true) {
        foreach ([1, 2] as $index) {
            if (!is_resource($pipes[$index] ?? null)) {
                continue;
            }
            while (true) {
                $chunk = @fread($pipes[$index], 65_536);
                if (!is_string($chunk) || $chunk === '') {
                    break;
                }
                if ($index === 1 && strlen($output) < 4_194_304) {
                    $remaining = 4_194_304 - strlen($output);
                    $output .= substr($chunk, 0, $remaining);
                    if (strlen($chunk) > $remaining) {
                        $truncated = true;
                    }
                } elseif ($index === 1) {
                    $truncated = true;
                }
            }
        }

        $status = @proc_get_status($process);
        if (!is_array($status) || !($status['running'] ?? false)) {
            break;
        }
        if (microtime(true) >= $deadline) {
            $timedOut = true;
            @proc_terminate($process, 15);
            $graceDeadline = microtime(true) + 0.10;
            do {
                usleep(10_000);
                $status = @proc_get_status($process);
            } while (
                is_array($status)
                && ($status['running'] ?? false)
                && microtime(true) < $graceDeadline
            );
            if (is_array($status) && ($status['running'] ?? false)) {
                @proc_terminate($process, 9);
                $graceDeadline = microtime(true) + 0.25;
                do {
                    usleep(10_000);
                    $status = @proc_get_status($process);
                } while (
                    is_array($status)
                    && ($status['running'] ?? false)
                    && microtime(true) < $graceDeadline
                );
            }
            break;
        }
        usleep(10_000);
    }

    foreach ($pipes as $pipe) {
        if (is_resource($pipe)) {
            @fclose($pipe);
        }
    }
    $status = @proc_get_status($process);
    if (is_array($status) && ($status['running'] ?? false)) {
        $GLOBALS['__SCF_BOOTSTRAP_DEFERRED_PROCESSES'][] = $process;
    } else {
        @proc_close($process);
    }
    return ($timedOut || $truncated) ? '' : $output;
    } finally {
        $GLOBALS['__SCF_BOOTSTRAP_PROCESS_OUTPUT_IN_FLIGHT'] = false;
    }
}

/**
 * 向目标进程列表广播退出信号。
 *
 * @param array<int, int> $pids 目标 PID 列表
 * @param int $signal Unix signal
 * @return void
 */
function scf_signal_processes(array $pids, int $signal): void {
    foreach ($pids as $pid) {
        $pid = (int)$pid;
        if ($pid <= 0) {
            continue;
        }
        if (!function_exists('posix_kill')) {
            $kill = is_executable('/bin/kill') ? '/bin/kill' : '/usr/bin/kill';
            scf_process_output([$kill, '-' . $signal, (string)$pid], 1.0);
            continue;
        }
        @posix_kill($pid, $signal);
    }
}

/**
 * 等待一组端口真正从监听态退出。
 *
 * @param array<int, int> $ports
 * @param int $timeoutSeconds
 * @param int $intervalMs
 * @return bool
 */
function scf_wait_ports_released(array $ports, int $timeoutSeconds = 10, int $intervalMs = 200): bool {
    $deadline = microtime(true) + max(1, $timeoutSeconds);
    while (microtime(true) < $deadline) {
        $occupied = false;
        foreach ($ports as $port) {
            if ((int)$port > 0 && scf_is_port_listening('127.0.0.1', (int)$port)) {
                $occupied = true;
                break;
            }
        }
        if (!$occupied) {
            return true;
        }
        usleep(max(50, $intervalMs) * 1000);
    }

    return false;
}

function scf_is_port_listening(string $host, int $port, float $timeoutSeconds = 0.2): bool {
    $errno = 0;
    $errstr = '';
    $socket = @stream_socket_client(
        sprintf('tcp://%s:%d', $host, $port),
        $errno,
        $errstr,
        max(0.01, $timeoutSeconds),
        STREAM_CLIENT_CONNECT
    );
    if (!is_resource($socket)) {
        return false;
    }

    fclose($socket);
    return true;
}
