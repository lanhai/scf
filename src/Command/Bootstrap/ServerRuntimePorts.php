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

    scf_stdout('[bootstrap] found existing command listeners, attempting graceful reclaim: ports=' . implode(', ', $ports) . '; pids=' . implode(', ', $pids));
    scf_signal_processes($pids, SIGTERM);
    if (scf_wait_ports_released($ports, 8)) {
        scf_stdout('[bootstrap] command ports released');
        return;
    }

    $remaining = scf_conflicting_listener_pids($argv, $ports);
    if (!$remaining) {
        return;
    }

    scf_stderr('[bootstrap] listeners still active after SIGTERM, forcing reclaim: pids=' . implode(', ', $remaining));
    scf_signal_processes($remaining, SIGKILL);
    scf_wait_ports_released($ports, 5);
}

function scf_wait_command_ports_released(array $argv, int $timeoutSeconds = 20, int $intervalMs = 200): void {
    $ports = scf_command_listen_ports($argv);
    if (!$ports) {
        return;
    }

    $deadline = microtime(true) + max(1, $timeoutSeconds);
    while (microtime(true) < $deadline) {
        $occupied = false;
        foreach ($ports as $port) {
            if ($port <= 0) {
                continue;
            }
            if (scf_is_port_listening('127.0.0.1', $port)) {
                $occupied = true;
                break;
            }
        }
        if (!$occupied) {
            return;
        }
        usleep(max(50, $intervalMs) * 1000);
    }
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
    $app = (string)(scf_option_value($argv, 'app') ?: (getenv('APP_DIR') ?: 'app'));
    $expectedMarker = match ($command) {
        'gateway' => 'boot gateway start',
        'server' => 'boot server start',
        default => '',
    };
    if ($expectedMarker === '') {
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
            if ($processCommand === '') {
                continue;
            }
            if (!str_contains($processCommand, $expectedMarker)) {
                continue;
            }
            if ($app !== '' && str_contains($processCommand, '-app=' . $app)) {
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

    $output = @shell_exec('lsof -nP -t -iTCP:' . $port . ' -sTCP:LISTEN 2>/dev/null');
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

    $output = @shell_exec('ps -p ' . $pid . ' -o command= 2>/dev/null');
    return trim((string)$output);
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
            @exec('kill -' . $signal . ' ' . $pid . ' >/dev/null 2>&1');
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
