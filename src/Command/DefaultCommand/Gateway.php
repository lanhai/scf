<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Server\Proxy\LocalIpc;
use Scf\Server\Proxy\CliBootstrap;
use Swoole\Process;

/**
 * Gateway CLI 命令入口。
 *
 * 该命令类只负责把外部 `./server gateway ...` / `./server start ...`
 * 风格的指令映射到 gateway 控制面动作，避免调用方直接感知本地 IPC 或内部 HTTP。
 */
class Gateway implements CommandInterface {

    /**
     * 返回该 CLI 命令的注册名。
     *
     * @return string 命令名
     */
    public function commandName(): string {
        return 'gateway';
    }

    /**
     * 返回该 CLI 命令的简短说明。
     *
     * @return string 命令描述
     */
    public function desc(): string {
        return '代理网关服务';
    }

    /**
     * 构建 gateway 命令帮助信息。
     *
     * @param Help $commandHelp 帮助信息容器，方法会向其中追加动作和参数说明
     * @return Help 返回同一个帮助对象，便于调用方继续链式处理
     */
    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('start', '启动代理网关');
        $commandHelp->addAction('stop', '停止代理网关');
        $commandHelp->addAction('reload', '重启业务平面');
        $commandHelp->addAction('restart', '重启代理网关');
        $commandHelp->addAction('restart_crontab', '重启 Gateway 排程子进程');
        $commandHelp->addAction('restart_redisqueue', '重启 Gateway Redis 队列子进程');
        $commandHelp->addAction('status', '查看代理网关状态');
        $commandHelp->addActionOpt('-app', '应用目录名');
        $commandHelp->addActionOpt('-env', '运行环境, 例如 dev');
        $commandHelp->addActionOpt('-port', 'Gateway 监听端口');
        $commandHelp->addActionOpt('-rpc_port', 'Gateway 对外 RPC 监听端口, 默认沿用应用 rpc_port');
        $commandHelp->addActionOpt('-host', 'Gateway 监听地址');
        $commandHelp->addActionOpt('-control_port', 'Gateway 控制面监听端口, tcp模式下默认自动偏移');
        $commandHelp->addActionOpt('-upstream_host', '业务 server 地址, 默认 127.0.0.1');
        $commandHelp->addActionOpt('-upstream_port', '业务 server 端口, 不传则自动分配');
        $commandHelp->addActionOpt('-upstream_rpc_port', '业务 server 内部 RPC 端口, 由 Gateway 转发到此端口, 不传则自动分配');
        $commandHelp->addActionOpt('-upstream_version', 'upstream 版本标签');
        $commandHelp->addActionOpt('-upstream_role', 'upstream 节点角色, 默认继承当前 Gateway 角色');
        $commandHelp->addActionOpt('-spawn_upstream', '是否自动启动业务 server, 默认 on');
        $commandHelp->addActionOpt('-reuse_upstream', '是否复用已存在的 upstream 端口, 默认 off');
        return $commandHelp;
    }

    /**
     * 执行 gateway CLI 命令。
     *
     * @return string|null 返回命令执行提示，`start` 分支会直接进入 bootstrap 流程并返回 null
     */
    public function exec(): ?string {
        $action = (string)(Manager::instance()->getArg(0) ?: 'start');
        if (in_array($action, ['start', 'stop', 'reload', 'restart', 'restart_crontab', 'restart_redisqueue', 'status'], true)) {
            Env::initialize(MODE_CGI);
        }
        if ($action === 'stop') {
            return $this->stop();
        }
        if ($action === 'reload') {
            return $this->control('reload');
        }
        if ($action === 'restart') {
            return $this->control('restart');
        }
        if ($action === 'restart_crontab') {
            return $this->control('restart_crontab');
        }
        if ($action === 'restart_redisqueue') {
            return $this->control('restart_redisqueue');
        }
        if ($action === 'status') {
            return $this->status();
        }
        if ($action === 'start') {
            $this->prepareGatewayStart();
            CliBootstrap::bootedRun();
            return null;
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    /**
     * 启动 gateway 前先清理占用目标监听端口的旧同类实例。
     *
     * `./server start` 本质上是“确保该 app/role 的 gateway 进入运行态”。
     * 如果当前端口已经被上一轮遗留的 gateway 进程占着，继续直接启动只会在
     * `GatewayServer` 里反复等待并最终报 `Address already in use`。
     *
     * 这里在真正进入 bootedRun 之前，只针对“命令行可识别为同类 gateway”
     * 且确实占着目标监听端口的旧进程做定向终止，避免误伤其它服务。
     *
     * @return void
     */
    protected function prepareGatewayStart(): void {
        $ports = $this->resolveGatewayListenPortsForStart();
        if (!$ports) {
            return;
        }

        $pids = $this->discoverConflictingGatewayListenerPids($ports);
        if (!$pids) {
            return;
        }

        Console::warning('发现旧 Gateway 实例占用目标端口，准备回收: ports=' . implode(', ', $ports) . '; pids=' . implode(', ', $pids));
        $this->terminateProcesses($pids, SIGTERM);
        if ($this->waitForPortsReleased($ports, 8)) {
            Console::success('旧 Gateway 监听端口已释放，继续启动');
            return;
        }

        $remainingPids = $this->discoverConflictingGatewayListenerPids($ports);
        if (!$remainingPids) {
            return;
        }

        Console::warning('旧 Gateway 实例未在预期时间内退出，升级为强制回收: pids=' . implode(', ', $remainingPids));
        $this->terminateProcesses($remainingPids, SIGKILL);
        $this->waitForPortsReleased($ports, 5);
    }

    /**
     * 直接向 gateway master 发送停止信号。
     *
     * @return string 停止处理结果
     */
    protected function stop(): string {
        $this->markLoopStop();
        $app = (string)(Manager::instance()->getOpt('app') ?: getenv('APP_DIR') ?: 'app');
        $role = (string)(Manager::instance()->getOpt('role') ?: getenv('SERVER_ROLE') ?: 'master');
        $gatewayPort = $this->resolveGatewayPort();
        $pidFile = dirname(SCF_ROOT) . '/var/' . $app . '_gateway_' . $role . '.pid';
        $pidFromFile = 0;
        if (is_file($pidFile)) {
            $pidFromFile = (int)file_get_contents($pidFile);
        }
        $gatewayPids = $this->discoverGatewayRuntimePids($app, $role, $gatewayPort);
        if ($pidFromFile > 0) {
            $gatewayPids[$pidFromFile] = $pidFromFile;
        }
        $gatewayPids = array_values($gatewayPids);
        $upstreamPids = $gatewayPort > 0
            ? $this->discoverGatewayUpstreamRuntimePids($app, $gatewayPort)
            : [];

        if (!$gatewayPids && !$upstreamPids) {
            @unlink($pidFile);
            return 'Gateway 进程不存在';
        }

        if ($gatewayPids) {
            $this->terminateProcesses($gatewayPids, SIGTERM);
            if (!$this->waitForProcessesExited($gatewayPids, 8)) {
                $remainingGatewayPids = $this->filterAlivePids($gatewayPids);
                if ($remainingGatewayPids) {
                    Console::warning('Gateway 进程未在预期时间内退出，升级为强制回收: pids=' . implode(', ', $remainingGatewayPids));
                    $this->terminateProcesses($remainingGatewayPids, SIGKILL);
                    $this->waitForProcessesExited($remainingGatewayPids, 3);
                }
            }
        }

        if ($upstreamPids) {
            $this->terminateProcesses($upstreamPids, SIGTERM);
            if (!$this->waitForProcessesExited($upstreamPids, 5)) {
                $remainingUpstreamPids = $this->filterAlivePids($upstreamPids);
                if ($remainingUpstreamPids) {
                    Console::warning('关联 upstream 进程未在预期时间内退出，升级为强制回收: pids=' . implode(', ', $remainingUpstreamPids));
                    $this->terminateProcesses($remainingUpstreamPids, SIGKILL);
                    $this->waitForProcessesExited($remainingUpstreamPids, 2);
                }
            }
        }

        if ($pidFromFile <= 0 || !Process::kill($pidFromFile, 0)) {
            @unlink($pidFile);
        }
        $remainingGatewayPids = $this->filterAlivePids($gatewayPids);
        $remainingUpstreamPids = $this->filterAlivePids($upstreamPids);
        if ($remainingGatewayPids || $remainingUpstreamPids) {
            return 'Gateway 停止未完全收敛'
                . ($remainingGatewayPids ? '; gateway=' . implode(',', $remainingGatewayPids) : '')
                . ($remainingUpstreamPids ? '; upstream=' . implode(',', $remainingUpstreamPids) : '');
        }
        return 'Gateway 已停止'
            . ($gatewayPids ? ': gateway_pids=' . implode(',', $gatewayPids) : '')
            . ($upstreamPids ? ', upstream_pids=' . implode(',', $upstreamPids) : '');
    }

    /**
     * 通过本地控制面转发 gateway 内部命令。
     *
     * 优先走 LocalIpc，只有本地 socket 不可用时才回退到控制面 HTTP，
     * 保证控制指令尽量走本机轻量通道。
     *
     * @param string $command 需要下发的 gateway 内部命令
     * @return string 命令发送结果描述
     */
    protected function control(string $command): string {
        $port = $this->resolveGatewayPort();
        if ($port <= 0) {
            return 'Gateway 端口未配置';
        }
        $result = $this->requestLocalGateway($port, '/_gateway/internal/command', 'POST', [
            'command' => $command,
        ]);
        if ($result['status'] !== 200) {
            return 'Gateway 指令发送失败:' . ($result['body'] ?: ('HTTP ' . $result['status']));
        }
        if ($command === 'restart') {
            return '已发送 Gateway 重启指令';
        }
        if ($command === 'reload') {
            return '已发送 Gateway 业务平面重启指令';
        }
        if ($command === 'restart_crontab') {
            return '已发送 Gateway Crontab 子进程重启指令';
        }
        if ($command === 'restart_redisqueue') {
            return '已发送 Gateway RedisQueue 子进程重启指令';
        }
        return '已发送 Gateway 指令:' . $command;
    }

    /**
     * 查询 gateway 控制面健康状态。
     *
     * @return string 在线状态描述
     */
    protected function status(): string {
        $port = $this->resolveGatewayPort();
        if ($port <= 0) {
            return 'Gateway 端口未配置';
        }
        $result = $this->requestLocalGateway($port, '/_gateway/healthz');
        if ($result['status'] !== 200) {
            return 'Gateway 不在线';
        }
        return 'Gateway 在线';
    }

    /**
     * 向本地 gateway 控制面发送请求。
     *
     * 常用控制动作会优先使用 Unix Domain Socket，避免依赖 HTTP 监听是否已经对外开放。
     *
     * @param int $port gateway 监听端口
     * @param string $path 请求路径
     * @param string $method HTTP 方法
     * @param array $payload 请求体数据
     * @return array{status:int,body:string} 标准化后的响应结构
     */
    protected function requestLocalGateway(int $port, string $path, string $method = 'GET', array $payload = []): array {
        $ipcAction = match ([$method, $path]) {
            ['POST', '/_gateway/internal/command'] => 'gateway.command',
            ['GET', '/_gateway/internal/console/subscription'] => 'gateway.console.subscription',
            ['GET', '/_gateway/healthz'] => 'gateway.health',
            default => '',
        };
        if ($ipcAction !== '') {
            $ipcResponse = LocalIpc::request(LocalIpc::gatewaySocketPath($port), $ipcAction, $payload, 2.0);
            if (is_array($ipcResponse)) {
                return [
                    'status' => (int)($ipcResponse['status'] ?? 0),
                    'body' => json_encode($ipcResponse['data'] ?? ['message' => (string)($ipcResponse['message'] ?? '')], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
                ];
            }
        }
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            2.0,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return ['status' => 0, 'body' => $errstr ?: 'connect failed'];
        }

        stream_set_timeout($socket, 2);
        $body = $payload ? json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : '';
        $request = "{$method} {$path} HTTP/1.1\r\n"
            . "Host: 127.0.0.1:{$port}\r\n"
            . "Connection: close\r\n";
        if ($method === 'POST') {
            $request .= "Content-Type: application/json\r\n";
            $request .= "Content-Length: " . strlen($body) . "\r\n";
        }
        $request .= "\r\n" . $body;
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);

        if (!is_string($response) || $response === '') {
            return ['status' => 0, 'body' => 'empty response'];
        }
        $status = 0;
        if (preg_match('#^HTTP/\\S+\\s+(\\d{3})#', $response, $matches)) {
            $status = (int)$matches[1];
        }
        $parts = explode("\r\n\r\n", $response, 2);
        return [
            'status' => $status,
            'body' => $parts[1] ?? '',
        ];
    }

    /**
     * 给 server loop 写入停止标记，避免外层守护循环马上再拉起当前 gateway。
     *
     * @return void
     */
    protected function markLoopStop(): void {
        $app = (string)(Manager::instance()->getOpt('app') ?: getenv('APP_DIR') ?: 'app');
        $role = (string)(Manager::instance()->getOpt('role') ?: getenv('SERVER_ROLE') ?: 'master');
        $flagFile = dirname(SCF_ROOT) . '/var/' . $app . '_gateway_' . $role . '.stop';
        @file_put_contents($flagFile, (string)time());
    }

    /**
     * 解析 gateway 业务入口端口。
     *
     * @return int gateway 业务监听端口
     */
    protected function resolveGatewayPort(): int {
        $explicitPort = (int)(Manager::instance()->getOpt('port') ?: 0);
        if ($explicitPort > 0) {
            return $explicitPort;
        }

        $app = (string)(Manager::instance()->getOpt('app') ?: getenv('APP_DIR') ?: APP_DIR_NAME ?: 'app');
        $role = (string)(Manager::instance()->getOpt('role') ?: getenv('SERVER_ROLE') ?: 'master');
        $runtimePort = $this->resolveGatewayPortFromRuntime($app, $role);
        if ($runtimePort > 0) {
            return $runtimePort;
        }

        $serverConfig = Config::server();
        $configuredPort = (int)($serverConfig['port'] ?? 0);
        if ($configuredPort > 0) {
            if ($role === 'slave') {
                // 在未显式传 -port 且没有运行态线索时，保持与 gateway/slave 的默认
                // 端口偏移约定一致，避免 stop/status 误命中 master 端口。
                return $configuredPort + 100;
            }
            return $configuredPort;
        }

        $defaultPort = (int)(defined('SERVER_PORT') ? SERVER_PORT : 0);
        if ($defaultPort > 0 && $role === 'slave') {
            return $defaultPort + 100;
        }
        return $defaultPort;
    }

    /**
     * 依据运行态线索推断目标 role 的 gateway 端口。
     *
     * 优先级：
     * 1. var pid file 对应进程命令行；
     * 2. update 目录 lease（running/restarting）；
     * 3. update 目录 registry 文件名。
     *
     * @param string $app 应用目录名
     * @param string $role 节点角色
     * @return int
     */
    protected function resolveGatewayPortFromRuntime(string $app, string $role): int {
        $pidFile = dirname(SCF_ROOT) . '/var/' . $app . '_gateway_' . $role . '.pid';
        if (is_file($pidFile)) {
            $pid = (int)file_get_contents($pidFile);
            if ($pid > 0 && @Process::kill($pid, 0)) {
                $command = $this->readProcessCommand($pid);
                if ($command !== '' && preg_match('/-port=(\d+)/', $command, $matches)) {
                    $port = (int)($matches[1] ?? 0);
                    if ($port > 0) {
                        return $port;
                    }
                }
            }
        }

        $updateDir = dirname(SCF_ROOT) . '/apps/' . $app . '/update';
        if (!is_dir($updateDir)) {
            return 0;
        }

        $bestPort = 0;
        $bestScore = 0;
        $leaseFiles = glob($updateDir . '/gateway_lease_' . $app . '_*_' . $role . '_*.json') ?: [];
        foreach ($leaseFiles as $leaseFile) {
            if (!is_file($leaseFile)) {
                continue;
            }
            $payload = json_decode((string)@file_get_contents($leaseFile), true);
            if (!is_array($payload)) {
                continue;
            }
            $state = (string)($payload['state'] ?? '');
            if (!in_array($state, ['running', 'restarting'], true)) {
                continue;
            }
            $port = (int)($payload['gateway_port'] ?? 0);
            if ($port <= 0 && preg_match('/_(\d+)\.json$/', basename($leaseFile), $matches)) {
                $port = (int)($matches[1] ?? 0);
            }
            if ($port <= 0) {
                continue;
            }
            $score = max((int)($payload['updated_at'] ?? 0), (int)@filemtime($leaseFile));
            if ($score >= $bestScore) {
                $bestScore = $score;
                $bestPort = $port;
            }
        }
        if ($bestPort > 0) {
            return $bestPort;
        }

        $registryFiles = glob($updateDir . '/gateway_registry_' . $role . '_*.json') ?: [];
        usort($registryFiles, static function (string $a, string $b): int {
            return (@filemtime($b) ?: 0) <=> (@filemtime($a) ?: 0);
        });
        foreach ($registryFiles as $registryFile) {
            if (preg_match('/gateway_registry_' . preg_quote($role, '/') . '_(\d+)\.json$/', basename($registryFile), $matches)) {
                $port = (int)($matches[1] ?? 0);
                if ($port > 0) {
                    return $port;
                }
            }
        }

        return 0;
    }

    /**
     * 解析当前 start 命令会占用的 gateway 监听端口集合。
     *
     * @return array<int, int>
     */
    protected function resolveGatewayListenPortsForStart(): array {
        $serverConfig = Config::server();
        $bindPort = $this->resolveGatewayPort();
        $configPort = (int)($serverConfig['port'] ?? $bindPort);
        $ports = [];

        if ($bindPort > 0) {
            $ports[] = $bindPort;
            $ports[] = $this->resolveGatewayControlPort($bindPort, $configPort, $serverConfig);
        }

        $rpcPort = $this->resolveGatewayRpcPort($bindPort, $configPort, $serverConfig);
        if ($rpcPort > 0) {
            $ports[] = $rpcPort;
        }

        return array_values(array_unique(array_filter(array_map('intval', $ports), static fn(int $port) => $port > 0)));
    }

    /**
     * 解析当前 gateway 的控制面端口。
     *
     * 该逻辑与 CliBootstrap 保持一致，确保 start 前的冲突清理与真正启动时命中的端口一致。
     *
     * @param int $bindPort gateway 业务监听端口
     * @param int $configPort server.php 中配置的业务端口
     * @param array $serverConfig server 配置数组
     * @return int
     */
    protected function resolveGatewayControlPort(int $bindPort, int $configPort, array $serverConfig): int {
        $explicitPort = (int)(Manager::instance()->getOpt('control_port') ?: Manager::instance()->getOpt('gateway_control_port', 0));
        if ($explicitPort > 0) {
            return $explicitPort;
        }

        $configured = (int)($serverConfig['gateway_control_port'] ?? 0);
        if ($configured > 0) {
            $offset = max(1, $configured - $configPort);
            return $bindPort + $offset;
        }

        return $bindPort + 1000;
    }

    /**
     * 解析当前 gateway 的 RPC 监听端口。
     *
     * @param int $bindPort gateway 业务监听端口
     * @param int $configPort server.php 中配置的业务端口
     * @param array $serverConfig server 配置数组
     * @return int
     */
    protected function resolveGatewayRpcPort(int $bindPort, int $configPort, array $serverConfig): int {
        $explicitPort = (int)(Manager::instance()->getOpt('rpc_port') ?: Manager::instance()->getOpt('rport', 0));
        if ($explicitPort > 0) {
            return $explicitPort;
        }

        $configured = (int)($serverConfig['rpc_port'] ?? 0);
        if ($configured <= 0) {
            return 0;
        }

        if ($bindPort === $configPort) {
            return $configured;
        }

        $offset = max(1, $configured - $configPort);
        return max(1, $bindPort + $offset);
    }

    /**
     * 发现当前目标端口上运行的旧 gateway 监听进程。
     *
     * 只回收命令行能识别为同一个 app 的 gateway 主链进程，避免把其它占同端口的
     * 非 gateway 服务误判为可回收对象。
     *
     * @param array<int, int> $ports 目标端口集合
     * @return array<int, int>
     */
    protected function discoverConflictingGatewayListenerPids(array $ports): array {
        $app = (string)(Manager::instance()->getOpt('app') ?: getenv('APP_DIR') ?: APP_DIR_NAME ?: 'app');
        $role = (string)(Manager::instance()->getOpt('role') ?: getenv('SERVER_ROLE') ?: 'master');
        $selfPid = getmypid() ?: 0;
        $pids = [];

        foreach ($ports as $port) {
            if ($port <= 0) {
                continue;
            }
            foreach ($this->discoverListeningPidsByPort($port) as $pid) {
                if ($pid <= 0 || $pid === $selfPid) {
                    continue;
                }
                $command = $this->readProcessCommand($pid);
                if ($command === '') {
                    continue;
                }
                if (!str_contains($command, 'boot gateway start')) {
                    continue;
                }
                if (!str_contains($command, '-app=' . $app)) {
                    continue;
                }
                if (!str_contains($command, '-role=' . $role)) {
                    continue;
                }
                $pids[$pid] = $pid;
            }
        }

        ksort($pids);
        return array_values($pids);
    }

    /**
     * 发现当前 app/role 对应的 gateway 全量运行进程。
     *
     * stop 场景不能再只依赖 pid_file，因为在外层 loop 重拉或异常恢复后，旧 pid
     * 可能已经漂移。这里直接按命令行维度收敛同链进程，确保 stop 能一次性停干净。
     *
     * @param string $app 应用目录名
     * @param string $role 节点角色
     * @param int $gatewayPort gateway 业务端口（可选）
     * @return array<int, int>
     */
    protected function discoverGatewayRuntimePids(string $app, string $role, int $gatewayPort = 0): array {
        $snapshot = $this->readProcessSnapshot();
        if (!$snapshot) {
            return [];
        }
        $selfPid = getmypid() ?: 0;
        $portFlag = $gatewayPort > 0 ? ('-port=' . $gatewayPort) : '';
        $matched = [];
        foreach ($snapshot as $pid => $meta) {
            $pid = (int)$pid;
            $command = (string)($meta['command'] ?? '');
            if ($pid <= 0 || $pid === $selfPid) {
                continue;
            }
            if (!str_contains($command, 'boot gateway start')) {
                continue;
            }
            if (!str_contains($command, '-app=' . $app) || !str_contains($command, '-role=' . $role)) {
                continue;
            }
            if ($portFlag !== '' && !str_contains($command, $portFlag)) {
                continue;
            }
            $matched[$pid] = (int)($meta['ppid'] ?? 0);
        }
        return $this->reduceMatchedProcessRoots($matched);
    }

    /**
     * 发现绑定到当前 gateway_port 的 upstream 运行进程。
     *
     * stop 结束后如果还残留上游进程，会持续占用动态端口并污染下一轮分配。
     * 这里按 `app + gateway_port` 精确收敛对应 upstream 链路。
     *
     * @param string $app 应用目录名
     * @param int $gatewayPort 当前 gateway 业务端口
     * @return array<int, int>
     */
    protected function discoverGatewayUpstreamRuntimePids(string $app, int $gatewayPort): array {
        if ($gatewayPort <= 0) {
            return [];
        }
        $snapshot = $this->readProcessSnapshot();
        if (!$snapshot) {
            return [];
        }
        $selfPid = getmypid() ?: 0;
        $gatewayPortFlag = '-gateway_port=' . $gatewayPort;
        $matched = [];
        foreach ($snapshot as $pid => $meta) {
            $pid = (int)$pid;
            $command = (string)($meta['command'] ?? '');
            if ($pid <= 0 || $pid === $selfPid) {
                continue;
            }
            if (!str_contains($command, 'boot gateway_upstream start')) {
                continue;
            }
            if (!str_contains($command, '-app=' . $app) || !str_contains($command, $gatewayPortFlag)) {
                continue;
            }
            $matched[$pid] = (int)($meta['ppid'] ?? 0);
        }
        return $this->reduceMatchedProcessRoots($matched);
    }

    /**
     * 读取当前系统进程快照（pid/ppid/command）。
     *
     * stop 场景要避免“杀 manager 导致 master 补拉 worker”的副作用，因此需要
     * 先拿到父子关系，再把匹配到的进程集合折叠为根 PID 列表。
     *
     * @return array<int, array{ppid:int,command:string}>
     */
    protected function readProcessSnapshot(): array {
        $output = @shell_exec('ps -axo pid=,ppid=,command=');
        if (!is_string($output) || trim($output) === '') {
            return [];
        }
        $snapshot = [];
        foreach (preg_split('/\r?\n/', $output) ?: [] as $line) {
            $line = trim((string)$line);
            if ($line === '' || !preg_match('/^(\d+)\s+(\d+)\s+(.+)$/', $line, $matches)) {
                continue;
            }
            $pid = (int)($matches[1] ?? 0);
            $ppid = (int)($matches[2] ?? 0);
            $command = trim((string)($matches[3] ?? ''));
            if ($pid <= 0 || $command === '') {
                continue;
            }
            $snapshot[$pid] = [
                'ppid' => $ppid,
                'command' => $command,
            ];
        }
        return $snapshot;
    }

    /**
     * 将命中条件的进程集合收敛成“树根 PID”列表。
     *
     * 同一条 gateway/upstream 进程树里的 manager/worker 与 master 命令行通常一致。
     * stop 若按全量 PID 发信号，会误打 manager/worker 并触发 master 补拉。这里统一
     * 只返回每棵匹配树的根 PID（通常就是 Swoole master）。
     *
     * @param array<int, int> $matchedPpid pid => ppid
     * @return array<int, int>
     */
    protected function reduceMatchedProcessRoots(array $matchedPpid): array {
        if (!$matchedPpid) {
            return [];
        }
        $roots = [];
        foreach (array_keys($matchedPpid) as $pid) {
            $current = (int)$pid;
            $depth = 0;
            while ($current > 0 && isset($matchedPpid[$current]) && $depth < 64) {
                $depth++;
                $parent = (int)($matchedPpid[$current] ?? 0);
                if ($parent <= 0 || $parent === $current || !isset($matchedPpid[$parent])) {
                    break;
                }
                $current = $parent;
            }
            if ($current > 0) {
                $roots[$current] = $current;
            }
        }
        ksort($roots);
        return array_values($roots);
    }

    /**
     * 过滤掉已退出的 PID，保留仍存活的目标进程。
     *
     * @param array<int, int> $pids 候选 PID 列表
     * @return array<int, int>
     */
    protected function filterAlivePids(array $pids): array {
        $alive = [];
        foreach ($pids as $pid) {
            $pid = (int)$pid;
            if ($pid > 0 && Process::kill($pid, 0)) {
                $alive[$pid] = $pid;
            }
        }
        ksort($alive);
        return array_values($alive);
    }

    /**
     * 等待一组进程退出。
     *
     * @param array<int, int> $pids 目标 PID 列表
     * @param int $timeoutSeconds 最长等待秒数
     * @param int $intervalMs 轮询间隔毫秒
     * @return bool 全部退出返回 true
     */
    protected function waitForProcessesExited(array $pids, int $timeoutSeconds = 8, int $intervalMs = 200): bool {
        $targets = array_values(array_filter(array_map('intval', $pids), static fn(int $pid): bool => $pid > 0));
        if (!$targets) {
            return true;
        }
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            if (!$this->filterAlivePids($targets)) {
                return true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        return !$this->filterAlivePids($targets);
    }

    /**
     * 读取指定监听端口上的进程 PID。
     *
     * @param int $port
     * @return array<int, int>
     */
    protected function discoverListeningPidsByPort(int $port): array {
        $output = @shell_exec('lsof -nP -t -iTCP:' . (int)$port . ' -sTCP:LISTEN 2>/dev/null');
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
     * 读取进程命令行，供冲突进程过滤使用。
     *
     * @param int $pid
     * @return string
     */
    protected function readProcessCommand(int $pid): string {
        if ($pid <= 0) {
            return '';
        }

        $output = @shell_exec('ps -p ' . $pid . ' -o command= 2>/dev/null');
        return trim((string)$output);
    }

    /**
     * 向一组进程发送退出信号。
     *
     * @param array<int, int> $pids
     * @param int $signal
     * @return void
     */
    protected function terminateProcesses(array $pids, int $signal): void {
        foreach ($pids as $pid) {
            $pid = (int)$pid;
            if ($pid <= 0 || !Process::kill($pid, 0)) {
                continue;
            }
            @Process::kill($pid, $signal);
        }
    }

    /**
     * 等待目标端口释放。
     *
     * @param array<int, int> $ports
     * @param int $timeoutSeconds
     * @param int $intervalMs
     * @return bool
     */
    protected function waitForPortsReleased(array $ports, int $timeoutSeconds = 10, int $intervalMs = 200): bool {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            $occupied = false;
            foreach ($ports as $port) {
                if ($port > 0 && $this->isPortListening($port)) {
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

    /**
     * 判断本机端口是否仍处于监听状态。
     *
     * @param int $port
     * @return bool
     */
    protected function isPortListening(int $port): bool {
        $socket = @stream_socket_client(
            'tcp://127.0.0.1:' . $port,
            $errno,
            $errstr,
            0.2,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return false;
        }

        fclose($socket);
        return true;
    }
}
