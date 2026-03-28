<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Scf\Core\Console;
use Scf\Core\Server as CoreServer;
use Swoole\Coroutine;
use Swoole\Coroutine\Http\Client as CoroutineHttpClient;
use Swoole\Process;
use Swoole\Server;
use Throwable;
use const SIGKILL;
use const SIGTERM;

/**
 * gateway 托管 upstream 进程的启动与回收协调器。
 *
 * 这一层负责把 gateway 的运行参数翻译为可执行子进程命令，完成端口与健康就绪探测，
 * 并在回收阶段通过内部控制接口触发优雅停机、业务 quiesce 和兜底强杀。
 */
class AppServerLauncher {

    protected const GRACEFUL_SHUTDOWN_PATH = '/_gateway/internal/upstream/shutdown';
    protected const STATUS_PATH = '/_gateway/internal/upstream/status';
    protected const HEALTH_PATH = '/_gateway/internal/upstream/health';
    protected const HTTP_PROBE_PATH = '/_gateway/internal/upstream/http_probe';
    protected const QUIESCE_BUSINESS_PATH = '/_gateway/internal/upstream/quiesce';
    public const NORMAL_RECYCLE_GRACE_SECONDS = 1800;
    protected const RECYCLE_WARN_AFTER_SECONDS = 60;
    protected const RECYCLE_WARN_INTERVAL_SECONDS = 60;

    /**
     * 探测指定主机端口是否已在监听。
     *
     * @param string $host 目标主机名或 IP。
     * @param int $port 目标端口。
     * @param float $timeoutSeconds 连接探测超时，单位秒。
     * @return bool 端口可连接时返回 true，否则返回 false。
     */
    public function isListening(string $host, int $port, float $timeoutSeconds = 0.3): bool {
        if ($port <= 0) {
            return false;
        }
        $host = $this->normalizeProbeHost($host);

        // gateway 托管的 upstream 端口探测绝大多数发生在本机。这里优先复用框架
        // 现有的“监听进程扫描”能力，而不是在协程定时器里走 TCP connect。
        // 这样可以避免 reload / recycle watcher 只剩少量协程时，再因为 connect
        // 挂起把 worker 带进 "all coroutines are asleep" 死锁态。
        if ($this->isLocalProbeHost($host)) {
            return CoreServer::isListeningPortInUse($port);
        }

        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $timeoutSeconds,
            STREAM_CLIENT_CONNECT
        );
        if (is_resource($socket)) {
            fclose($socket);
            return true;
        }
        return false;
    }

    /**
     * 归一化端口探测目标主机。
     *
     * @param string $host
     * @return string
     */
    protected function normalizeProbeHost(string $host): string {
        $host = trim($host);
        if ($host === '' || in_array($host, ['127.0.0.1', 'localhost', '0.0.0.0', '::', '::1'], true)) {
            return '127.0.0.1';
        }

        if (defined('SERVER_HOST') && $host === SERVER_HOST) {
            return $host === '0.0.0.0' ? '127.0.0.1' : $host;
        }

        return $host;
    }

    /**
     * 判断探测目标是否仍可视为当前节点本机地址。
     *
     * gateway 托管的 upstream 都运行在本机，因此对这类地址直接走“端口是否被监听”
     * 的进程扫描更稳，不需要再做一次 TCP connect。
     *
     * @param string $host
     * @return bool
     */
    protected function isLocalProbeHost(string $host): bool {
        if ($host === '127.0.0.1') {
            return true;
        }

        if (!defined('SERVER_HOST')) {
            return false;
        }

        return trim((string)SERVER_HOST) !== '' && $host === trim((string)SERVER_HOST);
    }

    /**
     * 从起始端口开始扫描可用端口。
     *
     * @param string $host 目标主机名或 IP。
     * @param int $startPort 扫描起始端口。
     * @param int $maxScan 最大扫描次数。
     * @return int 找到的可用端口。
     * @throws RuntimeException 当连续扫描后仍未找到可用端口时抛出。
     */
    public function findAvailablePort(string $host, int $startPort, int $maxScan = 200): int {
        $port = max(1025, $startPort);
        for ($i = 0; $i < $maxScan; $i++, $port++) {
            if ($this->isListening($host, $port, 0.1)) {
                continue;
            }
            $server = @stream_socket_server("tcp://{$host}:{$port}", $errno, $errstr);
            if ($server === false) {
                continue;
            }
            fclose($server);
            return $port;
        }
        throw new RuntimeException("未找到可用端口，起始端口:{$startPort}");
    }

    /**
     * 构造一个可由 gateway 托管的 upstream 进程对象。
     *
     * @param array<string, mixed> $options 启动参数集合。
     * @return array<string, mixed> 包含 Process 对象及其派生运行信息的规格描述。
     * @throws RuntimeException 当关键启动参数缺失时抛出。
     */
    public function createManagedProcess(array $options): array {
        $app = (string)($options['app'] ?? '');
        $env = (string)($options['env'] ?? 'production');
        $role = (string)($options['role'] ?? 'master');
        $port = (int)($options['port'] ?? 0);
        $rpcPort = (int)($options['rpc_port'] ?? 0);
        $src = (string)($options['src'] ?? '');
        $extra = is_array($options['extra'] ?? null) ? $options['extra'] : [];

        if ($app === '') {
            throw new RuntimeException('启动业务 server 缺少 app');
        }
        if ($port <= 0) {
            throw new RuntimeException('启动业务 server 缺少有效 port');
        }

        $command = $this->buildCommand($app, $env, $role, $port, $rpcPort, $src, $extra);
        $binary = PHP_BINARY;
        $args = array_slice($command, 1);

        $process = new Process(function (Process $process) use ($binary, $args) {
            $process->exec($binary, $args);
            exit(1);
        }, false, SOCK_DGRAM, false);

        return [
            'process' => $process,
            'port' => $port,
            'host' => (string)($options['host'] ?? '127.0.0.1'),
            'role' => $role,
            'command' => $binary . ' ' . implode(' ', array_map('escapeshellarg', $args)),
        ];
    }

    /**
     * 将托管进程挂到 gateway 主 server 的子进程列表。
     *
     * @param Server $server gateway 主 server 实例。
     * @param array<string, mixed> $spec 由 createManagedProcess() 生成的进程规格。
     * @return array<string, mixed> 原样返回进程规格，便于继续传递。
     * @throws RuntimeException 当规格中没有有效 Process 对象时抛出。
     */
    public function attachManagedProcess(Server $server, array $spec): array {
        /** @var Process|null $process */
        $process = $spec['process'] ?? null;
        if (!$process instanceof Process) {
            throw new RuntimeException('托管业务 server 缺少有效进程对象');
        }

        $server->addProcess($process);
        return $spec;
    }

    /**
     * 拉起一个 managed upstream，并返回其 pid / command 等运行态信息。
     *
     * @param array<string, mixed> $options 启动参数集合。
     * @return array<string, mixed> 包含 pid、host、port、role 和 command 的运行态信息。
     * @throws RuntimeException 当进程启动失败时抛出。
     */
    public function launch(array $options): array {
        $spec = $this->createManagedProcess($options);
        /** @var Process $process */
        $process = $spec['process'];
        $pid = $process->start();
        if ($pid <= 0) {
            throw new RuntimeException('拉起业务 server 失败:process start failed');
        }

        $spec['pid'] = $pid;
        unset($spec['process']);
        return $spec;
    }

    /**
     * 从进程描述中读取启动后的 pid。
     *
     * @param array<string, mixed> $spec 进程规格。
     * @return int 进程 pid；不存在时返回 0。
     */
    public function processPid(array $spec): int {
        /** @var Process|null $process */
        $process = $spec['process'] ?? null;
        if (!$process instanceof Process) {
            return 0;
        }
        return (int)($process->pid ?? 0);
    }

    /**
     * 组装 upstream 启动命令。
     *
     * 这里负责继承 gateway 的开发模式、打包模式、源码类型以及 gateway 端口，
     * 保证子进程与父进程处在同一运行语义下。
     *
     * @param string $app 业务应用名。
     * @param string $env 运行环境。
     * @param string $role 角色标识。
     * @param int $port HTTP 端口。
     * @param int $rpcPort RPC 端口。
     * @param string $src 源码载体类型。
     * @param array<int, string> $extra 额外启动参数。
     * @return array<int, string> 可直接用于 exec 的命令数组。
     */
    protected function buildCommand(string $app, string $env, string $role, int $port, int $rpcPort, string $src, array $extra): array {
        $extra = $this->sanitizeExtraFlags($extra);
        $hasDevFlag = in_array('-dev', $extra, true);
        $hasPackModeFlag = in_array('-pack', $extra, true) || in_array('-nopack', $extra, true);
        $hasDirFlag = in_array('-dir', $extra, true);
        $hasPharFlag = in_array('-phar', $extra, true);
        $command = [
            PHP_BINARY,
            SCF_ROOT . '/boot',
            'gateway_upstream',
            'start',
            "-app={$app}",
            "-env={$env}",
            "-role={$role}",
            "-port={$port}",
        ];

        if (strtolower($env) === 'dev' && !$hasDevFlag) {
            $command[] = '-dev';
        }
        if (defined('FRAMEWORK_IS_PHAR') && FRAMEWORK_IS_PHAR === false && !$hasPackModeFlag) {
            $command[] = '-nopack';
        }

        if ($src === 'dir' && !$hasDirFlag && !$hasPharFlag) {
            $command[] = '-dir';
        } elseif ($src === 'phar' && !$hasPharFlag && !$hasDirFlag) {
            $command[] = '-phar';
        }

        foreach ($extra as $arg) {
            if (!is_string($arg) || $arg === '') {
                continue;
            }
            $command[] = $arg;
        }

        if ($rpcPort > 0) {
            $command[] = '-rport=' . $rpcPort;
        }

        return $command;
    }

    /**
     * 清理需要由 launcher 接管的启动参数，避免重复注入或冲突。
     *
     * @param array<int, mixed> $extra 额外启动参数。
     * @return array<int, string> 过滤后的参数列表。
     */
    protected function sanitizeExtraFlags(array $extra): array {
        return array_values(array_filter($extra, static function ($arg) {
            if (!is_string($arg) || $arg === '') {
                return false;
            }
            return !str_starts_with($arg, '-rport=');
        }));
    }

    /**
     * 停止一个 managed upstream。
     *
     * 先尝试内部 quiesce/shutdown，再在超时后退化为 SIGTERM/SIGKILL。
     *
     * @param array<string, mixed> $instance upstream 实例运行态信息。
     * @param int $graceSeconds 优雅回收等待时间，单位秒。
     * @return void
     */
    public function stop(array $instance, int $graceSeconds = self::NORMAL_RECYCLE_GRACE_SECONDS): void {
        $metadata = (array)($instance['metadata'] ?? []);
        if (($metadata['managed'] ?? false) !== true) {
            return;
        }
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $rpcPort = (int)($metadata['rpc_port'] ?? ($instance['rpc_port'] ?? 0));
        $pid = (int)($metadata['pid'] ?? 0);
        $startedAt = microtime(true);
        $deadline = $startedAt + max(1, $graceSeconds);
        $nextWarnAt = $startedAt + self::RECYCLE_WARN_AFTER_SECONDS;
        $gracefulAccepted = $this->requestGracefulShutdown($host, $port);
        $termSent = false;
        $termAt = $gracefulAccepted ? $deadline : microtime(true);

        while (microtime(true) < $deadline) {
            $httpAlive = $port > 0 && $this->isListening($host, $port, 0.2);
            $rpcAlive = $rpcPort > 0 && $this->isListening($host, $rpcPort, 0.2);
            $pidAlive = $this->isProcessRunning($pid);
            if (!$httpAlive && !$rpcAlive && !$pidAlive) {
                return;
            }
            $now = microtime(true);
            if ($now >= $nextWarnAt) {
                $elapsed = max(1, (int)floor($now - $startedAt));
                $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                Console::warning(
                    "【Gateway】业务实例仍在等待平滑回收({$elapsed}s): {$host}:{$port}{$rpcInfo}, "
                    . "pid={$pid}, http=" . ($httpAlive ? 'alive' : 'down')
                    . ", rpc=" . ($rpcAlive ? 'alive' : 'down')
                    . ", process=" . ($pidAlive ? 'alive' : 'down')
                );
                $nextWarnAt += self::RECYCLE_WARN_INTERVAL_SECONDS;
            }
            if (!$termSent && microtime(true) >= $termAt) {
                if ($this->isProcessRunning($pid)) {
                    @Process::kill($pid, SIGTERM);
                }
                $termSent = true;
            }
            usleep(200000);
        }

        $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
        Console::warning("【Gateway】业务实例回收超时，开始强制终止: {$host}:{$port}{$rpcInfo}, pid={$pid}");
        if ($this->isProcessRunning($pid)) {
            @Process::kill($pid, SIGKILL);
        }
        if ($port > 0 && $this->isListening($host, $port, 0.2)) {
            \Scf\Core\Server::killProcessByPort($port);
        }
        if ($rpcPort > 0 && $this->isListening($host, $rpcPort, 0.2)) {
            \Scf\Core\Server::killProcessByPort($rpcPort);
        }
    }

    /**
     * 向 upstream 发起优雅停机请求。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return bool 请求成功并返回 200 时为 true。
     */
    protected function requestGracefulShutdown(string $host, int $port, float $timeoutSeconds = 1.0): bool {
        return $this->requestInternalPost($host, $port, self::GRACEFUL_SHUTDOWN_PATH, $timeoutSeconds);
    }

    /**
     * 判断指定 pid 是否仍然存活且不是僵尸进程。
     *
     * @param int $pid 进程号。
     * @return bool 进程仍存活且非僵尸时返回 true。
     */
    protected function isProcessRunning(int $pid): bool {
        if ($pid <= 0 || !@Process::kill($pid, 0)) {
            return false;
        }
        $stat = @shell_exec("ps -o stat= -p " . (int)$pid . " 2>/dev/null");
        if (!is_string($stat) || trim($stat) === '') {
            return false;
        }
        return !str_contains(trim($stat), 'Z');
    }

    /**
     * 向 upstream 发起业务面 quiesce，请其停止接新任务但保留尾流。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return bool 请求成功并返回 200 时为 true。
     */
    public function requestBusinessQuiesce(string $host, int $port, float $timeoutSeconds = 1.0): bool {
        return $this->requestInternalPost($host, $port, self::QUIESCE_BUSINESS_PATH, $timeoutSeconds);
    }

    /**
     * 发送内部 POST 控制请求。
     *
     * 优先走本机 UDS，再按 coroutine / blocking TCP 兜底。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param string $path 控制接口路径。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return bool 请求成功并返回 200 时为 true。
     */
    protected function requestInternalPost(string $host, int $port, string $path, float $timeoutSeconds = 1.0): bool {
        if ($port <= 0) {
            return false;
        }
        $ipcAction = $this->mapUpstreamControlPathToIpcAction($path);
        if ($ipcAction === '') {
            return false;
        }
        $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $timeoutSeconds);
        return is_array($ipcResponse) && (int)($ipcResponse['status'] ?? 0) === 200;
    }

    /**
     * 仅等待端口开始监听，不判断业务健康状态。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port 目标端口。
     * @param int $timeoutSeconds 超时时间，单位秒。
     * @param int $intervalMs 轮询间隔，单位毫秒。
     * @return bool 端口在超时内监听成功时返回 true。
     */
    public function waitUntilReady(string $host, int $port, int $timeoutSeconds = 20, int $intervalMs = 200): bool {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            if ($this->isListening($host, $port, 0.2)) {
                return true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        return false;
    }

    /**
     * 等待 upstream 的 HTTP/RPC 端口、server 健康状态以及真实 HTTP worker 探针同时 ready。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port HTTP 端口。
     * @param int $rpcPort RPC 端口，传 0 表示不检查。
     * @param int $timeoutSeconds 超时时间，单位秒。
     * @param int $intervalMs 轮询间隔，单位毫秒。
     * @param bool $verbose 是否输出等待日志。
     * @return bool 三项条件在超时内同时满足时返回 true。
     */
    public function waitUntilServicesReady(string $host, int $port, int $rpcPort = 0, int $timeoutSeconds = 20, int $intervalMs = 200, bool $verbose = true): bool {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $logged = false;
        while (microtime(true) < $deadline) {
            $httpReady = $this->isListening($host, $port, 0.2);
            $rpcReady = $rpcPort <= 0 || $this->isListening($host, $rpcPort, 0.2);
            $statusReady = false;
            $httpProbeReady = false;
            if ($httpReady) {
                $status = $this->fetchHealthStatus($host, $port, min(1.0, max(0.2, $intervalMs / 1000)));
                $statusReady = is_array($status)
                    && (bool)($status['server_is_ready'] ?? false) === true
                    && (bool)($status['server_is_alive'] ?? false) === true
                    && (bool)($status['server_is_draining'] ?? false) === false;
                if ($statusReady) {
                    // 端口在监听且 server 自报 ready 还不够，只有真实 HTTP 探针也成功才算业务 worker 可用。
                    $httpProbeReady = $this->probeHttpConnectivity($host, $port, self::HTTP_PROBE_PATH, min(1.0, max(0.2, $intervalMs / 1000)));
                }
            }
            if ($httpReady && $rpcReady && $statusReady && $httpProbeReady) {
                if ($verbose && $logged) {
                    $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                    echo Console::timestamp() . " 【Gateway】业务实例端口就绪: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
                }
                return true;
            }
            if ($verbose && !$logged) {
                $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                echo Console::timestamp() . " 【Gateway】等待业务实例端口就绪: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
                $logged = true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        if ($verbose && $logged) {
            $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
            echo Console::timestamp() . " 【Gateway】等待业务实例端口就绪超时: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
        }
        return false;
    }

    /**
     * 通过真实 HTTP GET 请求验证 upstream worker 是否还能正常收发请求。
     *
     * 这条探针专门命中 upstream 的 onRequest 分支，用来兜住“server ready 但 worker 假死”的场景。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param string $path 探针路径。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return bool 请求收到 200 且 payload 为合法 JSON 时返回 true。
     */
    public function probeHttpConnectivity(string $host, int $port, string $path = self::HTTP_PROBE_PATH, float $timeoutSeconds = 0.5): bool {
        if ($host === '' || $port <= 0) {
            return false;
        }
        $timeoutSeconds = max(0.1, $timeoutSeconds);
        if (Coroutine::getCid() > 0) {
            try {
                $client = new CoroutineHttpClient($host, $port);
                $client->set(['timeout' => $timeoutSeconds, 'keep_alive' => false]);
                $client->setHeaders([
                    'Host' => $host . ':' . $port,
                    'Connection' => 'close',
                ]);
                $ok = $client->get($path);
                $body = $client->body ?? '';
                $statusCode = (int)($client->statusCode ?? 0);
                $client->close();
                return $ok && $statusCode === 200 && is_string($body) && $body !== '';
            } catch (Throwable) {
                return false;
            }
        }

        $context = stream_context_create([
            'http' => [
                'method' => 'GET',
                'timeout' => $timeoutSeconds,
                'ignore_errors' => true,
                'header' => implode("\r\n", [
                    'Host: ' . $host . ':' . $port,
                    'Connection: close',
                ]),
            ],
        ]);
        $body = @file_get_contents('http://' . $host . ':' . $port . $path, false, $context);
        if (!is_string($body) || $body === '') {
            return false;
        }
        $decoded = json_decode($body, true);
        if (!is_array($decoded) || json_last_error() !== JSON_ERROR_NONE) {
            return false;
        }
        $statusCode = 0;
        foreach ((array)($http_response_header ?? []) as $headerLine) {
            if (preg_match('#^HTTP/\S+\s+(\d{3})#', (string)$headerLine, $matches)) {
                $statusCode = (int)$matches[1];
                break;
            }
        }
        return $statusCode === 200;
    }

    /**
     * 拉取 upstream 的运行态统计。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return array<string, mixed>|null 成功时返回运行态数据，否则返回 null。
     */
    protected function fetchRuntimeStatus(string $host, int $port, float $timeoutSeconds = 0.5): ?array {
        return $this->fetchInternalJsonStatus($host, $port, self::STATUS_PATH, $timeoutSeconds);
    }

    /**
     * 拉取 upstream 的健康状态。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return array<string, mixed>|null 成功时返回健康状态数据，否则返回 null。
     */
    protected function fetchHealthStatus(string $host, int $port, float $timeoutSeconds = 0.5): ?array {
        return $this->fetchInternalJsonStatus($host, $port, self::HEALTH_PATH, $timeoutSeconds);
    }

    /**
     * 从 upstream 的内部状态接口读取 JSON 数据。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param string $path 内部状态接口路径。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return array<string, mixed>|null 返回接口 data 字段，失败时返回 null。
     */
    protected function fetchInternalJsonStatus(string $host, int $port, string $path, float $timeoutSeconds = 0.5): ?array {
        if ($port <= 0) {
            return null;
        }
        $ipcAction = $this->mapUpstreamStatusPathToIpcAction($path);
        if ($ipcAction === '') {
            return null;
        }
        $effectiveTimeout = $path === self::STATUS_PATH ? max($timeoutSeconds, 5.0) : $timeoutSeconds;
        $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $effectiveTimeout);
        if (!is_array($ipcResponse) || (int)($ipcResponse['status'] ?? 0) !== 200) {
            return null;
        }
        $data = $ipcResponse['data'] ?? null;
        return is_array($data) ? $data : null;
    }

    /**
     * 把 upstream 状态接口路径映射为本地 IPC 动作名。
     *
     * @param string $path 内部状态接口路径。
     * @return string 对应的 IPC action；未映射时返回空字符串。
     */
    protected function mapUpstreamStatusPathToIpcAction(string $path): string {
        return match ($path) {
            self::STATUS_PATH => 'upstream.status',
            self::HEALTH_PATH => 'upstream.health',
            default => '',
        };
    }

    /**
     * 把 upstream 控制接口路径映射为本地 IPC 动作名。
     *
     * @param string $path 内部控制接口路径。
     * @return string 对应的 IPC action；未映射时返回空字符串。
     */
    protected function mapUpstreamControlPathToIpcAction(string $path): string {
        return match ($path) {
            self::GRACEFUL_SHUTDOWN_PATH => 'upstream.shutdown',
            self::QUIESCE_BUSINESS_PATH => 'upstream.quiesce',
            default => '',
        };
    }

}
