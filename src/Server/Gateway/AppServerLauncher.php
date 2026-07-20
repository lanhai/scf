<?php

namespace Scf\Server\Gateway;

use RuntimeException;
use Scf\Core\Console;
use Scf\Core\Server as CoreServer;
use Scf\Util\ProcessCommandLine;
use Scf\Util\ProcessInspector;
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
    protected const PROCESS_STATE_CACHE_TTL_SECONDS = 5;
    protected const PROCESS_STATE_FAILURE_CACHE_TTL_SECONDS = 1;
    protected array $processStateCache = [];
    /**
     * @var array<int, array{token:string,lock_file:string,started_at:float}>
     */
    protected array $trustedLaunches = [];

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

        // 高频路径只允许原生 connect。PID ownership 仅在真正发送信号的
        // 低频边界查询，禁止健康/状态轮询周期性启动 lsof。
        if ($this->isLocalProbeHost($host)) {
            return $this->probeTcpConnectivity($host, $port, min(0.05, max(0.01, $timeoutSeconds)));
        }

        return $this->probeTcpConnectivity($host, $port, $timeoutSeconds);
    }

    /**
     * 用 TCP connect 快速判断端口是否可达。
     *
     * @param string $host
     * @param int $port
     * @param float $timeoutSeconds
     * @return bool
     */
    protected function probeTcpConnectivity(string $host, int $port, float $timeoutSeconds): bool {
        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $timeoutSeconds,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return false;
        }
        fclose($socket);
        return true;
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
     * gateway 托管的 upstream 都运行在本机，因此本机地址可以走轻量 bind 冲突探测，
     * 避免高频端口扫描时频繁触发外部进程级探测开销。
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
        $bindHost = $this->normalizeBindHost($host);
        for ($i = 0; $i < $maxScan; $i++, $port++) {
            if (!CoreServer::isPortInUse($port, $bindHost)) {
                return $port;
            }
        }
        throw new RuntimeException("未找到可用端口，起始端口:{$startPort}");
    }

    /**
     * 从起始端口开始扫描可用端口，并排除一组保留端口。
     *
     * 这个方法用于 gateway/upstream 联合编排场景：除了“端口当前没人监听”，
     * 还要保证不和 gateway 业务入口、gateway RPC、gateway 控制面及本轮
     * 其它已预留端口冲突，从根上避免“计划端口可用但运行时被控制面占掉”。
     *
     * @param string $host 目标主机名或 IP。
     * @param int $startPort 扫描起始端口。
     * @param array<int, int> $reservedPorts 保留端口集合。
     * @param int $maxScan 最大扫描次数。
     * @return int 找到的可用端口。
     * @throws RuntimeException 当连续扫描后仍未找到可用端口时抛出。
     */
    public function findAvailablePortAvoiding(string $host, int $startPort, array $reservedPorts = [], int $maxScan = 200): int {
        $reserved = [];
        foreach ($reservedPorts as $reservedPort) {
            $reservedPort = (int)$reservedPort;
            if ($reservedPort > 0) {
                $reserved[$reservedPort] = true;
            }
        }

        $port = max(1025, $startPort);
        $bindHost = $this->normalizeBindHost($host);
        for ($i = 0; $i < $maxScan; $i++, $port++) {
            if (isset($reserved[$port])) {
                continue;
            }
            if (!CoreServer::isPortInUse($port, $bindHost)) {
                return $port;
            }
        }
        throw new RuntimeException("未找到可用端口(含保留端口约束)，起始端口:{$startPort}");
    }

    /**
     * 归一化真实 socket_bind 使用的地址；与 connect 探测语义分开。
     */
    protected function normalizeBindHost(string $host): string {
        $host = trim($host);
        return match ($host) {
            '', '::' => '0.0.0.0',
            'localhost', '::1' => '127.0.0.1',
            default => $host,
        };
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
        $launchToken = bin2hex(random_bytes(16));
        $launchLockFile = rtrim(sys_get_temp_dir(), DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR . 'scf-upstream-launch-' . $launchToken . '.lock';

        $process = new Process(function (Process $process) use ($binary, $args, $launchToken, $launchLockFile) {
            // 在 exec(PHP_BINARY) 之前取得唯一身份锁。即便新 PHP 卡在
            // macOS 的 _dyld_start，父进程也能在不依赖 ps/lsof 的情况下
            // 精确识别并回收自己刚创建的 PID。
            $lock = @fopen($launchLockFile, 'c+');
            if (!is_resource($lock) || !@flock($lock, LOCK_EX | LOCK_NB)) {
                if (is_resource($lock)) {
                    @fclose($lock);
                }
                exit(1);
            }
            $identity = json_encode([
                'pid' => getmypid(),
                'token' => $launchToken,
                'started_at' => microtime(true),
            ], JSON_UNESCAPED_SLASHES);
            @rewind($lock);
            @ftruncate($lock, 0);
            @fwrite($lock, is_string($identity) ? $identity : '');
            @fflush($lock);
            $process->exec($binary, $args);
            // exec 只有失败才会返回；成功时文件描述符随新进程继续持锁。
            @flock($lock, LOCK_UN);
            @fclose($lock);
            @unlink($launchLockFile);
            exit(1);
        }, false, SOCK_DGRAM, false);

        return [
            'process' => $process,
            'port' => $port,
            'host' => (string)($options['host'] ?? '127.0.0.1'),
            'role' => $role,
            'command' => $binary . ' ' . implode(' ', array_map('escapeshellarg', $args)),
            'launch_token' => $launchToken,
            'launch_lock_file' => $launchLockFile,
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
        $this->reapTrustedLaunches();
        $spec = $this->createManagedProcess($options);
        /** @var Process $process */
        $process = $spec['process'];
        $pid = $process->start();
        if ($pid <= 0) {
            @unlink((string)($spec['launch_lock_file'] ?? ''));
            throw new RuntimeException('拉起业务 server 失败:process start failed');
        }

        $spec['pid'] = $pid;
        $spec['launch_pid'] = $pid;
        $this->trustedLaunches[$pid] = [
            'token' => (string)($spec['launch_token'] ?? ''),
            'lock_file' => (string)($spec['launch_lock_file'] ?? ''),
            'started_at' => microtime(true),
        ];
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
                // 回收升级不能直接对 metadata.pid 发 TERM：
                // 在代际切换与状态延迟窗口里，pid 可能短暂指向 manager/worker，
                // 直接杀它会被 master 立即补拉 worker，表现为“正在关闭后又启动”。
                // 这里统一复用 owner 感知的树形回收，确保信号命中真正的实例根进程。
                $this->forceStopManagedInstance($instance, false);
                $termSent = true;
            }
            usleep(200000);
        }

        $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
        Console::warning("【Gateway】业务实例回收超时，开始强制终止: {$host}:{$port}{$rpcInfo}, pid={$pid}");
        $this->forceStopManagedInstance($instance, true);
        // 强制回收后短暂等待一次监听态收敛，再做最后一次 owner-aware 硬回收。
        usleep(200000);
        $stillHttpAlive = $port > 0 && $this->isListening($host, $port, 0.2);
        $stillRpcAlive = $rpcPort > 0 && $this->isListening($host, $rpcPort, 0.2);
        if ($stillHttpAlive || $stillRpcAlive) {
            $this->forceStopManagedInstance($instance, true);
        }
    }

    /**
     * 对指定实例发起“仅优雅停机请求”，不在此方法内等待回收完成。
     *
     * 这个入口用于 gateway 的异步回收状态机：先让 upstream 自己进入
     * draining/shutdown，再由外层定时驱动决定是否升级到 TERM/KILL。
     *
     * @param array<string, mixed> $instance upstream 实例运行态信息。
     * @param float $timeoutSeconds 请求超时，单位秒。
     * @return bool 请求被 upstream 接收时返回 true。
     */
    public function requestManagedGracefulShutdown(array $instance, float $timeoutSeconds = 1.0): bool {
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        if ($port <= 0) {
            return false;
        }
        return $this->requestGracefulShutdown($host, $port, $timeoutSeconds);
    }

    /**
     * 对指定实例执行一次强制回收动作（可重复调用）。
     *
     * soft 模式优先发送 SIGTERM；hard 模式直接 SIGKILL。
     *
     * 注意：这里不能再“按端口盲杀全部监听者”。滚动重启期间端口可能被下一代实例复用，
     * 若不做 owner 约束会把新代实例误杀，导致回收永远不收敛。
     * 该方法不阻塞等待结果，适合作为异步 reaper 的周期动作。
     *
     * @param array<string, mixed> $instance upstream 实例运行态信息。
     * @param bool $hard 是否执行硬回收。
     * @return void
     */
    public function forceStopManagedInstance(array $instance, bool $hard = false): void {
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $metadata = (array)($instance['metadata'] ?? []);
        $rpcPort = (int)($instance['rpc_port'] ?? ($metadata['rpc_port'] ?? 0));
        $masterPid = (int)($metadata['master_pid'] ?? 0);
        $managerPid = (int)($metadata['manager_pid'] ?? 0);
        $pid = (int)($metadata['pid'] ?? 0);
        $launchPid = (int)($metadata['launch_pid'] ?? 0);
        $signal = $hard ? SIGKILL : SIGTERM;
        // 单次回收只允许采一份 pid/ppid/command 快照，后续 owner 解析与
        // 进程树回收全部复用，禁止每个 PID 再启动一次 ps。
        $snapshot = $this->loadProcessSnapshot();
        $targetRootPids = [];
        $hasOwnedRoot = false;
        $masterCommand = (string)($snapshot[$masterPid]['command'] ?? '');
        if (
            $masterPid > 0
            && @Process::kill($masterPid, 0)
            && $this->commandMatchesManagedOwnership($masterCommand, $instance)
        ) {
            $targetRootPids[$masterPid] = $masterPid;
            $hasOwnedRoot = true;
        }
        if ($managerPid > 0 && @Process::kill($managerPid, 0)) {
            $resolvedRootPid = $this->resolveOwnedManagedRootPid($managerPid, $snapshot, $instance);
            if ($resolvedRootPid > 0) {
                $targetRootPids[$resolvedRootPid] = $resolvedRootPid;
                $hasOwnedRoot = true;
            }
        }
        if ($pid > 0) {
            // metadata.pid 可能短暂指向 manager/worker。强制回收必须先解析到 owner 根进程，
            // 否则只杀 manager 会被 master 立刻补拉 worker，出现“关闭后又启动”的假象。
            $resolvedRootPid = $this->resolveOwnedManagedRootPid($pid, $snapshot, $instance);
            if ($resolvedRootPid > 0) {
                $targetRootPids[$resolvedRootPid] = $resolvedRootPid;
                $hasOwnedRoot = true;
            }
        }
        if (
            !$hasOwnedRoot
            && $launchPid > 0
            && $this->isTrustedLaunchedProcess($launchPid, $metadata)
        ) {
            // ps 本身也可能卡在系统验证链。唯一身份锁允许这里只回收
            // 本 launcher 刚创建的准确 PID，不信任普通 metadata.pid。
            $targetRootPids[$launchPid] = $launchPid;
            $hasOwnedRoot = true;
        }
        // 只有 metadata/父链无法解析 owner 时才做端口级兜底；正常路径不再执行 lsof。
        if (!$hasOwnedRoot) {
            if ($port > 0) {
                foreach ($this->collectOwnedManagedRootPids($instance, $port, true, $snapshot) as $ownerRootPid) {
                    $targetRootPids[$ownerRootPid] = $ownerRootPid;
                }
            }
            if ($rpcPort > 0) {
                foreach ($this->collectOwnedManagedRootPids($instance, $rpcPort, false, $snapshot) as $ownerRootPid) {
                    $targetRootPids[$ownerRootPid] = $ownerRootPid;
                }
            }
        }

        // 强制回收不能只依赖 metadata.pid：
        // pending 项可能短暂持有旧 pid（例如 manager）。这里统一补充“按端口 ownership
        // 反查得到的实例根 pid”，确保软/硬回收都能命中真正的 upstream master 进程。
        foreach ($targetRootPids as $targetPid) {
            $this->killProcessTree((int)$targetPid, $signal, $snapshot);
        }
    }

    /**
     * 校验启动身份锁是否仍由指定 PID/token 的子进程持有。
     *
     * @param int $pid
     * @param array<string, mixed> $metadata
     * @return bool
     */
    protected function isTrustedLaunchedProcess(int $pid, array $metadata): bool {
        if ($pid <= 0 || !@Process::kill($pid, 0)) {
            $this->forgetTrustedLaunch($pid);
            return false;
        }

        $registered = (array)($this->trustedLaunches[$pid] ?? []);
        $token = (string)($metadata['launch_token'] ?? ($registered['token'] ?? ''));
        $lockFile = (string)($metadata['launch_lock_file'] ?? ($registered['lock_file'] ?? ''));
        if (
            $token === ''
            || $lockFile === ''
            || (string)($registered['token'] ?? $token) !== $token
            || (string)($registered['lock_file'] ?? $lockFile) !== $lockFile
            || !is_file($lockFile)
        ) {
            if ($registered && (microtime(true) - (float)($registered['started_at'] ?? 0)) >= 1.0) {
                $this->forgetTrustedLaunch($pid);
            }
            return false;
        }

        $lock = @fopen($lockFile, 'r+');
        if (!is_resource($lock)) {
            return false;
        }
        if (@flock($lock, LOCK_EX | LOCK_NB)) {
            // 文件仍在但已无人持锁，说明原进程已退出；清理残留身份文件。
            @flock($lock, LOCK_UN);
            @fclose($lock);
            $this->forgetTrustedLaunch($pid);
            return false;
        }

        @rewind($lock);
        $raw = @stream_get_contents($lock);
        @fclose($lock);
        $identity = is_string($raw) ? json_decode($raw, true) : null;
        return is_array($identity)
            && (int)($identity['pid'] ?? 0) === $pid
            && hash_equals($token, (string)($identity['token'] ?? ''));
    }

    /**
     * 清除已经失效的 launcher 身份记录和锁文件。
     */
    protected function forgetTrustedLaunch(int $pid): void {
        $registered = (array)($this->trustedLaunches[$pid] ?? []);
        unset($this->trustedLaunches[$pid]);
        $lockFile = (string)($registered['lock_file'] ?? '');
        if ($lockFile !== '') {
            @unlink($lockFile);
        }
    }

    /**
     * 回收已经退出的启动身份，避免正常运行期间累积无主临时文件。
     */
    protected function reapTrustedLaunches(): void {
        foreach (array_keys($this->trustedLaunches) as $pid) {
            $pid = (int)$pid;
            $registered = (array)($this->trustedLaunches[$pid] ?? []);
            if ((microtime(true) - (float)($registered['started_at'] ?? 0)) < 1.0) {
                continue;
            }
            $this->isTrustedLaunchedProcess($pid, []);
        }
    }

    /**
     * 沿父链回溯到当前进程树的根 PID。
     *
     * @param int $pid 起始 PID
     * @param array<int, array{ppid:int, command:string}> $snapshot 进程快照
     * @return int 根 PID，不可解析时返回 0
     */
    protected function resolveProcessTreeRootPid(int $pid, array $snapshot): int {
        if ($pid <= 0 || !isset($snapshot[$pid])) {
            return 0;
        }
        $currentPid = $pid;
        $depth = 0;
        while ($currentPid > 0 && isset($snapshot[$currentPid]) && $depth < 64) {
            $depth++;
            $parentPid = (int)($snapshot[$currentPid]['ppid'] ?? 0);
            if ($parentPid <= 1 || $parentPid === $currentPid || !isset($snapshot[$parentPid])) {
                return $currentPid;
            }
            $currentPid = $parentPid;
        }
        return $currentPid > 0 ? $currentPid : 0;
    }

    /**
     * 判断某个 root PID 的进程树是否拥有指定监听端口。
     *
     * @param int $rootPid 候选根 PID
     * @param int $port 监听端口
     * @param array<int, array{ppid:int, command:string}> $snapshot 进程快照
     * @return bool
     */
    protected function processTreeOwnsPort(int $rootPid, int $port, array $snapshot): bool {
        if ($rootPid <= 0 || $port <= 0) {
            return false;
        }
        foreach (CoreServer::findPidsByPort($port) as $listenerPid) {
            $listenerPid = (int)$listenerPid;
            if ($listenerPid <= 0) {
                continue;
            }
            if ($this->isPidDescendantOf($listenerPid, $rootPid, $snapshot)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断 child PID 是否位于 ancestor PID 的进程树内。
     *
     * @param int $childPid 子进程 PID
     * @param int $ancestorPid 祖先进程 PID
     * @param array<int, array{ppid:int, command:string}> $snapshot 进程快照
     * @return bool
     */
    protected function isPidDescendantOf(int $childPid, int $ancestorPid, array $snapshot): bool {
        if ($childPid <= 0 || $ancestorPid <= 0 || !isset($snapshot[$childPid])) {
            return false;
        }
        $currentPid = $childPid;
        $depth = 0;
        while ($currentPid > 0 && isset($snapshot[$currentPid]) && $depth < 64) {
            if ($currentPid === $ancestorPid) {
                return true;
            }
            $depth++;
            $parentPid = (int)($snapshot[$currentPid]['ppid'] ?? 0);
            if ($parentPid <= 1 || $parentPid === $currentPid) {
                break;
            }
            $currentPid = $parentPid;
        }
        return false;
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
            unset($this->processStateCache[$pid]);
            return false;
        }
        $cached = $this->processStateCache[$pid] ?? null;
        if (
            is_array($cached)
            && (microtime(true) - (float)($cached['sampled_at'] ?? 0)) < (
                ($cached['sample_ok'] ?? false)
                    ? self::PROCESS_STATE_CACHE_TTL_SECONDS
                    : self::PROCESS_STATE_FAILURE_CACHE_TTL_SECONDS
            )
        ) {
            return (bool)($cached['running'] ?? false);
        }
        $processInfo = ProcessInspector::find($pid);
        if (!is_array($processInfo)) {
            // kill(0) 已确认 PID 存在；ps 快照超时或进程恰好退出时不能把它
            // 误判成已回收，否则旧实例可能仍在监听。下轮会再次核验。
            $this->processStateCache[$pid] = [
                'sampled_at' => microtime(true),
                'sample_ok' => false,
                'running' => true,
            ];
            return true;
        }
        $running = !str_contains((string)($processInfo['state'] ?? ''), 'Z');
        $this->processStateCache[$pid] = [
            'sampled_at' => microtime(true),
            'sample_ok' => true,
            'running' => $running,
        ];
        return $running;
    }

    /**
     * 对外提供“进程是否真实存活”的统一判定。
     *
     * 注意这里会过滤僵尸进程（Z）。gateway 的异步回收状态机若把 zombie 当成
     * alive，会导致实例端口已释放但回收永远不完成，持续进入重试/隔离循环。
     *
     * @param int $pid 目标 PID
     * @return bool true 表示进程存在且非僵尸
     */
    public function isProcessAlive(int $pid): bool {
        return $this->isProcessRunning($pid);
    }

    /**
     * 杀掉指定 root pid 及其当前可见子孙进程。
     *
     * Swoole upstream 的 master/manager/worker 是一棵进程树。
     * 仅杀 manager/worker 往往会被 master 立即补拉，因此这里统一以“树”为单位回收。
     *
     * @param int $rootPid 根进程 PID
     * @param int $signal SIGTERM 或 SIGKILL
     * @return void
     */
    protected function killProcessTree(int $rootPid, int $signal, array $snapshot): void {
        if ($rootPid <= 0 || !@Process::kill($rootPid, 0)) {
            return;
        }
        $tree = $this->collectDescendantPids($rootPid, $snapshot);
        // 先杀子进程再杀根，避免 manager 在 root 存活期间立刻补拉 worker。
        foreach (array_reverse($tree) as $pid) {
            if ($pid <= 0) {
                continue;
            }
            if (@Process::kill($pid, 0)) {
                @Process::kill($pid, $signal);
            }
        }
        if (@Process::kill($rootPid, 0)) {
            @Process::kill($rootPid, $signal);
        }
    }

    /**
     * 收集 root pid 的子孙进程列表。
     *
     * @param int $rootPid 根进程 PID
     * @return array<int, int>
     */
    protected function collectDescendantPids(int $rootPid, array $snapshot): array {
        if ($rootPid <= 0) {
            return [];
        }

        $childrenByParent = [];
        foreach ($snapshot as $pid => $processInfo) {
            $pid = (int)$pid;
            $ppid = (int)($processInfo['ppid'] ?? 0);
            if ($pid <= 0 || $ppid <= 0) {
                continue;
            }
            $childrenByParent[$ppid][] = $pid;
        }

        $queue = [$rootPid];
        $seen = [$rootPid => true];
        $descendants = [];
        while ($queue) {
            $parent = array_shift($queue);
            foreach ((array)($childrenByParent[$parent] ?? []) as $childPid) {
                $childPid = (int)$childPid;
                if ($childPid <= 0 || isset($seen[$childPid])) {
                    continue;
                }
                $seen[$childPid] = true;
                $descendants[] = $childPid;
                $queue[] = $childPid;
            }
        }
        return $descendants;
    }

    /**
     * 从端口监听者里筛选“可确认属于当前 managed upstream 实例”的 PID。
     *
     * @param array<string, mixed> $instance upstream 实例运行态信息
     * @param int $listenPort 当前检查的监听端口
     * @param bool $matchHttpPort true 表示按 `-port` 匹配；false 表示按 `-rport` 匹配
     * @return array<int, int>
     */
    protected function collectOwnedManagedListenerPids(
        array $instance,
        int $listenPort,
        bool $matchHttpPort,
        array $snapshot
    ): array {
        if ($listenPort <= 0) {
            return [];
        }
        $metadata = (array)($instance['metadata'] ?? []);
        $httpPort = (int)($instance['port'] ?? 0);
        $rpcPort = (int)($instance['rpc_port'] ?? ($metadata['rpc_port'] ?? 0));
        $gatewayPort = (int)($metadata['gateway_port'] ?? 0);
        $ownerEpoch = (int)($metadata['owner_epoch'] ?? 0);
        $owned = [];
        foreach (CoreServer::findPidsByPort($listenPort, true) as $pid) {
            $pid = (int)$pid;
            if ($pid <= 0 || !@Process::kill($pid, 0)) {
                continue;
            }
            $command = trim((string)($snapshot[$pid]['command'] ?? ''));
            if ($command === '' || !str_contains($command, 'boot gateway_upstream start')) {
                continue;
            }
            if (!ProcessCommandLine::hasOptionValue($command, 'app', APP_DIR_NAME)) {
                continue;
            }
            $expectedPortOption = $matchHttpPort ? 'port' : 'rport';
            $expectedPort = $matchHttpPort ? $httpPort : $rpcPort;
            if ($expectedPort > 0 && !ProcessCommandLine::hasOptionValue($command, $expectedPortOption, $expectedPort)) {
                continue;
            }

            // 只清理 owner 匹配的实例，避免误伤同机其它 gateway/upstream。
            if ($gatewayPort > 0 && !ProcessCommandLine::hasOptionValue($command, 'gateway_port', $gatewayPort)) {
                continue;
            }
            if ($ownerEpoch > 0) {
                if (!ProcessCommandLine::hasOptionValue($command, 'gateway_epoch', $ownerEpoch)) {
                    continue;
                }
            }
            $owned[$pid] = $pid;
        }
        ksort($owned);
        return array_values($owned);
    }

    /**
     * 将“端口监听 PID”提升为对应 managed upstream 的根 PID。
     *
     * 监听端口通常落在 worker 上，直接对 worker/manager 发信号会被 master 补拉，
     * 造成“服务器正在关闭 + Workers 启动完成”反复出现。这里通过父链上溯，统一
     * 收敛到同 owner 的根进程（通常是 upstream master）。
     *
     * @param array<string, mixed> $instance upstream 实例运行态信息
     * @param int $listenPort 当前检查的监听端口
     * @param bool $matchHttpPort true 表示按 `-port` 匹配；false 表示按 `-rport` 匹配
     * @return array<int, int>
     */
    protected function collectOwnedManagedRootPids(
        array $instance,
        int $listenPort,
        bool $matchHttpPort,
        array $snapshot
    ): array {
        $listenerPids = $this->collectOwnedManagedListenerPids($instance, $listenPort, $matchHttpPort, $snapshot);
        if (!$listenerPids) {
            return [];
        }
        $roots = [];
        foreach ($listenerPids as $listenerPid) {
            $rootPid = $this->resolveOwnedManagedRootPid((int)$listenerPid, $snapshot, $instance);
            if ($rootPid > 0) {
                $roots[$rootPid] = $rootPid;
            }
        }
        ksort($roots);
        return array_values($roots);
    }

    /**
     * 读取当前系统进程快照（pid/ppid/command）。
     *
     * @return array<int, array{ppid:int, command:string}>
     */
    protected function loadProcessSnapshot(): array {
        return ProcessInspector::snapshot(true);
    }

    /**
     * 把指定 PID 沿父链上溯到同 owner 的最顶层 upstream 进程。
     *
     * @param int $pid 起始 pid（通常是监听 worker）
     * @param array<int, array{ppid:int, command:string}> $snapshot 进程快照
     * @param array<string, mixed> $instance upstream 实例运行态信息
     * @return int
     */
    protected function resolveOwnedManagedRootPid(int $pid, array $snapshot, array $instance): int {
        if ($pid <= 0 || !isset($snapshot[$pid])) {
            return 0;
        }
        $metadata = (array)($instance['metadata'] ?? []);
        $expectedMasterPid = (int)($metadata['master_pid'] ?? 0);
        $currentPid = $pid;
        $lastOwnedPid = 0;
        $depth = 0;
        while ($currentPid > 0 && isset($snapshot[$currentPid]) && $depth < 64) {
            $depth++;
            $command = (string)($snapshot[$currentPid]['command'] ?? '');
            if (!$this->commandMatchesManagedOwnership($command, $instance)) {
                break;
            }
            if ($expectedMasterPid > 0 && $currentPid === $expectedMasterPid) {
                return $currentPid;
            }
            $lastOwnedPid = $currentPid;
            $parentPid = (int)($snapshot[$currentPid]['ppid'] ?? 0);
            if ($parentPid <= 1 || $parentPid === $currentPid) {
                break;
            }
            $currentPid = $parentPid;
        }
        return $lastOwnedPid;
    }

    /**
     * 判断某条命令行是否属于当前实例 owner。
     *
     * @param string $command 进程命令行
     * @param array<string, mixed> $instance upstream 实例运行态信息
     * @return bool
     */
    protected function commandMatchesManagedOwnership(string $command, array $instance): bool {
        $command = trim($command);
        if ($command === '' || !str_contains($command, 'boot gateway_upstream start')) {
            return false;
        }

        $metadata = (array)($instance['metadata'] ?? []);
        if (!ProcessCommandLine::hasOptionValue($command, 'app', APP_DIR_NAME)) {
            return false;
        }

        $gatewayPort = (int)($metadata['gateway_port'] ?? 0);
        if ($gatewayPort > 0 && !ProcessCommandLine::hasOptionValue($command, 'gateway_port', $gatewayPort)) {
            return false;
        }

        $ownerEpoch = (int)($metadata['owner_epoch'] ?? 0);
        if ($ownerEpoch > 0 && !ProcessCommandLine::hasOptionValue($command, 'gateway_epoch', $ownerEpoch)) {
            return false;
        }

        $httpPort = (int)($instance['port'] ?? 0);
        if ($httpPort > 0 && !ProcessCommandLine::hasOptionValue($command, 'port', $httpPort)) {
            return false;
        }
        $rpcPort = (int)($instance['rpc_port'] ?? ($metadata['rpc_port'] ?? 0));
        if ($rpcPort > 0 && !ProcessCommandLine::hasOptionValue($command, 'rport', $rpcPort)) {
            return false;
        }
        return true;
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
        $ipcResponse = $this->requestUpstreamIpc($port, $path, $timeoutSeconds);
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
        $effectiveTimeout = $path === self::STATUS_PATH ? max($timeoutSeconds, 5.0) : $timeoutSeconds;
        $ipcResponse = $this->requestUpstreamIpc($port, $path, $effectiveTimeout);
        if (!is_array($ipcResponse) || (int)($ipcResponse['status'] ?? 0) !== 200) {
            return null;
        }
        $data = $ipcResponse['data'] ?? null;
        return is_array($data) ? $data : null;
    }

    /**
     * 统一把 upstream 内部 HTTP 路径映射到本地 IPC 动作。
     *
     * 控制路径和状态路径最终都会落到同一个 UDS 请求面上，区别只在调用方
     * 如何解释返回值。这里统一做路径到 action 的映射，避免状态/控制两套
     * `match` 再各自维护一遍。
     *
     * @param string $path 内部 HTTP 路径。
     * @return string 对应的 IPC action；未映射时返回空字符串。
     */
    protected function mapUpstreamPathToIpcAction(string $path): string {
        return match ($path) {
            self::STATUS_PATH => 'upstream.status',
            self::HEALTH_PATH => 'upstream.health',
            self::GRACEFUL_SHUTDOWN_PATH => 'upstream.shutdown',
            self::QUIESCE_BUSINESS_PATH => 'upstream.quiesce',
            default => '',
        };
    }

    /**
     * 通过 UDS 向 upstream 发送内部 IPC 请求。
     *
     * @param int $port upstream HTTP 端口，用于推导 UDS 路径
     * @param string $path 目标内部 HTTP 路径
     * @param float $timeoutSeconds 请求超时
     * @return array<string, mixed>|null
     */
    protected function requestUpstreamIpc(int $port, string $path, float $timeoutSeconds): ?array {
        $ipcAction = $this->mapUpstreamPathToIpcAction($path);
        if ($port <= 0 || $ipcAction === '') {
            return null;
        }
        $ipcResponse = LocalIpc::request(LocalIpc::upstreamSocketPath($port), $ipcAction, [], $timeoutSeconds);
        return is_array($ipcResponse) ? $ipcResponse : null;
    }

}
