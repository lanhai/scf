<?php

namespace Scf\Core;

use Scf\Util\BoundedProcessRunner;
use Swoole\Process;
use Throwable;

abstract class Server {

    protected static Server $_SERVER;
    protected static array $_instances = [];
    protected static array $portPidSnapshotCache = [];
    protected static array $portPidSnapshotInFlight = [];
    /** @var array{sampled_at:float,ports:array<int, array<int, int>>} */
    protected static array $darwinListeningPortPidSnapshot = ['sampled_at' => 0.0, 'ports' => []];
    protected static bool $darwinListeningPortPidSnapshotInFlight = false;
    protected const PORT_PID_SNAPSHOT_TTL_SECONDS = 1.0;

    abstract public static function create(string $role, string $host = '0.0.0.0', int $port = 0);

    /**
     * 获取单例
     * @return static
     * @throws Exception
     */
    public static function instance(): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            throw new Exception('当前不在Server运行环境中');
        }
        return self::$_instances[$class];
    }

    public static function kill($pid, $try = 0): bool {
        if ($try >= 10) {
            return false;
        }
        if (Process::kill($pid, 0)) {
            $try++;
            Process::kill($pid, SIGKILL);
            sleep(1);
            return self::kill($pid, $try);
        }
        return true;
    }

    public static function killall($port, $try = 0): bool {
        if ($try >= 3) {
            if (self::killProcessByPort($port)) {
                return true;
            }
            return false;
        }
        if (self::isPortInUse($port)) {
            $try++;
            //exec("killall php");
            self::killProcessByPort($port);
            sleep(1);
            return self::killall($port, $try);
        }
        return true;
    }

    /**
     * 获取监听指定 TCP 端口的进程 PID 列表（兼容 Linux 容器 & Linux/macOS 宿主机）
     * 优先使用 /proc 扫描（容器最稳），其次尝试 lsof，最后尝试 ss。
     */
    public static function findPidsByPort(int $port, bool $forceRefresh = false): array {
        if ($port <= 0) {
            return [];
        }
        $cached = self::$portPidSnapshotCache[$port] ?? null;
        if (
            !$forceRefresh
            && is_array($cached)
            && (microtime(true) - (float)($cached['sampled_at'] ?? 0)) < self::PORT_PID_SNAPSHOT_TTL_SECONDS
        ) {
            return (array)($cached['pids'] ?? []);
        }
        if (isset(self::$portPidSnapshotInFlight[$port])) {
            // 强制/破坏性调用不能消费旧 PID 快照；宁可本轮不杀，也不能因 PID
            // 重用误伤刚好占用了旧 PID 的无关进程。
            return $forceRefresh ? [] : (is_array($cached) ? (array)($cached['pids'] ?? []) : []);
        }

        self::$portPidSnapshotInFlight[$port] = true;
        $seen = [];
        try {
            if (PHP_OS_FAMILY === 'Linux' && is_readable('/proc/net/tcp')) {
                // /proc/net/tcp 可读不代表 /proc/<pid>/fd 可读。hidepid、
                // 容器权限或安全策略会让 inode 存在但 PID 反查为空。
                foreach (self::findPidsByPortProcfs($port) as $pid) {
                    $seen[$pid] = true;
                }
                if (
                    !$seen
                    && (self::isListeningPortInUse($port) || self::isPortInUse($port))
                ) {
                    // 只在端口确实占用且 procfs 失效时低频回退，避免普通
                    // 健康轮询周期性拉起 lsof/ss。
                    foreach (self::findPidsByPortLsof($port) as $pid) {
                        $seen[$pid] = true;
                    }
                    if (!$seen) {
                        foreach (self::findPidsByPortSs($port) as $pid) {
                            $seen[$pid] = true;
                        }
                    }
                }
            } elseif (PHP_OS_FAMILY === 'Darwin') {
                foreach (self::findPidsByPortLsof($port, $forceRefresh) as $pid) {
                    $seen[$pid] = true;
                }
            } else {
                foreach (self::findPidsByPortLsof($port) as $pid) {
                    $seen[$pid] = true;
                }
                if (!$seen) {
                    foreach (self::findPidsByPortSs($port) as $pid) {
                        $seen[$pid] = true;
                    }
                }
            }
            $me = function_exists('getmypid') ? getmypid() : 0;
            unset($seen[$me]);
            $pids = array_map('intval', array_keys($seen));
            self::$portPidSnapshotCache[$port] = [
                'sampled_at' => microtime(true),
                'pids' => $pids,
            ];
            return $pids;
        } finally {
            unset(self::$portPidSnapshotInFlight[$port]);
        }
    }

    /**
     * 通过 /proc 扫描查找监听端口的进程（仅 Linux）。
     */
    protected static function findPidsByPortProcfs(int $port): array {
        if (!is_dir('/proc') || !is_readable('/proc/net/tcp')) return [];
        $inodes = [];
        $scan = function (string $path) use (&$inodes, $port) {
            $fh = @fopen($path, 'r');
            if (!$fh) return;
            fgets($fh); // skip header
            while (($line = fgets($fh)) !== false) {
                $line = trim($line);
                if ($line === '') continue;
                $cols = preg_split('/\s+/', $line);
                if (!isset($cols[1], $cols[3], $cols[9])) continue;
                $local = $cols[1];      // e.g. 0100007F:1F90
                $st = strtoupper($cols[3]); // 0A == LISTEN
                $inode = $cols[9];
                if ($st !== '0A') continue; // not LISTEN
                $parts = explode(':', $local);
                if (count($parts) !== 2) continue;
                $p = hexdec($parts[1]);
                if ($p === $port) {
                    $inodes[$inode] = true;
                }
            }
            fclose($fh);
        };
        $scan('/proc/net/tcp');
        if (is_readable('/proc/net/tcp6')) $scan('/proc/net/tcp6');
        if (!$inodes) return [];
        $pids = [];
        foreach (glob('/proc/[0-9]*/fd/*') as $fd) {
            $link = @readlink($fd);
            if ($link === false) continue;
            if (preg_match('/socket:\[(\d+)\]/', $link, $m)) {
                $inode = $m[1];
                if (isset($inodes[$inode])) {
                    if (preg_match('#^/proc/(\d+)/fd/\d+$#', $fd, $pm)) {
                        $pid = (int)$pm[1];
                        if ($pid > 0) $pids[$pid] = true;
                    }
                }
            }
        }
        return array_map('intval', array_keys($pids));
    }

    /**
     * 通过 lsof 查找（macOS/Linux）。
     */
    protected static function findPidsByPortLsof(int $port, bool $forceRefresh = false): array {
        $bin = self::firstExecutable(['/usr/sbin/lsof', '/usr/bin/lsof', '/opt/homebrew/sbin/lsof']);
        if ($bin === '') {
            return [];
        }
        if (PHP_OS_FAMILY === 'Darwin') {
            $snapshot = self::darwinListeningPortPidSnapshot($bin, $forceRefresh);
            return array_values((array)($snapshot[$port] ?? []));
        }
        $output = self::runExternalCommand([$bin, '-nP', '-t', "-iTCP:{$port}", '-sTCP:LISTEN']);
        $pids = [];
        foreach (preg_split('/\r?\n/', trim($output)) as $line) {
            $pid = (int)trim($line);
            if ($pid > 0) {
                $pids[$pid] = true;
            }
        }
        return array_map('intval', array_keys($pids));
    }

    /**
     * macOS 一次 lsof 读取全部 TCP listener，所有 pending recycle watcher 共用。
     *
     * @return array<int, array<int, int>> port => pids
     */
    protected static function darwinListeningPortPidSnapshot(string $lsof, bool $forceRefresh): array {
        $sampledAt = (float)(self::$darwinListeningPortPidSnapshot['sampled_at'] ?? 0.0);
        $age = microtime(true) - $sampledAt;
        if (!$forceRefresh && $sampledAt > 0 && $age < self::PORT_PID_SNAPSHOT_TTL_SECONDS) {
            return (array)(self::$darwinListeningPortPidSnapshot['ports'] ?? []);
        }
        if (self::$darwinListeningPortPidSnapshotInFlight) {
            return $forceRefresh
                ? []
                : (array)(self::$darwinListeningPortPidSnapshot['ports'] ?? []);
        }

        self::$darwinListeningPortPidSnapshotInFlight = true;
        try {
            $output = self::runExternalCommand([
                $lsof,
                '-nP',
                '-a',
                '-iTCP',
                '-sTCP:LISTEN',
                '-FpPn',
            ]);
            $ports = [];
            $currentPid = 0;
            foreach (preg_split('/\r?\n/', trim($output)) ?: [] as $line) {
                $line = trim((string)$line);
                if ($line === '') {
                    continue;
                }
                if ($line[0] === 'p') {
                    $currentPid = (int)substr($line, 1);
                    continue;
                }
                if ($line[0] !== 'n' || $currentPid <= 0) {
                    continue;
                }
                if (!preg_match('/:(\d+)$/', substr($line, 1), $matches)) {
                    continue;
                }
                $port = (int)($matches[1] ?? 0);
                if ($port > 0) {
                    $ports[$port][$currentPid] = $currentPid;
                }
            }
            ksort($ports);
            self::$darwinListeningPortPidSnapshot = [
                'sampled_at' => microtime(true),
                'ports' => $ports,
            ];
            return $ports;
        } finally {
            self::$darwinListeningPortPidSnapshotInFlight = false;
        }
    }

    /**
     * 通过 ss 查找（多数 Linux）。
     */
    protected static function findPidsByPortSs(int $port): array {
        $bin = self::firstExecutable(['/usr/sbin/ss', '/usr/bin/ss', '/bin/ss']);
        if ($bin === '') {
            return [];
        }
        $output = self::runExternalCommand([$bin, '-lntp']);
        $pids = [];
        foreach (preg_split('/\r?\n/', $output) as $line) {
            if (!preg_match('/:' . preg_quote((string)$port, '/') . '\s/', $line)) {
                continue;
            }
            if (preg_match('/pid=(\d+)/', $line, $m)) {
                $pid = (int)$m[1];
                if ($pid > 0) $pids[$pid] = true;
            }
        }
        return array_map('intval', array_keys($pids));
    }

    protected static function firstExecutable(array $candidates): string {
        foreach ($candidates as $candidate) {
            if (is_string($candidate) && is_executable($candidate)) {
                return $candidate;
            }
        }
        return '';
    }

    protected static function runExternalCommand(array $command): string {
        return BoundedProcessRunner::output($command, 2.0);
    }

    /**
     * 向控制台输出消息
     * @param string $str
     */
    public function log(string $str): void {
        $push = defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true;
        Console::info("【Server】" . $str, $push);
    }

    /**
     * 获取可用端口
     * @param $port
     * @return int
     */
    public static function getUseablePort($port): int {
        if (self::isPortInUse($port)) {
            return self::getUseablePort($port + 1);
        }
        return $port;
    }

    /**
     * 检查 TCP 端口是否存在真实监听进程
     * 仅用于启动前等待/平滑重启等待，避免把 CLOSED/TIME_WAIT 误判成占用。
     */
    public static function isListeningPortInUse(int $port): bool {
        if ($port <= 0) {
            return false;
        }
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            0.05,
            STREAM_CLIENT_CONNECT
        );
        if (is_resource($socket)) {
            fclose($socket);
            return true;
        }
        // LISTEN 语义只认真实 connect。TIME_WAIT/普通 bind 冲突不能被误判为活实例；
        // 启动前需要判断“是否可绑定”的调用方应显式使用 isPortInUse()。
        return false;
    }

    /**
     * 检查指定 host:port 是否仍不可绑定。
     *
     * 端口可用性判断必须与真实监听目标保持一致；否则 gateway 侧若按
     * `127.0.0.1` 选端口，而 child 侧又用 `0.0.0.0` 探测，就会出现
     * “父进程认为可用，子进程却立即判断占用”的不一致。
     *
     * @param int $port
     * @param string $host
     * @return bool
     */
    public static function isPortInUse(int $port, string $host = '0.0.0.0'): bool {
        try {
            $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
            if ($socket === false) {
                return false;
            }
            $bindHost = trim($host) !== '' ? trim($host) : '0.0.0.0';
            $result = @socket_bind($socket, $bindHost, $port);
            socket_close($socket);
            if ($result === false) {
                return true;
            }
            return false;
        } catch (Throwable) {
            return true;
        }
    }

    public static function killProcessByPort(int $port): bool {
        $pids = self::findPidsByPort($port, true);
        if (!$pids) {
            return !self::isPortInUse($port);
        }
        foreach ($pids as $pid) {
            Console::info("【Server】结束进程 $pid 占用端口:$port");
            // 先 TERM 再 KILL（更安全）
            @Process::kill($pid, SIGTERM);
        }
        // 安装切流/滚动升级会在回收旧实例后立刻拉起新监听者，因此这里不能只判断
        // PID 是否退出，还必须确认端口本身真的已经从 LISTEN 状态释放。
        if (self::waitUntilPortReleased($port, 3, 200)) {
            return true;
        }
        foreach ($pids as $pid) {
            if (@Process::kill($pid, 0)) {
                @Process::kill($pid, SIGKILL);
            }
        }
        return self::waitUntilPortReleased($port, 5, 200);
    }

    /**
     * 等待端口真正退出监听态。
     *
     * 仅检查 PID 是否存在还不够，因为 Swoole master/manager 在优雅关闭阶段可能已经
     * 开始退出，但监听 FD 尚未释放。这里统一用“监听态 + bind 冲突”双重条件确认端口
     * 已可被下一代实例复用。
     *
     * @param int $port 目标端口
     * @param int $timeoutSeconds 最长等待秒数
     * @param int $intervalMs 轮询间隔毫秒
     * @return bool 端口在超时内可复用时返回 true
     */
    public static function waitUntilPortReleased(int $port, int $timeoutSeconds = 5, int $intervalMs = 200): bool {
        if ($port <= 0) {
            return true;
        }

        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            if (!self::isListeningPortInUse($port) && !self::isPortInUse($port)) {
                return true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }

        return !self::isListeningPortInUse($port) && !self::isPortInUse($port);
    }

}
