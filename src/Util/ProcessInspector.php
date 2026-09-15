<?php

namespace Scf\Util;

/**
 * 当前系统进程表的短 TTL 快照。
 *
 * 多个 Gateway 生命周期分支需要 pid/ppid/state/command。统一采一份快照可避免
 * 对每个 PID 分别启动 ps。Linux 直接读取 procfs，兼容不含 procps/BusyBox ps 的容器；
 * 其它系统使用有时限的 ps。采样失败会短暂负缓存，强制读取绝不返回旧快照。
 */
final class ProcessInspector {
    private const CACHE_TTL_SECONDS = 1.0;
    private const FAILURE_CACHE_TTL_SECONDS = 0.5;
    private const COMMAND_TIMEOUT_SECONDS = 1.0;

    /** @var array<int, array{ppid:int,state:string,command:string}> */
    private static array $snapshot = [];
    private static float $sampledAt = 0.0;
    private static bool $sampleOk = false;
    private static bool $sampleInFlight = false;

    /**
     * 读取当前进程树；普通遥测可复用短 TTL，生命周期回收要求完整的新采样。
     * @param bool $forceRefresh 为 true 时不返回失败或并发采样之前的旧结果。
     * @return array<int, array{ppid:int,state:string,command:string}>
     */
    public static function snapshot(bool $forceRefresh = false): array {
        $age = microtime(true) - self::$sampledAt;
        $ttl = self::$sampleOk ? self::CACHE_TTL_SECONDS : self::FAILURE_CACHE_TTL_SECONDS;
        if (!$forceRefresh && self::$sampledAt > 0 && $age < $ttl) {
            return self::$snapshot;
        }
        if (self::$sampleInFlight) {
            // destructive force-stop 不能消费旧快照；普通状态读取可安全使用 last-good。
            return $forceRefresh ? [] : self::$snapshot;
        }

        self::$sampleInFlight = true;
        try {
            if (PHP_OS_FAMILY === 'Linux') {
                $snapshot = self::linuxSnapshot();
                self::$sampledAt = microtime(true);
                self::$sampleOk = $snapshot !== null && $snapshot !== [];
                if (!self::$sampleOk) return $forceRefresh ? [] : self::$snapshot;
                return self::$snapshot = $snapshot;
            }
            $ps = is_executable('/bin/ps') ? '/bin/ps' : '/usr/bin/ps';
            $result = BoundedProcessRunner::run([
                $ps,
                '-axo',
                'pid=,ppid=,state=,command=',
            ], self::COMMAND_TIMEOUT_SECONDS, 32 * 1024 * 1024);
            self::$sampledAt = microtime(true);
            if (
                !$result['started']
                || $result['timed_out']
                || $result['truncated']
                || ($result['exit_code'] ?? -1) !== 0
                || trim($result['output']) === ''
            ) {
                self::$sampleOk = false;
                return $forceRefresh ? [] : self::$snapshot;
            }

            $snapshot = [];
            foreach (preg_split('/\r?\n/', trim($result['output'])) ?: [] as $line) {
                $line = trim((string)$line);
                if ($line === '' || !preg_match('/^(\d+)\s+(\d+)\s+(\S+)\s+(.+)$/', $line, $matches)) {
                    continue;
                }
                $pid = (int)($matches[1] ?? 0);
                $ppid = (int)($matches[2] ?? 0);
                $state = trim((string)($matches[3] ?? ''));
                $command = trim((string)($matches[4] ?? ''));
                if ($pid <= 0 || $ppid < 0 || $command === '') {
                    continue;
                }
                $snapshot[$pid] = [
                    'ppid' => $ppid,
                    'state' => $state,
                    'command' => $command,
                ];
            }

            self::$snapshot = $snapshot;
            self::$sampleOk = true;
            return $snapshot;
        } finally {
            self::$sampleInFlight = false;
        }
    }

    /**
     * 查询单个进程；Linux 强制核验直接读取该 PID，避免 nginx 控制依赖整张进程表。
     * @param int $pid 目标 PID。
     * @param bool $forceRefresh 是否绕过快照缓存读取当前身份。
     * @return array{ppid:int,state:string,command:string}|null
     */
    public static function find(int $pid, bool $forceRefresh = false): ?array {
        if ($pid <= 0) {
            return null;
        }
        if ($forceRefresh && PHP_OS_FAMILY === 'Linux') {
            return self::readLinuxProcess($pid);
        }
        return self::snapshot($forceRefresh)[$pid] ?? null;
    }

    /**
     * 从当前 PID namespace 的 procfs 读取进程树，不派生子进程或依赖镜像内的 ps。
     * 超时/不可读时放弃快照，禁止将半张进程表用于破坏性回收。
     * @return array<int,array{ppid:int,state:string,command:string}>|null
     */
    private static function linuxSnapshot(): ?array {
        $directory = @opendir('/proc');
        if ($directory === false) return null;
        $snapshot = [];
        $deadline = microtime(true) + self::COMMAND_TIMEOUT_SECONDS;
        try {
            while (($entry = readdir($directory)) !== false) {
                if (microtime(true) >= $deadline) return null;
                if (!ctype_digit($entry)) continue;
                $pid = (int)$entry;
                $process = self::readLinuxProcess($pid);
                // 枚举期间退出的进程可以忽略；仍存在但不可读的条目不能当成“不存在”。
                if ($process === null) {
                    clearstatcache(true, '/proc/' . $pid);
                    if (is_dir('/proc/' . $pid)) return null;
                    continue;
                }
                $snapshot[$pid] = $process;
            }
        } finally {
            closedir($directory);
        }
        return $snapshot;
    }

    /**
     * 用 stat 的启动时刻夹住 cmdline 读取，拒绝 PID 复用/读取失败形成的混合身份。
     * procfs 是内核提供的小型本地文件；worker 中无需另起 ps 或引入阻塞进程等待。
     * @param int $pid 当前 namespace 中的 PID。
     * @return array{ppid:int,state:string,command:string}|null null 表示身份不可确认。
     */
    private static function readLinuxProcess(int $pid): ?array {
        $path = '/proc/' . $pid;
        $before = self::parseLinuxStat((string)@file_get_contents($path . '/stat'), $pid);
        if ($before === null) return null;
        $cmdline = @file_get_contents($path . '/cmdline');
        $after = self::parseLinuxStat((string)@file_get_contents($path . '/stat'), $pid);
        if ($cmdline === false || $after === null || $before['start_time'] !== $after['start_time']
            || $before['name'] !== $after['name']) return null;
        $command = trim(str_replace("\0", ' ', $cmdline));
        return ['ppid' => $after['ppid'], 'state' => $after['state'],
            'command' => $command !== '' ? $command : '[' . $after['name'] . ']'];
    }

    /**
     * 解析 /proc/PID/stat；comm 可以包含空格及右括号，不能按整行空白直接拆字段。
     * @param string $stat 内核 stat 内容。
     * @param int $pid 应有 PID。
     * @return array{ppid:int,state:string,name:string,start_time:string}|null
     */
    private static function parseLinuxStat(string $stat, int $pid): ?array {
        $open = strpos($stat, '(');
        $close = strrpos($stat, ')');
        if ($open === false || $close === false || $close <= $open
            || trim(substr($stat, 0, $open)) !== (string)$pid) return null;
        $fields = preg_split('/\s+/', trim(substr($stat, $close + 1))) ?: [];
        // 去掉 pid/comm 后，下标 0/1/19 分别为 state/ppid/starttime（原始字段 3/4/22）。
        if (count($fields) < 20 || strlen($fields[0]) !== 1
            || !ctype_digit($fields[1]) || !ctype_digit($fields[19])) return null;
        return ['ppid' => (int)$fields[1], 'state' => $fields[0],
            'name' => substr($stat, $open + 1, $close - $open - 1), 'start_time' => $fields[19]];
    }

    public static function command(int $pid, bool $forceRefresh = false): string {
        return (string)(self::find($pid, $forceRefresh)['command'] ?? '');
    }
}
