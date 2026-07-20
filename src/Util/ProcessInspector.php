<?php

namespace Scf\Util;

/**
 * 当前系统进程表的短 TTL 快照。
 *
 * 多个 Gateway 生命周期分支需要 pid/ppid/state/command。统一采一份快照可避免
 * 对每个 PID 分别启动 ps；采样失败会短暂负缓存，防止系统已经拥堵时继续重试。
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
     * @return array{ppid:int,state:string,command:string}|null
     */
    public static function find(int $pid, bool $forceRefresh = false): ?array {
        if ($pid <= 0) {
            return null;
        }
        return self::snapshot($forceRefresh)[$pid] ?? null;
    }

    public static function command(int $pid, bool $forceRefresh = false): string {
        return (string)(self::find($pid, $forceRefresh)['command'] ?? '');
    }
}
