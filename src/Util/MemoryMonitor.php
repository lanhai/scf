<?php

namespace Scf\Util;

use Exception;
use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\SocketConnectionTable;
use Scf\Helper\ArrayHelper;
use Scf\Server\Dashboard;
use Swoole\Coroutine;
use Swoole\Coroutine\System;
use Swoole\Event;
use Swoole\Timer;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;
use Throwable;

class MemoryMonitor {
    private const PROCESS_MEMORY_SNAPSHOT_TTL_SECONDS = 5;
    private const SYSTEM_MEMORY_SNAPSHOT_TTL_SECONDS = 5;

    private static bool $processMemoryRefreshInFlight = false;
    private static bool $systemMemoryRefreshInFlight = false;
    /** @var array<string, array<string, mixed>> */
    private static array $localSnapshots = [];
    /**
     * 协程友好的文件读取：在协程中用 System::readFile，其他环境回退到 file_get_contents
     * @return string|false
     */
    private static function readFileCo(): bool|string {
        try {
            if (Coroutine::getCid() > 0 && method_exists(System::class, 'readFile')) {
                return System::readFile('/proc/meminfo');
            }
        } catch (\Throwable $e) {
            // ignore and fallback
        }
        return @file_get_contents('/proc/meminfo');
    }

    protected static int $timerId = 0;

    public static function start(
        string $processName = 'worker',
        int    $interval = 2000,//延时两秒
        int    $limitMb = 1024,
        bool   $autoRestart = false
    ): void {
        $run = function () use (&$run, $processName, $interval, $limitMb, $autoRestart) {
            $usage = memory_get_usage(true);
            $real = memory_get_usage();
            $peak = memory_get_peak_usage(true);
            $usageMb = round($usage / 1048576, 2);
            $realMb = round($real / 1048576, 2);
            $peakMb = round($peak / 1048576, 2);
            $processInfo = MemoryMonitorTable::instance()->get($processName) ?: [
                'process' => $processName,
                'limit_memory_mb' => $limitMb,
                'auto_restart' => $autoRestart ? 1 : 0,
                'restart_ts' => 0,
                'restart_count' => 0,
            ];
            $processInfo['pid'] = posix_getpid();
            $processInfo['usage_mb'] = $usageMb;
            $processInfo['real_mb'] = $realMb;
            $processInfo['peak_mb'] = $peakMb;
            $processInfo['rss_mb'] = 0;
            $processInfo['pss_mb'] = 0;
            $processInfo['os_actual'] = 0;
            $processInfo['usage_updated'] = time();
            $processInfo['updated'] = time();
            MemoryMonitorTable::instance()->set($processName, $processInfo);
        };
        // 启动第一次
        $run();
    }

    public static function updateUsage($processName): void {
        $usage = memory_get_usage(true);
        $real = memory_get_usage();
        $peak = memory_get_peak_usage(true);
        $usageMb = round($usage / 1048576, 2);
        $realMb = round($real / 1048576, 2);
        $peakMb = round($peak / 1048576, 2);
        $processInfo = MemoryMonitorTable::instance()->get($processName);
        if ($processInfo) {
            $processInfo['usage_mb'] = $usageMb;
            $processInfo['real_mb'] = $realMb;
            $processInfo['peak_mb'] = $peakMb;
            $processInfo['usage_updated'] = time();
            MemoryMonitorTable::instance()->set($processName, $processInfo);
        }
    }

    /**
     * 批量获取进程内存占用。Darwin/其他 Unix 每个采样周期最多启动一次 ps，
     * Linux 继续直接读取 /proc，不产生外部进程。
     *
     * @param array<int, int> $pids
     * @param bool $forceRefresh 是否忽略 5 秒共享快照
     * @return array<int, array{pss_kb:int|null,rss_kb:int|null}>
     */
    public static function getPssRssByPids(array $pids, bool $forceRefresh = false): array {
        $pids = array_values(array_unique(array_filter(
            array_map('intval', $pids),
            static fn(int $pid): bool => $pid > 0
        )));
        if (!$pids) {
            return [];
        }

        $snapshot = self::readRuntimeSnapshot(Key::RUNTIME_PROCESS_MEMORY_SNAPSHOT);
        $values = is_array($snapshot['values'] ?? null) ? $snapshot['values'] : [];
        $updatedAt = (int)($snapshot['updated_at'] ?? 0);
        $fresh = $updatedAt > 0 && (time() - $updatedAt) < self::PROCESS_MEMORY_SNAPSHOT_TTL_SECONDS;
        if (!$forceRefresh && $fresh) {
            return self::selectPidMemoryValues($pids, $values);
        }

        // Gateway 有专职 MemoryUsageCount 采样器；其它进程只消费 last-good，
        // 避免多个控制面进程在同一时刻各自启动 ps。
        $monitorPid = (int)(self::readRuntimeValue(Key::RUNTIME_MEMORY_MONITOR_PID) ?? 0);
        if (!$forceRefresh && $monitorPid > 0) {
            return self::selectPidMemoryValues($pids, $values);
        }
        if (self::$processMemoryRefreshInFlight) {
            return self::selectPidMemoryValues($pids, $values);
        }

        self::$processMemoryRefreshInFlight = true;
        try {
            $sampledValues = self::collectPssRssByPids($pids);
            // 单次 ps 超时只代表本轮采样失败。保留 last-good，避免系统短暂拥堵
            // 时把 Dashboard 指标整体抹成 null；下一轮成功后会自然覆盖。
            foreach ($pids as $pid) {
                $previous = self::selectPidMemoryValues([$pid], $values)[$pid];
                $sampled = $sampledValues[$pid] ?? ['pss_kb' => null, 'rss_kb' => null];
                $values[$pid] = [
                    'pss_kb' => isset($sampled['pss_kb']) && is_numeric($sampled['pss_kb'])
                        ? (int)$sampled['pss_kb']
                        : $previous['pss_kb'],
                    'rss_kb' => isset($sampled['rss_kb']) && is_numeric($sampled['rss_kb'])
                        ? (int)$sampled['rss_kb']
                        : $previous['rss_kb'],
                ];
            }
            self::writeRuntimeSnapshot(Key::RUNTIME_PROCESS_MEMORY_SNAPSHOT, [
                'updated_at' => time(),
                'values' => $values,
            ]);
            return self::selectPidMemoryValues($pids, $values);
        } finally {
            self::$processMemoryRefreshInFlight = false;
        }
    }

    /**
     * 兼容旧调用点的单 PID 包装。
     *
     * @return array{pss_kb:int|null,rss_kb:int|null}
     */
    public static function getPssRssByPid(int $pid): array {
        return self::getPssRssByPids([$pid])[$pid] ?? ['pss_kb' => null, 'rss_kb' => null];
    }

    /**
     * @param array<int, int> $pids
     * @return array<int, array{pss_kb:int|null,rss_kb:int|null}>
     */
    private static function collectPssRssByPids(array $pids): array {
        $values = [];
        foreach ($pids as $pid) {
            $values[$pid] = ['pss_kb' => null, 'rss_kb' => null];
        }

        if (PHP_OS_FAMILY === 'Linux') {
            foreach ($pids as $pid) {
                $smapsRollup = "/proc/{$pid}/smaps_rollup";
                $smaps = "/proc/{$pid}/smaps";
                $status = "/proc/{$pid}/status";
                if (is_readable($smapsRollup)) {
                    [$pssKb, $rssKb] = self::parseSmapsLike($smapsRollup);
                    $values[$pid] = ['pss_kb' => $pssKb, 'rss_kb' => $rssKb];
                    continue;
                }
                if (is_readable($smaps)) {
                    [$pssKb, $rssKb] = self::parseSmapsLike($smaps);
                    $values[$pid] = ['pss_kb' => $pssKb, 'rss_kb' => $rssKb];
                    continue;
                }
                if (is_readable($status)) {
                    foreach ((array)@file($status) as $line) {
                        if (str_starts_with($line, 'VmRSS:') && preg_match('/(\d+)/', $line, $matches)) {
                            $values[$pid]['rss_kb'] = (int)$matches[1];
                            break;
                        }
                    }
                }
            }
            return $values;
        }

        $psBinary = is_executable('/bin/ps') ? '/bin/ps' : '/usr/bin/ps';
        $output = self::runExternalCommand([
            $psBinary,
            '-o',
            'pid=,rss=',
            '-p',
            implode(',', $pids),
        ]);
        foreach (preg_split('/\r?\n/', trim($output)) as $line) {
            if (!preg_match('/^\s*(\d+)\s+(\d+)\s*$/', $line, $matches)) {
                continue;
            }
            $pid = (int)$matches[1];
            if (isset($values[$pid])) {
                $values[$pid]['rss_kb'] = (int)$matches[2];
            }
        }
        return $values;
    }

    /**
     * 刷新系统内存 last-good。Gateway 的 MemoryUsageCount 会 force 每 5 秒采样；
     * upstream master 没有专职采样器时，由 status 首次/过期请求单飞刷新。
     *
     * @return array{updated_at?:int,total_mem_mb?:float|null,free_mem_mb?:float|null,page_size?:int}
     */
    public static function refreshSystemMemorySnapshot(bool $forceRefresh = false): array {
        $snapshot = self::readRuntimeSnapshot(Key::RUNTIME_SYSTEM_MEMORY_SNAPSHOT);
        $updatedAt = (int)($snapshot['updated_at'] ?? 0);
        $fresh = $updatedAt > 0 && (time() - $updatedAt) < self::SYSTEM_MEMORY_SNAPSHOT_TTL_SECONDS;
        if (!$forceRefresh && $fresh) {
            return $snapshot;
        }

        $monitorPid = (int)(self::readRuntimeValue(Key::RUNTIME_MEMORY_MONITOR_PID) ?? 0);
        if (!$forceRefresh && $monitorPid > 0) {
            return $snapshot;
        }
        if (self::$systemMemoryRefreshInFlight) {
            return $snapshot;
        }

        self::$systemMemoryRefreshInFlight = true;
        try {
            $sample = self::collectSystemMemorySnapshot($snapshot);
            $sample['updated_at'] = time();
            self::writeRuntimeSnapshot(Key::RUNTIME_SYSTEM_MEMORY_SNAPSHOT, $sample);
            return $sample;
        } finally {
            self::$systemMemoryRefreshInFlight = false;
        }
    }

    private static function collectSystemMemorySnapshot(array $previous): array {
        $totalMemMb = isset($previous['total_mem_mb']) && is_numeric($previous['total_mem_mb'])
            ? (float)$previous['total_mem_mb']
            : null;
        $freeMemMb = isset($previous['free_mem_mb']) && is_numeric($previous['free_mem_mb'])
            ? (float)$previous['free_mem_mb']
            : null;
        $pageSize = (int)($previous['page_size'] ?? 4096);

        if (PHP_OS_FAMILY === 'Linux') {
            $meminfo = self::readFileCo();
            if (is_string($meminfo) && $meminfo !== '') {
                if (preg_match('/^MemTotal:\s+(\d+)\s+kB/im', $meminfo, $match)) {
                    $totalMemMb = round(((int)$match[1]) / 1024, 2);
                }
                if (preg_match('/^MemAvailable:\s+(\d+)\s+kB/im', $meminfo, $match)) {
                    $freeMemMb = round(((int)$match[1]) / 1024, 2);
                } else {
                    $freeKb = 0;
                    foreach (['MemFree', 'Buffers', 'Cached'] as $field) {
                        if (preg_match('/^' . $field . ':\s+(\d+)\s+kB/im', $meminfo, $match)) {
                            $freeKb += (int)$match[1];
                        }
                    }
                    $freeMemMb = $freeKb > 0 ? round($freeKb / 1024, 2) : null;
                }
            }
        } elseif (PHP_OS_FAMILY === 'Darwin') {
            if ($totalMemMb === null) {
                $memSize = trim(self::runExternalCommand(['/usr/sbin/sysctl', '-n', 'hw.memsize']));
                if (is_numeric($memSize)) {
                    $totalMemMb = round(((int)$memSize) / 1048576, 2);
                }
            }
            $vmStat = self::runExternalCommand(['/usr/bin/vm_stat']);
            if (preg_match('/page size of\s+(\d+)\s+bytes/i', $vmStat, $match)) {
                $pageSize = (int)$match[1];
            }
            if ($vmStat !== '') {
                $readPages = static function (string $key) use ($vmStat): int {
                    return preg_match('/^' . preg_quote($key, '/') . ':\s+(\d+)/m', $vmStat, $match)
                        ? (int)$match[1]
                        : 0;
                };
                $freePages = $readPages('Pages free')
                    + $readPages('Pages inactive')
                    + $readPages('Pages speculative');
                $freeMemMb = $freePages > 0 ? round(($freePages * $pageSize) / 1048576, 2) : null;
            }
        }

        return [
            'total_mem_mb' => $totalMemMb,
            'free_mem_mb' => $freeMemMb,
            'page_size' => $pageSize,
        ];
    }

    private static function runExternalCommand(array $command): string {
        return BoundedProcessRunner::output($command, 1.0);
    }

    private static function readRuntimeSnapshot(string $key): array {
        $value = self::readRuntimeValue($key);
        $runtimeSnapshot = is_array($value) ? $value : [];
        $localSnapshot = self::$localSnapshots[$key] ?? [];
        return (int)($localSnapshot['updated_at'] ?? 0) > (int)($runtimeSnapshot['updated_at'] ?? 0)
            ? $localSnapshot
            : $runtimeSnapshot;
    }

    private static function readRuntimeValue(string $key): mixed {
        try {
            return Runtime::instance()->get($key);
        } catch (Throwable) {
            return null;
        }
    }

    private static function writeRuntimeSnapshot(string $key, array $value): void {
        self::$localSnapshots[$key] = $value;
        try {
            Runtime::instance()->set($key, $value);
        } catch (Throwable) {
        }
    }

    private static function selectPidMemoryValues(array $pids, array $values): array {
        $selected = [];
        foreach ($pids as $pid) {
            $value = $values[$pid] ?? $values[(string)$pid] ?? null;
            $selected[$pid] = is_array($value)
                ? [
                    'pss_kb' => isset($value['pss_kb']) && is_numeric($value['pss_kb']) ? (int)$value['pss_kb'] : null,
                    'rss_kb' => isset($value['rss_kb']) && is_numeric($value['rss_kb']) ? (int)$value['rss_kb'] : null,
                ]
                : ['pss_kb' => null, 'rss_kb' => null];
        }
        return $selected;
    }

    /**
     * 解析 smaps/smaps_rollup：汇总 Pss/Rss
     * 返回 [pssKb, rssKb]
     */
    private static function parseSmapsLike(string $file): array {
        $pss = 0;
        $rss = 0;
        $hasPss = false;
        $hasRss = false;
        // 使用 @file 读取，避免在进程退出时 fgets 报错
        $lines = @file($file);
        if ($lines === false) {
            return [null, null];
        }
        foreach ($lines as $line) {
            if (strncmp($line, 'Pss:', 4) === 0) {
                if (preg_match('/(\d+)/', $line, $m)) {
                    $pss += (int)$m[1];
                    $hasPss = true;
                }
            } elseif (strncmp($line, 'Rss:', 4) === 0) {
                if (preg_match('/(\d+)/', $line, $m)) {
                    $rss += (int)$m[1];
                    $hasRss = true;
                }
            }
        }
        return [$hasPss ? $pss : null, $hasRss ? $rss : null];
    }

    /**
     * 占用统计
     * @param $filter
     * @return array
     * @throws Exception
     */
    public static function sum($filter = null): array {
        // 一次性拉取数据 -> 组装行
        $buildRows = function () use ($filter) {
            $rows = [];
            $online = 0;
            $offline = 0;
            $realTotal = 0.0; // 累计实际内存占用（MB）
            $usageTotal = 0.0; // 累计分配（MB）
            $peakTotal = 0.0; // 累计峰值（MB）
            $rssTotal = 0.0; // 累计RSS（MB）
            $pssTotal = 0.0; // 累计PSS（MB'])
            $osActualTotal = 0.0; // 累计 OS 视角实际占用（优先PSS, 其次RSS）
            $processList = MemoryMonitorTable::instance()->rows();
            $memoryByPid = self::getPssRssByPids(array_map(
                static fn(array $row): int => (int)($row['pid'] ?? 0),
                array_values(array_filter($processList, 'is_array'))
            ));
            if ($processList) {
                $workerConnectionStats = SocketConnectionTable::instance()->workerConnectionStats();
                foreach ($processList as $data) {
                    $key = $data['process'];
                    if ($filter && !str_contains($key, $filter)) {
                        continue;
                    }
                    $process = $data['process'] ?? '--';
                    $pid = $data['pid'] ?? '--';
                    $usage = (float)($data['usage_mb'] ?? 0);
                    $real = (float)($data['real_mb'] ?? 0);
                    $peak = (float)($data['peak_mb'] ?? 0);
                    $usageTotal += $usage;
                    $realTotal += $real; // 统计累计实际内存
                    $peakTotal += $peak;

                    $rssMb = null;
                    $pssMb = null;
                    $pidMemory = $memoryByPid[(int)$pid] ?? [];
                    if (isset($pidMemory['rss_kb']) && is_numeric($pidMemory['rss_kb'])) {
                        $rssMb = round(((float)$pidMemory['rss_kb']) / 1024, 1);
                    } elseif (!empty($data['rss_mb']) && is_numeric($data['rss_mb'])) {
                        $rssMb = (float)$data['rss_mb'];
                    }
                    if (isset($pidMemory['pss_kb']) && is_numeric($pidMemory['pss_kb'])) {
                        $pssMb = round(((float)$pidMemory['pss_kb']) / 1024, 1);
                    } elseif (!empty($data['pss_mb']) && is_numeric($data['pss_mb'])) {
                        $pssMb = (float)$data['pss_mb'];
                    }
                    // OS 实际占用：优先使用 PSS，否则退化为 RSS
                    $osActualMb = null;
                    if ($pssMb !== null) {
                        $osActualMb = $pssMb;
                    } elseif ($rssMb !== null) {
                        $osActualMb = $rssMb;
                    }

                    if ($rssMb !== null) {
                        $rssTotal += $rssMb;
                    }
                    if ($pssMb !== null) {
                        $pssTotal += $pssMb;
                    }
                    if ($osActualMb !== null) {
                        $osActualTotal += $osActualMb;
                    }
                    $status = Color::green('正常');
                    $online++;
                    if (str_starts_with($process, 'worker:')) {
                        $workerId = (int)str_replace('worker:', '', $process);
                        $connection = $workerConnectionStats[$workerId] ?? 0;
                    } else {
                        $connection = 0;
                    }
                    $rows[] = [
                        'name' => $process,
                        'pid' => $pid,
                        'usage' => number_format($usage, 2) . ' MB',
                        'real' => number_format($real, 2) . ' MB',
                        'peak' => number_format($peak, 2) . ' MB',
                        'os_actual' => $osActualMb === null ? '-' : (number_format($osActualMb, 2) . ' MB'),
                        'rss' => $rssMb === null ? '-' : (number_format($rssMb, 2) . ' MB'),
                        'pss' => $pssMb === null ? '-' : (number_format($pssMb, 2) . ' MB'),
                        'updated' => $data['updated'],
                        'usage_updated' => $data['usage_updated'],
                        'status' => $status,
                        'connection' => $connection,
                        'os_actual_num' => $osActualMb ?? null,
                        'restart_ts' => $data['restart_ts'],
                        'restart_count' => $data['restart_count'],
                        'limit_memory_mb' => $data['limit_memory_mb']
                    ];
                }
                ArrayHelper::multisort($rows, 'os_actual_num', SORT_DESC);
                // 排序完成后移除临时字段 os_actual_num，避免对外输出
                foreach ($rows as &$__row) {
                    if (array_key_exists('os_actual_num', $__row)) {
                        unset($__row['os_actual_num']);
                    }
                }
                unset($__row);
            }
            $systemMemory = self::refreshSystemMemorySnapshot();
            $totalMemMb = isset($systemMemory['total_mem_mb']) && is_numeric($systemMemory['total_mem_mb'])
                ? (float)$systemMemory['total_mem_mb']
                : null;
            $freeMemMb = isset($systemMemory['free_mem_mb']) && is_numeric($systemMemory['free_mem_mb'])
                ? (float)$systemMemory['free_mem_mb']
                : null;
            return [
                'rows' => $rows,
                'online' => $online,
                'offline' => $offline,
                'total' => count($processList),
                'usage_total_mb' => round($usageTotal, 2),
                'real_total_mb' => round($realTotal, 2), // 累计实际内存占用（MB）
                'peak_total_mb' => round($peakTotal, 2),
                'os_actual_total_mb' => round($osActualTotal, 2),
                'rss_total_mb' => round($rssTotal, 2),
                'pss_total_mb' => round($pssTotal, 2),
                'system_total_mem_gb' => $totalMemMb ? round($totalMemMb / 1024, 2) : '--',
                'system_free_mem_gb' => $freeMemMb ? round($freeMemMb / 1024, 2) : '--',
            ];
        };
        return $buildRows();
    }

    public static function stop(): void {
        if (self::$timerId) {
            Timer::clear(self::$timerId);
            self::$timerId = 0;
        }
    }
}
