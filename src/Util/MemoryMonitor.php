<?php

namespace Scf\Util;

use Exception;
use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Helper\ArrayHelper;
use Scf\Server\Dashboard;
use Swoole\Coroutine;
use Swoole\Coroutine\System;
use Swoole\Event;
use Swoole\Timer;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;

class MemoryMonitor {
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
                'restart_count' => 0
            ];
            $processInfo['pid'] = posix_getpid();
            $processInfo['usage_mb'] = $usageMb;
            $processInfo['real_mb'] = $realMb;
            $processInfo['peak_mb'] = $peakMb;
            $processInfo['time'] = date('Y-m-d H:i:s');
            $processInfo['rss_mb'] = 0;
            $processInfo['pss_mb'] = 0;
            $processInfo['os_actual'] = 0;
            MemoryMonitorTable::instance()->set($processName, $processInfo);
        };
        // 启动第一次
        $run();
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
            if ($processList) {
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
                    if (!empty($data['rss_mb']) && is_numeric($data['rss_mb'])) {
                        $rssMb = (float)$data['rss_mb'];
                    }
                    if (!empty($data['pss_mb']) && is_numeric($data['pss_mb'])) {
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
                    $time = date('H:i:s', strtotime($data['time'])) ?? date('H:i:s');
                    $status = Color::green('正常');
                    $online++;
                    if (str_starts_with($process, 'worker:')) {
                        $connection = Counter::instance()->get($process . ":connection") ?: 0;
                    } else {
                        $connection = 0;
                    }
                    $rows[] = [
                        'name' => $process,
                        'pid' => $pid,
                        'time' => strtotime($data['time']),
                        'usage' => number_format($usage, 2) . ' MB',
                        'real' => number_format($real, 2) . ' MB',
                        'peak' => number_format($peak, 2) . ' MB',
                        'os_actual' => $osActualMb === null ? '-' : (number_format($osActualMb, 2) . ' MB'),
                        'rss' => $rssMb === null ? '-' : (number_format($rssMb, 2) . ' MB'),
                        'pss' => $pssMb === null ? '-' : (number_format($pssMb, 2) . ' MB'),
                        'updated' => $time,
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
            // 获取服务器总物理内存 (MB)
            $totalMemMb = null;
            if (PHP_OS_FAMILY === 'Linux') {
                $meminfo = @self::readFileCo();
                if ($meminfo && preg_match('/^MemTotal:\s+(\d+)\s+kB/im', $meminfo, $m)) {
                    $totalMemMb = round(((int)$m[1]) / 1024, 2);
                }
            } else {
                // macOS / others
                $out = @shell_exec('sysctl -n hw.memsize 2>/dev/null');
                if ($out && is_numeric(trim($out))) {
                    $totalMemMb = round(((int)trim($out)) / 1048576, 2);
                }
            }
            // 获取服务器剩余可用内存 (MB)
            $freeMemMb = null;
            if (PHP_OS_FAMILY === 'Linux') {
                if (!isset($meminfo)) {
                    $meminfo = @self::readFileCo();
                }
                if ($meminfo) {
                    if (preg_match('/^MemAvailable:\s+(\d+)\s+kB/im', $meminfo, $mA)) {
                        $freeMemMb = round(((int)$mA[1]) / 1024, 2);
                    } else {
                        // 退化方案：MemFree + Buffers + Cached（近似）
                        $mf = $bu = $ca = 0;
                        if (preg_match('/^MemFree:\s+(\d+)\s+kB/im', $meminfo, $mF)) {
                            $mf = (int)$mF[1];
                        }
                        if (preg_match('/^Buffers:\s+(\d+)\s+kB/im', $meminfo, $mB)) {
                            $bu = (int)$mB[1];
                        }
                        if (preg_match('/^Cached:\s+(\d+)\s+kB/im', $meminfo, $mC)) {
                            $ca = (int)$mC[1];
                        }
                        $freeKb = $mf + $bu + $ca;
                        if ($freeKb > 0) {
                            $freeMemMb = round($freeKb / 1024, 2);
                        }
                    }
                }
            } else {
                // macOS: 通过 vm_stat 估算可用内存（free + inactive + speculative）
                $vm = @shell_exec('vm_stat 2>/dev/null');
                $pgSizeOut = @shell_exec('sysctl -n hw.pagesize 2>/dev/null');
                $pageSize = is_numeric(trim($pgSizeOut)) ? (int)trim($pgSizeOut) : 4096;
                if (is_string($vm) && $vm !== '') {
                    $get = function ($key) use ($vm) {
                        return preg_match('/^' . preg_quote($key, '/') . ':\s+(\d+)/m', $vm, $m) ? (int)$m[1] : 0;
                    };
                    $freePages = $get('Pages free') + $get('Pages inactive') + $get('Pages speculative');
                    if ($freePages > 0) {
                        $freeMemMb = round(($freePages * $pageSize) / 1048576, 2);
                    }
                }
            }
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

    /**
     * @param $filter
     */
    public static function usage($filter = null): void {
        Coroutine::create(function () use ($filter) {
            $client = Http::create(Dashboard::host() . '/memory');
            $result = $client->get();
            if ($result->hasError()) {
                Console::error($result->getMessage());
                return;
            }
            $data = $result->getData('data');
            $output = new ConsoleOutput();
            $table = new Table($output);
            // 初次构建
            $start = time();
            // 富表格输出
            $table->setHeaders([
                Color::notice('进程名'),
                Color::notice('PID'),
                Color::notice('分配内存(PHP)'),
                Color::notice('保留内存(PHP real)'),
                Color::notice('峰值内存(PHP peak)'),
                Color::notice('OS实际占用(PSS/RSS)'),
                Color::notice('RSS'),
                Color::notice('PSS'),
                Color::notice('更新时间'),
                Color::notice('状态'),
            ])->setRows(array_values($data['rows']));
            $table->render();
            $cost = (time() - $start) ?: 1;
            Console::write(
                "共" . Color::notice($data['total']) .
                "个进程,离线" . Color::red($data['offline']) .
                "个, 实际占用(PHP real):" . Color::cyan(($data['real_total_mb']) . "MB") .
                ", OS实际累计(PSS/RSS):" . Color::cyan(($data['os_actual_total_mb']) . "MB") .
                ", RSS累计:" . Color::cyan(($data['rss_total_mb']) . "MB") .
                (isset($data['pss_total_mb']) ? ", PSS累计:" . Color::cyan(($data['pss_total_mb']) . "MB") : '') .
                ", 系统总内存:" . Color::cyan(($data['system_total_mem_gb'] ?? '-') . "GB") .
                ", 系统空闲:" . Color::cyan(($data['system_free_mem_gb'] ?? '-') . "GB") .
                ", 查询耗时:" . $cost . "秒"
            );
        });
        Event::wait();
    }


    public static function stop(): void {
        if (self::$timerId) {
            Timer::clear(self::$timerId);
            self::$timerId = 0;
        }
    }
}