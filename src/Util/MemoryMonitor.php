<?php

namespace Scf\Util;

use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Helper\ArrayHelper;
use Scf\Server\Dashboard;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Timer;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;

class MemoryMonitor {
    protected static int $timerId = 0;

    /**
     * 占用统计
     * @param $filter
     * @return array
     * @throws \Exception
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
            //$globalSetKey = 'MEMORY_MONITOR_KEYS_' . APP_NODE_ID;
            //$keys = Redis::pool()->sMembers($globalSetKey) ?: [];
            $keys = Runtime::instance()->get('MEMORY_MONITOR_KEYS') ?: [];
            //根据id排序
            usort($keys, function ($a, $b) {
                // 取中间部分
                $aParts = explode(':', $a);
                $bParts = explode(':', $b);
                $aMid = $aParts[1] ?? $a;
                $bMid = $bParts[1] ?? $b;
                // 提取数字
                preg_match('/\d+$/', $aMid, $ma);
                preg_match('/\d+$/', $bMid, $mb);
                if ($ma && $mb) {
                    return intval($ma[0]) <=> intval($mb[0]);
                }
                // 没数字时走自然排序
                return strnatcmp($aMid, $bMid);
            });
            foreach ($keys as $key) {
                if ($filter && !str_contains($key, $filter)) {
                    continue;
                }
                $data = MemoryMonitorTable::instance()->get($key);
                if (!$data) {
                    // 认为离线
                    $offline++;
                    $rows[] = [
                        'name' => $key,
                        'pid' => '--',
                        'useage' => '--',
                        'real' => '--',
                        'peak' => '--',
                        'rss' => '--',
                        'pss' => '--',
                        'updated' => '--',
                        'status' => '离线',
                        'rss_num' => '--',
                    ];
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

                if ($rssMb !== null) {
                    $rssTotal += $rssMb;
                }
                if ($pssMb !== null) {
                    $pssTotal += $pssMb;
                }

                $time = date('H:i:s', strtotime($data['time'])) ?? date('H:i:s');
                $status = Color::green('正常');
                $online++;
                $rows[] = [
                    'name' => $process,
                    'pid' => $pid,
                    'useage' => number_format($usage, 2) . ' MB',
                    'real' => number_format($real, 2) . ' MB',
                    'peak' => number_format($peak, 2) . ' MB',
                    'rss' => $rssMb === null ? '-' : (number_format($rssMb, 2) . ' MB'),
                    'pss' => $pssMb === null ? '-' : (number_format($pssMb, 2) . ' MB'),
                    'updated' => $time,
                    'status' => $status,
                    'rss_num' => $rssMb
                ];
            }
            ArrayHelper::multisort($rows, 'rss_num', SORT_DESC);
            // 排序完成后移除临时字段 rss_num，避免对外输出
            foreach ($rows as &$__row) {
                if (array_key_exists('rss_num', $__row)) {
                    unset($__row['rss_num']);
                }
            }
            unset($__row);
            return [
                'rows' => $rows,
                'online' => $online,
                'offline' => $offline,
                'total' => count($keys),
                'usage_total_mb' => round($usageTotal, 2),
                'real_total_mb' => round($realTotal, 2), // 累计实际内存占用（MB）
                'peak_total_mb' => round($peakTotal, 2),
                'rss_total_mb' => round($rssTotal, 2),
                'pss_total_mb' => round($pssTotal, 2),
            ];
        };
        return $buildRows();
    }

    /**
     * @param $filter
     */
    public static function useage($filter = null): void {
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
                Color::notice('分配内存'),
                Color::notice('实际占用'),
                Color::notice('峰值占用'),
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
                "个, 实际占用:" . Color::cyan(($data['real_total_mb']) . "MB") .
                ", RSS累计:" . Color::cyan(($data['rss_total_mb']) . "MB") .
                (isset($data['pss_total_mb']) ? ", PSS累计:" . Color::cyan(($data['pss_total_mb']) . "MB") : '') .
                ", 查询耗时:" . $cost . "秒"
            );
        });
        Event::wait();
    }

    public static function start(
        string $processName = 'worker',
        int    $interval = 10000,
        int    $limitMb = 1024,
        bool   $forceExit = false
    ): void {
        if (self::$timerId) {
            return; // 避免重复启动
        }
        $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
        $run = function () use (&$run, &$managerId, $processName, $interval, $limitMb, $forceExit) {
            $currentManagerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
            if ($managerId !== $currentManagerId) {
                $managerId = $currentManagerId;
                Timer::clear(self::$timerId);
                return;
            }
            $usage = memory_get_usage(true);
            $real = memory_get_usage(false);
            $peak = memory_get_peak_usage(true);
            $usageMb = round($usage / 1048576, 2);
            $realMb = round($real / 1048576, 2);
            $peakMb = round($peak / 1048576, 2);
            $vmrssMb = null;
            $pssMb = null;
            if (PHP_OS_FAMILY === 'Linux') {
                $statusPath = '/proc/self/status';
                if (is_readable($statusPath)) {
                    $statusTxt = @file_get_contents($statusPath) ?: '';
                    if ($statusTxt && preg_match('/^VmRSS:\s+(\d+)\s+kB/im', $statusTxt, $m1)) {
                        $vmrssMb = round(((int)$m1[1]) / 1024, 2);
                    }
                }
                $smapsPath = '/proc/self/smaps_rollup';
                if (is_readable($smapsPath)) {
                    $smapsTxt = @file_get_contents($smapsPath) ?: '';
                    if ($smapsTxt && preg_match('/^Pss:\s+(\d+)\s+kB/im', $smapsTxt, $m3)) {
                        $pssMb = round(((int)$m3[1]) / 1024, 2);
                    }
                }
            } else {
                // macOS / other UNIX-like systems without /proc
                $pid = posix_getpid();
                $rssOut = @shell_exec('ps -o rss= -p ' . (int)$pid . ' 2>/dev/null');
                if (is_string($rssOut) && ($rssKb = (int)trim($rssOut)) > 0) {
                    $vmrssMb = round($rssKb / 1024, 2);
                }
            }

            $key = $processName;
            $data = [
                'process' => $processName,
                'usage_mb' => $usageMb,
                'real_mb' => $realMb,
                'peak_mb' => $peakMb,
                'pid' => posix_getpid(),
                'time' => date('Y-m-d H:i:s'),
                'rss_mb' => $vmrssMb ?? '-',
                'pss_mb' => $pssMb ?? '-',
            ];
            MemoryMonitorTable::instance()->set($key, $data);
            $processList = Runtime::instance()->get('MEMORY_MONITOR_KEYS') ?: [];
            if (!in_array($key, $processList)) {
                $processList[] = $key;
            }
            Runtime::instance()->set('MEMORY_MONITOR_KEYS', $processList);
            if ($limitMb > 0 && $usageMb > $limitMb) {
                Console::warning("[MemoryMonitor][WARN] {$processName} memory exceed {$limitMb}MB, current={$usageMb}MB");
                if ($forceExit) {
                    Console::warning("[MemoryMonitor][EXIT] {$processName} exit due to memory overflow");
                    exit(1);
                }
            }
            // 递归调度下一次
            self::$timerId = Timer::after($interval, $run);
        };
        $run();
        // 启动第一次
        self::$timerId = Timer::after($interval, $run);
    }

    public static function stop(): void {
        if (self::$timerId) {
            Timer::clear(self::$timerId);
            self::$timerId = 0;
        }
    }
}