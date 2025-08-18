<?php

namespace Scf\Util;

use Scf\Command\Color;
use Scf\Core\Console;
use Swoole\Timer;
use Scf\Cache\Redis;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;

class MemoryMonitor {
    protected static int $timerId = 0;

    /**
     * @param $filter
     * @param bool $print
     * @return array|void
     */
    public static function useage($filter = null, bool $print = false) {
        // 一次性拉取数据 -> 组装行
        $buildRows = function () use ($filter) {
            $globalSetKey = 'MEMORY_MONITOR_KEYS_' . SERVER_NODE_ID;
            $rows = [];
            $online = 0;
            $offline = 0;
            $realTotal = 0.0; // 累计实际内存占用（MB）
            $keys = Redis::pool()->sMembers($globalSetKey) ?: [];
            //根据id排序
            usort($keys, function ($a, $b) {
                // 尝试提取字符串里的数字
                preg_match('/\d+$/', $a, $ma);
                preg_match('/\d+$/', $b, $mb);
                if ($ma && $mb) {
                    // 都有数字 -> 数字比较
                    return intval($ma[0]) <=> intval($mb[0]);
                }
                // 其他情况 -> 字符串比较
                return strcmp($a, $b);
            });
            foreach ($keys as $key) {
                if ($filter && !str_contains($key, $filter)) {
                    continue;
                }
                // 解析进程标识：MEMORY_MONITOR:{process}:{pid}
                $parts = explode(':', (string)$key);
                $process = $parts[1] ?? 'unknown';
                $data = Redis::pool()->get($key);
                $pid = $data['pid'] ?? '--';
                if (!$data) {
                    // 认为离线
                    $offline++;
                    $rows[] = [
                        $process,
                        $pid,
                        '-',
                        '-',
                        '-',
                        '-',
                        Color::red('离线')
                    ];
                    continue;
                }
                $usage = (float)($data['usage_mb'] ?? 0);
                $real = (float)($data['real_mb'] ?? 0);
                $realTotal += $real; // 统计累计实际内存
                $peak = (float)($data['peak_mb'] ?? 0);
                $time = date('H:i:s', strtotime($data['time'])) ?? date('H:i:s');
                $status = Color::green('正常');
                $online++;
                $rows[] = [
                    'name' => $process,
                    'pid' => $pid,
                    'useage' => number_format($usage, 2) . ' MB',
                    'real' => number_format($real, 2) . ' MB',
                    'peak' => number_format($peak, 2) . ' MB',
                    'updated' => $time,
                    'status' => $status
                ];
            }
            return [
                'rows' => $rows,
                'online' => $online,
                'offline' => $offline,
                'total' => count($keys),
                'real_total_mb' => round($realTotal, 2), // 累计实际内存占用（MB）
            ];
        };
        $data = $buildRows();
        if (!$print) {
            return $data['rows'];
        }
        $output = new ConsoleOutput();
        $table = new Table($output);
        // 初次构建
        $start = time();

        $updated = date('H:i:s');
        // 富表格输出
        $table->setHeaders([
            Color::notice('进程名'),
            Color::notice('PID'),
            Color::notice('分配'),
            Color::notice('占用'),
            Color::notice('峰值'),
            Color::notice('更新时间'),
            Color::notice('状态'),
        ])->setRows(array_values($data['rows']));
        $table->render();
        $cost = (time() - $start) ?: 1;
        Console::write(
            "共" . Color::notice($data['total'])
            . "个进程,在线" . Color::green($data['online'])
            . "个,离线" . Color::red($data['offline'])
            . "个,累计占用:" . Color::cyan($data['real_total_mb'] . "MB") . ",数据更新时间:" . Color::notice($updated)
            . ",查询耗时:" . $cost . "秒"
        );
    }

    /**
     * 启动内存监控
     * @param string $processName 当前进程名称 (worker-x / crontab / task-x)
     * @param int $interval 监控间隔 (毫秒)
     * @param int $limitMb 内存限制 (MB)
     * @param bool $forceExit 超限是否强制退出
     */
    public static function start(
        string $processName = 'worker',
        int    $interval = 10000,
        int    $limitMb = 512,
        bool   $forceExit = false
    ): void {
        if (self::$timerId) {
            return; // 避免重复启动
        }
        self::$timerId = Timer::tick($interval, function () use ($processName, $interval, $limitMb, $forceExit,) {
            $globalSetKey = 'MEMORY_MONITOR_KEYS_' . SERVER_NODE_ID;
            $usage = memory_get_usage(true);
            $real = memory_get_usage(false);
            $peak = memory_get_peak_usage(true);
            $usageMb = round($usage / 1048576, 2);
            $realMb = round($real / 1048576, 2);
            $peakMb = round($peak / 1048576, 2);
            $key = "MEMORY_MONITOR:" . $processName . ":" . SERVER_NODE_ID;// . ":" . posix_getpid();
            $data = [
                'usage_mb' => $usageMb,
                'real_mb' => $realMb,
                'peak_mb' => $peakMb,
                'pid' => posix_getpid(),
                'time' => date('Y-m-d H:i:s')
            ];
            // 存储进程数据
            Redis::pool()->set($key, $data, intval($interval / 1000) + 5);
            // 把进程 key 加入全局集合，方便统一获取
            if (!Redis::pool()->sIsMember($globalSetKey, $key)) {
                Redis::pool()->sAdd($globalSetKey, $key);
            }
            //Console::log("[MemoryMonitor][$processName] PID=" . posix_getpid() . " Usage={$usageMb}MB Real={$realMb}MB Peak={$peakMb}MB");
            if ($limitMb > 0 && $usageMb > $limitMb) {
                Console::warning("[MemoryMonitor][WARN] {$processName} memory exceed {$limitMb}MB, current={$usageMb}MB");
                if ($forceExit) {
                    Console::warning("[MemoryMonitor][EXIT] {$processName} exit due to memory overflow");
                    exit(1);
                }
            }
        });
    }

    public static function stop(): void {
        if (self::$timerId) {
            Timer::clear(self::$timerId);
            self::$timerId = 0;
        }
    }
}