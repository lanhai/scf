<?php

namespace Scf\Util;

use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Timer;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;

class MemoryMonitor {
    protected static int $timerId = 0;

    /**
     * @param $filter
     */
    public static function useage($filter = null): void {
        Coroutine::create(function () use ($filter) {
            $client = Http::create('http://localhost:' . File::read(SERVER_PORT_FILE) . '/memory');
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
            $updated = date('H:i:s');
            // 富表格输出
            $table->setHeaders([
                Color::notice('进程名'),
                Color::notice('PID'),
                Color::notice('分配'),
                Color::notice('占用'),
                Color::notice('峰值'),
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
                "个, PHP占用累计:" . Color::cyan(($data['real_total_mb']) . "MB") .
                ", RSS累计:" . Color::cyan(($data['rss_total_mb']) . "MB") .
                (isset($data['pss_total_mb']) ? ", PSS累计:" . Color::cyan(($data['pss_total_mb']) . "MB") : '') .
                ", 数据更新时间:" . Color::notice($updated) .
                ", 查询耗时:" . $cost . "秒"
            );
        });
        Event::wait();
    }

    public static function start(
        string $processName = 'worker',
        int    $interval = 10000,
        int    $limitMb = 512,
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
                //Timer::clear(self::$timerId);
                Timer::clearAll();
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

            $key = $processName;// "MEMORY_MONITOR:" . $processName . ":" . SERVER_NODE_ID;
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
            //Redis::pool()->set($key, $data, intval($interval / 1000) + 5);
            //$globalSetKey = 'MEMORY_MONITOR_KEYS_' . SERVER_NODE_ID;
//            if (!Redis::pool()->sIsMember($globalSetKey, $key)) {
//                Redis::pool()->sAdd($globalSetKey, $key);
//            }

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