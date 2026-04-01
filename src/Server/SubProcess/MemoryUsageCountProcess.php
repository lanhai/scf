<?php

namespace Scf\Server\SubProcess;

use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Util\MemoryMonitor;
use Swoole\Process;
use Throwable;

/**
 * MemoryUsageCount 子进程运行逻辑。
 *
 * 责任边界：
 * - 周期采样 gateway 控制面子进程内存，并补齐 rss/pss/os_actual。
 * - 历史兼容地保留 worker:* 超限 SIGTERM 分支；但在当前 gateway-only 架构中，
 *   upstream worker 轮换主链路已切到 `gateway -> upstream.restart_worker -> server->stop`，
 *   因此该分支通常不会成为 upstream worker 的主治理入口。
 */
class MemoryUsageCountProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        return new Process(function (Process $process) {
            $this->call('mark_gateway_sub_process_context');
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            Runtime::instance()->set(Key::RUNTIME_MEMORY_MONITOR_PID, (int)$process->pid);
            Runtime::instance()->set(Key::RUNTIME_MEMORY_MONITOR_HEARTBEAT_AT, time());
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【MemoryMonitor】内存监控PID:" . $process->pid, false);
            }
            MemoryMonitor::start('MemoryMonitor');
            $nextTickAt = 0.0;
            while (true) {
                Runtime::instance()->set(Key::RUNTIME_MEMORY_MONITOR_HEARTBEAT_AT, time());
                $msg = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($msg === false) {
                    $msg = '';
                }
                if ($msg === 'shutdown') {
                    MemoryMonitor::stop();
                    Console::warning("【MemoryMonitor】收到shutdown,安全退出", (bool)$this->call('should_push_managed_lifecycle_log'));
                    break;
                }

                if (microtime(true) >= $nextTickAt) {
                    try {
                        $processList = MemoryMonitorTable::instance()->rows();
                        if ($processList) {
                            foreach ($processList as $processInfo) {
                                $processName = $processInfo['process'];
                                $limitMb = $processInfo['limit_memory_mb'] ?? 300;
                                $pid = (int)$processInfo['pid'];

                                if (PHP_OS_FAMILY !== 'Darwin' && !Process::kill($pid, 0)) {
                                    continue;
                                }

                                $mem = MemoryMonitor::getPssRssByPid($pid);
                                $rss = isset($mem['rss_kb']) ? round($mem['rss_kb'] / 1024, 1) : null;
                                $pss = isset($mem['pss_kb']) ? round($mem['pss_kb'] / 1024, 1) : null;
                                $osActualMb = $pss ?? $rss;

                                $processInfo['rss_mb'] = $rss;
                                $processInfo['pss_mb'] = $pss;
                                $processInfo['os_actual'] = $osActualMb;
                                $processInfo['updated'] = time();

                                $autoRestart = $processInfo['auto_restart'] ?? STATUS_OFF;
                                // 兼容旧分支：仅当本进程表里出现 worker:* 行时才会触发。
                                // 当前架构下 upstream worker 的主内存轮换并不依赖这里，
                                // 而是由 gateway 在汇总阶段判定后经 IPC 请求 upstream 平滑 stop。
                                if (
                                    $autoRestart == STATUS_ON
                                    && PHP_OS_FAMILY !== 'Darwin'
                                    && str_starts_with($processName, 'worker:')
                                    && $osActualMb !== null
                                    && $osActualMb > $limitMb
                                    && time() - ($processInfo['restart_ts'] ?? 0) >= 120
                                ) {
                                    Log::instance()->setModule('system')
                                        ->error("{$processName}[PID:$pid] 内存 {$osActualMb}MB ≥ {$limitMb}MB，强制重启");
                                    Process::kill($pid, SIGTERM);
                                    $processInfo['restart_ts'] = time();
                                    $processInfo['restart_count'] = ($processInfo['restart_count'] ?? 0) + 1;
                                }

                                $curr = MemoryMonitorTable::instance()->get($processName);
                                if ($curr && $curr['pid'] !== $processInfo['pid']) {
                                    $processInfo['pid'] = $curr['pid'];
                                }
                                MemoryMonitorTable::instance()->set($processName, $processInfo);
                            }
                        } else {
                            Console::warning("【MemoryMonitor】暂无待统计进程", false);
                        }
                        MemoryMonitor::updateUsage('MemoryMonitor');
                    } catch (Throwable $e) {
                        Log::instance()->error("【MemoryMonitor】调度异常:" . $e->getMessage());
                        Process::kill($process->pid, SIGTERM);
                    }
                    $nextTickAt = microtime(true) + 5;
                }

                usleep(200000);
            }
            Runtime::instance()->set(Key::RUNTIME_MEMORY_MONITOR_HEARTBEAT_AT, 0);
            Runtime::instance()->set(Key::RUNTIME_MEMORY_MONITOR_PID, 0);
            is_resource($commandPipe) and fclose($commandPipe);
        }, false, SOCK_DGRAM);
    }
}
