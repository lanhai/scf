<?php

namespace Scf\Server\SubProcess;

use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Util\Date;
use Scf\Util\MemoryMonitor;
use Swoole\Process;

/**
 * LogBackup 子进程运行逻辑。
 *
 * 责任边界：
 * - 定时日志归档与过期清理。
 */
class LogBackupProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        return new Process(function (Process $process) {
            $this->call('mark_gateway_sub_process_context');
            Runtime::instance()->set(Key::RUNTIME_LOG_BACKUP_PID, (int)$process->pid);
            Runtime::instance()->set(Key::RUNTIME_LOG_BACKUP_HEARTBEAT_AT, time());
            if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                Console::info("【LogBackup】日志备份PID:" . $process->pid, false);
            }
            App::mount();
            MemoryMonitor::start('LogBackup');
            $serverConfig = Config::server();
            $logger = Log::instance();
            $logExpireDays = $serverConfig['log_expire_days'] ?? 15;
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $nextTickAt = 0.0;

            $clearCount = $logger->clear($logExpireDays);
            if ($clearCount) {
                Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
            }

            while (true) {
                Runtime::instance()->set(Key::RUNTIME_LOG_BACKUP_HEARTBEAT_AT, time());
                $cmd = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($cmd === false) {
                    $cmd = '';
                }
                if ($cmd == 'shutdown') {
                    MemoryMonitor::stop();
                    Console::warning("【LogBackup】管理进程退出,结束备份", false);
                    break;
                }
                if (microtime(true) >= $nextTickAt) {
                    if ((int)Runtime::instance()->get('_LOG_CLEAR_DAY_') !== (int)Date::today()) {
                        $clearCount = $logger->clear($logExpireDays);
                        if ($clearCount) {
                            Console::log("【LogBackup】已清理过期日志:" . Color::cyan($clearCount), false);
                        }
                        $countKeyDay = Key::COUNTER_REQUEST . Date::leftday(2);
                        if (Counter::instance()->get($countKeyDay)) {
                            Counter::instance()->delete($countKeyDay);
                        }
                    }
                    $logger->backup();
                    MemoryMonitor::updateUsage('LogBackup');
                    $nextTickAt = microtime(true) + 5;
                }
                usleep(200000);
            }
            Runtime::instance()->set(Key::RUNTIME_LOG_BACKUP_HEARTBEAT_AT, 0);
            Runtime::instance()->set(Key::RUNTIME_LOG_BACKUP_PID, 0);
            is_resource($commandPipe) and fclose($commandPipe);
        }, false, SOCK_DGRAM);
    }
}
