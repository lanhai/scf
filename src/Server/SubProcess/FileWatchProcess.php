<?php

namespace Scf\Server\SubProcess;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Scf\Root;
use Scf\Util\Dir;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use function Co\run;

/**
 * FileWatch 子进程运行逻辑。
 *
 * 责任边界：
 * - 在开发态监听文件变更并触发 reload/restart。
 */
class FileWatchProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        return new Process(function (Process $process) {
            $this->call('mark_gateway_sub_process_context');
            run(function () use ($process) {
                Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_HEARTBEAT_AT, time());
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【FileWatcher】文件改动监听服务PID:" . $process->pid, false);
                }
                sleep(1);
                App::mount();
                MemoryMonitor::start('FileWatcher');
                $scanDirectories = function () {
                    if (APP_SRC_TYPE == 'dir') {
                        $appFiles = Dir::scan(APP_PATH . '/src');
                    } else {
                        $appFiles = [];
                    }
                    return [...$appFiles, ...Dir::scan(Root::dir())];
                };
                $files = $scanDirectories();
                $fileList = [];
                foreach ($files as $path) {
                    $meta = $this->call('read_file_watcher_meta', $path);
                    $meta && $fileList[$path] = $meta;
                }
                while (true) {
                    Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_HEARTBEAT_AT, time());
                    $socket = $process->exportSocket();
                    $msg = $socket->recv(timeout: 0.1);
                    if ($msg == 'shutdown') {
                        Timer::clearAll();
                        MemoryMonitor::stop();
                        Console::warning("【FileWatcher】管理进程退出,结束监听", false);
                        Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_HEARTBEAT_AT, 0);
                        Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_PID, 0);
                        $this->call('exit_coroutine_runtime');
                        return;
                    }
                    $changedFiles = [];
                    $currentFiles = $scanDirectories();
                    $currentSet = array_fill_keys($currentFiles, true);
                    foreach ($currentFiles as $path) {
                        $meta = $this->call('read_file_watcher_meta', $path);
                        if ($meta === null) {
                            continue;
                        }
                        if (!isset($fileList[$path])) {
                            $fileList[$path] = $meta;
                            $changedFiles[$path] = true;
                            continue;
                        }
                        if ($fileList[$path]['mtime'] !== $meta['mtime'] || $fileList[$path]['size'] !== $meta['size']) {
                            $fileList[$path] = $meta;
                            $changedFiles[$path] = true;
                        }
                    }

                    foreach (array_keys($fileList) as $path) {
                        if (!isset($currentSet[$path])) {
                            unset($fileList[$path]);
                            $changedFiles[$path] = true;
                        }
                    }

                    if ($changedFiles) {
                        $changedPaths = array_keys($changedFiles);
                        $shouldRestart = (bool)$this->call('should_restart_for_changed_files', $changedPaths);
                        Console::warning($shouldRestart
                            ? '---------以下文件发生变动,检测到Gateway核心目录变动,即将重启Gateway---------'
                            : '---------以下文件发生变动,即将重载业务平面---------');
                        foreach ($changedPaths as $f) {
                            Console::write($f);
                        }
                        Console::warning('-------------------------------------------');
                        if ($shouldRestart) {
                            $this->call('trigger_restart');
                            MemoryMonitor::stop();
                            Console::warning("【FileWatcher】管理进程退出,结束监听", false);
                            Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_HEARTBEAT_AT, 0);
                            Runtime::instance()->set(Key::RUNTIME_FILE_WATCHER_PID, 0);
                            $this->call('exit_coroutine_runtime');
                            return;
                        }
                        $this->call('trigger_reload');
                    }
                    MemoryMonitor::updateUsage('FileWatcher');
                    Coroutine::sleep(2);
                }
            });
        }, false, SOCK_DGRAM);
    }
}
