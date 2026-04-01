<?php

namespace Scf\Server\SubProcess;

use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Server\Task\CrontabManager;
use Swoole\Process;

/**
 * CrontabManager 子进程运行逻辑。
 *
 * 责任边界：
 * - 负责排程任务子进程拉起、回收、排空与故障重拉。
 */
class CrontabManagerProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        Counter::instance()->incr(Key::COUNTER_CRONTAB_PROCESS);

        return new Process(function (Process $process) {
            $this->call('mark_gateway_sub_process_context');
            Runtime::instance()->set(Key::RUNTIME_CRONTAB_MANAGER_PID, (int)$process->pid);
            Runtime::instance()->set(Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT, time());
            Console::info("【Crontab】排程任务管理PID:" . $process->pid, false);
            define('IS_CRONTAB_PROCESS', true);
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $managerId = (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0);
            $quiescing = false;
            while (true) {
                Runtime::instance()->set(Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT, time());
                while ($ret = Process::wait(false)) {
                    $pid = (int)($ret['pid'] ?? 0);
                    if ($pid <= 0) {
                        continue;
                    }
                    if ($task = CrontabManager::getTaskTableByPid($pid)) {
                        CrontabManager::removeTaskTable($task['id']);
                    }
                }
                $latestManagerId = (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0);
                if (!$quiescing && $latestManagerId !== $managerId) {
                    $quiescing = true;
                    Console::warning("【Crontab】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                }
                if (!$quiescing && !Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }
                if (!$quiescing && !Runtime::instance()->crontabProcessStatus() && Runtime::instance()->serverIsAlive() && !Runtime::instance()->serverIsDraining()) {
                    $taskList = CrontabManager::start();
                    Runtime::instance()->crontabProcessStatus(true);
                    if ($taskList) {
                        while ($ret = Process::wait(false)) {
                            if ($t = CrontabManager::getTaskTableByPid($ret['pid'])) {
                                CrontabManager::removeTaskTable($t['id']);
                            }
                        }
                    }
                }
                $tasks = CrontabManager::getTaskTable();
                if (!$tasks) {
                    Runtime::instance()->crontabProcessStatus(false);
                    if ($quiescing) {
                        Console::warning("【Crontab】#{$managerId} 管理进程排空完成,退出等待拉起");
                        break;
                    }
                } else {
                    foreach ($tasks as $processTask) {
                        if (!isset($processTask['id'])) {
                            Console::warning("【Crontab】任务ID为空:" . JsonHelper::toJson($processTask));
                            continue;
                        }
                        if (Counter::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR')) {
                            CrontabManager::errorReport($processTask);
                        }
                        $taskInstance = CrontabManager::getTaskTableById($processTask['id']);
                        $taskPid = (int)($taskInstance['pid'] ?? 0);
                        $taskAlive = $taskPid > 0 && Process::kill($taskPid, 0);
                        if (!$taskAlive) {
                            if ($quiescing || $taskInstance['manager_id'] !== $managerId) {
                                CrontabManager::removeTaskTable($processTask['id']);
                            } else {
                                CrontabManager::updateTaskTable($processTask['id'], [
                                    'process_is_alive' => STATUS_OFF,
                                ]);
                            }
                            continue;
                        }
                        if ($quiescing) {
                            if ((int)($taskInstance['is_busy'] ?? 0) <= 0) {
                                @Process::kill($taskPid, SIGKILL);
                            }
                            continue;
                        }
                        if ($taskInstance['process_is_alive'] == STATUS_OFF) {
                            sleep($processTask['retry_timeout'] ?? 60);
                            if ($quiescing) {
                                CrontabManager::removeTaskTable($processTask['id']);
                            } elseif ($managerId == $processTask['manager_id']) {
                                CrontabManager::createTaskProcess($processTask, $processTask['restart_num'] + 1);
                            } else {
                                CrontabManager::removeTaskTable($processTask['id']);
                            }
                        } elseif ($taskInstance['manager_id'] !== $managerId) {
                            CrontabManager::removeTaskTable($processTask['id']);
                        }
                    }
                }
                $msg = is_resource($commandPipe) ? stream_get_contents($commandPipe) : '';
                if ($msg === false) {
                    $msg = '';
                }
                if ($msg !== '') {
                    if (StringHelper::isJson($msg)) {
                        $payload = JsonHelper::recover($msg);
                        $command = $payload['command'] ?? 'unknow';
                        Console::log("【Crontab】#{$managerId} 收到命令:" . Color::cyan($command));
                        switch ($command) {
                            case 'upgrade':
                                $quiescing = true;
                                Console::warning("【Crontab】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                            case 'shutdown':
                                break;
                            default:
                                Console::info($command);
                        }
                    } elseif ($msg == 'shutdown') {
                        Console::warning("【Crontab】服务器已关闭,结束运行", (bool)$this->call('should_push_managed_lifecycle_log'));
                        break;
                    }
                }
                if ($msg == 'shutdown') {
                    Console::warning("【Crontab】服务器已关闭,结束运行", (bool)$this->call('should_push_managed_lifecycle_log'));
                    break;
                }
                sleep(1);
            }
            Runtime::instance()->set(Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT, 0);
            Runtime::instance()->set(Key::RUNTIME_CRONTAB_MANAGER_PID, 0);
            is_resource($commandPipe) and fclose($commandPipe);
        });
    }
}
