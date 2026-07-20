<?php

namespace Scf\Server\SubProcess;

use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Server\ProcessRespawnBackoff;
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
            $managerGeneration = $this->captureManagerGeneration();
            $managerPid = getmypid() ?: 0;
            if (!$this->claimRuntimeOwnership(
                $managerGeneration,
                Key::RUNTIME_CRONTAB_MANAGER_PID,
                Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT,
                $managerPid
            )) {
                return;
            }
            Console::info("【Crontab】排程任务管理PID:" . $managerPid, false);
            define('IS_CRONTAB_PROCESS', true);
            $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
            is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
            $managerId = (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0);
            $quiescing = false;
            $taskDiscoveryBackoff = new ProcessRespawnBackoff(5, 60, 30);
            $taskDiscoveryKey = 'crontab-task-discovery';
            while (true) {
                $generationCurrent = $this->ownsRuntimeProcess(
                    $managerGeneration,
                    Key::RUNTIME_CRONTAB_MANAGER_PID,
                    $managerPid
                );
                $pipeClosed = $this->managerCommandPipeClosed($commandPipe);
                $serverAlive = Runtime::instance()->serverIsAlive();
                if ($generationCurrent) {
                    $this->touchRuntimeOwnershipIfCurrent(
                        $managerGeneration,
                        Key::RUNTIME_CRONTAB_MANAGER_PID,
                        Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT,
                        $managerPid
                    );
                }
                $latestManagerId = (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0);
                if (
                    !$quiescing
                    && (
                        !$generationCurrent
                        || $pipeClosed
                        || !$serverAlive
                        || $latestManagerId !== $managerId
                    )
                ) {
                    $quiescing = true;
                    Console::warning("【Crontab】#{$managerId} 管理进程进入迭代排空,停止接新任务");
                }
                while ($ret = Process::wait(false)) {
                    $pid = (int)($ret['pid'] ?? 0);
                    if ($pid <= 0) {
                        continue;
                    }
                    if ($task = CrontabManager::getTaskTableByPid($pid)) {
                        if (!CrontabManager::taskMatchesOwner($task, $managerId, $managerGeneration, $pid)) {
                            continue;
                        }
                        if ($quiescing) {
                            CrontabManager::removeTaskTableIfOwned(
                                (string)$task['id'],
                                $managerId,
                                $managerGeneration,
                                $pid
                            );
                            continue;
                        }
                        if (!(int)($task['respawn_exit_recorded'] ?? 0)) {
                            $delay = CrontabManager::recordTaskExit($task);
                            $state = CrontabManager::taskRespawnState($task['id']);
                            Console::warning(
                                "【Crontab】{$task['namespace']} 任务进程退出，{$delay}s 后可重拉"
                                . ', attempts=' . (int)($state['attempts'] ?? 0)
                            );
                        }
                    }
                }
                if (!$quiescing && !$generationCurrent) {
                    $quiescing = true;
                }
                if (!$quiescing && !Runtime::instance()->serverIsReady()) {
                    sleep(1);
                    continue;
                }

                $tasks = CrontabManager::getTaskTable();
                $ownedTasks = array_filter(
                    $tasks,
                    static fn(array $task): bool => CrontabManager::taskMatchesOwner(
                        $task,
                        $managerId,
                        $managerGeneration
                    )
                );
                $foreignLiveTasks = [];
                if (!$quiescing) {
                    foreach ($tasks as $taskId => $task) {
                        if (CrontabManager::taskMatchesOwner($task, $managerId, $managerGeneration)) {
                            continue;
                        }
                        $foreignPid = max(0, (int)($task['pid'] ?? 0));
                        if ($foreignPid > 0 && @Process::kill($foreignPid, 0)) {
                            $foreignLiveTasks[$taskId] = $task;
                            continue;
                        }
                        $foreignManagerId = (int)($task['manager_id'] ?? 0);
                        $foreignGeneration = (string)($task['manager_generation'] ?? '');
                        if ($foreignManagerId > 0 && $foreignGeneration !== '') {
                            CrontabManager::removeTaskTableIfOwned(
                                (string)($task['id'] ?? $taskId),
                                $foreignManagerId,
                                $foreignGeneration,
                                $foreignPid
                            );
                        }
                    }
                    $tasks = CrontabManager::getTaskTable();
                    $ownedTasks = array_filter(
                        $tasks,
                        static fn(array $task): bool => CrontabManager::taskMatchesOwner(
                            $task,
                            $managerId,
                            $managerGeneration
                        )
                    );
                }
                if (
                    !$quiescing
                    && !$ownedTasks
                    && !$foreignLiveTasks
                    && Runtime::instance()->serverIsAlive()
                    && !Runtime::instance()->serverIsDraining()
                    && $taskDiscoveryBackoff->canStart($taskDiscoveryKey)
                ) {
                    Runtime::instance()->crontabProcessStatus(false);
                    $taskList = CrontabManager::start($managerId, $managerGeneration);
                    if ($taskList) {
                        Runtime::instance()->crontabProcessStatus(true);
                        $taskDiscoveryBackoff->reset($taskDiscoveryKey);
                        $ownedTasks = array_filter(
                            CrontabManager::getTaskTable(),
                            static fn(array $task): bool => CrontabManager::taskMatchesOwner(
                                $task,
                                $managerId,
                                $managerGeneration
                            )
                        );
                    } else {
                        Runtime::instance()->crontabProcessStatus(false);
                        $delay = $taskDiscoveryBackoff->recordStartFailure($taskDiscoveryKey);
                        Console::info("【Crontab】当前没有可启动任务，{$delay}s 后低频重查");
                    }
                }
                if (!$ownedTasks) {
                    if ($quiescing) {
                        Console::warning("【Crontab】#{$managerId} 管理进程排空完成,退出等待拉起");
                        break;
                    }
                    if (
                        $this->managerGenerationIsCurrent($managerGeneration)
                        && Runtime::instance()->crontabProcessStatus()
                    ) {
                        Runtime::instance()->crontabProcessStatus(false);
                        if ($taskDiscoveryBackoff->canStart($taskDiscoveryKey)) {
                            $taskDiscoveryBackoff->recordStartFailure($taskDiscoveryKey);
                        }
                    }
                } else {
                    foreach ($ownedTasks as $processTask) {
                        if (!isset($processTask['id'])) {
                            Console::warning("【Crontab】任务ID为空:" . JsonHelper::toJson($processTask));
                            continue;
                        }
                        $taskInstance = CrontabManager::getTaskTableById($processTask['id']);
                        if (
                            !$taskInstance
                            || !CrontabManager::taskMatchesOwner(
                                $taskInstance,
                                $managerId,
                                $managerGeneration,
                                (int)($processTask['pid'] ?? 0)
                            )
                        ) {
                            continue;
                        }
                        $taskPid = (int)($taskInstance['pid'] ?? 0);
                        $taskAlive = $taskPid > 0 && @Process::kill($taskPid, 0);
                        if ($quiescing) {
                            if (!$taskAlive) {
                                CrontabManager::removeTaskTableIfOwned(
                                    (string)$processTask['id'],
                                    $managerId,
                                    $managerGeneration,
                                    $taskPid
                                );
                            } elseif ((int)($taskInstance['is_busy'] ?? 0) <= 0) {
                                $latestTask = CrontabManager::getTaskTableById($processTask['id']);
                                if (CrontabManager::taskMatchesOwner(
                                    $latestTask,
                                    $managerId,
                                    $managerGeneration,
                                    $taskPid
                                )) {
                                    @Process::kill($taskPid, SIGTERM);
                                }
                            }
                            continue;
                        }
                        if (Counter::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR')) {
                            CrontabManager::errorReport($processTask);
                        }
                        if ($taskAlive) {
                            CrontabManager::markTaskStable($taskInstance);
                            continue;
                        }
                        if (!(int)($taskInstance['respawn_exit_recorded'] ?? 0)) {
                            $delay = CrontabManager::recordTaskExit($taskInstance);
                            $state = CrontabManager::taskRespawnState($taskInstance['id']);
                            Console::warning(
                                "【Crontab】{$taskInstance['namespace']} 检测到任务不在线，{$delay}s 后可重拉"
                                . ', attempts=' . (int)($state['attempts'] ?? 0)
                            );
                            $taskInstance = CrontabManager::getTaskTableById($processTask['id']);
                        }
                        if (CrontabManager::canStartTask($processTask['id'])) {
                            $taskInstance = CrontabManager::getTaskTableById($processTask['id']);
                            if (!CrontabManager::taskMatchesOwner(
                                $taskInstance,
                                $managerId,
                                $managerGeneration,
                                (int)($taskInstance['pid'] ?? 0)
                            )) {
                                continue;
                            }
                            CrontabManager::createTaskProcess(
                                $taskInstance,
                                (int)($taskInstance['restart_num'] ?? 0) + 1
                            );
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
                if ($this->managerCommandPipeClosed($commandPipe)) {
                    $quiescing = true;
                }
                sleep(1);
            }
            if ($this->ownsRuntimeProcess(
                $managerGeneration,
                Key::RUNTIME_CRONTAB_MANAGER_PID,
                $managerPid
            )) {
                Runtime::instance()->crontabProcessStatus(false);
            }
            $this->clearRuntimeOwnershipIfCurrent(
                $managerGeneration,
                Key::RUNTIME_CRONTAB_MANAGER_PID,
                Key::RUNTIME_CRONTAB_MANAGER_HEARTBEAT_AT,
                $managerPid
            );
            is_resource($commandPipe) and fclose($commandPipe);
        });
    }
}
