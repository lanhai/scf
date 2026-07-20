<?php

namespace Scf\Server\Task;

use Exception;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Result;
use Scf\Core\Table\Counter;
use Scf\Core\Table\CrontabTable;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Server\ProcessRespawnBackoff;
use Scf\Util\Date;
use Scf\Util\File;
use Swoole\Process;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;
use Throwable;

class CrontabManager {
    private const TASK_RESPAWN_STATE_KEY_PREFIX = 'r_ctrs_';

    protected static array $tasks = [];
    protected static array $_instances = [];
    protected static ?ProcessRespawnBackoff $taskRespawnBackoff = null;
    protected static array $loadedTaskRespawnStates = [];

    /**
     * @throws Exception
     */
    public static function copyInstance($namespace): Crontab {
        if (!isset(self::$_instances[$namespace])) {
            $task = self::getTaskTableByNamespace($namespace);
            if (!$task) {
                throw new Exception('定时任务不存在');
            }
            self::$_instances[$namespace] = Crontab::factory($task);
        }
        return self::$_instances[$namespace];
    }

    /**
     * 加载定时任务
     * @return void
     */
    private static function load(int $managerId, string $managerGeneration): void {
        self::$tasks = [];
        $serverConfig = Config::server();
        $list = [];
        $enableStatistics = $serverConfig['db_statistics_enable'] ?? false;
        if (App::isMaster() && $enableStatistics) {
            $list[] = [
                'name' => '统计数据入库',
                'namespace' => '\Scf\Database\Statistics\StatisticCrontab',
                'mode' => Crontab::RUN_MODE_LOOP,
                'interval' => $serverConfig['db_statistics_interval'] ?? 3,
                'timeout' => 3600,
                'status' => STATUS_ON,
            ];
        }
        if (!$modules = App::getCrontabModules()) {
            if ($list) {
                goto init;
            }
            return;
        }

        foreach ($modules as $module) {
            $crontabs = $module['crontabs'] ?? $module['background_tasks'] ?? [];
            if ($crontabs) {
                $list = $list ? [...$list, ...$crontabs] : $crontabs;
            }
            if (App::isMaster() && $masterCrontabls = $module['master_crontabs'] ?? null) {
                $list = $list ? [...$list, ...$masterCrontabls] : $masterCrontabls;
            }
            if (!App::isMaster() && $slaveCrontabls = $module['slave_crontabs'] ?? null) {
                $list = $list ? [...$list, ...$slaveCrontabls] : $slaveCrontabls;
            }
        }
        init:
        if ($list) {
            foreach ($list as $task) {
                $task['id'] = 'CRONTAB:' . md5(App::id() . $task['namespace']);
                $task['manager_id'] = $managerId;
                $task['manager_generation'] = $managerGeneration;
                $task['created'] = time();
                $task['timeout'] = $task['timeout'] ?? 3600;
                $respawnState = self::taskRespawnState($task['id']);
                $task['respawn_attempts'] = (int)($respawnState['attempts'] ?? 0);
                $task['next_retry_at'] = (int)($respawnState['next_retry_at'] ?? 0);
                $task['respawn_exit_recorded'] = 0;
                self::$tasks[substr($task['namespace'], 1)] = $task;
            }
        }
        self::hasTask();
    }


    /**
     * 开启进程
     * @return array
     */
    public static function start(?int $managerId = null, ?string $managerGeneration = null): array {
        $managerId ??= (int)(Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) ?: 0);
        $managerGeneration = trim((string)($managerGeneration
            ?? Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION)
            ?? ''));
        if (
            !App::isReady()
            || SERVER_CRONTAB_ENABLE != SWITCH_ON
            || $managerId <= 0
            || !self::managerGenerationIsCurrent($managerGeneration)
        ) {
            return [];
        }
        $process = new Process(function () use ($managerId, $managerGeneration) {
            if (!self::managerGenerationIsCurrent($managerGeneration)) {
                return;
            }
            App::mount();
            self::load($managerId, $managerGeneration);
            if (self::$tasks) {
                foreach (self::$tasks as $task) {
                    static::addIfOwnedGeneration($task['id'], $task, $managerId, $managerGeneration);
                }
            }
        });
        $loaderPid = (int)($process->start() ?: 0);
        if ($loaderPid <= 0) {
            return [];
        }
        while ($ret = Process::wait(false)) {
            if ((int)($ret['pid'] ?? 0) === $loaderPid) {
                break;
            }
        }
        if (@Process::kill($loaderPid, 0)) {
            Process::wait();
        }
        if (!self::managerGenerationIsCurrent($managerGeneration)) {
            return [];
        }
        sleep(1);
        $taskList = array_filter(
            static::getTaskTable(),
            static fn(array $task): bool => self::taskMatchesOwner(
                $task,
                $managerId,
                $managerGeneration
            )
        );
        if (!$taskList) {
            //没有任务返回空等待下一轮查询
            return [];
        }
        Console::info("【Crontab】#{$managerId} 开始创建任务进程");
        foreach ($taskList as &$task) {
            if (!self::managerGenerationIsCurrent($managerGeneration)) {
                break;
            }
            if (!static::canStartTask($task['id'])) {
                $state = static::taskRespawnState($task['id']);
                static::updateTaskTableIfOwned($task['id'], $managerId, $managerGeneration, [
                    'pid' => 0,
                    'process_is_alive' => STATUS_OFF,
                    'respawn_attempts' => (int)($state['attempts'] ?? 0),
                    'next_retry_at' => (int)($state['next_retry_at'] ?? 0),
                    'respawn_exit_recorded' => 1,
                ]);
                $task['pid'] = 0;
                continue;
            }
            $task['pid'] = static::createTaskProcess($task);
        }
        $output = new ConsoleOutput();
        $table = new Table($output);
        $renderData = [];
        $modes = [
            0 => '一次执行',      // RUN_MODE_ONECE
            1 => '循环执行',      // RUN_MODE_LOOP
            2 => '定时执行',      // RUN_MODE_TIMING
            3 => '间隔执行'       // RUN_MODE_INTERVAL
        ];
        foreach ($taskList as $item) {
            $renderData[] = [
                $item['name'],
                $item['namespace'],
                $modes[$item['mode']],
                isset($item['times']) ? $item['times'][0] . "..." . $item['times'][count($item['times']) - 1] : $item['interval'] ?? '一次',
                Color::cyan($item['pid'])
            ];
        }
        $table
            ->setHeaders([Color::cyan('任务名称'), Color::cyan('任务脚本'), Color::cyan('运行模式'), Color::cyan('间隔时间(秒)'), Color::cyan('进程ID')])
            ->setRows($renderData);
        $table->render();
        return $taskList;
    }

    /**
     * 创建任务进程
     * @param $task
     * @param int $restartNum
     * @return bool|int|array
     */
    public static function createTaskProcess($task, int $restartNum = 0): bool|int|array {
        $taskId = (string)($task['id'] ?? '');
        $managerId = (int)($task['manager_id'] ?? 0);
        $managerGeneration = (string)($task['manager_generation'] ?? '');
        if (
            $taskId === ''
            || !self::canStartTask($taskId)
            || !self::managerGenerationIsCurrent($managerGeneration)
            || !self::taskMatchesOwner(
                self::getTaskTableById($taskId),
                $managerId,
                $managerGeneration,
                (int)($task['pid'] ?? 0)
            )
        ) {
            return 0;
        }
        $process = new Process(function (Process $process) use ($task, $managerGeneration) {
            if (!self::managerGenerationIsCurrent($managerGeneration)) {
                return;
            }
            App::mount();
            // 保留 task 级 fatal 记录，避免这次 warning 修复把原有错误观测链一起删掉。
            register_shutdown_function(function () use ($task) {
                $error = error_get_last();
                if ($error && $error['type'] === E_ERROR) {
                    Counter::instance()->incr('CRONTAB_' . $task['id'] . '_ERROR');
                    Runtime::instance()->set('CRONTAB_' . $task['id'] . '_ERROR_INFO', $error['message']);
                }
            });
            $taskInstance = Crontab::factory($task);
            if (!$taskInstance->validate()) {
                Console::error($task['namespace'] . ':' . $taskInstance->getError());
            } else {
                if (!method_exists($taskInstance, 'run')) {
                    Console::error($task['namespace'] . ':任务脚本必须实现 run 方法');
                } else {
                    $taskInstance->start();
                }
            }
        }, false, 0, true);
        try {
            $pid = (int)($process->start() ?: 0);
        } catch (Throwable $throwable) {
            $pid = 0;
            Console::warning("【Crontab】{$task['namespace']} 任务进程启动异常: " . $throwable->getMessage());
        }
        if ($pid <= 0) {
            $delay = self::recordTaskStartFailure($task);
            Console::warning("【Crontab】{$task['namespace']} 任务进程启动失败，{$delay}s 后重试");
            return 0;
        }
        if (
            !self::managerGenerationIsCurrent($managerGeneration)
            || !self::taskMatchesOwner(
                self::getTaskTableById($taskId),
                $managerId,
                $managerGeneration,
                (int)($task['pid'] ?? 0)
            )
        ) {
            @Process::kill($pid, SIGTERM);
            return 0;
        }
        self::recordTaskStarted($task, $pid);
        self::updateTaskTableIfOwned($task['id'], $managerId, $managerGeneration, [
            'namespace' => $task['namespace'],
            'pid' => $pid,
            'process_is_alive' => STATUS_ON,
            'restart_num' => $restartNum,
            'manager_id' => $task['manager_id'],
            'manager_generation' => $managerGeneration,
            'respawn_exit_recorded' => 0,
        ]);
        return $pid;
    }

    /**
     * 判断任务是否已到允许重拉的时间。
     */
    public static function canStartTask(string $taskId, ?int $now = null): bool {
        self::loadTaskRespawnState($taskId);
        return self::taskRespawnBackoff()->canStart($taskId, $now);
    }

    /**
     * 记录任务进程已经成功 fork。状态写入 Runtime，manager 自身重建后仍可恢复。
     */
    public static function recordTaskStarted(array $task, int $pid, ?int $now = null): void {
        $taskId = (string)($task['id'] ?? '');
        if ($taskId === '') {
            return;
        }
        self::loadTaskRespawnState($taskId);
        self::taskRespawnBackoff()->recordStarted($taskId, $now);
        self::persistTaskRespawnState($taskId, (string)($task['namespace'] ?? ''));
        $state = self::taskRespawnState($taskId);
        self::updateTaskTableIfOwned(
            $taskId,
            (int)($task['manager_id'] ?? 0),
            (string)($task['manager_generation'] ?? ''),
            [
            'pid' => $pid,
            'process_is_alive' => STATUS_ON,
            'respawn_attempts' => (int)($state['attempts'] ?? 0),
            'next_retry_at' => (int)($state['next_retry_at'] ?? 0),
            'respawn_exit_recorded' => 0,
            ],
            (int)($task['pid'] ?? 0)
        );
    }

    /**
     * 记录短命退出并返回本次退避秒数。
     */
    public static function recordTaskExit(array $task, ?int $now = null): int {
        $taskId = (string)($task['id'] ?? '');
        if ($taskId === '') {
            return 60;
        }
        self::loadTaskRespawnState($taskId);
        $delay = self::taskRespawnBackoff()->recordExit($taskId, $now);
        self::persistTaskRespawnState($taskId, (string)($task['namespace'] ?? ''));
        $state = self::taskRespawnState($taskId);
        self::updateTaskTableIfOwned(
            $taskId,
            (int)($task['manager_id'] ?? 0),
            (string)($task['manager_generation'] ?? ''),
            [
            'pid' => 0,
            'process_is_alive' => STATUS_OFF,
            'respawn_attempts' => (int)($state['attempts'] ?? 0),
            'next_retry_at' => (int)($state['next_retry_at'] ?? 0),
            'respawn_exit_recorded' => 1,
            ],
            (int)($task['pid'] ?? 0)
        );
        return $delay;
    }

    /**
     * 记录 fork/start 失败；该路径没有可等待的 child，同样必须进入退避。
     */
    public static function recordTaskStartFailure(array $task, ?int $now = null): int {
        $taskId = (string)($task['id'] ?? '');
        if ($taskId === '') {
            return 60;
        }
        self::loadTaskRespawnState($taskId);
        $delay = self::taskRespawnBackoff()->recordStartFailure($taskId, $now);
        self::persistTaskRespawnState($taskId, (string)($task['namespace'] ?? ''));
        $state = self::taskRespawnState($taskId);
        self::updateTaskTableIfOwned(
            $taskId,
            (int)($task['manager_id'] ?? 0),
            (string)($task['manager_generation'] ?? ''),
            [
            'pid' => 0,
            'process_is_alive' => STATUS_OFF,
            'respawn_attempts' => (int)($state['attempts'] ?? 0),
            'next_retry_at' => (int)($state['next_retry_at'] ?? 0),
            'respawn_exit_recorded' => 1,
            ],
            (int)($task['pid'] ?? 0)
        );
        return $delay;
    }

    /**
     * 存活达到稳定窗口后清零历史失败次数，避免一次旧故障永久放大后续恢复时间。
     */
    public static function markTaskStable(array $task, ?int $now = null): bool {
        $taskId = (string)($task['id'] ?? '');
        if ($taskId === '') {
            return false;
        }
        self::loadTaskRespawnState($taskId);
        if (!self::taskRespawnBackoff()->markStable($taskId, $now)) {
            return false;
        }
        self::persistTaskRespawnState($taskId, (string)($task['namespace'] ?? ''));
        $state = self::taskRespawnState($taskId);
        self::updateTaskTableIfOwned(
            $taskId,
            (int)($task['manager_id'] ?? 0),
            (string)($task['manager_generation'] ?? ''),
            [
            'respawn_attempts' => (int)($state['attempts'] ?? 0),
            'next_retry_at' => (int)($state['next_retry_at'] ?? 0),
            ],
            (int)($task['pid'] ?? 0)
        );
        return true;
    }

    /**
     * @return array{attempts:int,next_retry_at:int,started_at:int,last_started_at:int,last_exit_at:int}
     */
    public static function taskRespawnState(string $taskId): array {
        self::loadTaskRespawnState($taskId);
        return self::taskRespawnBackoff()->state($taskId);
    }

    protected static function taskRespawnBackoff(): ProcessRespawnBackoff {
        return self::$taskRespawnBackoff ??= new ProcessRespawnBackoff();
    }

    protected static function loadTaskRespawnState(string $taskId): void {
        if ($taskId === '' || isset(self::$loadedTaskRespawnStates[$taskId])) {
            return;
        }
        self::$loadedTaskRespawnStates[$taskId] = true;
        $stored = Runtime::instance()->get(self::taskRespawnRuntimeKey($taskId));
        if (is_array($stored)) {
            $state = isset($stored['state']) && is_array($stored['state']) ? $stored['state'] : $stored;
            self::taskRespawnBackoff()->restoreState($taskId, $state);
        }
    }

    protected static function persistTaskRespawnState(string $taskId, string $namespace): void {
        Runtime::instance()->set(self::taskRespawnRuntimeKey($taskId), [
            'task_id' => $taskId,
            'namespace' => $namespace,
            'state' => self::taskRespawnBackoff()->state($taskId),
            'updated_at' => time(),
        ]);
    }

    protected static function taskRespawnRuntimeKey(string $taskId): string {
        return self::TASK_RESPAWN_STATE_KEY_PREFIX . md5($taskId);
    }

    public static function getTaskTable(): array {
        return CrontabTable::instance()->rows();
    }

    /**
     * 根据ID获取任务
     * @param string $id
     * @return array
     */
    public static function getTaskTableById(string $id): array {
        return CrontabTable::instance()->get($id) ?: [];
    }

    public static function getTaskTableByPid($pid): array {
        $tasks = CrontabTable::instance()->rows();
        $item = array_filter($tasks, function ($task) use ($pid) {
            return $task['pid'] == $pid;
        });
        return $item ? array_values($item)[0] : [];
    }

    /**
     * 根据命名空间获取定时任务
     * @param string $namespace
     * @return array
     */
    public static function getTaskTableByNamespace(string $namespace): array {
        $tasks = CrontabTable::instance()->rows();
        $item = array_filter($tasks, function ($task) use ($namespace) {
            return $task['namespace'] == $namespace;
        });
        return $item ? array_values($item)[0] : [];
    }

    /**
     * 更新任务数据
     * @param string $id
     * @param array $data
     * @return array
     */
    public static function updateTaskTable(string $id, array $data): array {
        $task = CrontabTable::instance()->get($id);
        if ($task) {
            foreach ($data as $key => $value) {
                if ($key == 'error_count') {
                    $task[$key] = ($task['error_count'] ?? 0) + $value;
                } else {
                    $task[$key] = $value;
                }
            }
            CrontabTable::instance()->set($id, $task);
        }
        return CrontabTable::instance()->rows();
    }

    /**
     * 按 manager generation 比较后更新，旧 task/manager 不得覆盖新代同名 row。
     */
    public static function updateTaskTableIfOwned(
        string $id,
        int $managerId,
        string $managerGeneration,
        array $data,
        ?int $expectedPid = null
    ): bool {
        $task = CrontabTable::instance()->get($id);
        if (!self::taskMatchesOwner($task ?: [], $managerId, $managerGeneration, $expectedPid)) {
            return false;
        }
        foreach ($data as $key => $value) {
            if ($key === 'error_count') {
                $task[$key] = ($task['error_count'] ?? 0) + $value;
            } else {
                $task[$key] = $value;
            }
        }
        return (bool)CrontabTable::instance()->set($id, $task);
    }

    private static function addIfOwnedGeneration(
        string $id,
        array $data,
        int $managerId,
        string $managerGeneration
    ): bool {
        if (!self::managerGenerationIsCurrent($managerGeneration)) {
            return false;
        }
        $existing = CrontabTable::instance()->get($id);
        if (
            $existing
            && !self::taskMatchesOwner((array)$existing, $managerId, $managerGeneration)
        ) {
            return false;
        }
        if ($existing && (int)($existing['pid'] ?? 0) > 0) {
            return true;
        }
        $data['manager_id'] = $managerId;
        $data['manager_generation'] = $managerGeneration;
        return (bool)CrontabTable::instance()->set($id, $data);
    }

    public static function removeTaskTable($id): array {
        if (CrontabTable::instance()->exist($id)) {
            CrontabTable::instance()->delete($id);
        }
        return CrontabTable::instance()->rows();
    }

    /**
     * 删除前重新比较完整 owner；用于旧 manager 排空，避免删掉新代复用 task id 的 row。
     */
    public static function removeTaskTableIfOwned(
        string $id,
        int $managerId,
        string $managerGeneration,
        ?int $expectedPid = null
    ): bool {
        $task = CrontabTable::instance()->get($id);
        if (!self::taskMatchesOwner($task ?: [], $managerId, $managerGeneration, $expectedPid)) {
            return false;
        }
        return (bool)CrontabTable::instance()->delete($id);
    }

    public static function taskMatchesOwner(
        array $task,
        int $managerId,
        string $managerGeneration,
        ?int $expectedPid = null
    ): bool {
        $taskGeneration = (string)($task['manager_generation'] ?? '');
        return $managerId > 0
            && $managerGeneration !== ''
            && (int)($task['manager_id'] ?? 0) === $managerId
            && $taskGeneration !== ''
            && hash_equals($managerGeneration, $taskGeneration)
            && ($expectedPid === null || (int)($task['pid'] ?? 0) === $expectedPid);
    }

    public static function managerGenerationIsCurrent(string $managerGeneration): bool {
        $current = (string)(Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION) ?? '');
        return $managerGeneration !== ''
            && $current !== ''
            && hash_equals($current, $managerGeneration);
    }

    /**
     * 是否存在任务
     * @return bool
     */
    public static function hasTask(): bool {
        return count(self::$tasks) > 0;
    }

    /**
     * 错误上报
     * @param $processTask
     * @return void
     */
    public static function errorReport($processTask): void {
        $errorKey = 'CRONTAB_' . $processTask['id'] . '_ERROR';
        $errorInfoKey = 'CRONTAB_' . $processTask['id'] . '_ERROR_INFO';
        $errorInfo = Runtime::instance()->get($errorInfoKey) ?: "未知错误";
        static::updateTaskTableIfOwned(
            (string)$processTask['id'],
            (int)($processTask['manager_id'] ?? 0),
            (string)($processTask['manager_generation'] ?? ''),
            [
            'process_is_alive' => STATUS_OFF,
            'remark' => "致命错误",
            'error_count' => 1
            ],
            (int)($processTask['pid'] ?? 0)
        );
        $sendError = new Process(function () use ($processTask, $errorInfo) {
            App::mount();
            Log::instance()->error("{$processTask['name']}[{$processTask['namespace']}]致命错误: " . $errorInfo);
        });
        $sendError->start();
        // 统一由 CrontabManager 主循环 wait(false) 回收。这里若直接 wait()，
        // 可能误收走另一个刚退出的任务 child，导致该任务永远收不到退出事件。
        $left = Counter::instance()->decr($errorKey);
        // 任务恢复后清理归零计数 key，避免历史 task id 长期占用 Counter 行。
        if ($left <= 0) {
            Counter::instance()->delete($errorKey);
            Runtime::instance()->delete($errorInfoKey);
        }
    }

    /**
     * 立即执行
     */
    public static function runRightNow($namespace, $host = null): bool|int {
        try {
            //TODO 向子节点发送运行指令
            return self::copyInstance($namespace)->runRightNow();
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * 保存配置文件
     * @param $data
     * @return Result
     */
    public static function saveOverrides($data): Result {
        $file = self::overridesConfigFile($data['namespace']);
        if (!$file) {
            return Result::error('创建配置文件夹失败');
        }
        foreach ($data as $key => $val) {
            if (is_numeric($val)) {
                $data[$key] = (int)$val;
            }
        }
        try {
            $crontab = self::copyInstance($data['namespace']);
            $data['expired'] = time();
            if (!$crontab->update($data)) {
                return Result::error('更新任务状态失败');
            }
            return Result::success(File::write($file, JsonHelper::toJson($data)));
        } catch (Throwable $e) {
            return Result::error($e->getMessage());
        }
    }

    /**
     * 获取配置文件路径
     * @return false|string
     */
    public static function overridesConfigFile(string $namespace): bool|string {
        $dir = APP_PATH . '/src/config/crontab';
        if (!file_exists($dir)) {
            try {
                mkdir($dir, 0777, true);
            } catch (Throwable) {
                return false;
            }
        }
        clearstatcache();
        return $dir . '/' . str_replace("\\", "", $namespace) . '.override.json';
    }

    /**
     * 任务状态
     * @return array
     */
    public static function allStatus(): array {
        $tasks = CrontabTable::instance()->rows();
        if (!$tasks) {
            return [];
        }
        $list = array_values($tasks);
        foreach ($list as &$task) {
            if (!isset($task['id'])) {
                continue;
            }
            foreach ($task as &$val) {
                if (is_numeric($val)) {
                    $val = (int)$val;
                }
            }
            $task['logs'] = [];
            $task['real_status'] = $task['status'];
            if ($task['mode'] != Crontab::RUN_MODE_TIMING && isset($task['interval'])) {
                $task['interval_humanize'] = Date::secondsHumanize($task['override']['interval'] ?? $task['interval']);
            }
        }
        return $list;
    }

    public static function busyCount(): int {
        $tasks = CrontabTable::instance()->rows();
        if (!$tasks) {
            return 0;
        }
        $count = 0;
        foreach ($tasks as $task) {
            if ((int)($task['is_busy'] ?? 0) === 1) {
                $count++;
            }
        }
        return $count;
    }

    public static function status($namespace): array {
        try {
            $crontab = self::copyInstance($namespace);
            $task = self::getTaskTableById($crontab->id);
            if (!$task) {
                return [];
            }
            foreach ($task as &$val) {
                if (is_numeric($val)) {
                    $val = (int)$val;
                }
            }
            $taskName = str_replace("AppCrontab", "", str_replace("\\", "", $task['namespace']));
            $task['real_status'] = $task['status'];
            $task['logs'] = Log::instance()->get('crontab', date('Y-m-d'), 0, 20, $taskName);
            if ($task['mode'] != Crontab::RUN_MODE_TIMING && isset($task['interval'])) {
                $task['interval_humanize'] = Date::secondsHumanize($task['override']['interval'] ?? $task['interval']);
            }
            return $task;
        } catch (Throwable $e) {
            return [];
        }
    }

    /**
     * 命名空间转换为任务名
     * @param string $namespace
     * @return array|string
     */
    public static function formatTaskName(string $namespace): array|string {
        $name = str_replace("\\", "", $namespace);
        if (str_starts_with($name, 'App')) {
            $name = substr($name, 3);
        }
        if (str_starts_with($name, 'Crontab')) {
            $name = substr($name, 7);
        }
        return $name;
    }

    /**
     * @return array
     */
    public static function list(): array {
        return self::$tasks;
    }

}
