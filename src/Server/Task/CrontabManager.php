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
use Scf\Util\Date;
use Scf\Util\File;
use Swoole\Event;
use Swoole\Process;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;
use Throwable;

class CrontabManager {
    protected static array $tasks = [];
    protected static array $_instances = [];

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
    private static function load(): void {
        self::$tasks = [];
        $serverConfig = Config::server();
        $list = [];
        $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
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
        if (!$modules = App::getModules()) {
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
                $task['created'] = time();
                $task['timeout'] = $task['timeout'] ?? 3600;
                self::$tasks[substr($task['namespace'], 1)] = $task;
            }
        }
        self::hasTask();
    }


    /**
     * 开启进程
     * @return array
     */
    public static function start(): array {
        if (!App::isReady() || SERVER_CRONTAB_ENABLE != SWITCH_ON) {
            return [];
        }
        $process = new Process(function () {
            App::mount();
            self::load();
            if (self::$tasks) {
                foreach (self::$tasks as $task) {
                    static::add($task['id'], $task);
                }
            }
        });
        $process->start();
        Process::wait();
        sleep(1);
        $taskList = static::getTaskTable();
        if (!$taskList) {
            //没有任务返回空等待下一轮查询
            return [];
        }
        foreach ($taskList as &$task) {
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
        $process = new Process(function (Process $process) use ($task) {
            App::mount();
            register_shutdown_function(function () use ($task) {
                $error = error_get_last();
                if ($error && $error['type'] === E_ERROR) {//标记致命错误
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
                    //Event::wait();
                }
            }
            //Event::exit();
            //$process->exit();
            // 注意：exit 后面的代码不会被执行
        }, false, 0, true);
        $pid = $process->start();
        self::updateTaskTable($task['id'], [
            'namespace' => $task['namespace'],
            'pid' => $pid,
            'process_is_alive' => STATUS_ON,
            'restart_num' => $restartNum,
            'manager_id' => $task['manager_id']
        ]);
        Process::wait(false);
        return $pid;
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

    private static function add(string $id, array $data): void {
        CrontabTable::instance()->set($id, $data);
    }

    public static function removeTaskTable($id): array {
        if (CrontabTable::instance()->exist($id)) {
            CrontabTable::instance()->delete($id);
        }
        return CrontabTable::instance()->rows();
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
        $errorInfo = Runtime::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR_INFO') ?: "未知错误";
        static::updateTaskTable($processTask['id'], [
            'process_is_alive' => STATUS_OFF,
            'remark' => $errorInfo,
            'error_count' => 1
        ]);
        $sendError = new Process(function () use ($processTask, $errorInfo) {
            App::mount();
            go(function () use ($processTask, $errorInfo) {
                Log::instance()->error("{$processTask['name']}[{$processTask['namespace']}]致命错误: " . $errorInfo);
            });
            Event::wait();
        });
        $sendError->start();
        Process::wait();
        Counter::instance()->decr('CRONTAB_' . $processTask['id'] . '_ERROR');
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