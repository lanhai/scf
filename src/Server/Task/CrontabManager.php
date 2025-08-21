<?php

namespace Scf\Server\Task;

use Exception;
use Scf\Cache\Redis;
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
use Scf\Server\Manager;
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
     * 加载定时任务
     * @return bool
     */
    public static function load(): bool {
        self::$tasks = [];
        $serverConfig = Config::server();
        $list = [];
        $enableStatistics = $serverConfig['db_statistics_enable'] ?? false;
        if (App::isMaster() && $enableStatistics) {
            $list[] = [
                'id' => 'CRONTAB:' . md5(SERVER_NODE_ID . App::id() . '\Scf\Database\Statistics\StatisticCrontab'),
                'status' => STATUS_ON,
                'name' => '统计数据入库',
                'namespace' => '\Scf\Database\Statistics\StatisticCrontab',
                'mode' => Crontab::RUN_MODE_LOOP,
                'interval' => $serverConfig['db_statistics_interval'] ?? 3,
                'timeout' => 3600
            ];
        }
        if (!$modules = App::getModules()) {
            if ($list) {
                goto init;
            }
            return false;
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
            $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
            foreach ($list as $task) {
                $task['id'] = 'CRONTAB:' . md5(SERVER_NODE_ID . App::id() . $task['namespace']);
                $task['manager_id'] = $managerId;
                $task['created'] = time();
                $task['timeout'] = $task['timeout'] ?? 3600;
                self::$tasks[substr($task['namespace'], 1)] = $task;
            }
        }
        return self::hasTask();
    }

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
     * 开启进程
     * @return int
     */
    public static function start(): int {
        if (!App::isReady() || SERVER_CRONTAB_ENABLE != SWITCH_ON) {
            return time();
        }
        $managerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
        $process = new Process(function () use ($managerId) {
            App::mount();
            $pool = Redis::pool(Manager::instance()->getConfig('service_center_server') ?: 'main');
            $key = self::redisSetKey();
            $members = $pool->sMembers($key);
            if ($members) {
                $pool->delete($key);
                foreach ($members as $id) {
                    $pool->delete($id);
                }
            }
            self::load();
            if (self::$tasks) {
                foreach (self::$tasks as $task) {
                    static::updateTaskTable($task['id'], $task);
                }
            }
        });
        $process->start();
        Process::wait();
        sleep(1);
        $taskList = static::getTaskTable();
        if (!$taskList) {
            //没有定时任务也启动一个计时器
            while (true) {
                if ($managerId !== Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS)) {
                    break;
                }
                sleep(App::isDevEnv() ? 5 : 60);
            }
            return time();
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
        while (true) {
            $tasks = static::getTaskTable();
            if (!$tasks) {
                break;
            }
            foreach ($tasks as $processTask) {
                if (Counter::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR')) {
                    static::errorReport($processTask);
                }
                $status = static::getTaskTableById($processTask['id']);
                if ($status['is_running'] == STATUS_OFF) {
                    sleep($processTask['retry_timeout'] ?? 5);
                    if (Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS) == $processTask['manager_id']) {
                        static::createTaskProcess($processTask);
                    } else {
                        static::removeTaskTable($processTask['id']);
                    }
                }
            }
            sleep(5);
        }
        return time();
    }

    /**
     * 创建任务进程
     * @param $task
     * @return bool|int|array
     */
    protected static function createTaskProcess($task): bool|int|array {
        $process = new Process(function () use ($task) {
            App::mount();
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
                    Event::wait();
                }
            }
            static::removeTaskTable($task['id']);
        });
        $pid = $process->start();
        static::updateTaskTable($task['id'], [
            'id' => $task['id'],
            'namespace' => $task['namespace'],
            'pid' => $pid,
            'is_running' => STATUS_ON,
        ]);
        return $pid;
    }

    protected static function getTaskTable(): array {
        return CrontabTable::instance()->rows();
    }

    public static function getTaskTableById($id) {
        return CrontabTable::instance()->get($id) ?: [];
    }

    /**
     * 根据命名空间获取定时任务
     * @param $namespace
     * @return array
     */
    public static function getTaskTableByNamespace($namespace): array {
        $tasks = CrontabTable::instance()->rows();
        $item = array_filter($tasks, function ($task) use ($namespace) {
            return $task['namespace'] == $namespace;
        });
        return $item ? array_values($item)[0] : [];
    }

    public static function updateTaskTable($id, $data): array {
        $task = CrontabTable::instance()->get($id);
        if ($task) {
            foreach ($data as $key => $value) {
                if ($key == 'error_count') {
                    $task[$key] = ($task['error_count'] ?? 0) + $value;
                } else {
                    $task[$key] = $value;
                }
            }
        } else {
            $task = $data;
        }
        CrontabTable::instance()->set($id, $task);
        return CrontabTable::instance()->rows();
    }

    protected static function removeTaskTable($id): array {
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
    protected static function errorReport($processTask): void {
        $errorInfo = Runtime::instance()->get('CRONTAB_' . $processTask['id'] . '_ERROR_INFO') ?: "未知错误";
        static::updateTaskTable($processTask['id'], [
            'is_running' => STATUS_OFF,
            'remark' => $errorInfo,
            'error_count' => 1
        ]);
        $sendError = new Process(function () use ($processTask, $errorInfo) {
            App::mount();
            go(function () use ($processTask, $errorInfo) {
                Log::instance()->error("【Crontab#{$processTask['manager_id']}】{$processTask['name']}[{$processTask['namespace']}]致命错误: " . $errorInfo);
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
    public static function runRightNow($namespace): bool|int {
        try {
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
        return $dir . '/' . md5($namespace) . '.override.json';
    }

    /**
     * 任务状态
     * @return array
     */
    public static function allStatus(): array {
        $pool = Redis::pool(Manager::instance()->getConfig('service_center_server') ?: 'main');
        $ids = $pool->sMembers(self::redisSetKey()) ?: [];
        $list = [];
        if ($ids) {
            foreach ($ids as $id) {
                if (!$task = $pool->hgetAll($id)) {
                    continue;
                }
                foreach ($task as &$val) {
                    if (is_numeric($val)) {
                        $val = (int)$val;
                    }
                }
                $taskName = str_replace("AppCrontab", "", str_replace("\\", "", $task['namespace']));
                $task['logs'] = Manager::instance()->getLog('crontab', date('Y-m-d'), 0, 20, $taskName);
                $task['real_status'] = $task['status'];
                if ($task['mode'] != Crontab::RUN_MODE_TIMING && isset($task['interval'])) {
                    $task['interval_humanize'] = Date::secondsHumanize($task['override']['interval'] ?? $task['interval']);
                }
                $list[] = $task;
            }
        }
        return $list;
    }

    public static function status($namespace) {
        try {
            $crontab = self::copyInstance($namespace);
            $pool = Redis::pool(Manager::instance()->getConfig('service_center_server') ?: 'main');
            $task = $pool->hgetAll($crontab->id);
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
            $task['logs'] = Manager::instance()->getLog('crontab', date('Y-m-d'), 0, 20, $taskName);
            if ($task['mode'] != Crontab::RUN_MODE_TIMING && isset($task['interval'])) {
                $task['interval_humanize'] = Date::secondsHumanize($task['override']['interval'] ?? $task['interval']);
            }
            return $task;
        } catch (Throwable $e) {
            return [];
        }

    }

    /**
     * @return array
     */
    public static function list(): array {
        return self::$tasks;
    }

    /**
     * Redis集合Key
     * @return string
     */
    public static function redisSetKey(): string {
        return 'CRONTABS_' . md5(App::id() . '_' . SERVER_NODE_ID);
    }
}