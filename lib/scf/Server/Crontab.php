<?php

namespace Scf\Server;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Instance;
use Scf\Core\Log;
use Scf\Runtime\MasterDB;
use Scf\Server\Table\Runtime;
use Scf\Server\Table\Counter;
use Scf\Util\Time;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Timer;

class Crontab {
    use Instance;

    protected static array $tasks = [];
    protected static array $tickers = [];
    protected int $id = 1;
    /**
     * 执行一次
     */
    const RUN_MODE_ONECE = 0;
    /**
     * 循环执行
     */
    const RUN_MODE_LOOP = 1;
    /**
     * 定时执行
     */
    const RUN_MODE_TIMEING = 2;
    /**
     * 严格间隔执行
     */
    const RUN_MODE_INTERVAL = 3;
    protected int $managerId = 0;

    /**
     * 加载后台任务
     * @return bool
     */
    public static function load(): bool {
        Counter::instance()->incr('crontab_manager_id');
        $managerId = Counter::instance()->get('crontab_manager_id');
        if (!$modules = App::getModules()) {
            return false;
        }
        $list = [];
        foreach ($modules as $module) {
            if (isset($module['background_tasks'])) {
                $list = $list ? [...$list, ...$module['background_tasks']] : $module['background_tasks'];
            }
        }
        if ($list) {
            foreach ($list as &$task) {
                $task['id'] = $managerId;
                $task['created'] = time();
                $task['manager_id'] = $managerId;
                $task['last_run'] = 0;
                $task['run_count'] = 0;
                $task['status'] = 1;
                $task['next_run'] = 0;
                self::$tasks[substr($task['namespace'], 1)] = $task;
            }
        }
        $key = Manager::instance()->getNodeId() . '_TASKS';
        MasterDB::set($key, self::$tasks);
        return self::hasTask();
    }

    /**
     * @return array
     */
    public static function list(): array {
        return self::$tasks;
    }

    /**
     * 日志
     * @param $msg
     * @return void
     */
    protected function log($msg) {
        $task = $this->getTask();
        Console::log('【' . $task['name'] . '】' . $msg);
        //保存日志到Redis&日志文件
        $key = strtolower('_SERVER_CRONTAB_ENABLE' . str_replace("\\", "_", $task['namespace']));
        MasterDB::addLog($key, ['message' => Log::filter($msg)]);
    }

    /**
     * 任务状态
     * @return array
     */
    public function stats(): array {
        $key = Manager::instance()->getNodeId() . '_TASKS';
        $tasks = MasterDB::get($key) ?: [];
        $list = [];
        foreach ($tasks as &$task) {
            $key = strtolower('_SERVER_CRONTAB_ENABLE' . str_replace("\\", "_", $task['namespace']));
            $task['logs'] = MasterDB::getLog($key, date('Y-m-d'), -100);
            $list[] = $task;
        }
        return $list;
    }


    /**
     * 开始任务
     * @return int
     */
    public function start(): int {
        foreach (self::$tasks as &$task) {
            Runtime::instance()->set('SERVER_CRONTAB_ENABLE_CREATED_' . md5($task['namespace']), $task['created']);
            $task['cid'] = Coroutine::create(function () use (&$task) {
                $worker = $this->createWorker($task['namespace']);
                if (!method_exists($worker, 'run')) {
                    Log::instance()->error('后台任务' . $task['name'] . '未定义run方法');
                } else {
                    switch ($task['mode']) {
                        case self::RUN_MODE_INTERVAL:
                            $task['ticker'] = Timer::tick($task['interval'] * 1000, function ($id) use ($worker) {
                                if ($worker->isAlive()) {
                                    $worker->updateRunTime();
                                    $worker->updateTask('next_run', time() + 1);
                                    $worker->run();
                                } else {
                                    Timer::clear($id);
                                    //$worker->clearTicker();
                                }
                            });
                            self::$tickers[] = $task['ticker'];
                            break;
                        case self::RUN_MODE_LOOP:
                            $this->loop($worker, $task['interval']);
                            break;
                        case self::RUN_MODE_TIMEING:
                            $this->timing($worker, $task);
                            break;
                        default:
                            $worker->updateRunTime();
                            $worker->run();
                            break;
                    }
                }
            });
        }
        return Counter::instance()->get('crontab_manager_id');
    }


    /**
     * 定时执行
     * @param Crontab $worker
     * @param $task
     * @return void
     */
    protected function timing(Crontab $worker, $task) {
        $nextRun = $this->getNextRunTime($task['times']);
        Console::log('[' . $task['name'] . ']下次运行时间(' . $nextRun['after'] . '秒后):' . $nextRun['date']);
        $worker->updateTask('next_run', time() + $nextRun['after']);
        Timer::after($nextRun['after'] * 1000, function () use ($worker, $task) {
            if ($worker->isAlive()) {
                $worker->updateRunTime();
                $worker->run();
                $this->timing($worker, $task);
            }
        });
    }

    /**
     * 获取下次运行时间
     * @param $times
     * @return array
     */
    #[ArrayShape(['after' => "mixed", 'date' => "string"])]
    protected function getNextRunTime($times): array {
        $now = time();
        $matched = null;
        $timestamps = [];
        foreach ($times as $time) {
            $timestamps[] = strtotime($time);
        }
        asort($timestamps);
        foreach ($timestamps as $timestamp) {
            if ($timestamp > $now) {
                $matched = $timestamp;
                break;
            }
        }
        if (is_null($matched)) {
            $matched = strtotime(date('Y-m-d H:i:s', $timestamps[0]) . '+1 day');
        }
        return [
            'after' => $matched - time(),
            'date' => date('m-d H:i:s', $matched)
        ];
    }

    /**
     * 执行循环任务
     * @param Crontab $worker
     * @param $timeout
     * @return void
     */
    protected function loop(Crontab $worker, $timeout) {
        if (!$worker->isAlive()) {
            return;
        }
        $worker->updateRunTime();
        $worker->updateTask('next_run', time() + $timeout);
        $worker->run();
        Timer::after($timeout * 1000, function () use ($worker, $timeout) {
            $this->loop($worker, $timeout);
        });
    }

    /**
     * 清除定时器
     * @return void
     */
    public function clearTicker() {
        $task = $this->getTask();
        if (isset($task['ticker'])) {
            Timer::clear($task['ticker']);
        }
    }

    /**
     * 是否活着
     * @return bool
     */
    public function isAlive(): bool {
        $task = $this->getTask();
        $latestManagerId = Counter::instance()->get('crontab_manager_id');
        if ($task['manager_id'] != $latestManagerId) {
            Timer::clearAll();
            return false;
        }
        return true;
//        return $task['created'] == Runtime::instance()->get('SERVER_CRONTAB_ENABLE_CREATED_' . md5($task['namespace']));
    }

    public function run() {
    }

    /**
     * 获取当前任务
     * @return array|mixed
     */
    protected function getTask(): mixed {
        $class = get_called_class();
        return self::$tasks[$class] ?? [];
    }

    /**
     * 记录允许时间
     * @return void
     */
    protected function updateRunTime() {
        $task = $this->getTask();
        $task['last_run'] = date('m-d H:i:s') . "." . substr(Time::millisecond(), -3);
        $task['run_count'] += 1;
        $class = get_called_class();
        isset(self::$tasks[$class]) and self::$tasks[$class] = $task;
        $key = Manager::instance()->getNodeId() . '_TASKS';
        MasterDB::set($key, self::$tasks);
    }

    /**
     * 更新任务
     * @param $key
     * @param $value
     * @return void
     */
    protected function updateTask($key, $value) {
        $task = $this->getTask();
        $task[$key] = $value;
        $class = get_called_class();
        isset(self::$tasks[$class]) and self::$tasks[$class] = $task;
        $key = Manager::instance()->getNodeId() . '_TASKS';
        MasterDB::set($key, self::$tasks);
    }


    /**
     * 获取正在运行的任务ID
     * @return array
     */
    public function getTaskid(): array {
        return Runtime::instance()->get('background_task_cid') ?: [];
    }

    /**
     * 获取任务池的任务
     * @return array
     */
    public function getTasks(): array {
        return self::$tasks;
    }

    /**
     * 是否存在任务
     * @return bool
     */
    public static function hasTask(): bool {
        return count(self::$tasks) > 0;
    }

    /**
     * @param $name
     * @return static
     */
    protected function createWorker($name): static {
        return new $name;
    }
}