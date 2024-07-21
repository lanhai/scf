<?php

namespace Scf\Server\Task;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Core\Result;
use Scf\Core\Traits\Singleton;
use Scf\Cache\MasterDB;
use Scf\Helper\JsonHelper;
use Scf\Command\Color;
use Scf\Server\Table\Counter;
use Scf\Server\Table\Runtime;
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\Time;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Process;
use Swoole\Timer;
use Throwable;

class Crontab {
    use Singleton;

    protected static array $tasks = [];
    protected static array $tickers = [];
    protected int $id = 1;
    protected int $timer = 0;
    protected array $attributes = [];
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

    /**
     * @return void
     */
    public static function startProcess(): void {
        if (!App::isReady()) {
            return;
        }
        $process = new Process(function () {
            App::mount();
            if (SERVER_CRONTAB_ENABLE == SWITCH_ON && self::load()) {
                self::instance()->start();
                Event::wait();
            }
        });
        $pid = $process->start();
        File::write(SERVER_CRONTAB_MANAGER_PID_FILE, $pid);
        Console::success('Crontab Manager PID:' . Color::info($pid));
    }

    /**
     * 加载定时任务
     * @return bool
     */
    public static function load(): bool {
        $managerId = Counter::instance()->get('_background_process_id_');
        if (!$modules = App::getModules()) {
            Console::warning("app模块加载失败");
            return false;
        }
        $list = [];
        foreach ($modules as $module) {
            $crontabs = $module['crontabs'] ?? $module['background_tasks'] ?? [];
            if ($crontabs) {
                $list = $list ? [...$list, ...$crontabs] : $crontabs;
            }
            if (App::isMaster() && $masterContabls = $module['master_crontabs'] ?? null) {
                $list = $list ? [...$list, ...$masterContabls] : $masterContabls;
            }
        }
        if ($list) {
            foreach ($list as &$task) {
                $task['id'] = $managerId;
                $task['created'] = time();
                $task['expired'] = 0;
                $task['manager_id'] = $managerId;
                $task['last_run'] = 0;
                $task['run_count'] = 0;
                $task['status'] = $task['status'] ?? 0;
                $task['next_run'] = 0;
                $task['is_busy'] = 0;
                //读取覆盖的配置
                $task['override'] = null;
                clearstatcache();
                $overrideConfig = self::factory($task['namespace'])->getOverridesConfigFileName();
                if (file_exists($overrideConfig)) {
                    $config = File::readJson($overrideConfig);
                    if (!empty($config['namespace'])) {
                        $task['override'] = $config;
                    }
                }
                self::$tasks[substr($task['namespace'], 1)] = $task;
            }
        }
        return self::hasTask();
    }

    /**
     * 开始任务
     * @return int
     */
    public function start(): int {
        $members = MasterDB::sMembers(App::id() . '_CRONTABS_');
        if ($members) {
            //MasterDB::sClear(App::id() . '_CRONTABS_');
            foreach ($members as $id) {
                MasterDB::delete('-crontabs-' . $id);
            }
        }
        $managerId = Counter::instance()->get('_background_process_id_');
        foreach (self::$tasks as &$task) {
            Runtime::instance()->set('SERVER_CRONTAB_ENABLE_CREATED_' . md5($task['namespace']), $task['created']);
            $task['cid'] = Coroutine::create(function () use (&$task, $managerId) {
                $worker = $this->createWorker($task['namespace']);
                if (!method_exists($worker, 'run')) {
                    Log::instance()->error('后台任务' . $task['name'] . '未定义run方法');
                } else {
                    $worker->register($task);
                }
            });
        }
        return Counter::instance()->get('_background_process_id_');
    }

    /**
     * 注册任务
     * @param $task
     * @return void
     */
    protected function register($task): void {
        if (!MasterDB::sIsMember(App::id() . '_CRONTABS_', $this->id())) {
            MasterDB::sAdd(App::id() . '_CRONTABS_', $this->id());
        }
        $this->attributes = $task;
        Timer::tick(5000, function () {
            //服务器已重启,终止现有计时器
            if ($this->attributes['manager_id'] != Counter::instance()->get('_background_process_id_')) {
                Console::info("【Crontab】#" . $this->attributes['manager_id'] . "已终止运行");
                Timer::clearAll();
                return;
            }
            $this->sync();
            if ($this->isExpired()) {
                //迭代
                $this->upgrade();
            }
        });
        $this->excute();
    }

    /**
     * 执行任务
     * @return void
     */
    protected function excute(): void {
        $mode = $this->attributes['override']['mode'] ?? $this->attributes['mode'];
        $interval = $this->attributes['override']['interval'] ?? $this->attributes['interval'] ?? 0;
        $status = $this->attributes['override']['status'] ?? $this->attributes['status'];
        if ($status == STATUS_OFF) {
            $this->refreshDB();
            return;
        }
        $id = $this->id;
        switch ($mode) {
            case self::RUN_MODE_INTERVAL:
                $this->processingFinish(time() + $interval);
                $this->attributes['ticker'] = Timer::tick($interval * 1000, function () use ($interval, $id) {
                    if ($this->isAlive($id)) {
                        $this->processingStart(time() + $interval);
                        try {
                            $this->run();
                        } catch (Throwable $throwable) {
                            Log::instance()->error("【Crontab】任务执行失败:" . $throwable->getMessage());
                            $this->log("任务执行失败:" . $throwable->getMessage());
                        }
                        $this->processingFinish();
                    }
                });
                self::$tickers[] = $this->attributes['ticker'];
                $this->setTimer($this->attributes['ticker']);
                break;
            case self::RUN_MODE_LOOP:
                $this->loop($interval, $id);
                break;
            case self::RUN_MODE_TIMEING:
                $this->timing($id);
                break;
            default:
                $this->updateRunTime();
                try {
                    //单次执行的任务如果是无限循环任务需要在循环逻辑里判断当前任务是否处于激活状态,且在结束循环时清理相关计时器
                    $this->run();
                } catch (Throwable $throwable) {
                    Log::instance()->error("【Crontab】任务执行失败:" . $throwable->getMessage());
                    $this->log("任务执行失败:" . $throwable->getMessage());
                }
                break;
        }
        $this->refreshDB();
    }


    /**
     * 升级迭代
     * @return void
     */
    protected function upgrade(): void {
        $this->attributes['expired'] = 0;
        $this->override();
        $this->id++;
        //清除定时器
        $this->timer and Timer::clear($this->timer);
        $this->setTimer(0);
        $this->updateTask('expired', 0);
        Console::info($this->attributes['namespace'] . "运行参数已变更,已升级迭代至#" . $this->id);
        //重新执行
        $this->excute();
    }

    /**
     * 加载覆盖配置
     * @return void
     */
    protected function override(): void {
        clearstatcache();
        $overrideConfig = $this->getOverridesConfigFileName();
        if (file_exists($overrideConfig)) {
            $config = File::readJson($overrideConfig);
            if (!empty($config['namespace'])) {
                $this->attributes['override'] = $config;
            }
        }
    }

    /**
     * 立即执行
     */
    public function runRightNow(): bool|int {
        $this->sync();
        return go(function () {
            $this->processingStart();
            try {
                $this->run();
            } catch (Throwable $throwable) {
                Log::instance()->error("【Crontab】任务执行失败:" . $throwable->getMessage());
                $this->log("任务执行失败:" . $throwable->getMessage());
            }
            $this->processingFinish();
        });
    }

    /**
     * 重启
     * @return array|mixed
     */
    public function reload(): mixed {
        $this->sync();
        return $this->updateTask('expired', time());
    }

    /**
     * 判断当前worker是否过期
     * @return bool
     */
    protected function isExpired(): bool {
        return ($this->attributes['expired'] ?? 0) > 0;
    }

    /**
     * 设置计时器
     * @param $timer
     * @return void
     */
    protected function setTimer($timer): void {
        $this->attributes['ticker'] = $timer;
        $this->timer = $timer;
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
    public function log($msg): void {
        Console::log('【Crontab】【' . $this->attributes['name'] . '】' . $msg);
        //保存日志到Redis&日志文件
        $key = strtolower('crontab' . str_replace("\\", "_", $this->attributes['namespace']));
        MasterDB::addLog($key, ['message' => Log::filter($msg)]);
    }

    /**
     * 当前状态
     * @return array
     */
    public function status(): array {
        $task = $this->sync();
        $mode = $task['override']['mode'] ?? $task['mode'];
        if ($mode != Crontab::RUN_MODE_TIMEING && isset($task['interval'])) {
            $task['interval_humanize'] = Date::secondsHumanize($task['override']['interval'] ?? $task['interval']);
        }
        $task['real_status'] = $task['override']['status'] ?? $task['status'];
        return $task;
    }

    /**
     * 任务状态
     * @return array
     */
    public function getList(): array {
        $ids = MasterDB::sMembers(App::id() . '_CRONTABS_') ?: [];
        $list = [];
        if ($ids) {
            foreach ($ids as $id) {
                $key = '-crontabs-' . $id;
                if (!$task = MasterDB::get($key)) {
                    continue;
                }
                $key = strtolower('crontab' . str_replace("\\", "_", $task['namespace']));
                $task['logs'] = MasterDB::getLog($key, date('Y-m-d'), -20);
                $task['real_status'] = $task['override']['status'] ?? $task['status'];
                $mode = $task['override']['mode'] ?? $task['mode'];
                if ($mode != Crontab::RUN_MODE_TIMEING && isset($task['interval'])) {
                    $task['interval_humanize'] = Date::secondsHumanize($task['override']['interval'] ?? $task['interval']);
                }

                $list[] = $task;
            }
        }
        return $list;
    }

    /**
     * 定时执行
     * @param int $id
     * @return void
     */
    protected function timing(int $id): void {
        $times = $this->attributes['override']['times'] ?? $this->attributes['times'];
        $nextRun = $this->getNextRunTime($times);
        $this->log('【' . $this->attributes['name'] . '】下次运行时间(' . Date::secondsHumanize($nextRun['after']) . '后):' . $nextRun['date']);
        $this->processingFinish(time() + $nextRun['after']);
        $timerId = Timer::after($nextRun['after'] * 1000, function () use ($id) {
            if ($this->isAlive($id)) {
                $this->processingStart();
                try {
                    $this->run();
                } catch (Throwable $throwable) {
                    Log::instance()->error("【Crontab】任务执行失败:" . $throwable->getMessage());
                    $this->log("任务执行失败:" . $throwable->getMessage());
                }
                $this->timing($id);
            }
        });
        $this->setTimer($timerId);
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
     * @param $timeout
     * @param $id
     * @return void
     */
    protected function loop($timeout, $id): void {
        if (!$this->isAlive($id)) {
            return;
        }
        $this->processingStart();
        try {
            $this->run();
        } catch (Throwable $throwable) {
            Log::instance()->error("【Crontab】任务执行失败:" . $throwable->getMessage());
            $this->log("任务执行失败:" . $throwable->getMessage());
        }
        $this->processingFinish(time() + $timeout);
        $timerId = Timer::after($timeout * 1000, function () use ($timeout, $id) {
            $this->loop($timeout, $id);
        });
        $this->setTimer($timerId);
    }


    /**
     * 开始本次执行
     * @param int $nextTime
     * @return void
     */
    protected function processingStart(int $nextTime = 0): void {
        $this->attributes['last_run'] = date('m-d H:i:s') . "." . substr(Time::millisecond(), -3);
        $this->attributes['run_count'] += 1;
        $this->attributes['is_busy'] = 1;
        $nextTime and $this->attributes['next_run'] = $nextTime;
        $this->refreshDB();
    }

    /**
     * 完成本次执行
     * @param int $nextTime
     * @return void
     */
    protected function processingFinish(int $nextTime = 0): void {
        $this->attributes['is_busy'] = 0;
        $nextTime and $this->attributes['next_run'] = $nextTime;
        $this->refreshDB();
    }

    /**
     * 是否活着
     * @param int $id
     * @return bool
     */
    public function isAlive(int $id = 1): bool {
        if ($this->isOrphan()) {
            Console::info("【Crontab】#" . $this->attributes['manager_id'] . "已终止运行");
            Timer::clearAll();
            return false;
        } elseif ($id !== $this->id) {
            return false;
        }
        return true;
    }

    /**
     * 判断是否孤儿
     * @param int $managerId
     * @return bool
     */
    public function isOrphan(int $managerId = 0): bool {
        $managerId = $managerId ?: $this->attributes['manager_id'];
        $latestManagerId = Counter::instance()->get('_background_process_id_');
        return $managerId !== $latestManagerId;
    }

    /**
     * 刷新数据库
     * @return mixed
     */
    protected function refreshDB(): mixed {
        return MasterDB::set('-crontabs-' . $this->id(), $this->attributes);
    }

    /**
     * 同步任务
     * @return array
     */
    protected function sync(): array {
        $this->attributes = MasterDB::get('-crontabs-' . $this->id()) ?: $this->attributes;
        return $this->attributes;
    }

    /**
     * 记录运行时间
     * @return void
     */
    protected function updateRunTime(): void {
        $this->attributes['last_run'] = date('m-d H:i:s') . "." . substr(Time::millisecond(), -3);
        $this->attributes['run_count'] += 1;
        $this->refreshDB();
    }

    /**
     * 更新任务
     * @param $key
     * @param $value
     * @return mixed
     */
    protected function updateTask($key, $value): mixed {
        if (!$this->attributes) {
            $this->sync();
        }
        $this->attributes[$key] = $value;
        return $this->refreshDB();
    }

    /**
     * 是否存在任务
     * @return bool
     */
    public static function hasTask(): bool {
        return count(self::$tasks) > 0;
    }

    /**
     * 保存配置文件
     * @param $data
     * @return Result
     */
    public function saveOverrides($data): Result {
        $file = $this->getOverridesConfigFileName();
        if (!$file) {
            return Result::error('创建配置文件夹失败');
        }
        foreach ($data as $key => $val) {
            if (is_numeric($val)) {
                $data[$key] = (int)$val;
            }
        }
        Console::info($data['name'] . "参数已变更:" . JsonHelper::toJson($data));
        $this->attributes['override'] = $data;
        return Result::success(File::write($file, JsonHelper::toJson($data)));
    }

    /**
     * 获取配置文件路径
     * @return false|string
     */
    protected function getOverridesConfigFileName(): bool|string {
        $dir = APP_PATH . '/src/config/crontab';
        if (!file_exists($dir)) {
            try {
                mkdir($dir, 0777, true);
            } catch (Throwable) {
                return false;
            }
        }
        return $dir . '/' . 'CRONTAB_' . md5(str_replace("\\", "_", static::class) . SERVER_NODE_ID) . '.override.json';
        //return $dir . '/' . strtolower(str_replace("\\", "_", static::class)) . '.override.json';
    }

    /**
     * 创建一个任务对象
     * @param $name
     * @return static
     */
    public static function factory($name): static {
        return new $name;
    }

    /**
     * @param $name
     * @return static
     */
    protected function createWorker($name): static {
        return new $name;
    }

    public function getId(): int {
        return $this->id;
    }

    public function getManagerId() {
        return $this->attributes['manager_id'];
    }

    /**
     * 返回ID标识
     * @return string
     */
    public function id(): string {
        return 'CRONTAB_' . md5(str_replace("\\", "_", static::class) . SERVER_NODE_ID);
        //return strtolower(str_replace("\\", "_", static::class) . SERVER_NODE_ID);
    }

    public function run() {

    }
}