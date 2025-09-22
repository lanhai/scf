<?php

namespace Scf\Server\Task;

use Generator;
use JetBrains\PhpStorm\ArrayShape;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Struct;
use Scf\Core\Table\Counter;
use Scf\Helper\JsonHelper;
use Scf\Util\Date;
use Scf\Util\File;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Timer;
use Throwable;

class Crontab extends Struct {
    /**
     * @required true|任务id配置错误
     */
    public string $id;
    /**
     * @required true|任务名称配置错误
     */
    public string $name;
    /**
     * @required true|脚本命名空间错误
     */
    public string $namespace;
    /**
     * @required true|状态配置错误
     * @default int:1
     */
    public int $status;
    /**
     * @required true|运行模式配置错误
     */
    public int $mode;
    /**
     * @default int:60
     */
    public ?int $interval;
    /**
     * @var ?array
     */
    public ?array $times;
    /**
     * @default int:3600
     */
    public ?int $timeout;
    /**
     * @default int:-1
     */
    public ?int $manager_id;
    /**
     * @default int:1
     */
    public ?int $running_version;
    /**
     * @default int:-1
     */
    public ?int $pid;
    /**
     * @default int:-1
     */
    public ?int $cid;
    /**
     * @default int:0
     */
    public ?int $created;
    /**
     * @default int:0
     */
    public ?int $expired;
    /**
     * @default string:''
     */
    public ?string $last_run;
    /**
     * @default int:0
     */
    public ?int $run_count;
    /**
     * @default int:0
     */
    public ?int $next_run;
    /**
     * @default int:0
     */
    public ?int $is_busy;
    /**
     * @default int:-1
     */
    public ?int $timer;
    /**
     * @default int:0
     */
    public ?int $latest_alive;

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
    const RUN_MODE_TIMING = 2;
    /**
     * 兼容旧拼写
     */
    const RUN_MODE_TIMEING = 2; // 兼容旧代码，建议使用 RUN_MODE_TIMING
    /**
     * 严格间隔执行
     */
    const RUN_MODE_INTERVAL = 3;

    /**
     * 当前状态
     * @return array
     */
    public function status(): array {
        $this->sync();
        $task = $this->asArray();
        if ($this->mode != Crontab::RUN_MODE_TIMING && isset($task['interval'])) {
            $task['interval_humanize'] = Date::secondsHumanize($task['interval']);
        }
        $task['real_status'] = $this->status;
        return $task;
    }

    /**
     * 开始任务(多进程模式)
     * @return void
     */
    public function start(): void {
        //内存占用统计
        MemoryMonitor::start('crontab-' . $this->name);
        //迭代检查计时器
        Timer::tick(5000, function () {
            if ($this->manager_id !== Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS)) {
                //Console::warning("【Crontab#" . $this->manager_id . "】[{$this->name}-{$this->namespace}]管理进程已迭代,清除所有定时器");
                MemoryMonitor::stop();
                Timer::clearAll();
                return;
            }
            if ($this->pid <= 0) {
                $this->pid = CrontabManager::getTaskTableById($this->id)['pid'] ?? 0;
                $this->update([
                    'pid' => $this->pid,
                ]);
            }
            $this->sync();
            if ($this->isExpired()) {
                //迭代
                $this->upgrade();
            }
        });
        $this->cid = Coroutine::create(function () {
            $this->startTask();
        });
    }

    /**
     * 执行任务
     * @return void
     */
    protected function startTask(): void {
        $this->latest_alive = time();
        $this->override();
        $this->update($this->asArray());
        if ($this->status == STATUS_OFF) {
            //任务关闭挂起等待状态更新
            $this->log('任务关闭,已挂起等待状态更新');
            Timer::tick(10 * 1000, function ($timerId) {
                if ($this->status == STATUS_ON) {
                    $this->startTask();
                    Timer::clear($timerId);
                }
            });
        } else {
            $version = $this->running_version;
            switch ($this->mode) {
                case self::RUN_MODE_INTERVAL:
                    $this->runIntervalNonReentrant($this->interval, $version);
                    break;
                case self::RUN_MODE_LOOP:
                    $this->loop($this->interval, $version);
                    break;
                case self::RUN_MODE_TIMING:
                    $this->timing($version);
                    break;
                case self::RUN_MODE_TIMEING: // 兼容旧常量
                    $this->timing($version);
                    break;
                default:
                    $this->updateRunTime();
                    try {
                        //单次执行的任务如果是无限循环任务需要在循环逻辑里判断当前任务是否处于激活状态,且在结束循环时清理相关计时器
                        $this->execute();
                        Timer::after(1000 * 2, function () {
                            CrontabManager::updateTaskTable($this->id, ['status' => 2]);
                        });
                    } catch (Throwable $throwable) {
                        Log::instance()->error("【{$this->name}|{$this->namespace}】任务执行失败:" . $throwable->getMessage());
                        $this->log("任务执行失败:" . $throwable->getMessage());
                        Timer::after(1000 * 2, function () use ($throwable) {
                            CrontabManager::updateTaskTable($this->id, ['status' => 0, 'remark' => $throwable->getMessage(), 'error_count' => 1]);
                        });
                    }
                    break;
            }
        }
    }

    /**
     * 间隔执行（非重入实现）
     * @param int $interval
     * @param int $version
     */
    protected function runIntervalNonReentrant(int $interval, int $version): void {
        $this->timer = Timer::tick($interval * 1000, function () use ($interval, $version) {
            if ($this->isAlive($version)) {
                if (!$this->is_busy) {
                    $this->processingStart(time() + $interval);
                    try {
                        $this->execute();
                    } catch (Throwable $throwable) {
                        Log::instance()->error("【Crontab#{$this->running_version}】任务执行失败:" . $throwable->getMessage());
                        $this->log("任务执行失败:" . $throwable->getMessage());
                    }
                    $this->processingFinish(time() + $interval);
                } else {
                    $this->log("上次执行未完成，跳过本次间隔执行");
                }
            }
        });
    }

    /**
     * 判断是否孤儿
     * @return bool
     */
    public function isOrphan(): bool {
        $latestManagerId = Counter::instance()->get(Key::COUNTER_CRONTAB_PROCESS);
        return $this->manager_id !== $latestManagerId;
    }

    /**
     * 是否活着
     * @param int $version
     * @return bool
     */
    public function isAlive(int $version = 1): bool {
        if ($this->isOrphan() || $version !== $this->running_version) {
            $this->log("孤儿进程,已取消执行");
            return false;
        }
        $this->latest_alive = time();
        $this->update([
            'latest_alive' => $this->latest_alive,
        ]);
        return true;
    }

    /**
     * 升级迭代
     * @return void
     */
    protected function upgrade(): void {
        $this->expired = 0;
        $this->running_version++;
        //清除定时器
        $this->timer and Timer::clear($this->timer);
        $this->timer = 0;
        $this->log("运行参数已变更,已升级迭代至#{$this->running_version}:" . JsonHelper::toJson($this->asArray()));
        //重新执行
        $this->startTask();
    }

    /**
     * 立即执行
     */
    public function runRightNow(): bool|int {
        $this->sync();
        return go(function () {
            $this->processingStart();
            try {
                $this->execute();
            } catch (Throwable $throwable) {
                Log::instance()->error("【{$this->name}|{$this->namespace}】任务执行失败:" . $throwable->getMessage());
                $this->log("任务执行失败:" . $throwable->getMessage());
            }
            $this->processingFinish();
        });
    }

    /**
     * 判断当前worker是否过期
     * @return bool
     */
    protected function isExpired(): bool {
        return $this->expired > 0;
    }

    /**
     * 日志
     * @param $msg
     * @return void
     */
    public function log($msg): void {
        $arr = explode("\\", $this->namespace);
        App::isDevEnv() and Console::log('【Crontab】[' . $this->name . '|' . array_pop($arr) . '|' . $this->manager_id . '-' . $this->running_version . ']' . $msg);
        //保存日志到Redis&日志文件
        $taskName = CrontabManager::formatTaskName($this->namespace);
        Log::instance()->crontab(['task' => $taskName, 'message' => Log::filter($msg)]);
    }

    /**
     * 定时执行
     * @param int $version
     * @return void
     */
    protected function timing(int $version): void {
        $times = $this->times;
        $nextRun = $this->getNextRunTime($times);
        $this->log('下次运行时间(' . Date::secondsHumanize($nextRun['after']) . '后):' . $nextRun['date']);
        $this->processingFinish(time() + $nextRun['after']);
        $this->timer = Timer::after($nextRun['after'] * 1000, function () use ($version) {
            if ($this->isAlive($version)) {
                $this->processingStart();
                try {
                    $this->execute();
                } catch (Throwable $throwable) {
                    Log::instance()->error("【{$this->name}|{$this->namespace}】任务执行失败:" . $throwable->getMessage());
                    $this->log("任务执行失败:" . $throwable->getMessage());
                }
                $this->timing($version);
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
        $today = date('Y-m-d');
        $timestamps = [];
        foreach ($times as $time) {
            // 支持 HH:MM(:SS) 或完整时间
            if (preg_match('/^\d{2}:\d{2}(:\d{2})?$/', $time)) {
                $full = $today . ' ' . $time;
                $ts = strtotime($full);
            } else {
                $ts = strtotime($time);
            }
            $timestamps[] = $ts;
        }
        asort($timestamps);
        $matched = null;
        foreach ($timestamps as $timestamp) {
            if ($timestamp > $now) {
                $matched = $timestamp;
                break;
            }
        }
        if (is_null($matched)) {
            // 明天的第一个
            $first = reset($times);
            if (preg_match('/^\d{2}:\d{2}(:\d{2})?$/', $first)) {
                $matched = strtotime($today . ' ' . $first . ' +1 day');
            } else {
                $matched = strtotime($first . ' +1 day');
            }
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

    private function loop($timeout, $id): void {
        if (!$this->isAlive($id)) {
            return;
        }
        $this->cancelFlag = false;
        $this->processingStart();
        $channel = new Coroutine\Channel(1);
        Coroutine::create(function () use ($channel, $timeout) {
            try {
                //$this->runWithCooperativeTimeout($this->timeout);
                $this->execute();
                $channel->push('success');
            } catch (Throwable $throwable) {
                Log::instance()->error("【{$this->name}|{$this->namespace}】任务执行失败:" . $throwable->getMessage());
                $this->log("任务执行失败:" . $throwable->getMessage());
                $channel->push('fail');
            }
        });
        $result = $channel->pop($this->timeout);
        if (!$result) {
            $this->requestCancel();
            $this->log("任务执行超时取消");
            //Console::warning("【Crontab#{$this->running_version}】{$this->name} 协作超时取消:" . get_called_class());
        }
        $this->processingFinish(time() + $timeout);
        $this->timer = Timer::after($timeout * 1000, function () use ($timeout, $id) {
            $this->loop($timeout, $id);
        });
    }

    /**
     * 协作式超时取消的 run 包装
     * @param int $timeout
     */
    private function runWithCooperativeTimeout(int $timeout): void {
        $begin = time();
        $this->cancelFlag = false;
        // 使用生成器方式执行，并用 foreach 正确驱动生成器（避免 valid()/next() 手动驱动的边界问题）
        $intervalMs = max(1, (int)($this->interval * 1000)); // 将 interval 秒转毫秒
        $gen = $this->runGenerator($intervalMs);
        if ($gen instanceof Generator) {
            foreach ($gen as $_) {
                if ($this->cancelFlag || (time() - $begin) >= $timeout) {
                    break;
                }
            }
        } else {
            // fallback: 非生成器直接执行
            $this->execute();
        }
    }

    /**
     * 用生成器方式调度 run() 方法
     * 可替代 runWithCooperativeTimeout
     */
    protected function runGenerator(int $intervalMs = 1000): ?Generator {
        $intervalUs = max(0, $intervalMs) * 1000;
        while (true) {
            // 支持外部协作式取消
            if ($this->cancelFlag) {
                break;
            }
            try {
                // 调用子类 run()/execute()
                $result = $this->execute();
                // 把结果交出去，外部 foreach($gen as $val) 可取
                yield $result;
                // 模拟定时器的等待（在 next() 之后继续执行）
                if ($intervalUs > 0) {
                    usleep($intervalUs);
                }
            } catch (\Throwable $e) {
                $this->handleError($e);
                // 不中断生成器，继续下一次循环
                yield null;
            }
        }
    }

    /**
     * 可选：异常处理（父类提供默认实现）
     */
    protected function handleError(\Throwable $e): void {
        // 这里可以写日志或者上报
        echo "Task error: " . $e->getMessage() . PHP_EOL;
    }

    /**
     * 协作式超时取消的循环执行
     * @param $timeout
     * @param $id
     * @return void
     */
    protected bool $cancelFlag = false;

    public function requestCancel(): void {
        $this->cancelFlag = true;
    }

    /**
     * 开始本次执行
     * @param int $nextTime
     * @return void
     */
    protected function processingStart(int $nextTime = 0): void {
        $lastRun = date('Y-m-d H:i:s') . "." . substr(\Scf\Util\Time::millisecond(), -3);
        $this->last_run = $lastRun;
        $this->run_count += 1;
        $this->is_busy = 1;
        $nextTime and $this->next_run = $nextTime;
        $this->update([
            'last_run' => $lastRun,
            'run_count' => $this->run_count,
            'is_busy' => 1,
            'next_run' => $nextTime ?: $this->next_run
        ]);
    }

    /**
     * 完成本次执行
     * @param int $nextTime
     * @return void
     */
    protected function processingFinish(int $nextTime = 0): void {
        $this->is_busy = 0;
        $nextTime and $this->next_run = $nextTime;
        $this->update([
            'is_busy' => 0,
            'next_run' => $nextTime ?: $this->next_run
        ]);
    }

    /**
     * 更新参数
     * @param array $datas
     * @return bool|array
     */
    public function update(array $datas): bool|array {
        if ($this->isOrphan()) {
            return false;
        }
        return CrontabManager::updateTaskTable($this->id, $datas);
    }

    /**
     * 记录运行时间
     * @return void
     */
    protected function updateRunTime(): void {
        $this->last_run = date('Y-m-d H:i:s') . "." . substr(\Scf\Util\Time::millisecond(), -3);
        $this->run_count += 1;
        $this->update([
            'last_run' => $this->last_run,
            'run_count' => $this->run_count,
        ]);
    }

    /**
     * 返回ID标识
     * @return string
     */
    public function id(): string {
        return $this->id;
    }


    public function execute(): int {
        /** @var static $app */
        $app = new $this->namespace;
        // 将当前对象的所有属性同步到子类实例
        $props = $this->asArray();//get_object_vars($this);
        foreach ($props as $prop => $val) {
            if (property_exists($app, $prop)) {
                $app->$prop = $val;
            }
        }
        $app->run();
        unset($app);
        return time();
    }

    /**
     * 同步任务状态
     * @return array
     */
    protected function sync(): array {
        $attributes = CrontabManager::getTaskTableById($this->id);
        if ($attributes) {
            $this->mode = $attributes['mode'] ?? $this->mode;
            $this->interval = $attributes['interval'] ?? $this->interval;
            $this->status = $attributes['status'] ?? $this->status;
            $this->times = $attributes['times'] ?? $this->times;
            $this->expired = $attributes['expired'] ?? $this->expired;
            return $attributes;
        }
        return [];
    }

    /**
     * 加载覆盖配置
     * @return void
     */
    protected function override(): void {
        $overrideConfig = CrontabManager::overridesConfigFile($this->namespace);
        if ($overrideConfig && file_exists($overrideConfig)) {
            $config = File::readJson($overrideConfig);
            $this->mode = $config['mode'] ?? $this->mode;
            $this->interval = $config['interval'] ?? $this->interval;
            $this->status = $config['status'] ?? $this->status;
            $this->times = $config['times'] ?? $this->times;
        }
    }

    public function getId(): int {
        return $this->id;
    }

    public function run() {
    }
}