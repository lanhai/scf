<?php

namespace Scf\Database\Statistics;

use Co\Channel;
use JetBrains\PhpStorm\ArrayShape;
use Scf\Cache\Redis;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Core\Table\Runtime;
use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\Singleton;
use Scf\Database\Dao;
use Scf\Database\Pdo;
use Scf\Database\Statistics\Dao\StatisticsDailyDAO;
use Scf\Database\Statistics\Dao\StatisticsTotalDAO;
use Scf\Database\Statistics\Dao\StatisticsTotalUvDAO;
use Scf\Database\Tools\Calculator;
use Scf\Database\Tools\Expr;
use Scf\Database\Tools\WhereBuilder;
use Scf\Mode\Web\Request;
use Scf\Root;
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\Math;
use Swoole\Coroutine;
use Swoole\Timer;
use Symfony\Component\Yaml\Yaml;
use Throwable;

class StatisticModel {
    use ComponentTrait, Singleton;

    protected string $_dbName = 'statistics';
    /**
     * 访问统计表
     */
    protected string $_table_daily = 'daily';
    protected string $_table_record = 'statistics_record';

    public function updateDB(): void {
        if (!isset(Config::get('database')['mysql']['statistics']) || !isset(Config::get('database')['mysql']['history'])) {
            Console::error('数据库配置statistics以及history不能为空');
        } else {
            $configDir = Root::dir() . '/Database/Statistics/Yml';
            $files = Dir::scan($configDir);
            foreach ($files as $file) {
                $table = Yaml::parseFile($file);
                $arr = explode("/", $table['dao']);
                $cls = implode('\\', $arr);
                if (!class_exists($cls)) {
                    Console::error($cls . " not exist");
                } else {
                    /** @var Dao $cls */
                    $dao = $cls::factory();
                    $dao->updateTable($table);
                }
            }
        }
    }

    /**
     * 执行统计操作
     * @param string $scene
     * @param int|string $dataId
     * @param int $uid
     * @param float $incValue
     * @param int $mchID
     * @return bool
     * @throws Throwable
     */
    public function excute(string $scene, int|string $dataId = 0, int $uid = 0, float $incValue = 0, int $mchID = 1): bool {
        try {
            $scene = strtolower($scene);
            $today = Date::today();
            $hour = date('H', time());
            //查询是否记录当前用户当天的记录
            $userKey = md5($mchID . $scene . $dataId . $uid);
            $key = md5($userKey . Date::today());
            $recordTable = $this->_table_record . '_' . $today;
            $tablePrefix = Pdo::master('history')->getConfig('prefix');
            $completeTable = $tablePrefix . $recordTable;
            $existUserRecord = true;
            $requestKey = $completeTable . '_' . $key;
            if (!$userRecordId = Redis::pool()->get($requestKey)) {
                //防并发
                if (!Redis::pool()->lock($requestKey, 10)) {
                    return false;
                }
                $sql = "SHOW TABLES LIKE '{$completeTable}'";
                $tableToday = Runtime::instance()->get('statistics_record_day');
                if (!$tableToday || $tableToday != Date::today()) {
                    Runtime::instance()->set('statistics_record_day', Date::today());
                    $existTable = Pdo::master('history')->getDatabase()->exec($sql)->get();
                    if (!$existTable && !$this->create_record_table()) {
                        Log::instance()->error('创建统计表失败:' . $completeTable);
                        return false;
                    }
                }
                $existUserRecord = Pdo::master('history')->getDatabase()->table($recordTable)->select('id')->where('search_key=?', $key)->first();
                if (!$existUserRecord) {
                    $userRecordId = Pdo::master('history')->getDatabase()->insert($recordTable, [
                        'mch_id' => $mchID,
                        'scene' => $scene,
                        'data_id' => $dataId,
                        'usr_id' => $uid,
                        'guest_id' => $guestId ?? '',
                        'ip' => Request::server('remote_addr'),
                        'trigger_time' => time(),
                        'trigger_times' => 1,
                        'updated' => time(),
                        'search_key' => $key
                    ])->lastInsertId();
                    $existUserRecord = false;
                } else {
                    $userRecordId = $existUserRecord['id'];
                }
                Redis::pool()->set($requestKey, $userRecordId, 120);
            }
            if ($existUserRecord) {
                Pdo::master('history')->getDatabase()->table($recordTable)->where('id=?', $userRecordId)->updates(['trigger_times' => new Expr('trigger_times + ?', 1), 'updated' => time()]);
            }
            //查询当前时段访问记录数据是否存在
            $dailySearchKey = md5($mchID . $scene . $dataId . $today . $hour);
            $hasDailyRecord = true;
            $dailyKey = '_STATISTICS_' . md5($this->_table_daily . '_' . $dailySearchKey);
            $dailyId = Redis::pool()->get($dailyKey);
            if ($dailyId === false || StatisticsDailyDAO::unique($dailyId)->ar()->notExist()) {
                $daily = StatisticsDailyDAO::select()->where(['search_key' => $dailySearchKey])->ar();
                if ($daily->notExist()) {
                    if (Runtime::instance()->get($dailyKey)) {
                        return false;
                    }
                    //防并发写入
                    Runtime::instance()->set($dailyKey, time());
                    Timer::after(10000, function () use ($dailyKey) {
                        Runtime::instance()->delete($dailyKey);
                    });
                    $daily = StatisticsDailyDAO::factory();
                    $daily->mch_id = $mchID;
                    $daily->scene = $scene;
                    $daily->data_id = $dataId;
                    $daily->day = $today;
                    $daily->hour = $hour;
                    $daily->uv = !$existUserRecord ? 1 : 0;
                    $daily->pv = 1;
                    $daily->value = $incValue;
                    $daily->updated = time();
                    $daily->search_key = $dailySearchKey;
                    if (!$daily->save()) {
                        return false;
                        //Log::instance()->error($daily->getError());
                    }
                    $hasDailyRecord = false;

                }
                $dailyId = $daily->id;
                Redis::pool()->set($dailyKey, $dailyId, 3600);
            }
            if ($hasDailyRecord && $dailyId) {
                $daily = StatisticsDailyDAO::unique($dailyId)->ar();
                !$existUserRecord and $daily->uv = Calculator::increase();
                $daily->pv = Calculator::increase();
                $daily->value = Math::add($daily->value, $incValue, 2);
                $daily->updated = time();
                $daily->save();
            }
            //更新总的统计
            $total = StatisticsTotalDAO::select()->where([
                'mch_id' => $mchID,
                'scene' => $scene,
                'data_id' => $dataId,
            ])->ar();
            if (!$totalUv = StatisticsTotalUvDAO::select()->where(['user_key' => $userKey])->count()) {
                if (Runtime::instance()->get('_STATISTICS_TOTAL_' . $userKey)) {
                    return false;
                }
                //防并发写入
                Runtime::instance()->set('_STATISTICS_TOTAL_' . $userKey, time());
                Timer::after(10000, function () use ($userKey) {
                    Runtime::instance()->delete('_STATISTICS_TOTAL_' . $userKey);
                });
                $totalUvAr = StatisticsTotalUvDAO::factory();
                $totalUvAr->mch_id = $mchID;
                $totalUvAr->scene = $scene;
                $totalUvAr->data_id = $dataId;
                $totalUvAr->user_key = $userKey;
                $totalUvAr->created = time();
                $totalUvAr->save();
            }
            if ($total->notExist()) {
                $total = StatisticsTotalDAO::factory();
                $total->mch_id = $mchID;
                $total->scene = $scene;
                $total->data_id = $dataId;
                $total->uv = 1;
                $total->pv = 1;
                $total->value = $incValue;
            } else {
                !$totalUv and $total->uv = Calculator::increase();
                $total->pv = Calculator::increase();
                $total->value = Math::add($total->value, $incValue, 2);
            }
            $total->updated = time();
            return $total->save();
        } catch (\PDOException $exception) {
            Log::instance()->error("埋点统计记录失败:" . $exception->getMessage() . ";params:" . implode(',', func_get_args()));
            return false;
        }
    }

    /**
     * 执行队列里的统计任务
     * @return int
     * @throws Throwable
     */
    public function runQueue(): int {
        $count = $this->countQueue();
        $finished = 0;
        if ($count) {
            for ($i = 0; $i < $count; $i++) {
                $params = Redis::pool()->rPop('_STATISTIC_QUEUE_');
                if ($params) {
                    if (!$this->excute(...$params)) {
                        Redis::pool()->rPush('_STATISTIC_QUEUE_', $params);
                    } else {
                        $finished++;
                    }
                }
            }
        }
        (App::isDevEnv() && $finished > 0) and Console::success('本次执行完成统计:' . $finished);
        return $finished;
    }

    /**
     * @return int
     */
    public function countQueue(): int {
        try {
            return Redis::pool()->lLength('_STATISTIC_QUEUE_');
        } catch (Throwable $err) {
            Console::warning('查询统计队列失败:' . $err->getMessage());
            return 0;
        }

    }

    /**
     * 埋点记录
     * @param string $scene 场景标识
     * @param int|string $dataId
     * @param int $uid
     * @param float $incValue
     * @param int $mchID
     * @return bool|int
     */
    public function record(string $scene, int|string $dataId = 0, int $uid = 0, float $incValue = 0, int $mchID = 1): bool|int {
        return Redis::pool()->lPush('_STATISTIC_QUEUE_', compact('scene', 'dataId', 'uid', 'incValue', 'mchID'));
    }

    /**
     *累计数量统计数据(实时计算)
     * @param array $scenes 模块配置
     * @param int $day 时间段 1:今天按小时统计 2:昨天按小时统计 -1:自定义时间段 N:过去N天到今天按天统计
     * @param array $searchParams 自定义时间段参数值 start:开始时间 end:结束时间 格式 Y-m-d
     * @return array
     */
    public function sum_data_rtc(array $scenes, int $day = 1, array $searchParams = []): array {
        switch ($day) {
            case 1://今天
                $day = Date::today('timestamp');
                foreach ($scenes as &$m) {
                    if ($m['scene'] == 'compute') {
                        continue;
                    }
                    $m['days'] = 1;
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? '';
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($day['date'])->init()->sum($sum)->getData();
                        $m['result'] = $result[0]['sum'];
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($day['date'])->init()->count($m['primary'] ?? 'id')->getData();
                        $m['result'] = $result[0]['count'];
                    }
                }
                break;
            case 2://昨天
                $day = Date::yesterday('timestamp');
                foreach ($scenes as &$m) {
                    if ($m['scene'] == 'compute') {
                        continue;
                    }
                    $m['days'] = 1;
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? '';
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($day['date'])->init()->sum($sum)->getData();
                        $m['result'] = $result[0]['sum'];
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($day['date'])->init()->count($m['primary'] ?? 'id')->getData();
                        $m['result'] = $result[0]['count'];
                    }
                }
                break;
            case -1://自定义
                $start = !empty($searchParams['start']) ? $searchParams['start'] : Date::yesterday('Y-m-d');
                $end = !empty($searchParams['end']) ? $searchParams['end'] : Date::today('Y-m-d');
                $days = Date::calendar($start, $end);
                foreach ($scenes as &$m) {
                    if ($m['scene'] == 'compute') {
                        continue;
                    }
                    $m['result'] = 0;
                    $m['days'] = count($days);
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? '';
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($start)->end($end)->init()->sum($sum)->getData();
                        foreach ($result as $d) {
                            $m['result'] += $d['sum'];
                        }
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($start)->end($end)->init()->count($m['primary'] ?? 'id')->getData();
                        foreach ($result as $d) {
                            $m['result'] += $d['count'];
                        }
                    }
                }
                break;
            default://过去多少天之内的数据
                foreach ($scenes as &$m) {
                    if ($m['scene'] == 'compute') {
                        continue;
                    }
                    $m['result'] = 0;
                    $m['days'] = $day;
                    $date = date("Y-m-d", strtotime("-" . $day . " days"));
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? '';
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($date)->init()->sum($sum)->getData();
                        foreach ($result as $d) {
                            $m['result'] += $d['sum'];
                        }
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($date)->init()->count($m['primary'] ?? 'id')->getData();
                        foreach ($result as $d) {
                            $m['result'] += $d['count'];
                        }
                    }
                }
                break;
        }
        //结果计算
        foreach ($scenes as &$m) {
            if ($m['scene'] == 'compute') {
                $formula = $m['formula'];
                $computeResult = 0;
                foreach ($formula as $fk => &$f) {
                    switch ($f['type']) {
                        case 'result_var'://scene统计结果
                            $f['result'] = $this->find_scene_result($f['source'], $scenes);
                            break;
                        case 'result_compute'://scene统计结果加减乘除
                            if (str_contains($f['source'], '+')) {
                                $arr = explode('+', $f['source']);
                                $symbol = '+';
                            } elseif (str_contains($f['source'], '-')) {
                                $arr = explode('-', $f['source']);
                                $symbol = '-';
                            } elseif (str_contains($f['source'], '*')) {
                                $arr = explode('*', $f['source']);
                                $symbol = '*';
                            } elseif (str_contains($f['source'], '/')) {
                                $arr = explode('/', $f['source']);
                                $symbol = '/';
                            } else {
                                $f['result'] = 0;
                                break;
                            }
                            $num1 = $arr[0];
                            $num2 = $arr[1];
                            if (!is_numeric($num1)) {
                                $num1 = $this->find_scene_result($num1, $scenes);
                            }
                            if (!is_numeric($num2)) {
                                $num2 = $this->find_scene_result($num2, $scenes);
                            }
                            switch ($symbol) {
                                case '+':
                                    $f['result'] = $num1 + $num2;
                                    break;
                                case '-':
                                    $f['result'] = $num1 - $num2;
                                    break;
                                case '*':
                                    $f['result'] = $num1 * $num2;
                                    break;
                                default:
                                    if ($num1 && $num2) {
                                        $f['result'] = $num1 / $num2;
                                    } else {
                                        $f['result'] = 0;
                                    }
                                    break;
                            }
                            break;
                        case 'result_sum'://scene统计结果累计
                            $f['result'] = 0;
                            foreach ($f['source'] as $s) {
                                $f['result'] += $this->find_scene_result($s, $scenes);
                            }
                            break;
                        case 'compute':
                            if (str_contains($f['source'], '+')) {
                                $arr = explode('+', $f['source']);
                                $symbol = '+';
                            } elseif (str_contains($f['source'], '-')) {
                                $arr = explode('-', $f['source']);
                                $symbol = '-';
                            } elseif (str_contains($f['source'], '*')) {
                                $arr = explode('*', $f['source']);
                                $symbol = '*';
                            } elseif (str_contains($f['source'], '/')) {
                                $arr = explode('/', $f['source']);
                                $symbol = '/';
                            } else {
                                $f['result'] = 0;
                                break;
                            }
                            $num1 = $arr[0];
                            $num2 = $arr[1];
                            if (!is_numeric($num1)) {
                                $num1 = $this->find_compute_result($num1, $formula);
                            }
                            if (!is_numeric($num2)) {
                                $num2 = $this->find_compute_result($num2, $formula);
                            }
                            switch ($symbol) {
                                case '+':
                                    $f['result'] = $num1 + $num2;
                                    break;
                                case '-':
                                    $f['result'] = $num1 - $num2;
                                    break;
                                case '*':
                                    $f['result'] = $num1 * $num2;
                                    break;
                                default:
                                    if ($num1 && $num2) {
                                        $f['result'] = $num1 / $num2;
                                    } else {
                                        $f['result'] = 0;
                                    }

                                    break;
                            }
                            break;
                    }
                    if ($fk == count($formula) - 1) {
                        $computeResult = $f['result'];
                    }
                }
                $m['result'] = $computeResult;
            }
        }
        return $scenes;
    }

    /**
     * 计算获取走势图数据(实时计算)
     * @param array $scenes 模块配置
     * @param int $day 时间段 1:今天按小时统计 2:昨天按小时统计 -1:自定义时间段 N:过去N天到今天按天统计
     * @param array $searchParams 自定义时间段参数值 start:开始时间 end:结束时间 格式 Y-m-d
     * @return array
     */
    #[ArrayShape(['title' => "string", 'types' => "array", 'axis_datas' => "array", 'series' => "array"])]
    public function chart_data_rtc(array $scenes, int $day = 1, array $searchParams = []): array {
        $axis = [];
        switch ($day) {
            case 1://今天
                $day = Date::today('timestamp');
                foreach ($scenes as $k => &$m) {
                    $scenes[$k]['total'] = 0;
                    $scenes[$k]['series'] = [];
                    $thisHour = date('H', time());
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? false;
                    if ($m['scene'] == 'compute') {
                        goto compute;
                    }
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('hour')->start($day['date'] . ' 00:00:00')->init()->sum($sum)->getData();
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('hour')->start($day['date'] . ' 00:00:00')->init()->count($m['primary'] ?? 'id')->getData();
                    }
                    compute:
                    for ($i = 0; $i <= $thisHour; $i++) {
                        if (!in_array($i . "点", $axis)) {
                            $axis[] = $i . "点";
                        }
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['series'][$i];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['series'][$i];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['series'][$i];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['series'][$i];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }
                            $m['series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            continue;
                        }
                        $scenes[$k]['series'][] = $sum ? $result[$i]['sum'] ?? 0 : $result[$i]['count'] ?? 0;
                        $scenes[$k]['total'] += $sum ? $result[$i]['sum'] ?? 0 : $result[$i]['count'] ?? 0;
                    }
                }
                break;
            case 2://昨天
                $day = Date::yesterday('timestamp');
                foreach ($scenes as &$m) {
                    $m['total'] = 0;
                    $m['series'] = [];
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? false;
                    if ($m['scene'] == 'compute') {
                        goto computeDay2;
                    }
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('hour')->start($day['date'] . ' 00:00:00')->init()->sum($sum)->getData();
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('hour')->start($day['date'] . ' 00:00:00')->init()->count($m['primary'] ?? 'id')->getData();
                    }
                    computeDay2:
                    for ($i = 0; $i <= 23; $i++) {
                        if (!in_array($i . "点", $axis)) {
                            $axis[] = $i . "点";
                        }
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['series'][$i];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['series'][$i];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['series'][$i];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['series'][$i];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }
                            $m['series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            continue;
                        }
                        $m['series'][] = $sum ? $result[$i]['sum'] ?? 0 : $result[$i]['count'] ?? 0;
                        $m['total'] += $sum ? $result[$i]['sum'] ?? 0 : $result[$i]['count'] ?? 0;
                    }
                }
                break;
            case -1://自定义
                $start = !empty($searchParams['start']) ? $searchParams['start'] : Date::yesterday('Y-m-d');
                $end = !empty($searchParams['end']) ? $searchParams['end'] : Date::today('Y-m-d');
                $days = Date::calendar($start, $end);
                $cycle = !empty($searchParams['cycle']) ? $searchParams['cycle'] : 'day';//数据展示周期
                foreach ($scenes as &$m) {
                    $weeks = [];
                    if ($cycle == 'week') {
                        $weeksTotal = ceil(count($days) / 7);//一共几周
                        for ($i = 1; $i <= $weeksTotal; $i++) {
                            $weeks[] = ['name' => '第' . $i . '周', 'result' => 0];
                        }
                    }
                    $months = [];
                    if ($cycle == 'month') {
                        foreach ($days as $item) {
                            $name = date('Y年m月', strtotime($item['day']));
                            if (!$this->check_month_exist($name, $months)) {
                                $months[] = ['name' => $name, 'result' => 0];
                            }
                        }
                    }
                    $m['total'] = 0;
                    $m['series'] = [];
                    $m['day_series'] = [];
                    $j = 0;
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? false;
                    if ($m['scene'] == 'compute') {
                        goto computeDay3;
                    }
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($start)->end($end)->init()->sum($sum)->getData();
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($start)->end($end)->init()->count($m['primary'] ?? 'id')->getData();
                    }
                    computeDay3:
                    foreach ($days as $dn => &$item) {
                        $date = $item['day'];
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['day_series'][$j];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['day_series'][$j];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['day_series'][$j];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['day_series'][$j];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }

                            $m['day_series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            $item['result'] = $computeResult;
                            $j++;
                            if ($cycle == 'week') {
                                $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                            } elseif ($cycle == 'month') {
                                foreach ($months as $mk => $month) {
                                    if (date('Y年m月', strtotime($date)) == $month['name']) {
                                        $months[$mk]['result'] += $item['result'];
                                    }
                                }
                            } else {
                                if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                    $axis[] = date("m月d日", strtotime($date));
                                }
                                $m['series'][] = $item['result'];
                            }
                            continue;
                        }
                        $m['day_series'][] = $sum ? $result[$dn]['sum'] ?? 0 : $result[$dn]['count'] ?? 0;
                        $m['total'] += $sum ? $result[$dn]['sum'] ?? 0 : $result[$dn]['count'] ?? 0;
                        $item['result'] = $sum ? $result[$dn]['sum'] ?? 0 : $result[$dn]['count'] ?? 0;
                        $j++;
                        if ($cycle == 'week') {
                            $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                        } elseif ($cycle == 'month') {
                            foreach ($months as $mk => $month) {
                                if (date('Y年m月', strtotime($date)) == $month['name']) {
                                    $months[$mk]['result'] += $item['result'];
                                }
                            }
                        } else {
                            if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                $axis[] = date("m月d日", strtotime($date));
                            }
                            $m['series'][] = $item['result'];
                        }
                    }
                    if ($cycle == 'week') {
                        foreach ($weeks as $week) {
                            if (!in_array($week['name'], $axis)) {
                                $axis[] = $week['name'];
                            }
                            $m['series'][] = $week['result'];
                        }
                    } elseif ($cycle == 'month') {
                        foreach ($months as $month) {
                            if (!in_array($month['name'], $axis)) {
                                $axis[] = $month['name'];
                            }
                            $m['series'][] = $month['result'];
                        }
                    }
                }
                break;
            default:
                $start = Date::pastDay($day, 'Y-m-d');
                $days = Date::calendar($start, Date::today('Y-m-d'));
                $cycle = !empty($searchParams['cycle']) ? $searchParams['cycle'] : 'day';//数据展示周期
                foreach ($scenes as &$m) {
                    $weeks = [];
                    if ($cycle == 'week') {
                        $weeksTotal = ceil(count($days) / 7);//一共几周
                        for ($i = 1; $i <= $weeksTotal; $i++) {
                            $weeks[] = ['name' => '第' . $i . '周', 'result' => 0];
                        }
                    }
                    $months = [];
                    if ($cycle == 'month') {
                        foreach ($days as $item) {
                            $name = date('Y年m月', strtotime($item['day']));
                            if (!$this->check_month_exist($name, $months)) {
                                $months[] = ['name' => $name, 'result' => 0];
                            }
                        }
                    }
                    $m['total'] = 0;
                    $m['series'] = [];
                    $m['day_series'] = [];
                    $j = 0;
                    if (!empty($m['conditions'])) {
                        $condition = [];
                        foreach ($m['conditions'] as $item) {
                            $condition[$item['key']] = $item['value'];
                        }
                    } else {
                        $condition = $m['condition'] ?? [];
                    }
                    $sum = $m['sum'] ?? false;
                    if ($m['scene'] == 'compute') {
                        goto computeDefault;
                    }
                    if ($sum) {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($start)->init()->sum($sum)->getData();
                    } else {
                        $result = StatisticsComponent::factory()->db($m['db'])->table($m['table'])->where($condition)->key($m['prop'])->type('day')->start($start)->init()->count($m['primary'] ?? 'id')->getData();
                    }
                    computeDefault:
                    foreach ($days as $dn => &$item) {
                        $date = $item['day'];
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['day_series'][$j];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['day_series'][$j];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['day_series'][$j];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['day_series'][$j];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }

                            $m['day_series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            $item['result'] = $computeResult;
                            $j++;
                            if ($cycle == 'week') {
                                $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                            } elseif ($cycle == 'month') {
                                foreach ($months as $mk => $month) {
                                    if (date('Y年m月', strtotime($date)) == $month['name']) {
                                        $months[$mk]['result'] += $item['result'];
                                    }
                                }
                            } else {
                                if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                    $axis[] = date("m月d日", strtotime($date));
                                }
                                $m['series'][] = $item['result'];
                            }
                            continue;
                        }
                        $m['day_series'][] = $sum ? $result[$dn]['sum'] ?? 0 : $result[$dn]['count'] ?? 0;
                        $m['total'] += $sum ? $result[$dn]['sum'] ?? 0 : $result[$dn]['count'] ?? 0;
                        $item['result'] = $sum ? $result[$dn]['sum'] ?? 0 : $result[$dn]['count'] ?? 0;
                        $j++;
                        if ($cycle == 'week') {
                            $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                        } elseif ($cycle == 'month') {
                            foreach ($months as $mk => $month) {
                                if (date('Y年m月', strtotime($date)) == $month['name']) {
                                    $months[$mk]['result'] += $item['result'];
                                }
                            }
                        } else {
                            if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                $axis[] = date("m月d日", strtotime($date));
                            }
                            $m['series'][] = $item['result'];
                        }
                    }
                    if ($cycle == 'week') {
                        foreach ($weeks as $week) {
                            if (!in_array($week['name'], $axis)) {
                                $axis[] = $week['name'];
                            }
                            $m['series'][] = $week['result'];
                        }
                    } elseif ($cycle == 'month') {
                        foreach ($months as $month) {
                            if (!in_array($month['name'], $axis)) {
                                $axis[] = $month['name'];
                            }
                            $m['series'][] = $month['result'];
                        }
                    }
                }
                break;
        }
        $title = "";
        $types = [];
        $series = [];
        foreach ($scenes as $v) {
            $types[] = $v['name'];
            $title .= $v['name'] . ":" . $this->_numberFormat($v['total']) . " ";
            $series[] = [
                'name' => $v['name'],
                'data' => $v['series']
            ];
        }
        return [
            'title' => $title,
            'types' => $types,
            'axis_datas' => $axis,
            'series' => $series
        ];
    }

    /**
     *累计数量统计数据
     * @param array $scenes 模块配置 mch_id:商户ID scene:统计模块标识 item_id:业务数据id type:需要统计的类型 uv/pv/value
     * @param int $day 时间段 1:今天按小时统计 2:昨天按小时统计 -1:自定义时间段 N:过去N天到今天按天统计
     * @param array $searchParams 自定义时间段参数值 start:开始时间 end:结束时间 格式 Y-m-d
     * @return array
     */
    public function sum_data(array $scenes, int $day = 1, array $searchParams = []): array {
        switch ($day) {
            case 1://今天
                foreach ($scenes as &$m) {
                    if ($m['scene'] == 'compute') {
                        continue;
                    }
                    $where = WhereBuilder::create(['scene' => $m['scene'], 'day' => Date::today()]);
                    if (!empty($m['mch_id'])) {
                        $where->and(['mch_id' => $m['mch_id']]);
                    }
                    if (!empty($m['data_id'])) {
                        $where->and(['data_id' => $m['data_id']]);
                    }
                    $m['days'] = 1;
                    try {
                        $build = $where->build();
                        $m['result'] = Pdo::slave('statistics')->getDatabase()->table($this->_table_daily)->select()->where($build['sql'], ...$build['match'])->sum($m['type'] ?? 'pv');
                    } catch (Throwable) {
                        $m['result'] = 0;
                    }

                }
                break;
            case 2://昨天
                foreach ($scenes as &$m) {
                    if ($m['scene'] == 'compute') {
                        continue;
                    }
                    $where = WhereBuilder::create(['scene' => $m['scene'], 'day' => Date::yesterday()]);
                    if (!empty($m['mch_id'])) {
                        $where->and(['mch_id' => $m['mch_id']]);
                    }
                    if (!empty($m['data_id'])) {
                        $where->and(['data_id' => $m['data_id']]);
                    }
                    $type = !empty($m['type']) ? $m['type'] : 'pv';
                    $m['days'] = 1;
                    $cacheKey = $this->_table_daily . '_sum_' . $type . md5($where->getWhereSql());
                    $cacheData = Redis::pool()->get($cacheKey);
                    if ($cacheData === false) {
                        try {
                            $build = $where->build();
                            $cacheData = Pdo::slave('statistics')->getDatabase()->table($this->_table_daily)->select()->where($build['sql'], ...$build['match'])->sum($type);
                        } catch (Throwable) {
                            $cacheData = 0;
                        }
                        Redis::pool()->set($cacheKey, $cacheData, 600);
                    }
                    $m['result'] = $cacheData;
                }
                break;
            case -1://自定义
                $start = !empty($searchParams['start']) ? $searchParams['start'] : Date::yesterday('Y-m-d');
                $end = !empty($searchParams['end']) ? $searchParams['end'] : Date::today('Y-m-d');
                $days = Date::calendar($start, $end);
                $sceneChannel = new Channel(count($scenes));
                foreach ($scenes as &$m) {
                    Coroutine::create(function () use (&$m, $sceneChannel, $days) {
                        if ($m['scene'] == 'compute') {
                            $sceneChannel->push(0);
                            return;
                        }
                        $m['result'] = 0;
                        $m['days'] = count($days);
                        $dayChannel = new Channel(count($days));
                        foreach ($days as $item) {
                            Coroutine::create(function () use ($m, $dayChannel, $item) {
                                $date = $item['day'];
                                $where = WhereBuilder::create(['scene' => $m['scene'], 'day' => $date]);
                                if (!empty($m['mch_id'])) {
                                    $where->and(['mch_id' => $m['mch_id']]);
                                }
                                if (!empty($m['data_id'])) {
                                    $where->and(['data_id' => $m['data_id']]);
                                }
                                $type = !empty($m['type']) ? $m['type'] : 'pv';
                                $cacheKey = $this->_table_daily . '_sum_' . $type . md5($where->getWhereSql());
                                $cacheData = Redis::pool()->get($cacheKey);
                                if ($cacheData === false || $date == Date::today()) {
                                    try {
                                        $build = $where->build();
                                        $cacheData = Pdo::slave('statistics')->getDatabase()->table($this->_table_daily)->select()->where($build['sql'], ...$build['match'])->sum($type);
                                    } catch (Throwable) {
                                        $cacheData = 0;
                                    }
                                    $date != Date::today() and Redis::pool()->set($cacheKey, $cacheData, 600);
                                }
                                $dayChannel->push($cacheData);
                            });
                        }
                        for ($i = 0; $i < count($days); $i++) {
                            $m['result'] += $dayChannel->pop(60);
                        }
                        $sceneChannel->push(1);
                    });
                }
                for ($si = 0; $si < count($scenes); $si++) {
                    $sceneChannel->pop(60);
                }
//                foreach ($scenes as &$m) {
//                    if ($m['scene'] == 'compute') {
//                        continue;
//                    }
//                    $m['result'] = 0;
//                    $m['days'] = count($days);
//                    foreach ($days as $item) {
//                        $date = $item['day'];
//                        $where = WhereBuilder::create(['scene' => $m['scene'], 'day' => $date]);
//                        if (!empty($m['mch_id'])) {
//                            $where->and(['mch_id' => $m['mch_id']]);
//                        }
//                        if (!empty($m['data_id'])) {
//                            $where->and(['data_id' => $m['data_id']]);
//                        }
//                        $type = !empty($m['type']) ? $m['type'] : 'pv';
//                        $cacheKey = $this->_table_daily . '_sum_' . $type . md5($where->getWhereSql());
//                        $cacheData = Redis::pool()->get($cacheKey);
//                        if ($cacheData === false || $date == Date::today()) {
//                            $cacheData = Pdo::slave('statistics')->connection()->sum($this->_table_daily, $type, $where->getWhereSql());
//                            $date != Date::today() and Redis::pool()->set($cacheKey, $cacheData, 600);
//                        }
//                        $m['result'] += $cacheData;
//                    }
//                }
                break;
            default://过去多少天之内的数据
                $sceneChannel = new Channel(count($scenes));
                foreach ($scenes as &$m) {
                    Coroutine::create(function () use (&$m, $sceneChannel, $day) {
                        if ($m['scene'] == 'compute') {
                            $sceneChannel->push(0);
                            return;
                        }
                        $m['result'] = 0;
                        $m['days'] = $day;
                        $dayChannel = new Channel($day);
                        for ($i = $day; $i >= 0; $i--) {
                            Coroutine::create(function () use ($m, $dayChannel, $i) {
                                $date = date("Ymd", strtotime("-" . $i . " days"));
                                $where = WhereBuilder::create(['scene' => $m['scene'], 'day' => $date]);
                                if (!empty($m['mch_id'])) {
                                    $where->and(['mch_id' => $m['mch_id']]);
                                }
                                if (!empty($m['data_id'])) {
                                    $where->and(['data_id' => $m['data_id']]);
                                }
                                $type = !empty($m['type']) ? $m['type'] : 'pv';
                                $cacheKey = $this->_table_daily . '_sum_' . $type . md5($where->getWhereSql());
                                $cacheData = Redis::pool()->get($cacheKey);
                                if ($cacheData === false || $date == Date::today()) {
                                    try {
                                        $build = $where->build();
                                        $cacheData = Pdo::slave('statistics')->getDatabase()->table($this->_table_daily)->select()->where($build['sql'], ...$build['match'])->sum($type);
                                    } catch (Throwable) {
                                        $cacheData = 0;
                                    }
                                    $date != Date::today() and Redis::pool()->set($cacheKey, $cacheData, 600);
                                }
                                $dayChannel->push($cacheData);
                            });
                        }
                        for ($i = 0; $i < $day; $i++) {
                            $m['result'] += $dayChannel->pop(60);
                        }
                        $sceneChannel->push(1);
                    });
                }
                for ($si = 0; $si < count($scenes); $si++) {
                    $sceneChannel->pop(60);
                }
                break;
        }
        //结果计算
        foreach ($scenes as &$m) {
            if ($m['scene'] == 'compute') {
                $formula = $m['formula'];
                $computeResult = 0;
                foreach ($formula as $fk => &$f) {
                    switch ($f['type']) {
                        case 'result_var'://scene统计结果
                            $f['result'] = $this->find_scene_result($f['source'], $scenes);
                            break;
                        case 'result_compute'://scene统计结果加减乘除
                            if (str_contains($f['source'], '+')) {
                                $arr = explode('+', $f['source']);
                                $symbol = '+';
                            } elseif (str_contains($f['source'], '-')) {
                                $arr = explode('-', $f['source']);
                                $symbol = '-';
                            } elseif (str_contains($f['source'], '*')) {
                                $arr = explode('*', $f['source']);
                                $symbol = '*';
                            } elseif (str_contains($f['source'], '/')) {
                                $arr = explode('/', $f['source']);
                                $symbol = '/';
                            } else {
                                $f['result'] = 0;
                                break;
                            }
                            $num1 = $arr[0];
                            $num2 = $arr[1];
                            if (!is_numeric($num1)) {
                                $num1 = $this->find_scene_result($num1, $scenes);
                            }
                            if (!is_numeric($num2)) {
                                $num2 = $this->find_scene_result($num2, $scenes);
                            }
                            switch ($symbol) {
                                case '+':
                                    $f['result'] = $num1 + $num2;
                                    break;
                                case '-':
                                    $f['result'] = $num1 - $num2;
                                    break;
                                case '*':
                                    $f['result'] = $num1 * $num2;
                                    break;
                                default:
                                    if ($num1 && $num2) {
                                        $f['result'] = $num1 / $num2;
                                    } else {
                                        $f['result'] = 0;
                                    }
                                    break;
                            }
                            break;
                        case 'result_sum'://scene统计结果累计
                            $f['result'] = 0;
                            foreach ($f['source'] as $s) {
                                $f['result'] += $this->find_scene_result($s, $scenes);
                            }
                            break;
                        case 'compute':
                            if (str_contains($f['source'], '+')) {
                                $arr = explode('+', $f['source']);
                                $symbol = '+';
                            } elseif (str_contains($f['source'], '-')) {
                                $arr = explode('-', $f['source']);
                                $symbol = '-';
                            } elseif (str_contains($f['source'], '*')) {
                                $arr = explode('*', $f['source']);
                                $symbol = '*';
                            } elseif (str_contains($f['source'], '/')) {
                                $arr = explode('/', $f['source']);
                                $symbol = '/';
                            } else {
                                $f['result'] = 0;
                                break;
                            }
                            $num1 = $arr[0];
                            $num2 = $arr[1];
                            if (!is_numeric($num1)) {
                                $num1 = $this->find_compute_result($num1, $formula);
                            }
                            if (!is_numeric($num2)) {
                                $num2 = $this->find_compute_result($num2, $formula);
                            }
                            switch ($symbol) {
                                case '+':
                                    $f['result'] = $num1 + $num2;
                                    break;
                                case '-':
                                    $f['result'] = $num1 - $num2;
                                    break;
                                case '*':
                                    $f['result'] = $num1 * $num2;
                                    break;
                                default:
                                    if ($num1 && $num2) {
                                        $f['result'] = $num1 / $num2;
                                    } else {
                                        $f['result'] = 0;
                                    }

                                    break;
                            }
                            break;
                    }
                    if ($fk == count($formula) - 1) {
                        $computeResult = $f['result'];
                    }
                }
                $m['result'] = $computeResult;
            }
        }
        return $scenes;
    }


    /**
     * 计算获取走势图数据
     * @param array $scenes 场景配置 mch_id:商户ID scene:统计模块标识 item_id:业务数据id type:需要统计的类型 uv/pv/value
     * @param int $day 时间段 1:今天按小时统计 2:昨天按小时统计 -1:自定义时间段 N:过去N天到今天按天统计
     * @param array $searchParams 自定义时间段参数值 start:开始时间 end:结束时间 格式 Y-m-d
     * @return array
     */
    #[ArrayShape(['title' => "string", 'types' => "array", 'axis_datas' => "array", 'series' => "array"])]
    public function chart_data(array $scenes, int $day = 1, array $searchParams = []): array {
        $axis = [];
        switch ($day) {
            case 1://今天
                foreach ($scenes as $k => &$m) {
                    $mchId = !empty($m['mch_id']) ? $m['mch_id'] : 0;
                    $dataId = !empty($m['data_id']) ? $m['data_id'] : 0;
                    $type = !empty($m['type']) ? $m['type'] : 'pv';
                    $scenes[$k]['total'] = 0;
                    $scenes[$k]['series'] = [];
                    $thisHour = date('H', time());
                    for ($i = 0; $i <= $thisHour; $i++) {
                        if (!in_array($i . "点", $axis)) {
                            $axis[] = $i . "点";
                        }
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['series'][$i];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['series'][$i];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['series'][$i];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['series'][$i];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }
                            $m['series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            continue;
                        }
                        $n = $this->sum_timeline($m['scene'], Date::today(), $type, $i, $mchId, $dataId);
                        $scenes[$k]['series'][] = $n;
                        $scenes[$k]['total'] += $n;
                    }
                }
                break;
            case 2://昨天
                foreach ($scenes as &$m) {
                    $mchId = !empty($m['mch_id']) ? $m['mch_id'] : 0;
                    $dataId = !empty($m['data_id']) ? $m['data_id'] : 0;
                    $type = !empty($m['type']) ? $m['type'] : 'pv';
                    $m['total'] = 0;
                    $m['series'] = [];
                    for ($i = 0; $i <= 23; $i++) {
                        if (!in_array($i . "点", $axis)) {
                            $axis[] = $i . "点";
                        }
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['series'][$i];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['series'][$i];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['series'][$i];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['series'][$i];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }
                            $m['series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            continue;
                        }
                        $n = $this->sum_timeline($m['scene'], Date::yesterday(), $type, $i, $mchId, $dataId);
                        $m['series'][] = $n;
                        $m['total'] += $n;
                    }
                }
                break;
            case -1://自定义
                $start = !empty($searchParams['start']) ? $searchParams['start'] : Date::yesterday('Y-m-d');
                $end = !empty($searchParams['end']) ? $searchParams['end'] : Date::today('Y-m-d');
                $days = Date::calendar($start, $end);
                $cycle = !empty($searchParams['cycle']) ? $searchParams['cycle'] : 'day';//数据展示周期
                foreach ($scenes as &$m) {
                    $weeks = [];
                    if ($cycle == 'week') {
                        $weeksTotal = ceil(count($days) / 7);//一共几周
                        for ($i = 1; $i <= $weeksTotal; $i++) {
                            $weeks[] = ['name' => '第' . $i . '周', 'result' => 0];
                        }
                    }

                    $months = [];
                    if ($cycle == 'month') {
                        foreach ($days as $item) {
                            $name = date('Y年m月', strtotime($item['day']));
                            if (!$this->check_month_exist($name, $months)) {
                                $months[] = ['name' => $name, 'result' => 0];
                            }
                        }
                    }
                    $mchId = !empty($m['mch_id']) ? $m['mch_id'] : 0;
                    $dataId = !empty($m['data_id']) ? $m['data_id'] : 0;
                    $type = !empty($m['type']) ? $m['type'] : 'pv';
                    $m['total'] = 0;
                    $m['series'] = [];
                    $m['day_series'] = [];
                    $j = 0;
                    foreach ($days as $dn => &$item) {
                        $date = $item['day'];
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['day_series'][$j];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['day_series'][$j];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['day_series'][$j];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['day_series'][$j];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }

                            $m['day_series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            $item['result'] = $computeResult;
                            $j++;
                            if ($cycle == 'week') {
                                $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                            } elseif ($cycle == 'month') {
                                foreach ($months as $mk => $month) {
                                    if (date('Y年m月', strtotime($date)) == $month['name']) {
                                        $months[$mk]['result'] += $item['result'];
                                    }
                                }
                            } else {
                                if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                    $axis[] = date("m月d日", strtotime($date));
                                }
                                $m['series'][] = $item['result'];
                            }
                            continue;
                        }
                        $num = $this->sum_timeline($m['scene'], $date, $type, -1, $mchId, $dataId);
                        $m['day_series'][] = $num;
                        $m['total'] += $num;
                        $item['result'] = $num;
                        $j++;
                        if ($cycle == 'week') {
                            $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                        } elseif ($cycle == 'month') {
                            foreach ($months as $mk => $month) {
                                if (date('Y年m月', strtotime($date)) == $month['name']) {
                                    $months[$mk]['result'] += $item['result'];
                                }
                            }
                        } else {
                            if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                $axis[] = date("m月d日", strtotime($date));
                            }
                            $m['series'][] = $item['result'];
                        }
                    }
                    if ($cycle == 'week') {
                        foreach ($weeks as $week) {
                            if (!in_array($week['name'], $axis)) {
                                $axis[] = $week['name'];
                            }
                            $m['series'][] = $week['result'];
                        }
                    } elseif ($cycle == 'month') {
                        foreach ($months as $month) {
                            if (!in_array($month['name'], $axis)) {
                                $axis[] = $month['name'];
                            }
                            $m['series'][] = $month['result'];
                        }
                    }
                }
                break;
            default:
                $start = Date::pastDay($day, 'Y-m-d');
                $days = Date::calendar($start, Date::today('Y-m-d'));
                $cycle = !empty($searchParams['cycle']) ? $searchParams['cycle'] : 'day';//数据展示周期
                foreach ($scenes as &$m) {
                    $weeks = [];
                    if ($cycle == 'week') {
                        $weeksTotal = ceil(count($days) / 7);//一共几周
                        for ($i = 1; $i <= $weeksTotal; $i++) {
                            $weeks[] = ['name' => '第' . $i . '周', 'result' => 0];
                        }
                    }

                    $months = [];
                    if ($cycle == 'month') {
                        foreach ($days as $item) {
                            $name = date('Y年m月', strtotime($item['day']));
                            if (!$this->check_month_exist($name, $months)) {
                                $months[] = ['name' => $name, 'result' => 0];
                            }
                        }
                    }
                    $mchId = !empty($m['mch_id']) ? $m['mch_id'] : 0;
                    $dataId = !empty($m['data_id']) ? $m['data_id'] : 0;
                    $type = !empty($m['type']) ? $m['type'] : 'pv';
                    $m['total'] = 0;
                    $m['series'] = [];
                    $m['day_series'] = [];
                    $j = 0;
                    foreach ($days as $dn => &$item) {
                        $date = $item['day'];
                        if ($m['scene'] == 'compute') {
                            $formula = $m['formula'];
                            $computeResult = 0;
                            foreach ($formula as $fk => &$f) {
                                switch ($f['type']) {
                                    case 'result_var'://scene统计结果
                                        $f['result'] = $this->find_scene($f['source'], $scenes)['day_series'][$j];
                                        break;
                                    case 'result_compute'://scene统计结果加减乘除
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_scene($num1, $scenes)['day_series'][$j];
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_scene($num2, $scenes)['day_series'][$j];
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                    case 'result_sum'://scene统计结果累计
                                        $f['result'] = 0;
                                        foreach ($f['source'] as $s) {
                                            $f['result'] += $this->find_scene($s, $scenes)['day_series'][$j];
                                        }
                                        break;
                                    case 'compute':
                                        if (str_contains($f['source'], '+')) {
                                            $arr = explode('+', $f['source']);
                                            $symbol = '+';
                                        } elseif (str_contains($f['source'], '-')) {
                                            $arr = explode('-', $f['source']);
                                            $symbol = '-';
                                        } elseif (str_contains($f['source'], '*')) {
                                            $arr = explode('*', $f['source']);
                                            $symbol = '*';
                                        } elseif (str_contains($f['source'], '/')) {
                                            $arr = explode('/', $f['source']);
                                            $symbol = '/';
                                        } else {
                                            $f['result'] = 0;
                                            break;
                                        }
                                        $num1 = $arr[0];
                                        $num2 = $arr[1];
                                        if (!is_numeric($num1)) {
                                            $num1 = $this->find_compute_result($num1, $formula);
                                        }
                                        if (!is_numeric($num2)) {
                                            $num2 = $this->find_compute_result($num2, $formula);
                                        }
                                        switch ($symbol) {
                                            case '+':
                                                $f['result'] = $num1 + $num2;
                                                break;
                                            case '-':
                                                $f['result'] = $num1 - $num2;
                                                break;
                                            case '*':
                                                $f['result'] = $num1 * $num2;
                                                break;
                                            default:
                                                if ($num1 && $num2) {
                                                    $f['result'] = $num1 / $num2;
                                                } else {
                                                    $f['result'] = 0;
                                                }
                                                break;
                                        }
                                        break;
                                }
                                if ($fk == count($formula) - 1) {
                                    $computeResult = $f['result'];
                                }
                            }
                            $m['day_series'][] = $computeResult;
                            $m['total'] += $computeResult;
                            $item['result'] = $computeResult;
                            $j++;
                            if ($cycle == 'week') {
                                $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                            } elseif ($cycle == 'month') {
                                foreach ($months as $mk => $month) {
                                    if (date('Y年m月', strtotime($date)) == $month['name']) {
                                        $months[$mk]['result'] += $item['result'];
                                    }
                                }
                            } else {
                                if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                    $axis[] = date("m月d日", strtotime($date));
                                }
                                $m['series'][] = $item['result'];
                            }
                            continue;
                        }
                        $num = $this->sum_timeline($m['scene'], $date, $type, -1, $mchId, $dataId);
                        $m['day_series'][] = $num;
                        $m['total'] += $num;
                        $item['result'] = $num;
                        $j++;
                        if ($cycle == 'week') {
                            $weeks[intval(ceil(($dn + 1) / 7)) - 1]['result'] += $item['result'];
                        } elseif ($cycle == 'month') {
                            foreach ($months as $mk => $month) {
                                if (date('Y年m月', strtotime($date)) == $month['name']) {
                                    $months[$mk]['result'] += $item['result'];
                                }
                            }
                        } else {
                            if (!in_array(date("m月d日", strtotime($date)), $axis)) {
                                $axis[] = date("m月d日", strtotime($date));
                            }
                            $m['series'][] = $item['result'];
                        }
                    }
                    if ($cycle == 'week') {
                        foreach ($weeks as $week) {
                            if (!in_array($week['name'], $axis)) {
                                $axis[] = $week['name'];
                            }
                            $m['series'][] = $week['result'];
                        }
                    } elseif ($cycle == 'month') {
                        foreach ($months as $month) {
                            if (!in_array($month['name'], $axis)) {
                                $axis[] = $month['name'];
                            }
                            $m['series'][] = $month['result'];
                        }
                    }
                }
                break;
        }
        $title = "";
        $types = [];
        $series = [];
        foreach ($scenes as $v) {
            $types[] = $v['name'];
            $title .= $v['name'] . ":" . $this->_numberFormat($v['total']) . " ";
            $series[] = [
                'name' => $v['name'],
                'data' => $v['series']
            ];
        }
        return [
            'title' => $title,
            'types' => $types,
            'axis_datas' => $axis,
            'series' => $series
        ];
    }

    /**
     * 统计某天的汇总数据
     * @param $scene
     * @param $day
     * @param string|array $field
     * @param int $hour
     * @param int $mchId
     * @param int $dataId
     * @return bool|int|float
     */
    public function sum_timeline($scene, $day, string|array $field = 'pv', int $hour = -1, int $mchId = 0, int $dataId = 0): bool|int|float {
        $where = WhereBuilder::create(['day' => $day, 'scene' => $scene]);
        if ($mchId) {
            $where->and(['mch_id' => $mchId]);
        }
        if ($dataId) {
            $where->and(['data_id' => $mchId]);
        }
        if ($hour != -1) {
            $where->and(['hour' => $hour]);
        }
        try {
            $build = $where->build();
            return Pdo::slave('statistics')->getDatabase()->table($this->_table_daily)->select()->where($build['sql'], ...$build['match'])->sum($field);
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * 统计指定日期汇总数据
     * @param $scene
     * @param int $mchId
     * @param mixed $dataId
     * @param int $day
     * @param string|array $field
     * @return bool|int
     */
    public function sum_day($scene, int $mchId = 0, mixed $dataId = 0, int $day = 1, string|array $field = 'pv'): bool|int {
        if ($day == 1) {
            $day = date('Ymd');
        }
        $where = WhereBuilder::create(['day' => $day, 'scene' => $scene]);
        if ($mchId) {
            $where->and(['mch_id' => $mchId]);
        }
        if ($dataId) {
            $where->and(['data_id' => $mchId]);
        }
        try {
            $build = $where->build();
            return Pdo::slave('statistics')->getDatabase()->table($this->_table_daily)->select()->where($build['sql'], ...$build['match'])->sum($field);
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * @param $scene
     * @param int $mchId
     * @param mixed $dataId
     * @param string $key
     * @return float|int
     */
    public function total($scene, int $mchId, mixed $dataId = 0, string $key = 'pv'): float|int {
        if ($dataId) {
            $total = StatisticsTotalDAO::select()->where(['scene' => $scene, 'mch_id' => $mchId, 'data_id' => $dataId])->ar();
            if ($total->notExist()) {
                return 0;
            }
            return $key == 'uv' ? $total->uv : ($key == 'pv' ? $total->pv : $total->value);
        }
        return StatisticsTotalDAO::select('id', $key)->where(['scene' => $scene, 'mch_id' => $mchId])->sum($key);
    }

    /**
     * 创建访问记录分表
     * @return bool
     * @throws Throwable
     */
    protected function create_record_table(): bool {
        $today = date('Ymd', time());
        $recordTable = $this->_table_record . '_' . $today;
        $sql = "CREATE TABLE `t_" . $recordTable . "` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `mch_id` int(10) DEFAULT NULL COMMENT '商户ID',
  `scene` varchar(50) DEFAULT NULL COMMENT '模块',
  `data_id` varchar(32) DEFAULT NULL COMMENT '模块数据ID',
  `usr_id` int(10) DEFAULT '0' COMMENT '访客uid',
  `guest_id` varchar(32) DEFAULT '' COMMENT '访客ID',
  `ip` varchar(50) DEFAULT NULL COMMENT '访客IP',
  `trigger_time` int(10) DEFAULT NULL COMMENT '访问时间',
  `trigger_times` int(10) DEFAULT '1' COMMENT '总访问次数',
  `updated` int(10) DEFAULT NULL,
  `search_key` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  KEY `scene` (`scene`),
  KEY `mch_id` (`mch_id`),
  KEY `data_id` (`data_id`),
  UNIQUE KEY `search_key` (`search_key`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;";
        try {
            Pdo::master('history')->getDatabase()->exec($sql);
            return true;
        } catch (\PDOException) {
            return false;
        }
    }

    /**
     * 数字单位转换
     * @param $number
     * @return string
     */
    protected function _numberFormat($number): string {
        if ($number > 99999) {
            return round($number / 10000, 1) . 'W';
        }
        return $number;
    }

    /**
     * 查询指定key的scene结果
     * @param $k
     * @param $arr
     * @return int
     */
    protected function find_scene_result($k, $arr): int {
        foreach ($arr as $a) {
            if ($a['scene'] == $k) {
                return $a['result'];
            }
        }
        return 0;
    }

    /**
     * 查询指定key的scene数据
     * @param $k
     * @param $arr
     * @return array
     */
    protected function find_scene($k, $arr): array {
        foreach ($arr as $a) {
            if ($a['scene'] == $k) {
                return $a;
            }
        }
        return [];
    }

    /**
     * 查询指定key的结算结果值
     * @param $k
     * @param $arr
     * @return int
     */
    protected function find_compute_result($k, $arr): int {
        foreach ($arr as $a) {
            if ($a['compute_key'] == $k) {
                return $a['result'];
            }
        }
        return 0;
    }

    /**
     * 查询指定月份是否存在
     * @param $name
     * @param $months
     * @return bool
     */
    protected function check_month_exist($name, $months): bool {
        foreach ($months as $m) {
            if ($m['name'] == $name) {
                return true;
            }
        }
        return false;
    }
}
