<?php

namespace Scf\Database\Statistics;

use Scf\Core\Coroutine\Component;
use Scf\Core\Log;
use Scf\Core\Result;
use Scf\Database\Pdo;
use Scf\Database\Statistics\Dao\StatisticsArchiveDAO;
use Scf\Database\Tools\WhereBuilder;
use Scf\Helper\ArrayHelper;
use Scf\Util\Arr;
use Scf\Util\Date;
use Swoole\Coroutine;
use Swoole\Coroutine\Barrier;
use Swoole\Exception;

class StatisticsComponent extends Component {
    protected string $dbName;
    protected string $tableName;
    protected string $dateKey;
    protected string $startMinute;
    protected string $endMinute;
    protected string $startHour;
    protected string $endHour;
    protected string $startDay;
    protected string $endDay;
    protected string $startMonth;
    protected string $endMonth;
    protected string $type = 'all';
    protected array $days = [];
    protected array $hours = [];
    protected array $months = [];
    protected array $errMsg = [];
    /**
     * @var ?WhereBuilder 查询语句构造器
     */
    protected WhereBuilder|null $_where = null;
    protected array $dateTypes = [
        'minute' => 1,
        'hour' => 2,
        'day' => 3,
        'month' => 4,
        'all' => 99
    ];

    /**
     * 统计数据总条数
     * @param string $primaryKey 数据表主键名
     * @return Result
     */
    public function count(string $primaryKey = 'id'): Result {
        $dateNow = date('Y-m-d 00:00:00');
        $timelines = [];
        $where = clone $this->where();
        switch ($this->type) {
            case 'minute':

                break;
            case 'hour':
                $dateNow = date('Y-m-d H:00:00');
                foreach ($this->hours as $hour) {
                    $timelines[] = [
                        'key' => md5('hour.count' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $hour['date'] . $where->getWhereSql()),
                        'start' => $hour['start'],
                        'end' => $hour['end'],
                        'date_type' => $this->dateTypes['hour'],
                        'date' => $hour['date'],
                        'day' => date('Ymd', strtotime($hour['date'])),
                        'hour' => date('H', strtotime($hour['date'])),
                    ];
                }
                break;
            case 'day':
                $dateNow = Date::today('Y-m-d');
                foreach ($this->days as $day) {
                    $timelines[] = [
                        'key' => md5('day.count' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $day['day']['date'] . $where->getWhereSql()),
                        'start' => $day['day']['start'],
                        'end' => $day['day']['end'],
                        'date_type' => $this->dateTypes['day'],
                        'date' => $day['day']['date'],
                        'day' => date('Ymd', strtotime($day['day']['date'])),
                        'hour' => 24,
                    ];
                }
                break;
            case 'month':
                $dateNow = Date::today('Y-m');
                foreach ($this->months as $month) {
                    $timelines[] = [
                        'key' => md5('month.count' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $month['date'] . $where->getWhereSql()),
                        'start' => $month['start'],
                        'end' => $month['end'],
                        'date_type' => $this->dateTypes['month'],
                        'date' => $month['date'],
                        'day' => 32,
                        'hour' => 0,
                    ];
                }
                break;
        }
        $list = [];
        if ($timelines) {
            $timelines = ArrayHelper::removeDuplicatesByKey($timelines, 'key');
            $barrier = Barrier::make();
            foreach ($timelines as $index => $timeline) {
                $where = clone $this->where();
                Coroutine::create(function () use ($barrier, $dateNow, $primaryKey, $timeline, $index, $where, &$list) {
                    $result = [
                        'index' => $index,
                        'date' => $timeline['date'],
                        'count' => 0
                    ];
                    $cache = StatisticsArchiveDAO::select()->where(['search_key' => $timeline['key']])->ar();
                    if ($cache->exist()) {
                        $result['count'] = $cache->value;
                    } else {
                        try {
                            $where->and([$this->dateKey . '[^]' => [$timeline['start'], $timeline['end']]]);
                            $build = $where->build();
                            $count = Pdo::slave($this->dbName)->getDatabase()->table($this->tableName)->select($primaryKey)->where($build['sql'], ...$build['match'])->count();
                            $result['count'] = $count;
                            //保存历史数据
                            if ($timeline['date'] != $dateNow) {
                                $cacheData = [
                                    'search_key' => $timeline['key'],
                                    'type' => 1,
                                    'date_type' => $timeline['date_type'],
                                    'db' => $this->dbName . '.' . $this->tableName . '.' . $this->dateKey,
                                    'where' => $where->getWhereSql(),
                                    'date' => $timeline['date'],
                                    'day' => $timeline['day'],
                                    'hour' => $timeline['hour'],
                                    'key' => $primaryKey,
                                    'value' => $count,
                                    'updated' => time()
                                ];
                                $cache = StatisticsArchiveDAO::factory($cacheData);
                                $cache->save();
                            }
                        } catch (\Throwable $exception) {
                            $result['count'] = 0;
                            Log::instance()->error('统计出错:' . $exception->getMessage());
                        }
                    }
                    $list[] = $result;
//                    \Co\defer(function () {
//                        Pdo::slave($this->dbName)->connection()->destory();
//                    });
                });
            }
            try {
                Barrier::wait($barrier);
            } catch (Exception $e) {
                return Result::error($e->getMessage());
            }
            return Result::success(Arr::sort($list, 'index', 'asc', true));
        }
        try {
            $searchKey = md5('all.count' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $this->where()->getWhereSql());
            $cache = StatisticsArchiveDAO::select()->where(['search_key' => $searchKey])->ar();
            if ($cache->notExist()) {
                $cache = StatisticsArchiveDAO::factory();
                $cache->search_key = $searchKey;
                $cache->date_type = $this->dateTypes['all'];
                $cache->db = $this->dbName . '.' . $this->tableName . '.' . $this->dateKey;
                $cache->type = 1;
                $cache->where = $this->where()->getWhereSql();
                $cache->date = 0;
                $cache->day = 0;
                $cache->hour = 0;
                $cache->key = $primaryKey;
                $cache->updated = 0;
            }
            if (time() - $cache->updated > 10) {
                $build = $this->where()->build();
                $count = Pdo::slave($this->dbName)->getDatabase()->table($this->tableName)->select($primaryKey)->where($build['sql'], ...$build['match'])->count();
                $cache->updated = time();
                $cache->value = $count;
                $cache->save();
            } else {
                $count = $cache->value;
            }
            return Result::success(intval($count));
        } catch (\Throwable $exception) {
            return Result::error('统计出错:' . $exception->getMessage());
        }

    }

    /**
     * 计算指定字段的总和
     * @param $key
     * @return Result
     */
    public function sum($key): Result {
        $thisTime = date('Y-m-d 00:00:00');
        $timelines = [];
        $where = clone $this->where();
        switch ($this->type) {
            case 'hour':
                $thisTime = date('Y-m-d H:00:00');
                foreach ($this->hours as $hour) {
                    $timelines[] = [
                        'key' => md5('hour.sum' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $hour['date'] . $where->getWhereSql()),
                        'start' => $hour['start'],
                        'end' => $hour['end'],
                        'date_type' => $this->dateTypes['hour'],
                        'date' => $hour['date'],
                        'day' => date('Ymd', strtotime($hour['date'])),
                        'hour' => date('H', strtotime($hour['date'])),
                    ];
                }
                break;
            case 'day':
                $thisTime = Date::today('Y-m-d');
                foreach ($this->days as $day) {
                    $timelines[] = [
                        'key' => md5('day.sum' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $day['day']['date'] . $where->getWhereSql()),
                        'start' => $day['day']['start'],
                        'end' => $day['day']['end'],
                        'date_type' => $this->dateTypes['day'],
                        'date' => $day['day']['date'],
                        'day' => date('Ymd', strtotime($day['day']['date'])),
                        'hour' => 24,
                    ];
                }
                break;
            case 'month':
                $thisTime = Date::today('Y-m');
                foreach ($this->months as $month) {
                    $timelines[] = [
                        'key' => md5('month.sum' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $month['date'] . $where->getWhereSql()),
                        'start' => $month['start'],
                        'end' => $month['end'],
                        'date_type' => $this->dateTypes['month'],
                        'date' => $month['date'],
                        'day' => 32,
                        'hour' => 0,
                    ];
                }
                break;
        }
        $list = [];
        if ($timelines) {
            $barrier = Barrier::make();
            foreach ($timelines as $index => $timeline) {
                $where = clone $this->where();
                Coroutine::create(function () use ($barrier, $thisTime, $key, $timeline, &$list, $index, $where) {
                    $result = [
                        'index' => $index,
                        'date' => $timeline['date'],
                        'sum' => 0
                    ];
                    $cache = StatisticsArchiveDAO::select()->where(['search_key' => $timeline['key']])->ar();
                    if ($cache->exist()) {
                        $result['sum'] = $cache->value;
                    } else {
                        try {
                            $where->and([$this->dateKey . '[^]' => [$timeline['start'], $timeline['end']]]);
                            $build = $where->build();
                            $sum = Pdo::slave($this->dbName)->getDatabase()->table($this->tableName)->select()->where($build['sql'], ...$build['match'])->sum($key);
                            $result['sum'] = $sum;
                            //保存历史数据
                            if ($timeline['date'] != $thisTime) {
                                $cacheData = [
                                    'search_key' => $timeline['key'],
                                    'type' => 2,
                                    'date_type' => $timeline['date_type'],
                                    'db' => $this->dbName . '.' . $this->tableName . '.' . $this->dateKey,
                                    'where' => $where->getWhereSql(),
                                    'day' => $timeline['day'],
                                    'hour' => $timeline['hour'],
                                    'date' => $timeline['date'],
                                    'key' => $key,
                                    'value' => $sum,
                                    'updated' => time()
                                ];
                                $cache = StatisticsArchiveDAO::factory($cacheData);
                                $cache->save();
                            }
                        } catch (\Throwable $exception) {
                            $result['sum'] = 0;
                            Log::instance()->error('统计出错:' . $exception->getMessage());
                            //return Result::error('统计出错:' . $exception->getMessage());
                        }
                    }
                    $list[] = $result;
//                    \Co\defer(function () {
//                        Pdo::slave($this->dbName)->connection()->destory();
//                    });
                });
            }
            try {
                Barrier::wait($barrier);
            } catch (Exception $e) {
                return Result::error($e->getMessage());
            }
            return Result::success(Arr::sort($list, 'index', 'asc', true));
        }
        try {
            $searchKey = md5('all.sum' . $this->dbName . '.' . $this->tableName . '.' . $this->dateKey . $this->where()->getWhereSql());
            $cache = StatisticsArchiveDAO::select()->where(['search_key' => $searchKey])->ar();
            if ($cache->notExist()) {
                $cache = StatisticsArchiveDAO::factory();
                $cache->search_key = $searchKey;
                $cache->date_type = $this->dateTypes['all'];
                $cache->db = $this->dbName . '.' . $this->tableName . '.' . $this->dateKey;
                $cache->type = 2;
                $cache->where = $this->where()->getWhereSql();
                $cache->date = 0;
                $cache->day = 0;
                $cache->hour = 0;
                $cache->key = $key;
                $cache->updated = 0;
            }
            if (time() - intval($cache->updated) > 10) {
                $build = $this->where()->build();
                $sum = Pdo::slave($this->dbName)->getDatabase()->table($this->tableName)->select()->where($build['sql'], ...$build['match'])->sum($key);
                $cache->updated = time();
                $cache->value = $sum;
                $cache->save();
            } else {
                $sum = $cache->value;
            }
            return Result::success($sum);
        } catch (\Throwable $exception) {
            return Result::error('统计出错:' . $exception->getMessage());
        }
    }


    /**
     * 统计数据库
     * @param $db
     * @return $this
     */
    public function db($db): StatisticsComponent {
        $this->dbName = $db;
        return $this;
    }

    /**
     * 统计数据表
     * @param $table
     * @return $this
     */
    public function table($table): StatisticsComponent {
        $this->tableName = $table;
        return $this;
    }

    public function where($where = null): WhereBuilder|static {
        if (is_null($where)) {
            if (is_null($this->_where)) {
                $this->_where = WhereBuilder::create();
            }
            return $this->_where;
        } else {
            if ($where instanceof WhereBuilder) {
                $this->_where = $where;
            } else {
                $this->_where = WhereBuilder::create($where);
            }
            return $this;
        }
    }

    /**
     * 统计时间字段名称
     * @param $key
     * @return $this
     */
    public function key($key): StatisticsComponent {
        $this->dateKey = $key;
        return $this;
    }

    /**
     * 起始时间
     * @param $start
     * @return $this
     */
    public function start($start): StatisticsComponent {
        switch ($this->type) {
            case 'minute':
                $this->startMinute = $start;
                break;
            case 'hour':
                $this->startHour = $start;
                break;
            case 'day':
                $this->startDay = $start;
                break;
            case 'month':
                $this->startMonth = $start;
                break;
        }
        return $this;
    }

    /**
     * 截止时间
     * @param $end
     * @return $this
     */
    public function end($end): StatisticsComponent {
        switch ($this->type) {
            case 'minute':
                $this->endMinute = $end;
                break;
            case 'hour':
                $this->endHour = $end;
                break;
            case 'day':
                $this->endDay = $end;
                break;
            case 'month':
                $this->endMonth = $end;
                break;
        }
        return $this;
    }


    /**
     * 获取错误清单
     * @return array|bool
     */
    public function getErrors(): bool|array {
        if (count($this->errMsg) > 0) {
            return $this->errMsg;
        }
        return false;
    }

    /**
     * 设置统计时间段类型
     * @param $type string $type 需要统计的时间单位分割 minute|hour|day|month
     * @return $this
     */
    public function type(string $type): StatisticsComponent {
        $this->type = $type;
        return $this;
    }

    /**
     * 初始化
     * @return $this
     */
    public function init(): StatisticsComponent {
        switch ($this->type) {
            case 'minute':
                $this->minuteInit();
                break;
            case 'hour':
                $this->hourInit();
                break;
            case 'day':
                $this->dayInit();
                break;
            case 'month':
                $this->monthInit();
                break;
        }
        return $this;
    }

    /**
     * 初始化分钟参数
     */
    protected function minuteInit(): void {
        if (!isset($this->startMinute)) {
            $this->startMinute = date('Y-m-d 00:00:00');
        }
        if (!isset($this->endMinute)) {
            $this->endMinute = date('Y-m-d H:i:s');
        }
        if ($this->validation()) {

        }
    }

    /**
     * 初始化小时参数
     */
    protected function hourInit() {
        if (!isset($this->startHour)) {
            $this->startHour = date('Y-m-d 00:00:00');
        }
        if (!isset($this->endHour)) {
            $this->endHour = date('Y-m-d H:i:s');
        }
        if ($this->validation()) {
            $this->hours = Date::timeline($this->startHour, $this->endHour, 'hour', 'timestamp');
            if (count($this->days) > 72) {
                $this->errMsg[] = '小时数不能超过72小时';
            }
        }
    }

    /**
     * 初始化天数参数
     */
    protected function dayInit() {
        if (!isset($this->startDay)) {
            $this->startDay = date('Y-m-01');
        }
        if (!isset($this->endDay)) {
            $this->endDay = date('Y-m-d');
        }
        if ($this->validation()) {
            $this->days = Date::calendar($this->startDay, $this->endDay, 'timestamp');
            if (count($this->days) > 90) {
                $this->errMsg[] = '天数跨度不能超过90天';
            }
        }
    }

    /**
     * 初始化月份参数
     */
    protected function monthInit() {
        if (!isset($this->startMonth)) {
            $this->startMonth = Date::pastMonth(6, 'Y-m');
        }
        if (!isset($this->endMonth)) {
            $this->endMonth = date('Y-m');
        }
        if ($this->validation()) {
            $this->months = Date::timeline($this->startMonth, $this->endMonth, 'month', 'timestamp');
            if (count($this->months) > 12) {
                $this->errMsg[] = '月数跨度不能超过12个月';
            }
        }
    }

    /**
     * 参数验证
     * @return bool
     */
    protected function validation(): bool {
        if (!$this->dbName) {
            $this->errMsg[] = '数据库未设置';
        }
        if (!$this->tableName) {
            $this->errMsg[] = '数据表未设置';
        }
        if (!$this->dateKey) {
            $this->errMsg[] = '数据表统计时间字段未设置';
        }
        switch ($this->type) {
            case 'minute':
                if (!$this->isTime($this->startMinute)) {
                    $this->errMsg[] = '统计起始时间不正确:' . $this->startMinute;
                }
                if (!$this->isTime($this->endMinute)) {
                    $this->errMsg[] = '统计截止时间不正确:' . $this->endMinute;
                }
                if (strtotime($this->endMinute) < strtotime($this->startMinute)) {
                    $this->errMsg[] = '截止时间不能小于起始时间';
                }
                break;
            case 'hour':
                if (!$this->isTime($this->startHour)) {
                    $this->errMsg[] = '统计起始小时不正确:' . $this->startHour;
                }
                if (!$this->isTime($this->endHour)) {
                    $this->errMsg[] = '统计截止小时不正确:' . $this->endHour;
                }
                if (strtotime($this->endHour) < strtotime($this->startHour)) {
                    $this->errMsg[] = '截止时间不能小于起始时间';
                }
                break;
            case 'day':
                if (!$this->isDay($this->startDay)) {
                    $this->errMsg[] = '统计起始日期不正确:' . $this->startDay;
                }
                if (!$this->isDay($this->endDay)) {
                    $this->errMsg[] = '统计截止日期不正确:' . $this->endDay;
                }
                if (strtotime($this->endDay) < strtotime($this->startDay)) {
                    $this->errMsg[] = '截止时间不能小于起始时间';
                }
                break;
            case 'month':
                if (!$this->isMonth($this->startMonth)) {
                    $this->errMsg[] = '统计起始月份不正确:' . $this->startMonth;
                }
                if (!$this->isMonth($this->endMonth)) {
                    $this->errMsg[] = '统计截止月份不正确:' . $this->endMonth;
                }
                if (strtotime($this->endMonth) < strtotime($this->startMonth)) {
                    $this->errMsg[] = '截止时间不能小于起始时间';
                }
                break;
        }

        if (count($this->errMsg) > 0) {
            return false;
        }
        return true;
    }

    protected function isTime($time): bool|int {
        $pattern = '/[\d]{4}-[\d]{1,2}-[\d]{1,2}\s[\d]{1,2}:[\d]{1,2}:[\d]{1,2}/';
        return preg_match($pattern, $time);
    }

    protected function isDay($time): bool|int {
        $pattern = '/^[\d]{4}-[\d]{1,2}-[\d]{1,2}$/';
        return preg_match($pattern, $time);
    }

    protected function isMonth($time): bool|int {
        $pattern = '/^[\d]{4}-[\d]{1,2}$/';
        return preg_match($pattern, $time);
    }
}