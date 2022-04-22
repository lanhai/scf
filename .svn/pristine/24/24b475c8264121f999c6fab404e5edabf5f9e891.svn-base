<?php

namespace Scf\Runtime;

use Scf\Core\Config;
use Scf\Database\Redis as RedisDriver;
use Scf\Helper\JsonHelper;
use Scf\Util\Arr;
use Scf\Util\Time;

class MasterDB {

    protected static RedisDriver $redisConnection;

    public static function __callStatic($name, $args) {
        return self::$redisConnection->$name(...$args);
    }

    /**
     * 获取redis连接
     * @return RedisDriver
     */
    public static function init(): RedisDriver {
        if (!isset(self::$redisConnection)) {
            self::$redisConnection = RedisDriver::factory()->getPool(
                [
                    'host' => Config::get('app')['master_host'],
                    'port' => Config::get('app')['master_db_port'],
                    'auth' => '',
                    'db_index' => 0,
                    'time_out' => 1,//连接超时时间
                    'size' => 256,
                ]
            );
        }
        return self::$redisConnection;
    }

    public static function check() {
        return self::get('check');
    }

    /**
     * @param $type
     * @param $v
     * @return mixed
     */
    public static function addLog($type, $v): mixed {
        if (Arr::isArray($v)) {
            if (!isset($v['date'])) {
                $v['date'] = date('m-d H:i:s') . "." . substr(Time::millisecond(), -3);
            }
            $v = JsonHelper::toJson($v);
        }
        return self::$redisConnection->command('addLog', $type, $v);
    }

    /**
     * @param $type
     * @param $day
     * @return mixed
     */
    public static function countLog($type, $day = null): mixed {
        $day = is_null($day) ? date('Y-m-d') : $day;
        return self::$redisConnection->command('countLog', $type, $day);
    }

    /**
     * @param $type
     * @param null $day
     * @param int $start
     * @param int $length
     * @return mixed
     */
    public static function getLog($type, $day = null, int $start = 0, int $length = 100): mixed {
        $day = is_null($day) ? date('Y-m-d') : $day;
        $result = self::$redisConnection->command('getLog', $type, $day, $start, $length);
        $logs = [];
        if ($result) {
            $logs = JsonHelper::recover($result);
        }
        if ($start < 0) {
            $logs = array_reverse($logs);
        }
        return $logs;
    }

    /**
     * @param $k
     * @param $v
     * @return mixed
     */
    public static function set($k, $v): mixed {
        if (Arr::isArray($v)) {
            $v = JsonHelper::toJson($v);
        }
        return self::$redisConnection->command('SET', $k, $v);
    }

    /**
     * @param $k
     * @return int
     */
    public static function lLength($k): int {
        return self::$redisConnection->lLength($k);
    }

    /**
     * @param $k
     * @param $start
     * @param $end
     * @return bool|array
     */
    public static function lRange($k, $start, $end): bool|array {
        $result = self::$redisConnection->command('listRange', $k, $start, $end);
        $list = [];
        if ($result) {
            $result = JsonHelper::recover($result);
            foreach ($result as $string) {
                $list[] = JsonHelper::is($string) ? JsonHelper::recover($string) : $string;
            }
        }
        return $list;
    }

    /**
     * @param $k
     * @return array|bool
     */
    public static function lAll($k): bool|array {
        return self::lRange($k, 0, -1);
    }


    /**
     * @param $k
     * @return mixed
     */
    public static function get($k): mixed {
        return self::$redisConnection->get($k, $k);
    }

    /**
     * @param $k
     * @param $v
     * @return bool|int
     */
    public static function lPush($k, $v): bool|int {
        return self::$redisConnection->lPush($k, $v);

    }

}

MasterDB::init();