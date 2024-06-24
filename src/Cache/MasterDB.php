<?php

namespace Scf\Cache;

use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\Singleton;
use Scf\Database\Exception\NullMasterDb;
use Scf\Database\Exception\NullPool;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Util\Arr;
use Scf\Util\Time;
use Swoole\Timer;

class MasterDB {
    use ComponentTrait, Singleton;

    protected static Redis $redisConnection;
    protected ?Redis $connectionPool = null;

    public static function __callStatic($method, $args) {
        return self::$redisConnection->$method(...$args);
    }

    public function __call($method, $args) {
        return $this->$method(...$args);
    }

    protected function connection(): Redis|NullMasterDb {
        $pool = Redis::instance()->create([
            'host' => App::isReady() ? Config::get('app')['master_host'] ?? '127.0.0.1' : '127.0.0.1',
            'port' => 16379,
            'auth' => '',
            'db_index' => 0,
            'time_out' => 1,//连接超时时间
            'size' => 8,
        ]);
        if ($pool instanceof NullPool) {
            Console::error("【MasterDB】连接失败:" . $pool->getError());
            return new NullMasterDb('MasterDB', $pool->getError());
        }
        return $pool;
    }

    /**
     * @return void
     */
    protected function idleCheck(): void {
        self::set("___connection_created___", date('Y-m-d H:i:s'));
        Timer::tick(1000, function ($id) {
            try {
                $result = $this->connectionPool->get("___connection_created___");
                //Console::info('idleCheck:' . $result);
                if (!$result) {
                    Console::warning("MasterDB连接池失效");
                    $this->connectionPool = null;
                    Timer::clear($id);
                }
                //Console::info("MasterDB CHECK:" . $result);
            } catch (\Throwable $exception) {
                Console::error("MasterDB连接池失效:" . $exception->getMessage());
                $this->connectionPool = null;
                Timer::clear($id);
            }
        });
    }


    private static function getConnection(): Redis|NullMasterDb {
        return self::instance()->connection();
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
        return self::getConnection()->command('addLog', $type, $v);
    }

    /**
     * @param $type
     * @param $day
     * @return mixed
     */
    public static function countLog($type, $day = null): mixed {
        $day = is_null($day) ? date('Y-m-d') : $day;
        return self::getConnection()->command('countLog', $type, $day);
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
        $result = self::getConnection()->command('getLog', $type, $day, $start, $length);
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
        return self::getConnection()->command('SET', $k, $v);
    }

    /**
     * @param $k
     * @return mixed
     */
    public static function get($k): mixed {
        $data= self::getConnection()->command('GET', $k);
        return StringHelper::isJson($data) ? JsonHelper::recover($data) : $data;
    }

    public static function delete($cacheKey): bool {
        return self::getConnection()->command('DELETE', $cacheKey);
    }

    /**
     * @param $k
     * @return int
     */
    public static function lLength($k): int {
        return self::getConnection()->command('lLength', $k);
    }

    /**
     * @param $k
     * @param $start
     * @param $end
     * @return bool|array
     */
    public static function lRange($k, $start, $end): bool|array {
        $result = self::getConnection()->command('listRange', $k, $start, $end);
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
     * @param $v
     * @return bool|int
     */
    public static function lPush($k, $v): bool|int {
        return self::getConnection()->command('lPush', $k, $v);
    }

    public static function hgetAll($k) {
        return self::getConnection()->command('hGetAll', $k);
    }

    public static function hget($k, $name) {
        return self::getConnection()->command('hGet', $k, $name);
    }

    public static function expire($k, $ttl): bool {
        return self::getConnection()->command('expire', $k, $ttl);
    }

    public static function hset(?string $sessionId, string $name, $value): bool|int {
        return self::getConnection()->command('hSet', $sessionId, $name, $value);
    }

    public static function hdel($k, $hashKey): bool|int {
        return self::getConnection()->command('hDel', $k, $hashKey);
    }

    public static function sAdd($key, ...$member): bool|int|\Redis {
        return self::getConnection()->command('sAdd', $key, ...$member);
    }

    public static function sMembers($k): bool|array|\Redis {
        return self::getConnection()->command('sMembers', $k);
    }

    public static function sIsMember($key, $member): bool|\Redis {
        return self::getConnection()->command('sIsMember', $key, $member);
    }

    public static function sRemove($key, ...$member): bool|int|\Redis {
        return self::getConnection()->command('sRemove', $key, ...$member);
    }

    public static function sClear($key): bool|int|\Redis {
        return self::getConnection()->command('sClear', $key);
    }

}