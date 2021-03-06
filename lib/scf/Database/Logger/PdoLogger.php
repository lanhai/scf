<?php

namespace Scf\Database\Logger;

use Mix\Database\LoggerInterface;
use Scf\Core\Console;
use Scf\Mode\Web\Log;
use Swoole\Coroutine;
use Throwable;

class PdoLogger implements LoggerInterface {
    /**
     * 数据库执行日志
     * @param float $time
     * @param string $sql
     * @param array $bindings
     * @param int $rowCount
     * @param Throwable|null $exception
     * @return void
     */
    public function trace(float $time, string $sql, array $bindings, int $rowCount, ?Throwable $exception): void {
        $logger = Log::instance();
        $num = substr_count($sql, '?');
        $executeSql = $sql;
        for ($i = 0; $i < $num; $i++) {
            $in = $bindings[$i];
            $executeSql = preg_replace("/\?/", "$in", $executeSql, 1);
        }
        if (str_starts_with($executeSql, 'INSERT') && count($bindings)) {
            preg_match('/(VALUES \((.*)\)\s*)/i', $executeSql, $match);
            $fields = $match[2] ?? '';
            $arr = explode(',', $fields);
            foreach ($arr as $k => $field) {
                $executeSql = str_replace($field, $bindings[trim(str_replace(':', '', $field))], $executeSql);
            }
        }
//        $cid = Coroutine::getCid();
//        $pcid = Coroutine::getPcid();
//        Console::log("#$pcid|{$cid}:{$executeSql}【{$time}ms】");
        $logger->isDebugEnable() and $logger->addSql("{$executeSql}【{$time}】ms");
        if (!is_null($exception)) {
            $logger->error($exception);
        }
    }
}