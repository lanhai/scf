<?php

namespace Scf\Database\Logger;

use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Mode\Web\Exception\AppError;
use Scf\Server\Env;
use Scf\Server\Http;
use Scf\Server\Table\Counter;
use Scf\Server\Worker\ProcessLife;
use Throwable;

class PdoLogger implements ILogger {
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
        $num = substr_count($sql, '?');
        $executeSql = $sql;
        for ($i = 0; $i < $num; $i++) {
            $in = $bindings[$i];
            if (is_null($in)) {
                $in = 'NULL';
            }
            $executeSql = preg_replace("/\?/", "$in", $executeSql, 1);
        }
        if (str_starts_with($executeSql, 'INSERT') && count($bindings)) {
            preg_match('/(VALUES \((.*)\)\s*)/i', $executeSql, $match);
            $fields = $match[2] ?? '';
            $arr = explode(',', $fields);
            foreach ($arr as $field) {
                if (!$field) {
                    continue;
                }
                $executeSql = str_replace($field, $bindings[trim(str_replace(':', '', $field))] ?? '', $executeSql);
            }
        }
        if (!is_null(Http::server())) {
            Counter::instance()->incr('_MYSQL_EXECUTE_COUNT_' . time());
            ProcessLife::instance()->addSql("{$executeSql}【{$time}】ms");
        }
        PRINT_MYSQL_LOG and Console::info("【Mysql】{$executeSql}【{$time}】ms");
        if (!is_null($exception) && !str_starts_with($executeSql, 'DESCRIBE')) {
            $backTrace = $exception->getTrace();
            $file = $backTrace[1]['file'] ?? null;
            $line = $backTrace[1]['line'] ?? null;
            $executeSql !== 'select 1' and Log::instance()->error($exception->getMessage() . ';SQL:' . $executeSql, file: $file, line: $line);
            //throw new AppError($exception->getMessage());
        }
    }
}