<?php

namespace Scf\Database\Logger;

use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Log;
use Scf\Core\Table\Counter;
use Scf\Server\Worker\ProcessLife;
use Scf\Util\File;
use Swoole\Timer;
use Throwable;

class PdoLogger implements ILogger {
    private const ERROR_LOG_INTERVAL = 600;

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
        $countKey = Key::COUNTER_MYSQL_PROCESSING . time();
        $count = Counter::instance()->incr($countKey);
        if ($count == 1) {
            Timer::after(2000, function () use ($countKey) {
                Counter::instance()->delete($countKey);
            });
        }
        ProcessLife::instance()->addSql("{$executeSql}【{$time}】ms");
        PRINT_MYSQL_LOG and Console::info("【Mysql】{$executeSql}【{$time}】ms");
        if (!is_null($exception) && !str_starts_with($executeSql, 'DESCRIBE')) {
            $backTrace = $exception->getTrace();
            $file = $backTrace[count($backTrace) - 2]['file'] ?? null;
            $line = $backTrace[count($backTrace) - 2]['line'] ?? null;
            if (str_contains($file, 'Server/Task/Crontab.php')) {
                $file = $backTrace[count($backTrace) - 4]['file'] ?? null;
                $line = $backTrace[count($backTrace) - 4]['line'] ?? null;
            }
            $goneAwayRecordFile = APP_TMP_PATH . '/mysql_gone_away_record.log';
            $recordLog = true;
            $connectionErrorKeywords = [
                'MySQL server has gone away',
                'Connection refused',
                'Connection timed out',
                'Connection reset by peer',
                'Connection was killed',
            ];
            $message = strtolower($exception->getMessage() ?? '');
            foreach ($connectionErrorKeywords as $keyword) {
                if (stripos($message, $keyword) !== false) {
                    $lastRecordTime = is_file($goneAwayRecordFile) ? File::read($goneAwayRecordFile) : 0;
                    $lastRecordTime = is_numeric($lastRecordTime) ? (int)$lastRecordTime : 0;
                    if (time() - $lastRecordTime > self::ERROR_LOG_INTERVAL) {
                        File::write($goneAwayRecordFile, (string)time());
                    } else {
                        $recordLog = false;
                    }
                    break;
                }
            }
            ($executeSql !== 'select 1' && $recordLog) and Log::instance()->error($exception->getMessage() . ';SQL:' . $executeSql, file: $file, line: $line);
            //throw new AppError($exception->getMessage());
        }
    }
}