<?php

namespace Scf\Database\Backup;

use Scf\Server\Task\Crontab;
use Throwable;

/**
 * 数据库备份定时任务。
 *
 * 该任务通过 Linux crontab 的一次性执行入口触发，读取当前运行环境数据库配置，
 * 将每个表导出到 `db/backup/{db_name}/{YmdHis}/{table}.sql`，并自动执行保留 3 次的清理。
 */
class DatabaseBackupCrontab extends Crontab {
    /**
     * 执行一次数据库备份。
     *
     * @return void
     * @throws Throwable
     */
    public function run(): void {
        $manager = new DatabaseBackupManager();
        $result = $manager->runBackup();
        $summary = '数据库备份完成，快照: ' . (string)($result['snapshot'] ?? '--')
            . '，数据库数: ' . count((array)($result['results'] ?? []));
        $this->log($summary);
    }
}
