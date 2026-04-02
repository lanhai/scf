<?php

namespace Scf\Database\Backup;

use Scf\Core\Exception;
use mysqli;
use Swoole\Coroutine;
use Swoole\Coroutine\System;
use Throwable;

/**
 * 数据库备份恢复管理器。
 *
 * 该组件负责从 `/db/backup/{db}/{snapshot}` 快照目录执行恢复，
 * 支持整库快照恢复与单表恢复两种场景，并与备份流程共享互斥锁语义。
 */
class DatabaseBackupRestoreManager {
    /**
     * 备份管理器。
     *
     * @var DatabaseBackupManager
     */
    protected DatabaseBackupManager $backupManager;

    /**
     * mysql 命令解析器。
     *
     * @var DatabaseBackupCommandResolver
     */
    protected DatabaseBackupCommandResolver $commandResolver;

    public function __construct(
        ?DatabaseBackupManager $backupManager = null,
        ?DatabaseBackupCommandResolver $commandResolver = null
    ) {
        $this->backupManager = $backupManager ?: new DatabaseBackupManager($commandResolver);
        $this->commandResolver = $commandResolver ?: new DatabaseBackupCommandResolver();
    }

    /**
     * 执行恢复。
     *
     * @param string $dbName 数据库名
     * @param string $snapshot 快照目录（YmdHis）
     * @param string|null $table 指定表名时仅恢复单表
     * @return array<string, mixed>
     * @throws Exception
     */
    public function restore(string $dbName, string $snapshot, ?string $table = null): array {
        // 恢复入口允许传真实库名或 database.mysql 别名，先归一化再查找快照目录和连接配置。
        $dbName = $this->backupManager->resolveConfiguredDatabaseName($dbName);
        $snapshot = $this->backupManager->assertSnapshot($snapshot);
        $tableFile = is_null($table) || trim($table) === '' ? null : $this->backupManager->assertTableFile($table);
        $snapshotDir = $this->backupManager->snapshotDirectory($dbName, $snapshot);
        if (!is_dir($snapshotDir)) {
            throw new Exception('备份快照不存在: ' . $dbName . '/' . $snapshot);
        }

        $server = $this->resolveDatabaseServer($dbName);
        return $this->backupManager->withRestoreLock(function () use ($server, $dbName, $snapshot, $snapshotDir, $tableFile): array {
            if ($tableFile) {
                $tablePath = $snapshotDir . '/' . $tableFile;
                if (!is_file($tablePath)) {
                    throw new Exception('表备份文件不存在: ' . $dbName . '/' . $snapshot . '/' . $tableFile);
                }
                $this->restoreFromSqlFile($server, $tablePath);
                return [
                    'mode' => 'table',
                    'db_name' => $dbName,
                    'snapshot' => $snapshot,
                    'table' => $tableFile,
                    'restored_files' => 1,
                ];
            }

            $files = $this->snapshotSqlFiles($snapshotDir);
            if (!$files) {
                throw new Exception('快照目录内未找到可恢复的 sql 文件');
            }

            $this->restoreFullSnapshot($server, $files);
            return [
                'mode' => 'snapshot',
                'db_name' => $dbName,
                'snapshot' => $snapshot,
                'restored_files' => count($files),
            ];
        });
    }

    /**
     * 解析目标数据库连接信息。
     *
     * @param string $dbName
     * @return array<string, mixed>
     * @throws Exception
     */
    protected function resolveDatabaseServer(string $dbName): array {
        $servers = $this->backupManager->databaseServers();
        if (!isset($servers[$dbName])) {
            throw new Exception('当前环境未配置该数据库: ' . $dbName);
        }
        return $servers[$dbName];
    }

    /**
     * 执行单文件恢复。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @param string $sqlFile SQL 文件路径
     * @return void
     * @throws Exception
     */
    protected function restoreFromSqlFile(array $server, string $sqlFile): void {
        $mysql = $this->commandResolver->mysqlOrNull();
        if ($mysql === '') {
            $this->restoreFromSqlFileViaMysqli($server, $sqlFile);
            return;
        }

        $command = implode(' ', [
            escapeshellarg($mysql),
            '--host=' . escapeshellarg((string)$server['host']),
            '--port=' . (int)$server['port'],
            '--user=' . escapeshellarg((string)$server['username']),
            '--database=' . escapeshellarg((string)$server['db_name']),
            '--default-character-set=' . escapeshellarg((string)$server['charset']),
        ]);

        if ($this->canUseSwooleCoroutine()) {
            $stderrFile = tempnam(sys_get_temp_dir(), 'scf_db_restore_err_');
            if ($stderrFile === false) {
                throw new Exception('创建 mysql 恢复错误输出文件失败');
            }
            try {
                $result = System::exec('/bin/sh -lc ' . escapeshellarg(
                    'MYSQL_PWD=' . escapeshellarg((string)$server['password'])
                    . ' ' . $command
                    . ' < ' . escapeshellarg($sqlFile)
                    . ' > /dev/null'
                    . ' 2> ' . escapeshellarg($stderrFile)
                ));
                if ($result === false) {
                    throw new Exception('执行 mysql 恢复命令失败');
                }
                $stderr = (string)(@file_get_contents($stderrFile) ?: '');
                if ((int)($result['code'] ?? 1) !== 0) {
                    throw new Exception('恢复失败: ' . trim($stderr ?: (string)($result['output'] ?? '')));
                }
                return;
            } finally {
                @unlink($stderrFile);
            }
        }

        $descriptor = [
            0 => ['file', $sqlFile, 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        $process = @proc_open($command, $descriptor, $pipes, null, $this->mysqlEnv((string)$server['password']));
        if (!is_resource($process)) {
            throw new Exception('启动 mysql 失败，无法恢复: ' . basename($sqlFile));
        }
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $exitCode = proc_close($process);
        if ($exitCode !== 0) {
            throw new Exception('恢复失败: ' . trim((string)($stderr ?: $stdout)));
        }
    }

    /**
     * 执行整快照恢复。
     *
     * 为了避免外键依赖导致中间阶段失败，这里把快照内所有 SQL 拼接到一个临时脚本，
     * 在同一 mysql 会话中关闭/恢复外键检查后统一导入。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @param array<int, string> $sqlFiles 快照内 SQL 文件路径
     * @return void
     * @throws Exception
     */
    protected function restoreFullSnapshot(array $server, array $sqlFiles): void {
        $tempSql = tempnam(sys_get_temp_dir(), 'scf_db_restore_');
        if ($tempSql === false) {
            throw new Exception('创建临时恢复脚本失败');
        }

        try {
            $fp = fopen($tempSql, 'wb');
            if (!$fp) {
                throw new Exception('写入临时恢复脚本失败');
            }
            fwrite($fp, "SET FOREIGN_KEY_CHECKS=0;\n");
            foreach ($sqlFiles as $file) {
                $in = fopen($file, 'rb');
                if (!$in) {
                    fclose($fp);
                    throw new Exception('读取备份文件失败: ' . $file);
                }
                stream_copy_to_stream($in, $fp);
                fwrite($fp, "\n");
                fclose($in);
            }
            fwrite($fp, "SET FOREIGN_KEY_CHECKS=1;\n");
            fclose($fp);

            $this->restoreFromSqlFile($server, $tempSql);
        } finally {
            @unlink($tempSql);
        }
    }

    /**
     * 扫描快照目录中的 SQL 文件。
     *
     * @param string $snapshotDir 快照目录路径
     * @return array<int, string>
     */
    protected function snapshotSqlFiles(string $snapshotDir): array {
        $items = @scandir($snapshotDir);
        if (!is_array($items)) {
            return [];
        }

        $files = [];
        foreach ($items as $item) {
            if ($item === '.' || $item === '..' || !str_ends_with($item, '.sql')) {
                continue;
            }
            $path = $snapshotDir . '/' . $item;
            if (is_file($path)) {
                $files[] = $path;
            }
        }
        sort($files, SORT_STRING);
        return $files;
    }

    /**
     * 使用 mysqli 执行 SQL 文件恢复。
     *
     * docker 运行镜像缺少 mysql 客户端时，恢复仍可借助 mysqli 扩展逐条执行 SQL。
     * 这里按语句边界流式解析文件，避免整份 SQL 一次性读入内存。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @param string $sqlFile SQL 文件路径
     * @return void
     * @throws Exception
     */
    protected function restoreFromSqlFileViaMysqli(array $server, string $sqlFile): void {
        $mysqli = $this->openMysqliConnection($server);

        try {
            foreach ($this->readSqlStatements($sqlFile) as $statement) {
                if (!@$mysqli->query($statement)) {
                    throw new Exception('恢复失败: ' . $mysqli->error);
                }
            }
        } finally {
            $mysqli->close();
        }
    }

    /**
     * 从 SQL 文件中逐条读取语句。
     *
     * 解析时会跳过常见注释，并识别字符串/反引号上下文，确保不会在值里的分号处误切分。
     * 这足以覆盖本项目备份文件以及常规 mysqldump 产出的绝大部分 SQL。
     *
     * @param string $sqlFile SQL 文件路径
     * @return \Generator<int, string>
     * @throws Exception
     */
    protected function readSqlStatements(string $sqlFile): \Generator {
        $handle = @fopen($sqlFile, 'rb');
        if (!$handle) {
            throw new Exception('读取备份文件失败: ' . $sqlFile);
        }

        $statement = '';
        $inSingleQuote = false;
        $inDoubleQuote = false;
        $inBacktick = false;
        $inBlockComment = false;
        $inLineComment = false;
        $escaped = false;

        try {
            while (($line = fgets($handle)) !== false) {
                $length = strlen($line);
                for ($index = 0; $index < $length; $index++) {
                    $char = $line[$index];
                    $next = $index + 1 < $length ? $line[$index + 1] : '';
                    $prev = $index > 0 ? $line[$index - 1] : '';

                    if ($inLineComment) {
                        if ($char === "\n") {
                            $inLineComment = false;
                        }
                        continue;
                    }

                    if ($inBlockComment) {
                        if ($char === '*' && $next === '/') {
                            $inBlockComment = false;
                            $index++;
                        }
                        continue;
                    }

                    if (!$inSingleQuote && !$inDoubleQuote && !$inBacktick) {
                        if ($char === '-' && $next === '-' && ($index + 2 >= $length || ctype_space($line[$index + 2]))) {
                            $inLineComment = true;
                            $index++;
                            continue;
                        }
                        if ($char === '#') {
                            $inLineComment = true;
                            continue;
                        }
                        if ($char === '/' && $next === '*') {
                            $inBlockComment = true;
                            $index++;
                            continue;
                        }
                    }

                    if ($char === "'" && !$inDoubleQuote && !$inBacktick && !$escaped) {
                        $inSingleQuote = !$inSingleQuote;
                    } elseif ($char === '"' && !$inSingleQuote && !$inBacktick && !$escaped) {
                        $inDoubleQuote = !$inDoubleQuote;
                    } elseif ($char === '`' && !$inSingleQuote && !$inDoubleQuote) {
                        $inBacktick = !$inBacktick;
                    }

                    $statement .= $char;

                    if ($char === ';' && !$inSingleQuote && !$inDoubleQuote && !$inBacktick) {
                        $trimmed = trim($statement);
                        if ($trimmed !== '') {
                            yield $trimmed;
                        }
                        $statement = '';
                    }

                    $escaped = $char === '\\' && !$escaped && ($inSingleQuote || $inDoubleQuote) && $prev !== '\\';
                    if ($char !== '\\') {
                        $escaped = false;
                    }
                }

                $inLineComment = false;
            }

            $tail = trim($statement);
            if ($tail !== '') {
                yield $tail;
            }
        } finally {
            fclose($handle);
        }
    }

    /**
     * 打开一条 mysqli 直连。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @return mysqli
     * @throws Exception
     */
    protected function openMysqliConnection(array $server): mysqli {
        $mysqli = mysqli_init();
        if (!$mysqli instanceof mysqli) {
            throw new Exception('初始化 mysqli 失败');
        }

        if (!@$mysqli->real_connect(
            (string)$server['host'],
            (string)$server['username'],
            (string)$server['password'],
            (string)$server['db_name'],
            (int)$server['port']
        )) {
            throw new Exception('连接数据库失败(' . $server['db_name'] . '): ' . mysqli_connect_error());
        }

        if (!$mysqli->set_charset((string)$server['charset'])) {
            $error = $mysqli->error;
            $mysqli->close();
            throw new Exception('设置数据库字符集失败(' . $server['db_name'] . '): ' . $error);
        }

        return $mysqli;
    }

    /**
     * 构建 mysql 命令环境变量。
     *
     * @param string $password 数据库密码
     * @return array<string, string>
     */
    protected function mysqlEnv(string $password): array {
        $env = $_ENV;
        $env['MYSQL_PWD'] = $password;
        return $env;
    }

    /**
     * 当前是否处于可安全使用 Swoole 协程系统调用的上下文。
     *
     * 恢复既可能由 dashboard 在 worker 协程中触发，也可能由一次性脚本调用。
     * 只有已进入协程环境时，才启用 `System::exec()` 避免阻塞当前 worker。
     *
     * @return bool
     */
    protected function canUseSwooleCoroutine(): bool {
        return class_exists(Coroutine::class)
            && class_exists(System::class)
            && Coroutine::getCid() > 0;
    }
}
