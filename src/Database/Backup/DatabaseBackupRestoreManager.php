<?php

namespace Scf\Database\Backup;

use Scf\Core\Exception;
use mysqli;
use Throwable;

/**
 * 数据库备份恢复管理器。
 *
 * 该组件负责从 `/db/backup/{db}/{snapshot}` 快照目录执行恢复，
 * 支持整库快照恢复与单表恢复两种场景，并与备份流程共享互斥锁语义。
 */
class DatabaseBackupRestoreManager {
    /** 大库恢复允许持续两小时，超过后仍必须 TERM/KILL 收口。 */
    protected const RESTORE_TIMEOUT_SECONDS = 7_200.0;

    /** mysqli 客户端兜底连接上限。 */
    protected const MYSQL_CONNECT_TIMEOUT_SECONDS = 15;

    /** mysqli 流式恢复读取上限。 */
    protected const MYSQL_READ_TIMEOUT_SECONDS = 7_200;

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

    /**
     * 数据库长任务外部进程执行器。
     *
     * @var DatabaseBackupProcessRunner
     */
    protected DatabaseBackupProcessRunner $processRunner;

    public function __construct(
        ?DatabaseBackupManager $backupManager = null,
        ?DatabaseBackupCommandResolver $commandResolver = null,
        ?DatabaseBackupProcessRunner $processRunner = null
    ) {
        $this->commandResolver = $commandResolver ?: new DatabaseBackupCommandResolver();
        $this->processRunner = $processRunner ?: new DatabaseBackupProcessRunner();
        $this->backupManager = $backupManager ?: new DatabaseBackupManager(
            $this->commandResolver,
            $this->processRunner
        );
    }

    /**
     * 请求取消当前恢复进程。
     *
     * 保留为控制面可直接复用的能力；runner 会先 TERM，宽限后再 KILL。
     */
    public function requestCancel(): void {
        $this->processRunner->requestCancel();
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

        $this->restoreSqlFilesWithClient($mysql, $server, [$sqlFile]);
    }

    /**
     * 使用 mysql 客户端流式导入一组 SQL 文件。
     *
     * @param string $mysql mysql/mariadb 绝对路径
     * @param array<string, mixed> $server 数据库连接配置
     * @param array<int, string> $sqlFiles SQL 文件列表
     * @param bool $wrapForeignKeyChecks 是否在同一会话外层包裹外键检查开关
     * @return void
     * @throws Exception
     */
    protected function restoreSqlFilesWithClient(
        string $mysql,
        array $server,
        array $sqlFiles,
        bool $wrapForeignKeyChecks = false
    ): void {
        $command = [
            $mysql,
            '--host=' . (string)$server['host'],
            '--port=' . (string)(int)$server['port'],
            '--user=' . (string)$server['username'],
            '--database=' . (string)$server['db_name'],
            '--default-character-set=' . (string)$server['charset'],
        ];
        $result = $this->processRunner->run(
            $command,
            self::RESTORE_TIMEOUT_SECONDS,
            $this->mysqlEnv((string)$server['password']),
            $sqlFiles,
            null,
            $wrapForeignKeyChecks ? "SET FOREIGN_KEY_CHECKS=0;\n" : '',
            $wrapForeignKeyChecks ? "SET FOREIGN_KEY_CHECKS=1;\n" : ''
        );

        if ($result['timed_out']) {
            throw new Exception('恢复超时: 已等待 ' . (int)self::RESTORE_TIMEOUT_SECONDS . ' 秒');
        }
        if ($result['cancelled']) {
            throw new Exception('恢复任务已取消');
        }
        if (!$result['started']) {
            throw new Exception('启动 mysql 失败，无法恢复: ' . trim($result['stderr']));
        }
        if ($result['truncated']) {
            throw new Exception('mysql 恢复输出超过安全上限，任务已终止');
        }
        if ($result['exit_code'] !== 0) {
            throw new Exception('恢复失败: ' . trim((string)($result['stderr'] ?: $result['stdout'])));
        }
    }

    /**
     * 执行整快照恢复。
     *
     * 为了避免外键依赖导致中间阶段失败，全部文件在同一会话中流式导入，并在
     * 会话外层关闭/恢复外键检查。不会再复制一份与快照同体积的临时 SQL。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @param array<int, string> $sqlFiles 快照内 SQL 文件路径
     * @return void
     * @throws Exception
     */
    protected function restoreFullSnapshot(array $server, array $sqlFiles): void {
        $mysql = $this->commandResolver->mysqlOrNull();
        if ($mysql !== '') {
            $this->restoreSqlFilesWithClient($mysql, $server, $sqlFiles, true);
            return;
        }

        $this->restoreSqlFilesViaMysqli($server, $sqlFiles);
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
     * 使用同一 mysqli 会话流式恢复整份快照。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @param array<int, string> $sqlFiles SQL 文件列表
     * @return void
     * @throws Exception
     */
    protected function restoreSqlFilesViaMysqli(array $server, array $sqlFiles): void {
        $mysqli = $this->openMysqliConnection($server);

        try {
            if (!@$mysqli->query('SET FOREIGN_KEY_CHECKS=0')) {
                throw new Exception('关闭外键检查失败: ' . $mysqli->error);
            }
            foreach ($sqlFiles as $sqlFile) {
                foreach ($this->readSqlStatements($sqlFile) as $statement) {
                    if (!@$mysqli->query($statement)) {
                        throw new Exception('恢复失败(' . basename($sqlFile) . '): ' . $mysqli->error);
                    }
                }
            }
        } finally {
            // 恢复失败时也尽力还原当前连接状态；关闭连接后服务端会销毁该 session，
            // 因此这里失败不会污染连接池里的其他连接。
            @$mysqli->query('SET FOREIGN_KEY_CHECKS=1');
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

        @$mysqli->options(MYSQLI_OPT_CONNECT_TIMEOUT, self::MYSQL_CONNECT_TIMEOUT_SECONDS);
        if (defined('MYSQLI_OPT_READ_TIMEOUT')) {
            @$mysqli->options(MYSQLI_OPT_READ_TIMEOUT, self::MYSQL_READ_TIMEOUT_SECONDS);
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

}
