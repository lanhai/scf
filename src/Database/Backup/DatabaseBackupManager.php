<?php

namespace Scf\Database\Backup;

use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Exception;
use Scf\Util\File;
use mysqli;
use mysqli_result;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\System;
use Throwable;

/**
 * 应用数据库备份管理器。
 *
 * 该组件负责读取当前运行环境的数据库配置，按库/时间/表的目录结构导出 SQL，
 * 并在每次成功备份后自动清理旧快照，确保每个数据库最多保留最近 3 次备份。
 */
class DatabaseBackupManager {
    /**
     * 备份根目录（相对 APP_PATH）。
     */
    public const BACKUP_SUB_DIRECTORY = '/db/backup';

    /**
     * 单库最多保留的快照数量。
     */
    public const RETENTION_LIMIT = 3;

    /**
     * 备份互斥锁文件名。
     */
    protected const BACKUP_LOCK_FILE_NAME = '.backup.lock';

    /**
     * 恢复互斥锁文件名。
     */
    protected const RESTORE_LOCK_FILE_NAME = '.restore.lock';

    /**
     * 单库表导出的最大协程并发数。
     *
     * 表级导出之间彼此独立，但同时拉起过多 mysqldump 会把 MySQL 和磁盘 IO 顶满。
     * 这里固定一个保守并发上限，在 Swoole 协程环境下做受控并行。
     */
    protected const TABLE_BACKUP_COROUTINE_LIMIT = 4;

    /**
     * 不参与自动备份的数据库配置别名。
     *
     * `history` 库承接统计历史明细，体量与用途都和业务主库不同。
     * 这里在备份源配置入口直接排除，避免进入后续快照、清理和页面展示链路。
     */
    protected const EXCLUDED_DATABASE_ALIASES = ['history'];

    /**
     * mysql/mysqldump 命令解析器。
     *
     * @var DatabaseBackupCommandResolver
     */
    protected DatabaseBackupCommandResolver $commandResolver;

    public function __construct(?DatabaseBackupCommandResolver $commandResolver = null) {
        $this->commandResolver = $commandResolver ?: new DatabaseBackupCommandResolver();
    }

    /**
     * 返回备份概览数据。
     *
     * @return array<string, mixed>
     * @throws Exception
     */
    public function overview(): array {
        $runtimeDiagnostics = $this->runtimeDiagnostics();
        return [
            'app' => APP_DIR_NAME,
            'env' => SERVER_RUN_ENV,
            'app_src_type' => APP_SRC_TYPE,
            'app_src' => App::src(),
            'database_config_source' => $this->databaseConfigSource(),
            'runtime_diagnostics' => $runtimeDiagnostics,
            'backup_root' => $this->backupRoot(),
            'retention_limit' => self::RETENTION_LIMIT,
            'configured_databases' => array_values(array_map(
                static fn(array $item): array => [
                    'db_name' => (string)$item['db_name'],
                    'aliases' => (array)$item['aliases'],
                    'host' => (string)$item['host'],
                    'port' => (int)$item['port'],
                    'charset' => (string)$item['charset'],
                ],
                $this->databaseServers()
            )),
            'databases' => $this->listBackupSnapshots(),
        ];
    }

    /**
     * 返回当前运行时的数据库备份/恢复执行诊断信息。
     *
     * dashboard 需要把“当前是否有 MySQL Client”以及“备份/恢复会走哪条方案”
     * 直接展示给运维人员，避免在容器环境里看到失败后还要再翻日志确认现场。
     *
     * @return array<string, mixed>
     */
    public function runtimeDiagnostics(): array {
        $mysqlCommand = $this->commandResolver->mysqlOrNull();
        $mysqldumpCommand = $this->commandResolver->mysqldumpOrNull();

        return [
            'mysql_command' => $mysqlCommand,
            'mysqldump_command' => $mysqldumpCommand,
            'mysql_client_available' => $mysqlCommand !== '' && $mysqldumpCommand !== '',
            'mysql_client_status' => match (true) {
                $mysqlCommand !== '' && $mysqldumpCommand !== '' => '完整可用',
                $mysqlCommand !== '' || $mysqldumpCommand !== '' => '部分可用',
                default => '不可用',
            },
            'backup_strategy' => $mysqldumpCommand !== '' ? 'mysqldump' : 'mysqli 直连兜底',
            'restore_strategy' => $mysqlCommand !== '' ? 'mysql client' : 'mysqli 直连兜底',
            'php_mysqli_available' => extension_loaded('mysqli'),
        ];
    }

    /**
     * 执行一次数据库备份。
     *
     * @param array<int, string> $targetDatabases 指定备份的数据库名，空数组表示全部
     * @return array<string, mixed>
     * @throws Exception
     */
    public function runBackup(array $targetDatabases = []): array {
        $targetFilter = [];
        foreach ($targetDatabases as $dbName) {
            $dbName = trim((string)$dbName);
            if ($dbName !== '') {
                // 外部入口可能传数据库别名（default/cp/wx 等），这里统一折叠到真实库名再参与过滤。
                $targetFilter[$this->resolveConfiguredDatabaseName($dbName)] = true;
            }
        }

        return $this->withBackupLock(function () use ($targetFilter): array {
            $servers = $this->databaseServers();
            if (!$servers) {
                throw new Exception('未检测到 database.mysql 配置');
            }

            $timestamp = date('YmdHis');
            $results = [];
            $errors = [];
            $matchedServers = [];
            foreach ($servers as $server) {
                $dbName = (string)$server['db_name'];
                if ($targetFilter && !isset($targetFilter[$dbName])) {
                    continue;
                }
                $matchedServers[$dbName] = true;
                try {
                    $snapshotResult = $this->backupSingleDatabase($server, $timestamp);
                    $cleanupResult = $this->cleanupOldSnapshots($dbName, self::RETENTION_LIMIT);
                    $snapshotResult['cleanup'] = $cleanupResult;
                    $results[] = $snapshotResult;
                } catch (\Throwable $throwable) {
                    $errors[] = [
                        'db_name' => $dbName,
                        'message' => $throwable->getMessage(),
                    ];
                }
            }

            // 只有一个目标库都没命中时，才说明 db_name 参数本身不对。
            // 如果命中了但备份失败，必须把真实导出/连接错误抛给上层，不能误报成未匹配。
            if (!$matchedServers && $targetFilter) {
                throw new Exception($this->buildUnmatchedDatabaseMessage(array_keys($targetFilter), $servers));
            }

            if ($errors) {
                throw new Exception('部分数据库备份失败: ' . json_encode($errors, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
            }

            return [
                'app' => APP_DIR_NAME,
                'env' => SERVER_RUN_ENV,
                'snapshot' => $timestamp,
                'results' => $results,
                'retention_limit' => self::RETENTION_LIMIT,
            ];
        });
    }

    /**
     * 删除备份快照或单表文件。
     *
     * @param string $dbName 数据库名
     * @param string $snapshot 快照目录名（YmdHis）
     * @param string|null $table 指定时只删除单表文件
     * @return array<string, mixed>
     * @throws Exception
     */
    public function deleteBackup(string $dbName, string $snapshot, ?string $table = null): array {
        $dbName = $this->resolveConfiguredDatabaseName($dbName);
        $snapshot = $this->assertSnapshot($snapshot);

        $snapshotDir = $this->snapshotDirectory($dbName, $snapshot);
        if (!is_dir($snapshotDir)) {
            throw new Exception('备份快照不存在: ' . $dbName . '/' . $snapshot);
        }

        if (is_null($table) || trim($table) === '') {
            File::removeDirectory($snapshotDir);
            $dbDir = $this->databaseBackupDirectory($dbName);
            if (is_dir($dbDir) && $this->directoryChildren($dbDir) === []) {
                @rmdir($dbDir);
            }
            return [
                'deleted' => 'snapshot',
                'db_name' => $dbName,
                'snapshot' => $snapshot,
            ];
        }

        $tableFile = $this->assertTableFile($table);
        $targetFile = $snapshotDir . '/' . $tableFile;
        if (!is_file($targetFile)) {
            throw new Exception('表备份文件不存在: ' . $dbName . '/' . $snapshot . '/' . $tableFile);
        }
        if (!@unlink($targetFile)) {
            throw new Exception('删除表备份文件失败: ' . $dbName . '/' . $snapshot . '/' . $tableFile);
        }

        // 删除最后一个表文件后，自动回收空快照目录，保持目录结构整洁。
        if ($this->directoryChildren($snapshotDir) === []) {
            @rmdir($snapshotDir);
        }

        return [
            'deleted' => 'table',
            'db_name' => $dbName,
            'snapshot' => $snapshot,
            'table' => $tableFile,
        ];
    }

    /**
     * 返回备份根目录。
     *
     * @return string
     */
    public function backupRoot(): string {
        return APP_PATH . self::BACKUP_SUB_DIRECTORY;
    }

    /**
     * 返回恢复锁文件路径。
     *
     * @return string
     */
    public function restoreLockFile(): string {
        return $this->backupRoot() . '/' . self::RESTORE_LOCK_FILE_NAME;
    }

    /**
     * 执行恢复互斥锁。
     *
     * @param callable $callback
     * @return mixed
     * @throws Exception
     */
    public function withRestoreLock(callable $callback): mixed {
        return $this->withFileLock($this->restoreLockFile(), $callback, '数据库恢复任务正在执行，请稍后再试');
    }

    /**
     * 返回数据库配置清单（按 db_name 聚合去重）。
     *
     * @return array<string, array<string, mixed>>
     */
    public function databaseServers(): array {
        $database = Config::get('database');
        $mysql = is_array($database) ? (array)($database['mysql'] ?? []) : [];

        $servers = [];
        foreach ($mysql as $alias => $config) {
            if ($this->shouldSkipBackupAlias((string)$alias)) {
                continue;
            }
            if (!is_array($config)) {
                continue;
            }

            $dbName = trim((string)($config['name'] ?? ''));
            $username = trim((string)($config['username'] ?? ''));
            $password = (string)($config['password'] ?? '');
            $port = (int)($config['port'] ?? 3306);
            $masterHost = trim((string)($config['master'] ?? ''));
            $slaveHost = trim((string)($config['slave'] ?? ''));
            $host = $masterHost !== '' ? $masterHost : $slaveHost;
            if ($dbName === '' || $username === '' || $host === '') {
                continue;
            }

            if (!isset($servers[$dbName])) {
                $servers[$dbName] = [
                    'db_name' => $dbName,
                    'aliases' => [(string)$alias],
                    'host' => explode(',', $host)[0],
                    'port' => $port > 0 ? $port : 3306,
                    'username' => $username,
                    'password' => $password,
                    'charset' => trim((string)($config['charset'] ?? 'utf8mb4')) ?: 'utf8mb4',
                ];
                continue;
            }

            if (!in_array((string)$alias, (array)$servers[$dbName]['aliases'], true)) {
                $servers[$dbName]['aliases'][] = (string)$alias;
            }
        }

        ksort($servers);
        return $servers;
    }

    /**
     * 判断当前数据库配置别名是否应跳过自动备份。
     *
     * @param string $alias database.mysql 下的配置别名
     * @return bool
     */
    protected function shouldSkipBackupAlias(string $alias): bool {
        return in_array(trim($alias), self::EXCLUDED_DATABASE_ALIASES, true);
    }

    /**
     * 列出备份快照。
     *
     * @return array<int, array<string, mixed>>
     * @throws Exception
     */
    public function listBackupSnapshots(): array {
        $root = $this->backupRoot();
        if (!is_dir($root)) {
            return [];
        }

        $databases = [];
        foreach ($this->directoryChildren($root) as $dbName) {
            if (str_starts_with($dbName, '.')) {
                continue;
            }
            $dbDir = $root . '/' . $dbName;
            if (!is_dir($dbDir)) {
                continue;
            }

            $snapshots = [];
            foreach ($this->directoryChildren($dbDir) as $snapshotName) {
                $snapshotDir = $dbDir . '/' . $snapshotName;
                if (!is_dir($snapshotDir)) {
                    continue;
                }

                $tableFiles = [];
                $totalSize = 0;
                foreach ($this->directoryChildren($snapshotDir) as $fileName) {
                    $filePath = $snapshotDir . '/' . $fileName;
                    if (!is_file($filePath) || !str_ends_with($fileName, '.sql')) {
                        continue;
                    }
                    $size = (int)(@filesize($filePath) ?: 0);
                    $totalSize += $size;
                    $tableFiles[] = [
                        'table' => $fileName,
                        'size' => $size,
                        'updated_at' => (int)(@filemtime($filePath) ?: 0),
                    ];
                }

                usort($tableFiles, static fn(array $left, array $right): int => strcmp((string)$left['table'], (string)$right['table']));
                $snapshots[] = [
                    'snapshot' => $snapshotName,
                    'created_at' => $this->snapshotCreatedAt($snapshotName, $snapshotDir),
                    'path' => $snapshotDir,
                    'table_count' => count($tableFiles),
                    'total_size' => $totalSize,
                    'tables' => $tableFiles,
                ];
            }

            usort($snapshots, static fn(array $left, array $right): int => strcmp((string)$right['snapshot'], (string)$left['snapshot']));
            $databases[] = [
                'db_name' => $dbName,
                'path' => $dbDir,
                'snapshot_count' => count($snapshots),
                'snapshots' => $snapshots,
            ];
        }

        usort($databases, static fn(array $left, array $right): int => strcmp((string)$left['db_name'], (string)$right['db_name']));
        return $databases;
    }

    /**
     * 执行单库备份。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @param string $timestamp 快照时间戳
     * @return array<string, mixed>
     * @throws Exception
     */
    protected function backupSingleDatabase(array $server, string $timestamp): array {
        $dbName = (string)$server['db_name'];
        $snapshotDir = $this->snapshotDirectory($dbName, $timestamp);
        $this->ensureDirectory($snapshotDir);

        try {
            $tables = $this->fetchDatabaseTables($server);
            if (!$tables) {
                throw new Exception('数据库不存在可备份表: ' . $dbName);
            }

            $mysqldump = $this->commandResolver->mysqldumpOrNull();
            $dumped = $this->dumpTables($mysqldump, $server, $tables, $snapshotDir);
        } catch (Throwable $throwable) {
            if (is_dir($snapshotDir)) {
                File::removeDirectory($snapshotDir);
            }
            throw $throwable;
        }

        return [
            'db_name' => $dbName,
            'snapshot' => $timestamp,
            'snapshot_dir' => $snapshotDir,
            'table_count' => count($dumped),
            'tables' => $dumped,
        ];
    }

    /**
     * 导出单库中的全部表。
     *
     * 在 Swoole 协程环境中，单表导出可以受控并行执行，从而把 mysqldump 的外部进程
     * 等待时间重叠起来；在普通 CLI 环境下则自动退回顺序导出，保持兼容性。
     *
     * @param string $mysqldumpPath mysqldump 路径
     * @param array<string, mixed> $server 数据库连接配置
     * @param array<int, string> $tables 需要导出的表名列表
     * @param string $snapshotDir 快照目录
     * @return array<int, array<string, mixed>>
     * @throws Exception
     */
    protected function dumpTables(string $mysqldumpPath, array $server, array $tables, string $snapshotDir): array {
        if (!$this->canUseSwooleCoroutine() || count($tables) <= 1) {
            return $this->dumpTablesSequentially($mysqldumpPath, $server, $tables, $snapshotDir);
        }

        $concurrency = min(self::TABLE_BACKUP_COROUTINE_LIMIT, count($tables));
        $queue = new Channel(count($tables));
        $results = new Channel(count($tables));

        foreach ($tables as $tableName) {
            $queue->push($tableName);
        }
        $queue->close();

        // 表级导出天然相互独立，适合用受限协程 worker 把外部命令等待时间并行化。
        for ($i = 0; $i < $concurrency; $i++) {
            Coroutine::create(function () use ($queue, $results, $mysqldumpPath, $server, $snapshotDir): void {
                while (true) {
                    $tableName = $queue->pop();
                    if ($tableName === false) {
                        break;
                    }

                    try {
                        $file = $snapshotDir . '/' . $tableName . '.sql';
                        $this->dumpTable($mysqldumpPath, $server, (string)$tableName, $file);
                        $results->push([
                            'ok' => true,
                            'table' => (string)$tableName,
                            'file' => $file,
                            'size' => (int)(@filesize($file) ?: 0),
                        ]);
                    } catch (Throwable $throwable) {
                        $results->push([
                            'ok' => false,
                            'table' => (string)$tableName,
                            'message' => $throwable->getMessage(),
                        ]);
                    }
                }
            });
        }

        $dumped = [];
        $errors = [];
        for ($i = 0, $count = count($tables); $i < $count; $i++) {
            $item = $results->pop();
            if (!is_array($item) || !($item['ok'] ?? false)) {
                $errors[] = [
                    'table' => (string)($item['table'] ?? ''),
                    'message' => (string)($item['message'] ?? '未知导出错误'),
                ];
                continue;
            }

            $dumped[] = [
                'table' => (string)$item['table'],
                'file' => (string)$item['file'],
                'size' => (int)$item['size'],
            ];
        }

        if ($errors) {
            throw new Exception('导出表失败: ' . json_encode($errors, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        }

        usort($dumped, static fn(array $left, array $right): int => strcmp((string)$left['table'], (string)$right['table']));
        return $dumped;
    }

    /**
     * 顺序导出全部表。
     *
     * 该路径作为非协程环境兜底，也用于极小表数时避免并发调度开销超过收益。
     *
     * @param string $mysqldumpPath mysqldump 路径
     * @param array<string, mixed> $server 数据库连接配置
     * @param array<int, string> $tables 表名列表
     * @param string $snapshotDir 快照目录
     * @return array<int, array<string, mixed>>
     * @throws Exception
     */
    protected function dumpTablesSequentially(string $mysqldumpPath, array $server, array $tables, string $snapshotDir): array {
        $dumped = [];
        foreach ($tables as $tableName) {
            $file = $snapshotDir . '/' . $tableName . '.sql';
            $this->dumpTable($mysqldumpPath, $server, $tableName, $file);
            $dumped[] = [
                'table' => $tableName,
                'file' => $file,
                'size' => (int)(@filesize($file) ?: 0),
            ];
        }
        return $dumped;
    }

    /**
     * 查询库内全部表名。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @return array<int, string>
     * @throws Exception
     */
    protected function fetchDatabaseTables(array $server): array {
        $mysql = $this->commandResolver->mysqlOrNull();
        if ($mysql === '') {
            return $this->fetchDatabaseTablesViaMysqli($server);
        }

        $command = implode(' ', [
            escapeshellarg($mysql),
            '--skip-column-names',
            '--batch',
            '--host=' . escapeshellarg((string)$server['host']),
            '--port=' . (int)$server['port'],
            '--user=' . escapeshellarg((string)$server['username']),
            '--database=' . escapeshellarg((string)$server['db_name']),
            '-e',
            escapeshellarg('SHOW TABLES'),
        ]);
        $result = $this->runCommand($command, $this->mysqlEnv((string)$server['password']));
        if ($result['exit_code'] !== 0) {
            throw new Exception('读取表清单失败(' . $server['db_name'] . '): ' . trim($result['stderr'] ?: $result['stdout']));
        }

        $tables = [];
        foreach (preg_split('/\r?\n/', (string)$result['stdout']) as $line) {
            $table = trim((string)$line);
            if ($table !== '') {
                $tables[] = $table;
            }
        }
        return $tables;
    }

    /**
     * 导出单表 SQL 文件。
     *
     * @param string $mysqldumpPath mysqldump 路径
     * @param array<string, mixed> $server 数据库连接配置
     * @param string $tableName 表名
     * @param string $targetFile 输出文件
     * @return void
     * @throws Exception
     */
    protected function dumpTable(string $mysqldumpPath, array $server, string $tableName, string $targetFile): void {
        if ($mysqldumpPath === '') {
            $this->dumpTableViaMysqli($server, $tableName, $targetFile);
            return;
        }

        $command = implode(' ', [
            escapeshellarg($mysqldumpPath),
            '--single-transaction',
            '--quick',
            '--skip-lock-tables',
            '--set-gtid-purged=OFF',
            '--default-character-set=' . escapeshellarg((string)$server['charset']),
            '--host=' . escapeshellarg((string)$server['host']),
            '--port=' . (int)$server['port'],
            '--user=' . escapeshellarg((string)$server['username']),
            escapeshellarg((string)$server['db_name']),
            escapeshellarg($tableName),
        ]);

        if ($this->canUseSwooleCoroutine()) {
            $stderrFile = tempnam(sys_get_temp_dir(), 'scf_db_dump_err_');
            if ($stderrFile === false) {
                throw new Exception('创建 mysqldump 错误输出文件失败');
            }
            try {
                $shellResult = $this->runShellCommand(
                    'MYSQL_PWD=' . escapeshellarg((string)$server['password'])
                    . ' ' . $command
                    . ' > ' . escapeshellarg($targetFile)
                    . ' 2> ' . escapeshellarg($stderrFile)
                );
                $stderr = (string)(@file_get_contents($stderrFile) ?: '');
                if ($shellResult['exit_code'] !== 0) {
                    @unlink($targetFile);
                    throw new Exception('导出表失败(' . $tableName . '): ' . trim($stderr ?: $shellResult['stdout']));
                }
            } finally {
                @unlink($stderrFile);
            }
            return;
        }

        $descriptor = [
            1 => ['file', $targetFile, 'w'],
            2 => ['pipe', 'w'],
        ];
        $process = @proc_open($command, $descriptor, $pipes, null, $this->mysqlEnv((string)$server['password']));
        if (!is_resource($process)) {
            throw new Exception('启动 mysqldump 失败: ' . $tableName);
        }

        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[2]);
        $exitCode = proc_close($process);
        if ($exitCode !== 0) {
            @unlink($targetFile);
            throw new Exception('导出表失败(' . $tableName . '): ' . trim((string)$stderr));
        }
    }

    /**
     * 执行备份互斥锁。
     *
     * @param callable $callback
     * @return mixed
     * @throws Exception
     */
    protected function withBackupLock(callable $callback): mixed {
        $lockFile = $this->backupRoot() . '/' . self::BACKUP_LOCK_FILE_NAME;
        return $this->withFileLock($lockFile, $callback, '数据库备份任务正在执行，请稍后再试');
    }

    /**
     * 执行文件锁保护。
     *
     * @param string $lockFile 锁文件路径
     * @param callable $callback 受保护的业务逻辑
     * @param string $busyMessage 锁冲突提示
     * @return mixed
     * @throws Exception
     */
    protected function withFileLock(string $lockFile, callable $callback, string $busyMessage): mixed {
        $this->ensureDirectory(dirname($lockFile));
        $fp = @fopen($lockFile, 'c+');
        if (!$fp) {
            throw new Exception('创建备份锁失败: ' . $lockFile);
        }

        try {
            if (!flock($fp, LOCK_EX | LOCK_NB)) {
                throw new Exception($busyMessage);
            }
            fwrite($fp, (string)time());
            fflush($fp);
            return $callback();
        } finally {
            @flock($fp, LOCK_UN);
            @fclose($fp);
        }
    }

    /**
     * 清理旧快照，仅保留最近 N 次。
     *
     * @param string $dbName 数据库名
     * @param int $keep 保留数量
     * @return array<string, mixed>
     */
    protected function cleanupOldSnapshots(string $dbName, int $keep): array {
        $dbDir = $this->databaseBackupDirectory($dbName);
        if (!is_dir($dbDir)) {
            return ['removed' => []];
        }

        $snapshots = [];
        foreach ($this->directoryChildren($dbDir) as $item) {
            if (is_dir($dbDir . '/' . $item)) {
                $snapshots[] = $item;
            }
        }
        rsort($snapshots, SORT_STRING);

        $removed = [];
        $toRemove = array_slice($snapshots, max(0, $keep));
        foreach ($toRemove as $snapshot) {
            $path = $dbDir . '/' . $snapshot;
            if (is_dir($path)) {
                File::removeDirectory($path);
                $removed[] = $snapshot;
            }
        }

        return [
            'removed' => $removed,
            'kept' => array_slice($snapshots, 0, $keep),
        ];
    }

    /**
     * 执行外部命令。
     *
     * @param string $command 命令串
     * @param array<string, string> $env 额外环境变量
     * @return array{exit_code:int,stdout:string,stderr:string}
     * @throws Exception
     */
    protected function runCommand(string $command, array $env = []): array {
        if ($this->canUseSwooleCoroutine()) {
            $prefix = '';
            if (isset($env['MYSQL_PWD'])) {
                $prefix = 'MYSQL_PWD=' . escapeshellarg((string)$env['MYSQL_PWD']) . ' ';
            }
            return $this->runShellCommand($prefix . $command . ' 2>&1');
        }

        $descriptor = [
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        $process = @proc_open($command, $descriptor, $pipes, null, $env);
        if (!is_resource($process)) {
            throw new Exception('启动系统命令失败: ' . $command);
        }

        $stdout = (string)stream_get_contents($pipes[1]);
        $stderr = (string)stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $exitCode = proc_close($process);

        return [
            'exit_code' => (int)$exitCode,
            'stdout' => $stdout,
            'stderr' => $stderr,
        ];
    }

    /**
     * 在协程环境中执行 shell 命令。
     *
     * `System::exec()` 会把子进程等待让出给协程调度器，适合当前这种 mysqldump/mysql
     * 外部命令密集的场景。这里统一返回与 proc_open 相近的结构，便于上层复用。
     *
     * @param string $shellCommand shell 命令
     * @return array{exit_code:int,stdout:string,stderr:string}
     * @throws Exception
     */
    protected function runShellCommand(string $shellCommand): array {
        $result = System::exec('/bin/sh -lc ' . escapeshellarg($shellCommand));
        if ($result === false) {
            throw new Exception('执行系统命令失败: ' . $shellCommand);
        }

        return [
            'exit_code' => (int)($result['code'] ?? 1),
            'stdout' => (string)($result['output'] ?? ''),
            'stderr' => '',
        ];
    }

    /**
     * 当前是否处于可安全使用 Swoole 协程系统调用的上下文。
     *
     * 备份能力既可能跑在 Swoole worker/task worker 内，也可能由 Linux crontab
     * 直接触发普通 CLI。只有已经进入协程上下文时，才能启用 `System::exec()` 与
     * Channel 并发调度；否则必须退回同步实现保持兼容。
     *
     * @return bool
     */
    protected function canUseSwooleCoroutine(): bool {
        return class_exists(Coroutine::class)
            && class_exists(System::class)
            && class_exists(Channel::class)
            && Coroutine::getCid() > 0;
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
     * 使用 mysqli 读取当前库中的物理表清单。
     *
     * 命令行客户端在 docker 镜像里并不一定存在，但应用运行本身依赖的 mysqli
     * 扩展通常是可用的。这里回退到数据库直连，避免备份能力被运行镜像缺包阻断。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @return array<int, string>
     * @throws Exception
     */
    protected function fetchDatabaseTablesViaMysqli(array $server): array {
        $mysqli = $this->openMysqliConnection($server);

        try {
            $result = $mysqli->query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");
            if (!$result instanceof mysqli_result) {
                throw new Exception('读取表清单失败(' . $server['db_name'] . '): ' . $mysqli->error);
            }

            $tables = [];
            while ($row = $result->fetch_row()) {
                $table = trim((string)($row[0] ?? ''));
                if ($table !== '') {
                    $tables[] = $table;
                }
            }
            $result->free();
            return $tables;
        } finally {
            $mysqli->close();
        }
    }

    /**
     * 使用 mysqli 直连导出单表 SQL。
     *
     * 结构由 `SHOW CREATE TABLE` 提供，数据通过流式查询按批次拼成 INSERT，
     * 从而在没有 mysqldump 的容器里仍然能产出可恢复的标准 SQL 文件。
     *
     * @param array<string, mixed> $server 数据库连接配置
     * @param string $tableName 表名
     * @param string $targetFile 输出 SQL 文件
     * @return void
     * @throws Exception
     */
    protected function dumpTableViaMysqli(array $server, string $tableName, string $targetFile): void {
        $mysqli = $this->openMysqliConnection($server);
        $fp = @fopen($targetFile, 'wb');
        if (!$fp) {
            $mysqli->close();
            throw new Exception('创建备份文件失败: ' . $targetFile);
        }

        try {
            $quotedTable = $this->quoteIdentifier($tableName);
            $createResult = $mysqli->query('SHOW CREATE TABLE ' . $quotedTable);
            if (!$createResult instanceof mysqli_result) {
                throw new Exception('读取建表语句失败(' . $tableName . '): ' . $mysqli->error);
            }

            $createRow = $createResult->fetch_assoc() ?: [];
            $createResult->free();
            $createSql = (string)($createRow['Create Table'] ?? '');
            if ($createSql === '') {
                throw new Exception('读取建表语句失败(' . $tableName . '): 未返回 Create Table');
            }

            fwrite($fp, "SET FOREIGN_KEY_CHECKS=0;\n");
            fwrite($fp, 'DROP TABLE IF EXISTS ' . $quotedTable . ";\n");
            fwrite($fp, $createSql . ";\n\n");

            $result = $mysqli->query('SELECT * FROM ' . $quotedTable, MYSQLI_USE_RESULT);
            if ($result === false) {
                throw new Exception('读取表数据失败(' . $tableName . '): ' . $mysqli->error);
            }

            $columns = [];
            $batchRows = [];
            while ($row = $result->fetch_assoc()) {
                if (!$columns) {
                    $columns = array_keys($row);
                }

                $values = [];
                foreach ($columns as $column) {
                    $values[] = $this->stringifySqlValue($mysqli, $row[$column] ?? null);
                }
                $batchRows[] = '(' . implode(', ', $values) . ')';

                if (count($batchRows) >= 200) {
                    $this->flushInsertBatch($fp, $tableName, $columns, $batchRows);
                    $batchRows = [];
                }
            }
            $result->free();

            if ($batchRows) {
                $this->flushInsertBatch($fp, $tableName, $columns, $batchRows);
            }

            fwrite($fp, "SET FOREIGN_KEY_CHECKS=1;\n");
        } catch (Throwable $throwable) {
            @unlink($targetFile);
            throw $throwable;
        } finally {
            fclose($fp);
            $mysqli->close();
        }
    }

    /**
     * 创建一条 mysqli 连接。
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
     * 刷出一批 INSERT 语句。
     *
     * @param resource $fp 文件句柄
     * @param string $tableName 表名
     * @param array<int, string> $columns 字段列表
     * @param array<int, string> $rows 已拼好的 VALUES 列表
     * @return void
     * @throws Exception
     */
    protected function flushInsertBatch($fp, string $tableName, array $columns, array $rows): void {
        if (!$rows) {
            return;
        }

        $columnSql = implode(', ', array_map(fn(string $column): string => $this->quoteIdentifier($column), $columns));
        $sql = 'INSERT INTO ' . $this->quoteIdentifier($tableName)
            . ' (' . $columnSql . ') VALUES ' . implode(",\n", $rows) . ";\n";
        if (@fwrite($fp, $sql) === false) {
            throw new Exception('写入表数据失败: ' . $tableName);
        }
    }

    /**
     * 把 PHP 值转成 SQL 字面量。
     *
     * @param mysqli $mysqli 数据库连接
     * @param mixed $value 字段值
     * @return string
     */
    protected function stringifySqlValue(mysqli $mysqli, mixed $value): string {
        if ($value === null) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        return "'" . $mysqli->real_escape_string((string)$value) . "'";
    }

    /**
     * 为 SQL 标识符加反引号。
     *
     * @param string $identifier 标识符
     * @return string
     */
    protected function quoteIdentifier(string $identifier): string {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    /**
     * 返回数据库备份目录。
     *
     * @param string $dbName 数据库名
     * @return string
     */
    public function databaseBackupDirectory(string $dbName): string {
        return $this->backupRoot() . '/' . $dbName;
    }

    /**
     * 返回快照目录。
     *
     * @param string $dbName 数据库名
     * @param string $snapshot 快照目录名
     * @return string
     */
    public function snapshotDirectory(string $dbName, string $snapshot): string {
        return $this->databaseBackupDirectory($dbName) . '/' . $snapshot;
    }

    /**
     * 校验 db_name。
     *
     * @param string $dbName
     * @return string
     * @throws Exception
     */
    public function assertDbName(string $dbName): string {
        $dbName = trim($dbName);
        if (!preg_match('/^[a-zA-Z0-9_]+$/', $dbName)) {
            throw new Exception('db_name 非法');
        }
        return $dbName;
    }

    /**
     * 把外部传入的数据库标识归一化为当前环境里的真实库名。
     *
     * backup/restore/dashboard 入口可能传真实库名，也可能传 database.mysql 的配置别名。
     * 这里统一折叠到最终 `name` 字段，避免同一个物理数据库因为别名不同而匹配失败。
     *
     * @param string $dbName 数据库名或 database.mysql 别名
     * @return string
     * @throws Exception
     */
    public function resolveConfiguredDatabaseName(string $dbName): string {
        $dbName = $this->assertDbName($dbName);
        $servers = $this->databaseServers();
        if (isset($servers[$dbName])) {
            return $dbName;
        }

        foreach ($servers as $configuredDbName => $server) {
            if (in_array($dbName, (array)($server['aliases'] ?? []), true)) {
                return (string)$configuredDbName;
            }
        }

        return $dbName;
    }

    /**
     * 推断当前运行时实际加载的数据库配置文件。
     *
     * 配置始终按 `SERVER_RUN_ENV` 叠加，但源码来源可能是目录版或 phar 版。
     * 这里返回最终应命中的 env 配置文件路径，方便 dashboard 直接展示现场信息。
     *
     * @return string
     */
    public function databaseConfigSource(): string {
        $src = App::src();
        $env = strtolower((string)SERVER_RUN_ENV);
        $candidates = [
            $src . '/config/app_' . $env . '.yml',
            $src . '/config/app_' . $env . '.php',
            $src . '/config/app.yml',
            $src . '/config/app.php',
        ];

        foreach ($candidates as $candidate) {
            if (is_file($candidate)) {
                return $candidate;
            }
        }

        return $src . '/config/app_' . $env . '.php';
    }

    /**
     * 生成目标数据库未匹配时的诊断消息。
     *
     * 这类错误最常见的原因是运行中的代码没有更新、当前环境配置源和预期不一致，
     * 或者外部入口传的是别名。报错里附带当前配置源和已识别数据库清单，便于现场排查。
     *
     * @param array<int, string> $targets 外部请求的目标数据库标识
     * @param array<string, array<string, mixed>> $servers 当前配置识别出的数据库清单
     * @return string
     */
    protected function buildUnmatchedDatabaseMessage(array $targets, array $servers): string {
        $configured = [];
        foreach ($servers as $dbName => $server) {
            $aliases = array_values(array_filter((array)($server['aliases'] ?? [])));
            $configured[] = $aliases ? $dbName . '(' . implode(',', $aliases) . ')' : $dbName;
        }

        return '未匹配到目标数据库，请检查 db_name 参数。'
            . ' target=' . implode(',', $targets)
            . '; env=' . SERVER_RUN_ENV
            . '; src_type=' . APP_SRC_TYPE
            . '; config=' . $this->databaseConfigSource()
            . '; configured=' . implode('|', $configured);
    }

    /**
     * 校验快照目录名。
     *
     * @param string $snapshot
     * @return string
     * @throws Exception
     */
    public function assertSnapshot(string $snapshot): string {
        $snapshot = trim($snapshot);
        if (!preg_match('/^\d{14}$/', $snapshot)) {
            throw new Exception('snapshot 非法，格式应为 YmdHis');
        }
        return $snapshot;
    }

    /**
     * 校验表 SQL 文件名。
     *
     * @param string $table
     * @return string
     * @throws Exception
     */
    public function assertTableFile(string $table): string {
        $table = trim($table);
        if ($table === '') {
            throw new Exception('table 不能为空');
        }
        if (!str_ends_with($table, '.sql')) {
            $table .= '.sql';
        }
        if (!preg_match('/^[a-zA-Z0-9_]+\.sql$/', $table)) {
            throw new Exception('table 非法');
        }
        return $table;
    }

    /**
     * 计算快照创建时间。
     *
     * @param string $snapshot 快照目录名
     * @param string $snapshotDir 快照目录路径
     * @return int
     */
    protected function snapshotCreatedAt(string $snapshot, string $snapshotDir): int {
        $time = strtotime($snapshot);
        if ($time !== false) {
            return $time;
        }
        return (int)(@filemtime($snapshotDir) ?: 0);
    }

    /**
     * 读取目录直接子项列表。
     *
     * @param string $dir
     * @return array<int, string>
     */
    protected function directoryChildren(string $dir): array {
        $items = @scandir($dir);
        if (!is_array($items)) {
            return [];
        }
        return array_values(array_filter($items, static fn(string $item): bool => $item !== '.' && $item !== '..'));
    }

    /**
     * 确保目录存在。
     *
     * @param string $dir
     * @return void
     * @throws Exception
     */
    protected function ensureDirectory(string $dir): void {
        if (is_dir($dir)) {
            return;
        }
        if (!@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new Exception('创建目录失败: ' . $dir);
        }
    }
}
