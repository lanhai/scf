<?php

namespace Scf\Database\Backup;

use Scf\Core\Exception;

/**
 * 数据库备份命令解析器。
 *
 * 该组件负责在运行态解析 `mysql` / `mysqldump` 的可执行路径，
 * 统一处理守护进程 PATH 精简场景，避免上层业务重复写命令探测逻辑。
 */
class DatabaseBackupCommandResolver {
    protected const MYSQL_BINARIES = ['mysql', 'mariadb'];
    protected const MYSQL_DUMP_BINARIES = ['mysqldump', 'mariadb-dump'];

    /**
     * mysql 可执行路径缓存。
     *
     * @var string|null
     */
    protected ?string $mysqlPath = null;

    /**
     * mysqldump 可执行路径缓存。
     *
     * @var string|null
     */
    protected ?string $mysqldumpPath = null;

    /**
     * 返回 mysql 命令路径。
     *
     * @return string
     * @throws Exception
     */
    public function mysql(): string {
        $path = $this->resolveMysqlPath();
        if ($path === '') {
            throw new Exception('系统未找到 mysql/mariadb 命令，请先安装 MySQL Client');
        }
        return $path;
    }

    /**
     * 返回 mysql 命令路径；未找到时返回空字符串而不是抛异常。
     *
     * backup/restore 在 docker 运行时允许回退到 PHP 数据库扩展实现，
     * 因此上层需要一个“可探测但不抛错”的接口来决定是否走命令行分支。
     *
     * @return string
     */
    public function mysqlOrNull(): string {
        return $this->resolveMysqlPath();
    }

    /**
     * 返回 mysqldump 命令路径。
     *
     * @return string
     * @throws Exception
     */
    public function mysqldump(): string {
        $path = $this->resolveMysqldumpPath();
        if ($path === '') {
            throw new Exception('系统未找到 mysqldump/mariadb-dump 命令，请先安装 MySQL Client');
        }
        return $path;
    }

    /**
     * 返回 mysqldump 命令路径；未找到时返回空字符串而不是抛异常。
     *
     * @return string
     */
    public function mysqldumpOrNull(): string {
        return $this->resolveMysqldumpPath();
    }

    /**
     * 解析 mysql 命令路径。
     *
     * @return string
     */
    protected function resolveMysqlPath(): string {
        if (!is_null($this->mysqlPath)) {
            return $this->mysqlPath;
        }
        $this->mysqlPath = $this->resolveCommandAliases(self::MYSQL_BINARIES);
        return $this->mysqlPath;
    }

    /**
     * 解析 mysqldump 命令路径。
     *
     * @return string
     */
    protected function resolveMysqldumpPath(): string {
        if (!is_null($this->mysqldumpPath)) {
            return $this->mysqldumpPath;
        }
        $this->mysqldumpPath = $this->resolveCommandAliases(self::MYSQL_DUMP_BINARIES);
        return $this->mysqldumpPath;
    }

    /**
     * 依次尝试一组等价命令名。
     *
     * 部分 docker 镜像内安装的是 MariaDB Client，二进制名会变成
     * `mariadb` / `mariadb-dump`，这里统一做兼容探测。
     *
     * @param array<int, string> $binaryNames
     * @return string
     */
    protected function resolveCommandAliases(array $binaryNames): string {
        foreach ($binaryNames as $binaryName) {
            $resolved = $this->resolveCommand($binaryName);
            if ($resolved !== '') {
                return $resolved;
            }
        }

        return '';
    }

    /**
     * 解析系统命令绝对路径。
     *
     * 先尝试 PATH，再兜底常见目录，兼容常驻进程环境变量不完整场景。
     *
     * @param string $binaryName 命令名
     * @return string
     */
    protected function resolveCommand(string $binaryName): string {
        $resolved = trim((string)@shell_exec('command -v ' . escapeshellarg($binaryName) . ' 2>/dev/null'));
        if ($resolved !== '' && is_executable($resolved)) {
            return $resolved;
        }

        $candidates = [
            '/usr/local/bin/' . $binaryName,
            '/usr/bin/' . $binaryName,
            '/bin/' . $binaryName,
            '/usr/sbin/' . $binaryName,
            '/opt/homebrew/bin/' . $binaryName,
        ];

        foreach ($candidates as $candidate) {
            if (is_executable($candidate)) {
                return $candidate;
            }
        }

        return '';
    }
}
