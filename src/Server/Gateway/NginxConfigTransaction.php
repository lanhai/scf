<?php

namespace Scf\Server\Gateway;

use RuntimeException;
use Swoole\Coroutine;

/**
 * 同一 nginx 主配置的跨进程写入事务。
 *
 * 锁、临时文件与退役备份放在 include 目录之外，兼容 `include servers/*`。
 * 单文件用 rename 发布；整个 write/test/reload/verify 序列串行执行，失败恢复原文件。
 * nginx worker 的连接排空仍由 nginx 原生 HUP 机制负责。
 */
final class NginxConfigTransaction {
    private mixed $lock;
    private array $originals = [];
    private string $workDir;

    /**
     * 取得 include 目录的串行配置写入权。
     * @param string $confDir nginx 的 include 目录。
     * @throws RuntimeException 锁或目录不可用。
     */
    public function __construct(string $confDir) {
        $confDir = realpath($confDir) ?: rtrim($confDir, '/');
        $this->workDir = dirname($confDir) . '/.scf-nginx-' . substr(hash('sha256', $confDir), 0, 16);
        if (!is_dir($this->workDir) && !@mkdir($this->workDir, 0700, true) && !is_dir($this->workDir)) {
            throw new RuntimeException('创建 nginx 事务目录失败: ' . $this->workDir);
        }
        $this->lock = @fopen($this->workDir . '/sync.lock', 'c');
        if (!is_resource($this->lock)) throw new RuntimeException('打开 nginx 同步锁失败');
        $deadline = microtime(true) + 15;
        while (!flock($this->lock, LOCK_EX | LOCK_NB)) {
            if (microtime(true) >= $deadline) {
                fclose($this->lock);
                throw new RuntimeException('等待 nginx 配置同步锁超时');
            }
            self::pause();
        }
    }

    /** @return void 协程内主动让出调度；启动前的普通 CLI 不依赖事件循环。 */
    public static function pause(): void {
        if (class_exists(Coroutine::class, false) && Coroutine::getCid() >= 0) {
            Coroutine::sleep(0.05);
        } else {
            usleep(50000);
        }
    }

    /**
     * 记录原内容并原子替换单个配置文件。
     * @param string $path 目标文件。
     * @param string $content 新内容。
     * @return bool 是否变化。
     * @throws RuntimeException 读取或原子替换失败。
     */
    public function write(string $path, string $content): bool {
        $this->remember($path);
        if (is_file($path) && file_get_contents($path) === $content) return false;
        $this->replace($path, $content);
        return true;
    }

    /**
     * 在 include 目录外保留原文件的可恢复备份。
     * @param string $path 已核实退役或被本端口替换的文件。
     * @return bool 是否已隔离。
     * @throws RuntimeException 备份或移除失败。
     */
    public function quarantine(string $path): bool {
        if (!is_file($path)) return false;
        $this->remember($path);
        $backup = $this->workDir . '/' . basename($path) . '.' . bin2hex(random_bytes(6)) . '.bak';
        if (!@copy($path, $backup) || !@unlink($path)) {
            throw new RuntimeException('隔离历史 nginx 配置失败: ' . $path);
        }
        return true;
    }

    /**
     * 恢复本事务修改的配置。
     * @return void
     * @throws RuntimeException 无法恢复原文件时中止并报告。
     */
    public function rollback(): void {
        foreach (array_reverse($this->originals, true) as $path => $original) {
            if ($original['content'] === null) {
                if (is_file($path) && !@unlink($path)) throw new RuntimeException('回滚 nginx 文件失败: ' . $path);
            } else {
                $this->replace($path, $original['content'], $original['mode']);
            }
        }
    }

    /** @return void 必须在 finally 中释放跨进程锁。 */
    public function close(): void {
        if (is_resource($this->lock)) {
            flock($this->lock, LOCK_UN);
            fclose($this->lock);
        }
    }

    private function remember(string $path): void {
        if (array_key_exists($path, $this->originals)) return;
        $content = is_file($path) ? file_get_contents($path) : null;
        if ($content === false) throw new RuntimeException('读取 nginx 原配置失败: ' . $path);
        $this->originals[$path] = ['content' => $content, 'mode' => is_file($path) ? (fileperms($path) & 0777) : 0644];
    }

    private function replace(string $path, string $content, ?int $mode = null): void {
        $temp = tempnam($this->workDir, 'config-');
        if ($temp === false) throw new RuntimeException('创建 nginx 临时文件失败');
        try {
            if (file_put_contents($temp, $content) !== strlen($content)
                || !chmod($temp, $mode ?? ($this->originals[$path]['mode'] ?? 0644))
                || !rename($temp, $path)) {
                throw new RuntimeException('原子写入 nginx 配置失败: ' . $path);
            }
        } finally {
            if (is_file($temp)) unlink($temp);
        }
    }
}
