<?php

namespace Scf\Core;

use Swoole\Process;
use Throwable;

abstract class Server {

    protected static Server $_SERVER;
    protected static array $_instances = [];

    abstract public static function create($role, string $host = '0.0.0.0', int $port = 9580);

    /**
     * 获取单例
     * @return static
     * @throws Exception
     */
    public static function instance(): static {
        $class = static::class;
        if (!isset(self::$_instances[$class])) {
            throw new Exception('尚未创建服务器对象');
        }
        return self::$_instances[$class];
    }

    public static function kill($pid, $try = 0): bool {
        if ($try >= 10) {
            return false;
        }
        if (Process::kill($pid, 0)) {
            $try++;
            Process::kill($pid, SIGKILL);
            sleep(1);
            return self::kill($pid, $try);
        }
        return true;
    }

    public static function killall($port, $try = 0): bool {
        if ($try >= 3) {
            if (self::killProcessByPort($port)) {
                return true;
            }
            return false;
        }
        if (self::isPortInUse($port)) {
            $try++;
            //exec("killall php");
            self::killProcessByPort($port);
            sleep(1);
            return self::killall($port, $try);
        }
        return true;
    }

    /**
     * 获取监听指定 TCP 端口的进程 PID 列表（兼容 Linux 容器 & Linux/macOS 宿主机）
     * 优先使用 /proc 扫描（容器最稳），其次尝试 lsof，最后尝试 ss。
     */
    public static function findPidsByPort(int $port): array {
        $seen = [];
        // 方式一：/proc 扫描（Linux/容器）
        foreach (self::findPidsByPortProcfs($port) as $pid) {
            $seen[$pid] = true;
        }
        // 方式二：lsof（macOS/Linux 常见）
        foreach (self::findPidsByPortLsof($port) as $pid) {
            $seen[$pid] = true;
        }
        // 方式三：ss（Linux 常见）
        foreach (self::findPidsByPortSs($port) as $pid) {
            $seen[$pid] = true;
        }
        // 过滤当前进程
        $me = function_exists('getmypid') ? getmypid() : 0;
        unset($seen[$me]);
        return array_map('intval', array_keys($seen));
    }

    /**
     * 通过 /proc 扫描查找监听端口的进程（仅 Linux）。
     */
    protected static function findPidsByPortProcfs(int $port): array {
        if (!is_dir('/proc') || !is_readable('/proc/net/tcp')) return [];
        $inodes = [];
        $scan = function (string $path) use (&$inodes, $port) {
            $fh = @fopen($path, 'r');
            if (!$fh) return;
            fgets($fh); // skip header
            while (($line = fgets($fh)) !== false) {
                $line = trim($line);
                if ($line === '') continue;
                $cols = preg_split('/\s+/', $line);
                if (!isset($cols[1], $cols[3], $cols[9])) continue;
                $local = $cols[1];      // e.g. 0100007F:1F90
                $st = strtoupper($cols[3]); // 0A == LISTEN
                $inode = $cols[9];
                if ($st !== '0A') continue; // not LISTEN
                $parts = explode(':', $local);
                if (count($parts) !== 2) continue;
                $p = hexdec($parts[1]);
                if ($p === $port) {
                    $inodes[$inode] = true;
                }
            }
            fclose($fh);
        };
        $scan('/proc/net/tcp');
        if (is_readable('/proc/net/tcp6')) $scan('/proc/net/tcp6');
        if (!$inodes) return [];
        $pids = [];
        foreach (glob('/proc/[0-9]*/fd/*') as $fd) {
            $link = @readlink($fd);
            if ($link === false) continue;
            if (preg_match('/socket:\[(\d+)\]/', $link, $m)) {
                $inode = $m[1];
                if (isset($inodes[$inode])) {
                    if (preg_match('#^/proc/(\d+)/fd/\d+$#', $fd, $pm)) {
                        $pid = (int)$pm[1];
                        if ($pid > 0) $pids[$pid] = true;
                    }
                }
            }
        }
        return array_map('intval', array_keys($pids));
    }

    /**
     * 通过 lsof 查找（macOS/Linux）。
     */
    protected static function findPidsByPortLsof(int $port): array {
        $bin = @trim((string)@shell_exec('command -v lsof'));
        if ($bin === '') return [];
        $out = [];
        @exec("{$bin} -nP -iTCP:{$port} -sTCP:LISTEN 2>/dev/null", $out);
        if (!$out) return [];
        $pids = [];
        foreach ($out as $i => $line) {
            if ($i === 0 && stripos($line, 'PID') !== false) continue; // 跳过表头
            $cols = preg_split('/\s+/', trim($line));
            if (isset($cols[1]) && ctype_digit($cols[1])) {
                $pid = (int)$cols[1];
                if ($pid > 0) $pids[$pid] = true;
            }
        }
        return array_map('intval', array_keys($pids));
    }

    /**
     * 通过 ss 查找（多数 Linux）。
     */
    protected static function findPidsByPortSs(int $port): array {
        $bin = @trim((string)@shell_exec('command -v ss'));
        if ($bin === '') return [];
        $out = [];
        @exec("{$bin} -lntp 2>/dev/null | grep -w ':{$port}'", $out);
        if (!$out) return [];
        $pids = [];
        foreach ($out as $line) {
            if (preg_match('/pid=(\d+)/', $line, $m)) {
                $pid = (int)$m[1];
                if ($pid > 0) $pids[$pid] = true;
            }
        }
        return array_map('intval', array_keys($pids));
    }

    /**
     * 向控制台输出消息
     * @param string $str
     */
    public function log(string $str): void {
        Console::info("【Server】" . $str, false);
    }

    /**
     * 获取可用端口
     * @param $port
     * @return int
     */
    public static function getUseablePort($port): int {
        if (self::isPortInUse($port)) {
            return self::getUseablePort($port + 1);
        }
        return $port;
    }

    /**
     * 检查端口号是否被占用
     * @param int $port
     * @return bool
     */
    public static function isPortInUse(int $port): bool {
        try {
            $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
            if ($socket === false) {
                return false;
            }
            $result = @socket_bind($socket, '0.0.0.0', $port);
            socket_close($socket);
            if ($result === false) {
                return true;
            }
            return false;
        } catch (Throwable) {
            return true;
        }
    }

    public static function killProcessByPort(int $port): bool {
        $pids = self::findPidsByPort($port);
        if (!$pids) {
            Console::info("【Server】没有进程占用端口:$port");
            return true;
        }
        foreach ($pids as $pid) {
            Console::info("【Server】结束进程 $pid 占用端口:$port");
            // 先 TERM 再 KILL（更安全）
            @Process::kill($pid, SIGTERM);
        }
        // 等待最多 3 秒
        $deadline = time() + 3;
        while (time() < $deadline) {
            $alive = false;
            foreach ($pids as $pid) {
                if (@Process::kill($pid, 0)) {
                    $alive = true;
                    break;
                }
            }
            if (!$alive) break;
            usleep(200 * 1000);
        }
        foreach ($pids as $pid) {
            if (@Process::kill($pid, 0)) {
                @Process::kill($pid, SIGKILL);
            }
        }
        return true;
    }

}