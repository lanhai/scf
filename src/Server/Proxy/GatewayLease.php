<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Swoole\Coroutine;

/**
 * Gateway 控制面租约仓储。
 *
 * 这个组件负责把 gateway 的 owner epoch、TTL 与状态（running/restarting/stopped）
 * 持久化到单机文件，供独立的 upstream 进程进行跨进程租约校验。
 *
 * 设计目标：
 * - gateway 可周期续租，异常退出时租约自动过期；
 * - 控制面重启时可在短窗口内复用 epoch，避免误伤业务实例；
 * - 新 owner 抢占后，旧 owner 不允许再覆盖更高 epoch 的租约状态。
 */
class GatewayLease {

    /**
     * 在 gateway 启动阶段创建/刷新租约，并返回当前 owner epoch。
     *
     * 当上一代 gateway 显式写入 `restarting` 时，本轮会尽量复用上一代 epoch：
     * - restarting 尚未过期：直接复用；
     * - restarting 刚过期但仍处在可恢复窗口：继续复用，避免“计划重启稍慢一点就换 epoch”。
     *
     * 其它场景（首次启动、stopped、running、超出恢复窗口）都会推进到新 epoch。
     *
     * @param int $gatewayPort gateway 业务端口。
     * @param string $role 节点角色。
     * @param int $ttlSeconds running 状态 TTL。
     * @param array<string, mixed> $meta 附加元数据。支持可选键 `restart_reuse_grace`（秒）。
     * @return array<string, mixed> 已落盘的租约内容。
     */
    public static function issueStartupLease(int $gatewayPort, string $role, int $ttlSeconds, array $meta = []): array {
        if ($gatewayPort <= 0) {
            return [];
        }
        $ttlSeconds = max(1, $ttlSeconds);
        return self::withExclusiveLeaseLock($gatewayPort, $role, function (string $stateFile) use ($gatewayPort, $role, $ttlSeconds, $meta): array {
            $now = time();
            $current = self::readLeaseFromFile($stateFile);
            $currentEpoch = (int)($current['epoch'] ?? 0);
            $state = (string)($current['state'] ?? '');
            $expiresAt = (int)($current['expires_at'] ?? 0);
            $restartReuseGrace = max(0, (int)($meta['restart_reuse_grace'] ?? 0));
            $reuseByUnexpiredRestart = $currentEpoch > 0 && $state === 'restarting' && $expiresAt >= $now;
            $reuseByGraceWindow = $currentEpoch > 0
                && $state === 'restarting'
                && $expiresAt > 0
                && $expiresAt < $now
                && $restartReuseGrace > 0
                && ($now - $expiresAt) <= $restartReuseGrace;
            $reuseEpoch = $reuseByUnexpiredRestart || $reuseByGraceWindow;
            $epoch = $reuseEpoch ? $currentEpoch : max(1, $currentEpoch + 1);
            $leaseMeta = $meta;
            unset($leaseMeta['restart_reuse_grace']);

            $payload = self::buildLeasePayload(
                $epoch,
                $gatewayPort,
                $role,
                'running',
                $ttlSeconds,
                $leaseMeta + [
                    'reused_epoch' => $reuseEpoch,
                    'reused_epoch_after_expiry' => $reuseByGraceWindow,
                ]
            );
            self::writeLease($stateFile, $payload);
            return $payload;
        }, []);
    }

    /**
     * 续租或更新 gateway 租约状态。
     *
     * 该方法会拒绝旧 epoch 覆盖当前更高 epoch，避免旧控制面在新 owner 已接管后
     * 把租约错误改回旧状态。
     *
     * @param int $gatewayPort gateway 业务端口。
     * @param string $role 节点角色。
     * @param int $epoch 当前 owner epoch。
     * @param string $state running/restarting/stopped。
     * @param int $ttlSeconds 状态 TTL；stopped 会被自动收敛为 0。
     * @param array<string, mixed> $meta 附加元数据。
     * @return bool 写入成功时返回 true。
     */
    public static function renewLease(
        int $gatewayPort,
        string $role,
        int $epoch,
        string $state,
        int $ttlSeconds,
        array $meta = []
    ): bool {
        if ($gatewayPort <= 0 || $epoch <= 0) {
            return false;
        }
        $state = in_array($state, ['running', 'restarting', 'stopped'], true) ? $state : 'running';
        $ttlSeconds = $state === 'stopped' ? 0 : max(1, $ttlSeconds);

        return self::withExclusiveLeaseLock($gatewayPort, $role, function (string $stateFile) use ($gatewayPort, $role, $epoch, $state, $ttlSeconds, $meta): bool {
            $current = self::readLeaseFromFile($stateFile);
            $currentEpoch = (int)($current['epoch'] ?? 0);
            if ($currentEpoch > 0 && $epoch < $currentEpoch) {
                return false;
            }
            if ($currentEpoch > 0 && $epoch !== $currentEpoch) {
                return false;
            }

            $payload = self::buildLeasePayload($epoch, $gatewayPort, $role, $state, $ttlSeconds, $meta);
            self::writeLease($stateFile, $payload);
            return true;
        }, false);
    }

    /**
     * 读取当前租约。
     *
     * @param int $gatewayPort gateway 业务端口。
     * @param string $role 节点角色。
     * @return array<string, mixed>|null
     */
    public static function readLease(int $gatewayPort, string $role): ?array {
        if ($gatewayPort <= 0) {
            return null;
        }
        $stateFile = self::leaseStateFile($gatewayPort, $role);
        return self::readLeaseFromFile($stateFile);
    }

    /**
     * 返回租约状态文件路径。
     *
     * @param int $gatewayPort gateway 业务端口。
     * @param string $role 节点角色。
     * @return string
     */
    public static function leaseStateFile(int $gatewayPort, string $role): string {
        $port = max(1, $gatewayPort);
        $safeRole = preg_replace('/[^a-zA-Z0-9_-]+/', '_', $role !== '' ? $role : SERVER_ROLE);
        $safeApp = preg_replace('/[^a-zA-Z0-9_-]+/', '_', APP_DIR_NAME ?: 'app');
        $safeEnv = preg_replace('/[^a-zA-Z0-9_-]+/', '_', SERVER_RUN_ENV ?: 'prod');
        $baseDir = defined('APP_UPDATE_DIR') && APP_UPDATE_DIR
            ? APP_UPDATE_DIR
            : (sys_get_temp_dir() . '/scf_gateway_lease');

        return rtrim($baseDir, '/\\') . "/gateway_lease_{$safeApp}_{$safeEnv}_{$safeRole}_{$port}.json";
    }

    /**
     * 组装规范化租约 payload。
     *
     * @param int $epoch owner epoch。
     * @param int $gatewayPort gateway 业务端口。
     * @param string $role 节点角色。
     * @param string $state 租约状态。
     * @param int $ttlSeconds TTL。
     * @param array<string, mixed> $meta 额外元数据。
     * @return array<string, mixed>
     */
    protected static function buildLeasePayload(
        int $epoch,
        int $gatewayPort,
        string $role,
        string $state,
        int $ttlSeconds,
        array $meta
    ): array {
        $now = time();
        return [
            'epoch' => max(1, $epoch),
            'gateway_port' => max(1, $gatewayPort),
            'role' => $role !== '' ? $role : SERVER_ROLE,
            'state' => $state,
            'ttl_seconds' => $state === 'stopped' ? 0 : max(1, $ttlSeconds),
            'heartbeat_at' => $now,
            'expires_at' => $state === 'stopped' ? $now : ($now + max(1, $ttlSeconds)),
            'updated_at' => $now,
            'host' => SERVER_HOST,
            'app' => APP_DIR_NAME,
            'env' => SERVER_RUN_ENV,
            'meta' => $meta,
        ];
    }

    /**
     * 读取并解析租约文件。
     *
     * @param string $stateFile 租约文件路径。
     * @return array<string, mixed>|null
     */
    protected static function readLeaseFromFile(string $stateFile): ?array {
        if (!is_file($stateFile)) {
            return null;
        }
        $raw = @file_get_contents($stateFile);
        if (!is_string($raw) || trim($raw) === '') {
            return null;
        }
        $decoded = json_decode($raw, true);
        return is_array($decoded) ? $decoded : null;
    }

    /**
     * 写租约文件（原子替换）。
     *
     * @param string $stateFile 目标租约文件路径。
     * @param array<string, mixed> $payload 租约数据。
     * @return void
     */
    protected static function writeLease(string $stateFile, array $payload): void {
        $dir = dirname($stateFile);
        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new RuntimeException('创建 gateway lease 目录失败:' . $dir);
        }
        $json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
        if ($json === false) {
            throw new RuntimeException('gateway lease 编码失败');
        }
        $tmp = $stateFile . '.tmp.' . getmypid();
        if (@file_put_contents($tmp, $json, LOCK_EX) === false) {
            @unlink($tmp);
            throw new RuntimeException('写入 gateway lease 临时文件失败:' . $stateFile);
        }
        if (!@rename($tmp, $stateFile)) {
            @unlink($tmp);
            throw new RuntimeException('替换 gateway lease 文件失败:' . $stateFile);
        }
    }

    /**
     * 在独占锁内执行租约读写。
     *
     * @template T
     * @param int $gatewayPort gateway 业务端口。
     * @param string $role 节点角色。
     * @param callable(string):T $callback 锁内执行逻辑，参数为 stateFile 路径。
     * @param T $fallback 失败时回退值。
     * @return T
     */
    protected static function withExclusiveLeaseLock(int $gatewayPort, string $role, callable $callback, mixed $fallback): mixed {
        $stateFile = self::leaseStateFile($gatewayPort, $role);
        // gateway lease 续租定时器运行在 worker 协程上下文时，`fopen + flock(LOCK_EX)` 可能触发
        // “all coroutines are asleep - deadlock”。协程场景下直接走无锁回调，避免控制面心跳被锁阻塞。
        if (class_exists(Coroutine::class) && Coroutine::getCid() > 0) {
            try {
                return $callback($stateFile);
            } catch (\Throwable) {
                return $fallback;
            }
        }

        $lockFile = $stateFile . '.lock';
        $lockDir = dirname($lockFile);
        if (!is_dir($lockDir) && !@mkdir($lockDir, 0775, true) && !is_dir($lockDir)) {
            return $fallback;
        }
        $lockFp = @fopen($lockFile, 'c+');
        if (!is_resource($lockFp)) {
            return $fallback;
        }
        try {
            if (!self::acquireExclusiveLock($lockFp)) {
                return $fallback;
            }
            return $callback($stateFile);
        } catch (\Throwable) {
            return $fallback;
        } finally {
            @flock($lockFp, LOCK_UN);
            @fclose($lockFp);
        }
    }

    /**
     * 尝试获取租约文件排它锁（非阻塞重试）。
     *
     * @param resource $lockFp
     * @return bool
     */
    protected static function acquireExclusiveLock(mixed $lockFp): bool {
        if (!is_resource($lockFp)) {
            return false;
        }

        $maxRetry = 20;
        $sleepMicroseconds = 5000;
        for ($i = 0; $i < $maxRetry; $i++) {
            if (@flock($lockFp, LOCK_EX | LOCK_NB)) {
                return true;
            }
            usleep($sleepMicroseconds);
        }

        return false;
    }
}
