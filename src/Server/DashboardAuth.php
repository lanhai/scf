<?php

namespace Scf\Server;

use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Table\Runtime;
use Scf\Util\Random;

/**
 * Dashboard 控制面的 token 鉴权工具。
 *
 * 该类只负责 dashboard 登录态和内部 socket 凭证的签发、校验与失效控制，
 * 不负责登录页面、权限判定或 session 以外的业务身份管理。
 */
class DashboardAuth {

    private const DASHBOARD_SESSION_PREFIX = 'dashboard:s:';
    private const DASHBOARD_TOKEN_TTL = 3600 * 24 * 7;
    private const INTERNAL_SOCKET_TOKEN_TTL = 60;

    /**
     * 读取 dashboard 超级密码。
     *
     * @return string|null 已配置时返回密码，未配置时返回 null
     */
    public static function dashboardPassword(): ?string {
        $password = trim((string)(Config::server()['dashboard_password'] ?? ''));
        return $password !== '' ? $password : null;
    }

    /**
     * 创建 dashboard 登录 token，并把 session 写入 Runtime + 持久化存储。
     *
     * 该方法会同时写入 Runtime session 记录，因此属于带副作用的签发流程。
     *
     * @param string $user 登录用户名
     * @param string|null $sessionId 可选的外部 session 标识
     * @param int|null $ttl token 有效期，单位秒
     * @return string|false 成功返回 token，未配置 dashboard 密码时返回 false
     */
    public static function createDashboardToken(string $user = 'system', ?string $sessionId = null, ?int $ttl = null): string|false {
        $password = self::dashboardPassword();
        if (!$password) {
            return false;
        }
        $ttl = max(300, $ttl ?? self::DASHBOARD_TOKEN_TTL);
        $now = time();
        $expired = $now + $ttl;
        $sessionId = $sessionId ?: Random::character(32);
        $sessionKey = self::dashboardSessionKey($sessionId);
        $existing = self::readDashboardSessionByKey($sessionKey);
        $session = [
            'type' => 'dashboard',
            'user' => $user,
            'issued_at' => (int)($existing['issued_at'] ?? $now),
            'last_active_at' => $now,
            'expired_at' => $expired,
        ];
        self::writeDashboardSessionByKey($sessionKey, $session);
        self::cleanupExpiredDashboardSessions();
        return self::encodeToken([
            'typ' => 'dashboard',
            'sid' => $sessionId,
            'sub' => $user,
            'iat' => $now,
            'exp' => $expired,
        ], self::dashboardSigningKey());
    }

    /**
     * 校验 dashboard 登录 token，并执行“7天无活跃请求过期”的滑动续期。
     *
     * @param string $token 待校验的 dashboard token
     * @return array<string, int|string>|false 校验成功返回 session 信息，失败返回 false
     */
    public static function validateDashboardToken(string $token): array|false {
        $payload = self::decodeToken($token, self::dashboardSigningKey(), false);
        if (!$payload || ($payload['typ'] ?? null) !== 'dashboard') {
            return false;
        }
        $sessionId = $payload['sid'] ?? '';
        if (!$sessionId) {
            return false;
        }
        $sessionKey = self::dashboardSessionKey($sessionId);
        $session = self::readDashboardSessionByKey($sessionKey);
        if (!$session || ($session['type'] ?? null) !== 'dashboard') {
            return false;
        }
        $now = time();
        $expired = (int)($session['expired_at'] ?? 0);
        if ($expired < $now) {
            self::deleteDashboardSessionByKey($sessionKey);
            return false;
        }
        $session['last_active_at'] = $now;
        $session['expired_at'] = $now + self::DASHBOARD_TOKEN_TTL;
        self::writeDashboardSessionByKey($sessionKey, $session);

        return [
            'session_id' => $sessionId,
            'user' => $session['user'] ?? ($payload['sub'] ?? 'system'),
            'issued_at' => (int)($session['issued_at'] ?? ($payload['iat'] ?? 0)),
            'expired_at' => (int)($session['expired_at'] ?? 0),
        ];
    }

    /**
     * 续签 dashboard token，但保持原 sessionId 不变。
     *
     * @param string $token 待续签的 token
     * @return string|false 续签成功返回新 token，失效时返回 false
     */
    public static function refreshDashboardToken(string $token): string|false {
        $session = self::validateDashboardToken($token);
        if (!$session) {
            return false;
        }
        return self::createDashboardToken($session['user'], $session['session_id']);
    }

    /**
     * 主动使 dashboard token 对应的 session 失效。
     *
     * @param string $token 需要失效的 dashboard token
     * @return void
     */
    public static function expireDashboardToken(string $token): void {
        $payload = self::decodeToken($token, self::dashboardSigningKey(), false);
        if (!$payload) {
            return;
        }
        $sessionId = $payload['sid'] ?? '';
        if ($sessionId) {
            self::deleteDashboardSessionByKey(self::dashboardSessionKey($sessionId));
        }
    }

    /**
     * 创建给节点桥接或本地 socket 使用的短时 token。
     *
     * @param string $subject token 主题标识
     * @param int|null $ttl token 有效期，单位秒
     * @return string 短时 token
     */
    public static function createInternalSocketToken(string $subject = 'internal', ?int $ttl = null): string {
        $ttl = max(10, $ttl ?? self::INTERNAL_SOCKET_TOKEN_TTL);
        $now = time();
        return self::encodeToken([
            'typ' => 'internal_socket',
            'sub' => $subject,
            'iat' => $now,
            'exp' => $now + $ttl,
        ], self::internalSocketSigningKey());
    }

    /**
     * socket 既允许 dashboard 登录 token，也允许内部短时 token。
     *
     * @param string $token 待校验 token
     * @return array<string, int|string>|false 校验成功返回 token 描述，失败返回 false
     */
    public static function validateSocketToken(string $token): array|false {
        $dashboard = self::validateDashboardToken($token);
        if ($dashboard) {
            return [
                'type' => 'dashboard',
                ...$dashboard
            ];
        }
        $payload = self::decodeToken($token, self::internalSocketSigningKey());
        if (!$payload || ($payload['typ'] ?? null) !== 'internal_socket') {
            return false;
        }
        return [
            'type' => 'internal_socket',
            'subject' => $payload['sub'] ?? 'internal',
            'issued_at' => (int)($payload['iat'] ?? 0),
            'expired_at' => (int)($payload['exp'] ?? 0),
        ];
    }

    /**
     * 清理 Runtime 与持久化存储里的过期 dashboard session。
     *
     * @return void
     */
    private static function cleanupExpiredDashboardSessions(): void {
        $runtime = Runtime::instance();
        $now = time();
        foreach ($runtime->rows() as $key => $row) {
            if (!str_starts_with((string)$key, self::DASHBOARD_SESSION_PREFIX)) {
                continue;
            }
            $expired = is_array($row) ? (int)($row['expired_at'] ?? 0) : 0;
            if ($expired > 0 && $expired < $now) {
                $runtime->delete($key);
            }
        }
        self::rewriteDashboardSessionStore(static function (array $sessions) use ($now): array {
            foreach ($sessions as $key => $session) {
                $expired = is_array($session) ? (int)($session['expired_at'] ?? 0) : 0;
                if ($expired > 0 && $expired < $now) {
                    unset($sessions[$key]);
                }
            }
            return $sessions;
        });
    }

    /**
     * 生成 dashboard session 的 Runtime 存储键。
     *
     * @param string $sessionId 原始 session 标识
     * @return string Runtime 中使用的归一化键名
     */
    private static function dashboardSessionKey(string $sessionId): string {
        return self::DASHBOARD_SESSION_PREFIX . substr(hash('sha256', $sessionId), 0, 32);
    }

    /**
     * 按 key 读取 dashboard session。
     *
     * 读取顺序：Runtime -> 持久化文件。命中持久化时会回填 Runtime，保证热点请求走内存。
     *
     * @param string $sessionKey 归一化 session key
     * @return array<string, mixed>|false
     */
    private static function readDashboardSessionByKey(string $sessionKey): array|false {
        $session = Runtime::instance()->get($sessionKey);
        if (is_array($session) && ($session['type'] ?? null) === 'dashboard') {
            return $session;
        }
        $stored = self::readDashboardSessionStore();
        $session = $stored[$sessionKey] ?? null;
        if (!is_array($session) || ($session['type'] ?? null) !== 'dashboard') {
            return false;
        }
        Runtime::instance()->set($sessionKey, $session);
        return $session;
    }

    /**
     * 写入一个 dashboard session 到 Runtime 与持久化文件。
     *
     * @param string $sessionKey 归一化 session key
     * @param array<string, mixed> $session session 数据
     * @return void
     */
    private static function writeDashboardSessionByKey(string $sessionKey, array $session): void {
        Runtime::instance()->set($sessionKey, $session);
        self::rewriteDashboardSessionStore(static function (array $sessions) use ($sessionKey, $session): array {
            $sessions[$sessionKey] = $session;
            return $sessions;
        });
    }

    /**
     * 删除一个 dashboard session。
     *
     * @param string $sessionKey 归一化 session key
     * @return void
     */
    private static function deleteDashboardSessionByKey(string $sessionKey): void {
        Runtime::instance()->delete($sessionKey);
        self::rewriteDashboardSessionStore(static function (array $sessions) use ($sessionKey): array {
            unset($sessions[$sessionKey]);
            return $sessions;
        });
    }

    /**
     * 返回 dashboard session 持久化文件路径。
     *
     * @return string
     */
    private static function dashboardSessionStoreFile(): string {
        $baseDir = defined('APP_UPDATE_DIR')
            ? (string)APP_UPDATE_DIR
            : (defined('APP_PATH') ? rtrim((string)APP_PATH, '/') . '/update' : sys_get_temp_dir());
        $app = defined('APP_DIR_NAME') ? (string)APP_DIR_NAME : 'app';
        $env = defined('SERVER_RUN_ENV') ? (string)SERVER_RUN_ENV : 'dev';
        $role = defined('SERVER_ROLE') ? (string)SERVER_ROLE : 'node';
        $port = (int)(Runtime::instance()->httpPort() ?: 0);
        $safeApp = preg_replace('/[^a-zA-Z0-9_-]+/', '_', $app) ?: 'app';
        $safeEnv = preg_replace('/[^a-zA-Z0-9_-]+/', '_', $env) ?: 'env';
        $safeRole = preg_replace('/[^a-zA-Z0-9_-]+/', '_', $role) ?: 'role';
        return rtrim($baseDir, '/\\') . "/dashboard_sessions_{$safeApp}_{$safeEnv}_{$safeRole}_{$port}.json";
    }

    /**
     * 读取持久化 session 文件。
     *
     * @return array<string, array<string, mixed>>
     */
    private static function readDashboardSessionStore(): array {
        $path = self::dashboardSessionStoreFile();
        if (!is_file($path)) {
            return [];
        }

        $fp = @fopen($path, 'rb');
        if (!$fp) {
            return [];
        }
        try {
            @flock($fp, LOCK_SH);
            $raw = stream_get_contents($fp);
        } finally {
            @flock($fp, LOCK_UN);
            fclose($fp);
        }
        if (!is_string($raw) || $raw === '') {
            return [];
        }
        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) {
            return [];
        }
        $sessions = $decoded['sessions'] ?? [];
        return is_array($sessions) ? $sessions : [];
    }

    /**
     * 以排它锁方式重写 session 文件，避免并发写入互相覆盖。
     *
     * @param callable $mutator 输入旧 sessions，返回新 sessions
     * @return void
     */
    private static function rewriteDashboardSessionStore(callable $mutator): void {
        $path = self::dashboardSessionStoreFile();
        $dir = dirname($path);
        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            return;
        }
        $fp = @fopen($path, 'c+');
        if (!$fp) {
            return;
        }
        try {
            if (!@flock($fp, LOCK_EX)) {
                return;
            }
            rewind($fp);
            $raw = stream_get_contents($fp);
            $decoded = is_string($raw) && $raw !== '' ? json_decode($raw, true) : null;
            $sessions = is_array($decoded) && is_array($decoded['sessions'] ?? null)
                ? $decoded['sessions']
                : [];
            $sessions = $mutator($sessions);
            if (!is_array($sessions)) {
                $sessions = [];
            }
            $payload = json_encode([
                'sessions' => $sessions,
                'updated_at' => time(),
            ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
            if (!is_string($payload)) {
                return;
            }
            rewind($fp);
            ftruncate($fp, 0);
            fwrite($fp, $payload);
            fflush($fp);
        } finally {
            @flock($fp, LOCK_UN);
            fclose($fp);
        }
    }

    /**
     * 生成 dashboard token 的签名密钥。
     *
     * @return string 签名密钥
     */
    private static function dashboardSigningKey(): string {
        return hash('sha256', 'dashboard:' . self::dashboardPassword());
    }

    /**
     * 生成内部 socket token 的签名密钥。
     *
     * @return string 签名密钥
     */
    private static function internalSocketSigningKey(): string {
        return hash('sha256', 'internal_socket:' . App::authKey());
    }

    /**
     * 将 payload 编码并签名为 token。
     *
     * @param array<string, mixed> $payload token 载荷
     * @param string $signingKey HMAC 签名密钥
     * @return string 编码后的 token
     */
    private static function encodeToken(array $payload, string $signingKey): string {
        $payloadEncoded = self::base64UrlEncode(json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        $signature = hash_hmac('sha256', $payloadEncoded, $signingKey, true);
        return $payloadEncoded . '.' . self::base64UrlEncode($signature);
    }

    /**
     * 解码并校验 token。
     *
     * @param string $token 待解码 token
     * @param string $signingKey HMAC 签名密钥
     * @return array<string, mixed>|false 校验成功返回 payload，失败返回 false
     */
    private static function decodeToken(string $token, string $signingKey, bool $enforceExpiration = true): array|false {
        $parts = explode('.', $token, 2);
        if (count($parts) !== 2) {
            return false;
        }
        [$payloadEncoded, $signatureEncoded] = $parts;
        if ($payloadEncoded === '' || $signatureEncoded === '') {
            return false;
        }
        $signature = self::base64UrlDecode($signatureEncoded);
        if ($signature === false) {
            return false;
        }
        $expected = hash_hmac('sha256', $payloadEncoded, $signingKey, true);
        if (!hash_equals($expected, $signature)) {
            return false;
        }
        $payloadJson = self::base64UrlDecode($payloadEncoded);
        if ($payloadJson === false) {
            return false;
        }
        $payload = json_decode($payloadJson, true);
        if (!is_array($payload)) {
            return false;
        }
        $expired = (int)($payload['exp'] ?? 0);
        if ($expired <= 0) {
            return false;
        }
        if ($enforceExpiration && time() > $expired) {
            return false;
        }
        return $payload;
    }

    /**
     * 将二进制数据编码为 base64url。
     *
     * @param string $data 原始二进制数据
     * @return string base64url 文本
     */
    private static function base64UrlEncode(string $data): string {
        return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
    }

    /**
     * 将 base64url 文本还原为二进制数据。
     *
     * @param string $data base64url 文本
     * @return string|false 成功返回二进制数据，失败返回 false
     */
    private static function base64UrlDecode(string $data): string|false {
        $remainder = strlen($data) % 4;
        if ($remainder > 0) {
            $data .= str_repeat('=', 4 - $remainder);
        }
        return base64_decode(strtr($data, '-_', '+/'), true);
    }
}
