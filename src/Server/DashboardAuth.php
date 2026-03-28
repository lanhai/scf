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
    private const DASHBOARD_TOKEN_TTL = 3600 * 12;
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
     * 创建 dashboard 登录 token，并把 session 记录在 Runtime 表里。
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
        Runtime::instance()->set(self::dashboardSessionKey($sessionId), [
            'type' => 'dashboard',
            'user' => $user,
            'issued_at' => $now,
            'expired_at' => $expired,
        ]);
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
     * 校验 dashboard 登录 token，并映射回 Runtime 中的 session。
     *
     * @param string $token 待校验的 dashboard token
     * @return array<string, int|string>|false 校验成功返回 session 信息，失败返回 false
     */
    public static function validateDashboardToken(string $token): array|false {
        $payload = self::decodeToken($token, self::dashboardSigningKey());
        if (!$payload || ($payload['typ'] ?? null) !== 'dashboard') {
            return false;
        }
        $sessionId = $payload['sid'] ?? '';
        if (!$sessionId) {
            return false;
        }
        $session = Runtime::instance()->get(self::dashboardSessionKey($sessionId));
        if (!$session || ($session['type'] ?? null) !== 'dashboard') {
            return false;
        }
        $expired = (int)($session['expired_at'] ?? 0);
        if ($expired < time()) {
            Runtime::instance()->delete(self::dashboardSessionKey($sessionId));
            return false;
        }
        return [
            'session_id' => $sessionId,
            'user' => $session['user'] ?? ($payload['sub'] ?? 'system'),
            'issued_at' => (int)($session['issued_at'] ?? ($payload['iat'] ?? 0)),
            'expired_at' => $expired,
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
        $payload = self::decodeToken($token, self::dashboardSigningKey());
        if (!$payload) {
            return;
        }
        $sessionId = $payload['sid'] ?? '';
        if ($sessionId) {
            Runtime::instance()->delete(self::dashboardSessionKey($sessionId));
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
     * 清理 Runtime 表里的过期 dashboard session。
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
    private static function decodeToken(string $token, string $signingKey): array|false {
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
        if ($expired <= 0 || time() > $expired) {
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
