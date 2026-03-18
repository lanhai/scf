<?php

namespace Scf\Server;

use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Table\Runtime;
use Scf\Util\Random;

class DashboardAuth {

    private const DASHBOARD_SESSION_PREFIX = 'dashboard:s:';
    private const DASHBOARD_TOKEN_TTL = 3600 * 12;
    private const INTERNAL_SOCKET_TOKEN_TTL = 60;

    public static function dashboardPassword(): ?string {
        $password = trim((string)(Config::server()['dashboard_password'] ?? ''));
        return $password !== '' ? $password : null;
    }

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

    public static function refreshDashboardToken(string $token): string|false {
        $session = self::validateDashboardToken($token);
        if (!$session) {
            return false;
        }
        return self::createDashboardToken($session['user'], $session['session_id']);
    }

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

    private static function dashboardSessionKey(string $sessionId): string {
        return self::DASHBOARD_SESSION_PREFIX . substr(hash('sha256', $sessionId), 0, 32);
    }

    private static function dashboardSigningKey(): string {
        return hash('sha256', 'dashboard:' . self::dashboardPassword());
    }

    private static function internalSocketSigningKey(): string {
        return hash('sha256', 'internal_socket:' . App::authKey());
    }

    private static function encodeToken(array $payload, string $signingKey): string {
        $payloadEncoded = self::base64UrlEncode(json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        $signature = hash_hmac('sha256', $payloadEncoded, $signingKey, true);
        return $payloadEncoded . '.' . self::base64UrlEncode($signature);
    }

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

    private static function base64UrlEncode(string $data): string {
        return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
    }

    private static function base64UrlDecode(string $data): string|false {
        $remainder = strlen($data) % 4;
        if ($remainder > 0) {
            $data .= str_repeat('=', 4 - $remainder);
        }
        return base64_decode(strtr($data, '-_', '+/'), true);
    }
}
