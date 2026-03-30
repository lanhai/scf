<?php
declare(strict_types=1);

/**
 * framework pack 存储与 active 指针函数。
 */

function scf_publish_framework_pack(string $sourcePack, array $versionInfo = [], bool $moveSource = false, ?string &$error = null): ?array {
    $error = null;
    if (!is_file($sourcePack)) {
        $error = 'framework 包不存在:' . $sourcePack;
        return null;
    }
    if (!scf_ensure_dir(scf_framework_runtime_dir(), $error) || !scf_ensure_dir(scf_framework_runtime_pack_dir(), $error)) {
        return null;
    }

    $packVersionInfo = scf_read_framework_pack_version($sourcePack) ?: [];
    $record = array_merge($packVersionInfo, $versionInfo);
    $targetPack = scf_framework_versioned_pack_path($record, $sourcePack);
    $targetReal = realpath($targetPack) ?: $targetPack;
    $sourceReal = realpath($sourcePack) ?: $sourcePack;

    if ($sourceReal !== $targetReal) {
        $published = $moveSource
            ? scf_atomic_replace($sourcePack, $targetPack, $error)
            : scf_atomic_copy($sourcePack, $targetPack, $error);
        if (!$published) {
            return null;
        }
    }

    $record['version'] = trim((string)($record['version'] ?? ''));
    $record['build'] = trim((string)($record['build'] ?? ''));
    if (!scf_write_framework_active_record($targetPack, $record, $error)) {
        return null;
    }
    if (!scf_sync_framework_fallback_pack($targetPack, $error)) {
        return null;
    }

    return scf_read_framework_active_record();
}

function scf_framework_versioned_pack_path(array $versionInfo, string $sourcePack): string {
    $version = preg_replace('/[^0-9A-Za-z._-]+/', '-', trim((string)($versionInfo['version'] ?? '')));
    if ($version === '') {
        $version = 'legacy-' . substr(sha1_file($sourcePack) ?: md5_file($sourcePack) ?: uniqid('', true), 0, 12);
    }

    return scf_framework_runtime_pack_dir() . '/' . $version . '.pack';
}

function scf_write_framework_active_record(string $packPath, array $versionInfo, ?string &$error = null): bool {
    $recordFile = scf_framework_active_record_path();
    $record = [
        'version' => trim((string)($versionInfo['version'] ?? '')),
        'build' => trim((string)($versionInfo['build'] ?? '')),
        'pack' => 'packs/' . basename($packPath),
        'activated_at' => date('Y-m-d H:i:s'),
    ];

    $json = json_encode($record, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if (!is_string($json) || $json === '') {
        $error = 'framework active 指针写入失败: JSON 编码失败';
        return false;
    }

    $tmp = dirname($recordFile) . '/.' . basename($recordFile) . '.tmp.' . getmypid();
    if (!scf_safe_call(static fn() => file_put_contents($tmp, $json), $error)) {
        return false;
    }
    scf_safe_call(static fn() => chmod($tmp, 0644));
    if (!scf_safe_call(static fn() => rename($tmp, $recordFile), $error)) {
        scf_safe_call(static fn() => unlink($tmp));
        return false;
    }

    return true;
}

function scf_read_framework_active_record(): ?array {
    $recordFile = scf_framework_active_record_path();
    if (!is_file($recordFile)) {
        return null;
    }

    $content = @file_get_contents($recordFile);
    if (!is_string($content) || trim($content) === '') {
        return null;
    }

    $record = json_decode($content, true);
    if (!is_array($record)) {
        return null;
    }

    $pack = trim((string)($record['pack'] ?? ''));
    if ($pack === '') {
        return null;
    }

    $record['path'] = str_starts_with($pack, '/')
        ? $pack
        : scf_framework_runtime_dir() . '/' . ltrim($pack, '/');
    return $record;
}

function scf_delete_framework_active_record(): void {
    $recordFile = scf_framework_active_record_path();
    if (is_file($recordFile)) {
        scf_safe_call(static fn() => unlink($recordFile));
    }
}

function scf_sync_framework_fallback_pack(string $pack, ?string &$error = null): bool {
    $srcPack = SCF_ROOT . '/build/src.pack';
    if (!is_file($pack)) {
        $error = 'framework 包不存在:' . $pack;
        return false;
    }

    if (is_file($srcPack) && scf_framework_pack_identity($srcPack) === scf_framework_pack_identity($pack)) {
        return true;
    }

    return scf_atomic_copy($pack, $srcPack, $error);
}

function scf_resolve_framework_active_pack(?string &$error = null): string {
    $activeRecord = scf_read_framework_active_record();
    if ($activeRecord && is_file((string)($activeRecord['path'] ?? ''))) {
        return (string)$activeRecord['path'];
    }

    $srcPack = SCF_ROOT . '/build/src.pack';
    if (is_file($srcPack)) {
        return $srcPack;
    }

    $legacyPack = scf_resolve_legacy_framework_pack();
    if ($legacyPack !== '') {
        return $legacyPack;
    }

    $remotePack = scf_bootstrap_fetch_remote_framework_pack($error);
    if ($remotePack !== '') {
        return $remotePack;
    }

    $error = 'framework 包不存在';
    return '';
}

function scf_resolve_legacy_framework_pack(): string {
    $srcPack = SCF_ROOT . '/build/src.pack';
    $gatewayPack = SCF_ROOT . '/build/gateway.pack';
    $srcExists = is_file($srcPack);
    $gatewayExists = is_file($gatewayPack);

    if ($srcExists && !$gatewayExists) {
        return $srcPack;
    }
    if (!$srcExists && $gatewayExists) {
        return $gatewayPack;
    }
    if (!$srcExists && !$gatewayExists) {
        return '';
    }

    return scf_compare_framework_pack($srcPack, $gatewayPack) >= 0 ? $srcPack : $gatewayPack;
}

function scf_framework_update_ready(): bool {
    if (!scf_bool_constant('FRAMEWORK_IS_PHAR')) {
        return false;
    }

    $activeRecord = scf_read_framework_active_record();
    $activePack = (string)($activeRecord['path'] ?? '');
    if ($activePack === '' || !is_file($activePack)) {
        return false;
    }

    $activeVersion = trim((string)($activeRecord['version'] ?? ''));
    $activeBuild = trim((string)($activeRecord['build'] ?? ''));
    $currentVersion = defined('FRAMEWORK_BUILD_VERSION') ? trim((string)FRAMEWORK_BUILD_VERSION) : '';
    $currentBuild = defined('FRAMEWORK_BUILD_TIME') ? trim((string)FRAMEWORK_BUILD_TIME) : '';
    if ($activeVersion !== '' && $currentVersion !== '') {
        if (version_compare($activeVersion, $currentVersion) !== 0) {
            return true;
        }
        if ($activeBuild !== '' && $currentBuild !== '' && $activeBuild !== $currentBuild) {
            return true;
        }
    }

    $activeIdentity = scf_framework_pack_identity($activePack);
    $currentIdentity = defined('FRAMEWORK_ACTIVE_PACK_SIGNATURE')
        ? (string)FRAMEWORK_ACTIVE_PACK_SIGNATURE
        : scf_framework_pack_identity((string)(defined('FRAMEWORK_ACTIVE_PACK') ? FRAMEWORK_ACTIVE_PACK : ''));
    if ($activeIdentity !== '' && $currentIdentity !== '') {
        return $activeIdentity !== $currentIdentity;
    }

    return $activeVersion !== '' && $currentVersion !== '' && $activeVersion !== $currentVersion;
}

function scf_cleanup_framework_history_packs(string $activePack, int $keepCount = 3): void {
    $packDir = scf_framework_runtime_pack_dir();
    if (!is_dir($packDir)) {
        return;
    }

    $packs = glob($packDir . '/*.pack') ?: [];
    if (!$packs) {
        return;
    }

    usort($packs, static function (string $left, string $right): int {
        return scf_compare_framework_pack($right, $left);
    });

    $keepCount = max(1, $keepCount);
    $activeReal = realpath($activePack) ?: $activePack;
    $keep = [];
    foreach ($packs as $pack) {
        $packReal = realpath($pack) ?: $pack;
        if ($packReal === $activeReal || count($keep) < $keepCount) {
            $keep[$packReal] = true;
        }
    }

    foreach ($packs as $pack) {
        $packReal = realpath($pack) ?: $pack;
        if (isset($keep[$packReal])) {
            continue;
        }
        scf_safe_call(static fn() => unlink($pack));
    }
}

/**
 * 当本地 runtime pack 与 fallback src.pack 都缺失时，尝试从更新服务器自愈拉取一份。
 *
 * 这条兜底只在 boot 极早期使用，目的不是“做完整升级”，而是让节点至少能拿到一份
 * 可启动的 framework 包继续拉起。成功后会复用版本化 pack 发布逻辑，把下载到的包
 * 记录到 active.json 并同步到 build/src.pack。
 *
 * @param string|null $error 失败原因
 * @return string 成功时返回本地可用 pack 路径
 */
function scf_bootstrap_fetch_remote_framework_pack(?string &$error = null): string {
    $error = null;
    $updateServer = trim((string)(ENV_VARIABLES['scf_update_server'] ?? ''));
    if ($updateServer === '') {
        $error = 'framework 包不存在，且未配置 scf_update_server';
        return '';
    }

    $versionBody = scf_http_get_text($updateServer, 8, $error);
    if (!is_string($versionBody) || trim($versionBody) === '') {
        $error = 'framework 包不存在，且版本服务器访问失败: ' . ($error ?: $updateServer);
        return '';
    }

    $versionRecord = json_decode($versionBody, true);
    if (!is_array($versionRecord)) {
        $error = 'framework 包不存在，且版本服务器返回非法 JSON';
        return '';
    }

    $packageUrl = trim((string)($versionRecord['url'] ?? ''));
    if ($packageUrl === '') {
        $error = 'framework 包不存在，且版本服务器未提供 url';
        return '';
    }

    $tempPack = scf_framework_runtime_dir() . '/bootstrap-download-' . getmypid() . '.pack';
    if (!scf_http_download_file($packageUrl, $tempPack, 30, $error)) {
        $error = 'framework 包不存在，且远端下载失败: ' . ($error ?: $packageUrl);
        return '';
    }

    $record = scf_publish_framework_pack($tempPack, $versionRecord, true, $error);
    if (!$record) {
        $error = 'framework 包下载成功但发布失败: ' . ($error ?: basename($tempPack));
        return '';
    }

    $path = (string)($record['path'] ?? '');
    if ($path === '' || !is_file($path)) {
        $error = 'framework 包下载成功但本地路径无效';
        return '';
    }

    scf_stdout('【Boot】已从 scf_update_server 拉取 framework 包: ' . ($record['version'] ?? basename($path)));
    return $path;
}
