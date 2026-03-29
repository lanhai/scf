<?php
declare(strict_types=1);

/**
 * framework pack 启动决策函数。
 *
 * 只负责决定当前 boot 该加载哪个 framework 包，以及启动前如何把
 * runtime 目录整理到一致状态。
 */

function scf_try_upgrade(array $argv, bool $boot = false): string {
    clearstatcache();
    $lockFile = SCF_ROOT . '/build/update.lock';
    $command = scf_runtime_boot_command($argv);
    $srcPack = SCF_ROOT . '/build/src.pack';

    if (!scf_bool_constant('FRAMEWORK_IS_PHAR')) {
        return $srcPack;
    }

    $lock = scf_open_lock_file($lockFile);
    if (is_resource($lock)) {
        scf_safe_call(static fn() => flock($lock, LOCK_EX));
    }

    $error = null;
    $activePack = '';
    if (scf_prepare_framework_runtime_storage($error)) {
        $activePack = scf_resolve_framework_boot_pack($command, $error);
    }
    scf_release_lock($lock);

    if ($activePack === '' || !file_exists($activePack)) {
        if ($error) {
            scf_stderr($error);
        }
        scf_stderr('内核文件不存在');
        exit(3);
    }

    return $activePack;
}

function scf_runtime_boot_command(array $argv): string {
    $command = trim((string)($argv[1] ?? ''));
    if ($command === 'gateway') {
        return 'gateway';
    }
    if ($command === 'gateway_upstream') {
        return 'gateway_upstream';
    }
    return 'default';
}

function scf_prepare_framework_runtime_storage(?string &$error = null): bool {
    $error = null;
    if (!scf_ensure_dir(SCF_ROOT . '/build', $error)) {
        return false;
    }
    if (!scf_ensure_dir(scf_framework_runtime_dir(), $error)) {
        return false;
    }
    if (!scf_ensure_dir(scf_framework_runtime_pack_dir(), $error)) {
        return false;
    }
    if (!scf_import_legacy_update_pack($error)) {
        return false;
    }

    $activeRecord = scf_read_framework_active_record();
    if ($activeRecord && is_file((string)($activeRecord['path'] ?? ''))) {
        return scf_sync_framework_fallback_pack((string)$activeRecord['path'], $error);
    }

    if ($activeRecord && !is_file((string)($activeRecord['path'] ?? ''))) {
        scf_delete_framework_active_record();
    }

    $legacyPack = scf_resolve_legacy_framework_pack();
    if ($legacyPack !== '') {
        return scf_sync_framework_fallback_pack($legacyPack, $error);
    }

    return true;
}

function scf_resolve_framework_boot_pack(string $command, ?string &$error = null): string {
    $pack = scf_resolve_framework_active_pack($error);
    if ($pack === '') {
        return '';
    }

    // 历史 framework pack 只能由 gateway 启动链负责清理。
    // upstream/server 可能仍在使用旧包，若它们自行裁剪 runtime packs，
    // 会把 gateway 当前仍需托管或切流中的版本误删。
    if ($command === 'gateway') {
        scf_cleanup_framework_history_packs($pack);
    }

    return $pack;
}

function scf_framework_runtime_dir(): string {
    return SCF_ROOT . '/build/framework';
}

function scf_framework_runtime_pack_dir(): string {
    return scf_framework_runtime_dir() . '/packs';
}

function scf_framework_active_record_path(): string {
    return scf_framework_runtime_dir() . '/active.json';
}

function scf_ensure_dir(string $dir, ?string &$error = null): bool {
    $error = null;
    if (is_dir($dir)) {
        return true;
    }

    if (scf_safe_call(static fn() => mkdir($dir, 0775, true), $error)) {
        return true;
    }

    if (is_dir($dir)) {
        return true;
    }

    $error = $error ?: ('目录创建失败:' . $dir);
    return false;
}

function scf_import_legacy_update_pack(?string &$error = null): bool {
    $updatePack = SCF_ROOT . '/build/update.pack';
    if (!is_file($updatePack)) {
        return true;
    }

    $versionInfo = scf_read_framework_pack_version($updatePack) ?: [];
    $record = scf_publish_framework_pack($updatePack, $versionInfo, true, $error);
    if (!$record) {
        return false;
    }

    scf_stdout('legacy update.pack 已迁移到版本化 framework pack');
    return true;
}
