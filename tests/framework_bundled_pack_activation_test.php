<?php

declare(strict_types=1);

require dirname(__DIR__) . '/src/Command/Bootstrap/bootstrap.php';

define('SCF_ROOT', sys_get_temp_dir() . '/scf-bundled-pack-' . bin2hex(random_bytes(6)));
define('FRAMEWORK_IS_PHAR', true);

function writeFixturePack(string $target, string $version): void {
    $archive = SCF_ROOT . '/fixture.phar';
    $pack = new Phar($archive);
    $pack['version.php'] = '<?php return ' . var_export(['version' => $version, 'build' => 'fixture'], true) . ';';
    unset($pack);
    rename($archive, $target);
}

function assertBootVersion(string $expected): void {
    $selected = scf_try_upgrade(['boot', 'gateway', 'start'], true);
    $version = scf_read_framework_pack_version($selected)['version'] ?? '';
    if ($version !== $expected || (scf_read_framework_active_record()['version'] ?? '') !== $expected) {
        throw new RuntimeException("Expected framework {$expected}, selected {$version}");
    }
}

mkdir(SCF_ROOT . '/build', 0700, true);
try {
    $bundled = SCF_ROOT . '/build/src.pack';
    $pending = SCF_ROOT . '/build/update.pack';

    writeFixturePack($pending, '1.3.89');
    assertBootVersion('1.3.89');

    writeFixturePack($bundled, '1.3.92');
    $releaseHash = hash_file('sha256', $bundled);
    assertBootVersion('1.3.92');
    assertBootVersion('1.3.92');
    if (hash_file('sha256', $bundled) !== $releaseHash) {
        throw new RuntimeException('Boot overwrote the newer bundled artifact');
    }

    writeFixturePack($bundled, '1.3.89');
    assertBootVersion('1.3.92');

    writeFixturePack($bundled, '1.3.93');
    writeFixturePack($pending, '1.3.89');
    assertBootVersion('1.3.89');
    assertBootVersion('1.3.89');

    echo "PASS bundled release activation, older bundle rejection and explicit rollback precedence\n";
} finally {
    $files = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator(SCF_ROOT, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::CHILD_FIRST
    );
    foreach ($files as $file) {
        $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
    }
    rmdir(SCF_ROOT);
}
