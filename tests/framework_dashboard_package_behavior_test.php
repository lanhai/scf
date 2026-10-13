<?php
declare(strict_types=1);

// Exercise the real dashboard packaging command and ZIP writer with an isolated
// build directory. Only console output and the remote OSS transport are replaced.
namespace Scf\Core {
    class Console {
        public static array $messages = [];

        public static function __callStatic(string $name, array $arguments): void {
            self::$messages[] = $arguments[0] ?? '';
        }
    }
}

namespace Scf\Cloud\Ali {
    class DashboardUploadResult {
        public function hasError(): bool { return false; }
        public function getData(): string { return 'fixture upload'; }
    }

    class Oss {
        public static array $uploads = [];

        public static function instance(array $config): self { return new self(); }
        public function bucket(): array { return ['cdn_domain' => 'https://fixture.invalid']; }

        public function uploadFile(string $file, string $object): DashboardUploadResult {
            $snapshot = BUILD_PATH . 'upload-' . count(self::$uploads);
            copy($file, $snapshot);
            self::$uploads[] = ['object' => $object, 'snapshot' => $snapshot];
            return new DashboardUploadResult();
        }
    }
}

namespace {
    use PhpZip\ZipFile;
    use Scf\Cloud\Ali\Oss;
    use Scf\Command\DefaultCommand\Framework;

    $sourceRoot = dirname(__DIR__);
    require $sourceRoot . '/vendor/autoload.php';
    spl_autoload_register(static function (string $class) use ($sourceRoot): void {
        if (str_starts_with($class, 'Scf\\')) {
            $file = $sourceRoot . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
            if (is_file($file)) require_once $file;
        }
    });

    $fixture = sys_get_temp_dir() . '/scf-dashboard-test-' . bin2hex(random_bytes(6));
    define('SCF_ROOT', $fixture);
    define('BUILD_PATH', $fixture . '/build/');
    $dashboard = $fixture . '/build/public/dashboard';
    mkdir($dashboard . '/static/js', 0775, true);
    mkdir($dashboard . '/static/css', 0775, true);
    mkdir(BUILD_PATH . 'framework', 0775, true);
    file_put_contents(BUILD_PATH . 'framework/config.php', '<?php return [];');
    file_put_contents($dashboard . '/index.html', '<script src="static/js/app.js"></script>');
    file_put_contents($dashboard . '/static/js/app.js', 'window.build = 2;');
    file_put_contents($dashboard . '/static/css/app.css', 'body { color: red; }');
    file_put_contents($dashboard . '/static/js/app.js.map', 'excluded source map');
    file_put_contents($dashboard . '/version.json', json_encode([
        'version' => '1.0.31',
        'md5' => md5_file($dashboard . '/index.html'),
    ]));

    $checks = 0;
    $check = static function (bool $condition, string $message) use (&$checks): void {
        if (!$condition) throw new \RuntimeException($message);
        $checks++;
    };
    $version = static function () use ($dashboard): array {
        clearstatcache();
        return json_decode(file_get_contents($dashboard . '/version.json'), true);
    };
    $command = new Framework();
    try {
        $command->dashboard();
        $check(count(Oss::$uploads) === 2, 'Unchanged index with changed assets must package and upload');
        $check(array_column(Oss::$uploads, 'object') === ['/scf/dashboard.zip', '/scf/dashboard-version.json'], 'Upload the archive before its version manifest');
        $check($version()['version'] === '1.0.32', 'Migrate the legacy index-only digest with one version increment');
        $zip = new ZipFile();
        try {
            $zip->openFile(Oss::$uploads[0]['snapshot'])->setReadPassword('scfdashboard');
            $check($zip->getEntryContents('static/js/app.js') === 'window.build = 2;', 'Package the current JS content');
            $check(!in_array('static/js/app.js.map', $zip->getListFiles(), true), 'Source maps remain excluded');
        } finally {
            $zip->close();
        }

        $publishedVersion = $version();
        $check(json_decode(file_get_contents(Oss::$uploads[1]['snapshot']), true) === $publishedVersion, 'Publish matching digest and version metadata');
        $command->dashboard();
        $check(count(Oss::$uploads) === 2 && $version() === $publishedVersion, 'Unchanged assets skip without bumping the version');

        file_put_contents($dashboard . '/static/js/app.js', 'window.build = 3;');
        $command->dashboard();
        $check(count(Oss::$uploads) === 4 && $version()['version'] === '1.0.33', 'JS changes alone must rebuild');
        file_put_contents($dashboard . '/static/css/app.css', 'body { color: blue; }');
        $command->dashboard();
        $check(count(Oss::$uploads) === 6 && $version()['version'] === '1.0.34', 'CSS changes alone must rebuild');
        file_put_contents($dashboard . '/static/js/extra.js', 'extra asset');
        $command->dashboard();
        $check(count(Oss::$uploads) === 8, 'New assets must rebuild');
        rename($dashboard . '/static/js/extra.js', $dashboard . '/static/js/renamed.js');
        $command->dashboard();
        $check(count(Oss::$uploads) === 10, 'Renaming an asset without changing its content must rebuild');
        unlink($dashboard . '/static/js/renamed.js');
        $command->dashboard();
        $check(count(Oss::$uploads) === 12, 'Deleting assets must rebuild');

        $beforeExcludedChanges = $version();
        file_put_contents($dashboard . '/static/js/app.js.map', 'new excluded source map');
        $beforeExcludedChanges['server'] = 'https://other-fixture.invalid';
        file_put_contents($dashboard . '/version.json', json_encode($beforeExcludedChanges));
        $command->dashboard();
        $check(count(Oss::$uploads) === 12 && $version() === $beforeExcludedChanges, 'Version metadata and excluded source maps do not cause self-triggered rebuilds');

        // Directory enumeration order must not affect the package digest.
        $js = file_get_contents($dashboard . '/static/js/app.js');
        unlink($dashboard . '/static/js/app.js');
        file_put_contents($dashboard . '/static/js/app.js', $js);
        $command->dashboard();
        $check(count(Oss::$uploads) === 12, 'Recreating identical files must preserve the digest');
        echo 'PASS ' . $checks . " dashboard package behavior checks\n";
    } finally {
        $files = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($fixture, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST
        );
        foreach ($files as $file) {
            $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        }
        rmdir($fixture);
    }
}
