<?php
declare(strict_types=1);
namespace Scf\Core {
    class Log {
        public static array $messages = [];
        public static function instance(): self { return new self(); }
        public function error(string $message, mixed ...$context): void { self::$messages[] = $message; }
    }
}
namespace Scf\Mode\Web {
    class RequestLifetime {}
    class Response {
        public static function instance(): self { return new self(); }
        public function isAncestorThread(): bool { return false; }
    }
}
namespace {
    require dirname(__DIR__) . '/src/Mode/Web/App.php';
    $app = new \Scf\Mode\Web\App();
    $previous = error_reporting(E_ALL);
    set_error_handler([$app,'errorHandler']);
    try {
        @stat(sys_get_temp_dir() . '/scf-missing-stat-' . bin2hex(random_bytes(8)));
        if (\Scf\Core\Log::$messages) throw new \RuntimeException('Suppressed probe warnings re-enter logging');
        trigger_error('visible warning', E_USER_WARNING);
        if (\Scf\Core\Log::$messages !== ['visible warning']) throw new \RuntimeException('Real warnings must still be logged');
    } finally { restore_error_handler(); error_reporting($previous); }
    echo "PASS actual Web error handler respects suppressed probes and preserves visible warnings\n";
}
