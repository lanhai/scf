<?php

declare(strict_types=1);

use Scf\Mode\Web\Request;
use Swoole\Http\Request as SwooleRequest;
use function Swoole\Coroutine\run;

$root = dirname(__DIR__);
require $root . '/vendor/autoload.php';
spl_autoload_register(static function (string $class) use ($root): void {
    if (str_starts_with($class, 'Scf\\')) {
        require $root . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
    }
});

set_error_handler(static function (int $severity, string $message, string $file, int $line): never {
    throw new ErrorException($message, 0, $severity, $file, $line);
}, E_DEPRECATED);

run(static function (): void {
    $body = '{"ids":[17,42],"release_tokens":{"17":"token"},"nested":{"a":["exp","Or","x"]}}';
    $raw = "POST /admin/kuake/video/review/watch/ HTTP/1.1\r\nHost: localhost\r\n"
        . "Content-Type: application/json\r\nContent-Length: " . strlen($body) . "\r\n\r\n" . $body;
    $request = SwooleRequest::create();
    $request->parse($raw);
    Request::instance()->register($request);

    Request::post(['ids' => [], 'release_tokens' => [], 'nested' => []])->assign($ids, $tokens, $nested);
    if ($ids !== [17, 42] || $tokens !== [17 => 'token'] || $nested !== ['a' => ['exp ', 'Or ', 'x']]) {
        throw new RuntimeException('POST assignment changed IDs, tokens or recursive expression filtering');
    }

    Request::post(['missing' => []])->assign($missing);
    if ($missing !== []) {
        throw new RuntimeException('Missing POST field should retain its array default');
    }
});

echo "PASS POST array assignment and recursive filtering under PHP 8.2\n";
