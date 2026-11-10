<?php

declare(strict_types=1);

use Scf\Client\WebSocket;
use Swoole\Coroutine\Http\Server;
use function Swoole\Coroutine\run;

require_once dirname(__DIR__) . '/src/Client/WebSocket.php';

set_error_handler(static function (int $level, string $message): never {
    throw new ErrorException($message, 0, $level);
});

run(static function (): void {
    $server = new Server('127.0.0.1', 0, false);
    $server->handle('/dashboard.socket', static function ($request, $response): void {
        if (($request->get['token'] ?? '') !== 'a+b c') {
            $response->status(403);
            $response->end();
            return;
        }
        if (!$response->upgrade()) {
            throw new RuntimeException('Fixture failed to upgrade');
        }
        $response->push($response->recv(1)->data);
    });
    go(static fn () => $server->start());

    try {
        $endpoint = 'ws://127.0.0.1:' . $server->port . '/dashboard.socket';
        try {
            WebSocket::connect($endpoint . '?token=wrong');
            throw new LogicException('Unauthorized WebSocket handshake succeeded');
        } catch (RuntimeException $exception) {
            if ($exception->getCode() !== 403) {
                throw $exception;
            }
        }

        $client = WebSocket::connect($endpoint . '?token=a%2Bb+c');
        try {
            if (!$client->connected || $client->statusCode !== 101 || !$client->push('scf')) {
                throw new RuntimeException('Node WebSocket handshake or send failed');
            }
            $reply = $client->recv(1);
            if (!$reply instanceof Swoole\WebSocket\Frame || $reply->data !== 'scf') {
                throw new RuntimeException('Expected native WebSocket frame with original payload');
            }
        } finally {
            $client->close();
        }
    } finally {
        $server->shutdown();
    }
});

echo "PASS Node WebSocket rejects unauthorized connections and exchanges native frames\n";
