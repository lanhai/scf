<?php

declare(strict_types=1);

use Swoole\Coroutine;
use Swoole\Coroutine\Socket;

if (!extension_loaded('swoole')) {
    fwrite(STDERR, "需要安装 swoole/open-swoole 扩展\n");
    exit(1);
}

$options = getopt('', [
    'listen-host::',
    'listen-port:',
    'upstream-host:',
    'upstream-port:',
    'backlog::',
    'connect-timeout::',
    'idle-timeout::',
    'verbose::',
]);

$listenHost = (string)($options['listen-host'] ?? '127.0.0.1');
$listenPort = (int)($options['listen-port'] ?? 0);
$upstreamHost = (string)($options['upstream-host'] ?? '127.0.0.1');
$upstreamPort = (int)($options['upstream-port'] ?? 0);
$backlog = max(16, (int)($options['backlog'] ?? 512));
$connectTimeout = max(0.1, (float)($options['connect-timeout'] ?? 3.0));
$idleTimeout = max(1.0, (float)($options['idle-timeout'] ?? 600.0));
$verbose = array_key_exists('verbose', $options);

if ($listenPort <= 0 || $upstreamPort <= 0) {
    $usage = <<<TXT
用法:
  php scf/tools/tcp_port_proxy.php --listen-port=44980 --upstream-host=127.0.0.1 --upstream-port=9581 [--listen-host=127.0.0.1] [--verbose]

参数:
  --listen-host       监听地址, 默认 127.0.0.1
  --listen-port       本地监听端口, 必填
  --upstream-host     上游地址, 必填
  --upstream-port     上游端口, 必填
  --backlog           backlog, 默认 512
  --connect-timeout   上游连接超时(秒), 默认 3
  --idle-timeout      双向转发空闲超时(秒), 默认 600
  --verbose           打印连接建立/断开日志
TXT;
    fwrite(STDERR, $usage . PHP_EOL);
    exit(1);
}

function relayLog(string $message, bool $verbose = true): void {
    if (!$verbose) {
        return;
    }
    $time = date('m-d H:i:s');
    fwrite(STDOUT, "{$time} [tcp-proxy] {$message}\n");
}

Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);

Coroutine\run(function () use (
    $listenHost,
    $listenPort,
    $upstreamHost,
    $upstreamPort,
    $backlog,
    $connectTimeout,
    $idleTimeout,
    $verbose
): void {
    $server = new Socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    if (!$server->bind($listenHost, $listenPort)) {
        fwrite(STDERR, "监听失败: {$listenHost}:{$listenPort} {$server->errMsg}\n");
        exit(1);
    }
    if (!$server->listen($backlog)) {
        fwrite(STDERR, "listen 失败: {$listenHost}:{$listenPort} {$server->errMsg}\n");
        exit(1);
    }

    relayLog("开始监听 {$listenHost}:{$listenPort} -> {$upstreamHost}:{$upstreamPort}", true);

    while (true) {
        $downstream = $server->accept(-1);
        if (!$downstream instanceof Socket) {
            continue;
        }

        Coroutine::create(function () use (
            $downstream,
            $upstreamHost,
            $upstreamPort,
            $connectTimeout,
            $idleTimeout,
            $verbose
        ): void {
            $clientInfo = $downstream->getpeername();
            $clientLabel = is_array($clientInfo)
                ? (($clientInfo['address'] ?? 'unknown') . ':' . ($clientInfo['port'] ?? 0))
                : 'unknown';

            $upstream = new Socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
            if (!$upstream->connect($upstreamHost, $upstreamPort, $connectTimeout)) {
                relayLog("上游连接失败 {$clientLabel} -> {$upstreamHost}:{$upstreamPort} {$upstream->errMsg}", true);
                try {
                    $downstream->close();
                } catch (Throwable) {
                }
                try {
                    $upstream->close();
                } catch (Throwable) {
                }
                return;
            }

            relayLog("连接建立 {$clientLabel} -> {$upstreamHost}:{$upstreamPort}", $verbose);

            $closed = false;
            $closeBoth = static function () use (&$closed, $downstream, $upstream, $clientLabel, $verbose): void {
                if ($closed) {
                    return;
                }
                $closed = true;
                try {
                    $downstream->close();
                } catch (Throwable) {
                }
                try {
                    $upstream->close();
                } catch (Throwable) {
                }
                relayLog("连接关闭 {$clientLabel}", $verbose);
            };

            $pipe = static function (Socket $from, Socket $to) use ($idleTimeout, $closeBoth): void {
                while (true) {
                    $data = $from->recv(65535, $idleTimeout);
                    if (!is_string($data) || $data === '') {
                        $closeBoth();
                        return;
                    }
                    $length = strlen($data);
                    $written = 0;
                    while ($written < $length) {
                        $sent = $to->send(substr($data, $written));
                        if (!is_int($sent) || $sent <= 0) {
                            $closeBoth();
                            return;
                        }
                        $written += $sent;
                    }
                }
            };

            Coroutine::create(static function () use ($downstream, $upstream, $pipe): void {
                $pipe($downstream, $upstream);
            });
            Coroutine::create(static function () use ($downstream, $upstream, $pipe): void {
                $pipe($upstream, $downstream);
            });
        });
    }
});
