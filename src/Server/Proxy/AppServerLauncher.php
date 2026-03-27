<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Scf\Core\Server as CoreServer;
use Swoole\Process;
use Swoole\Server;
use const SIGKILL;
use const SIGTERM;

class AppServerLauncher {

    protected const GRACEFUL_SHUTDOWN_PATH = '/_gateway/internal/upstream/shutdown';

    public function isListening(string $host, int $port, float $timeoutSeconds = 0.3): bool {
        if ($port <= 0) {
            return false;
        }
        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $timeoutSeconds,
            STREAM_CLIENT_CONNECT
        );
        if (is_resource($socket)) {
            fclose($socket);
            return true;
        }
        return false;
    }

    public function findAvailablePort(string $host, int $startPort, int $maxScan = 200): int {
        $port = max(1025, $startPort);
        for ($i = 0; $i < $maxScan; $i++, $port++) {
            if ($this->isListening($host, $port, 0.1)) {
                continue;
            }
            $server = @stream_socket_server("tcp://{$host}:{$port}", $errno, $errstr);
            if ($server === false) {
                continue;
            }
            fclose($server);
            return $port;
        }
        throw new RuntimeException("未找到可用端口，起始端口:{$startPort}");
    }

    public function createManagedProcess(array $options): array {
        $app = (string)($options['app'] ?? '');
        $env = (string)($options['env'] ?? 'production');
        $role = (string)($options['role'] ?? 'master');
        $port = (int)($options['port'] ?? 0);
        $rpcPort = (int)($options['rpc_port'] ?? 0);
        $src = (string)($options['src'] ?? '');
        $extra = is_array($options['extra'] ?? null) ? $options['extra'] : [];

        if ($app === '') {
            throw new RuntimeException('启动业务 server 缺少 app');
        }
        if ($port <= 0) {
            throw new RuntimeException('启动业务 server 缺少有效 port');
        }

        $command = $this->buildCommand($app, $env, $role, $port, $rpcPort, $src, $extra);
        $binary = PHP_BINARY;
        $args = array_slice($command, 1);

        $process = new Process(function (Process $process) use ($binary, $args) {
            $process->exec($binary, $args);
            exit(1);
        }, false, SOCK_DGRAM, false);

        return [
            'process' => $process,
            'port' => $port,
            'host' => (string)($options['host'] ?? '127.0.0.1'),
            'role' => $role,
            'command' => $binary . ' ' . implode(' ', array_map('escapeshellarg', $args)),
        ];
    }

    public function attachManagedProcess(Server $server, array $spec): array {
        /** @var Process|null $process */
        $process = $spec['process'] ?? null;
        if (!$process instanceof Process) {
            throw new RuntimeException('托管业务 server 缺少有效进程对象');
        }

        $server->addProcess($process);
        return $spec;
    }

    public function launch(array $options): array {
        $spec = $this->createManagedProcess($options);
        /** @var Process $process */
        $process = $spec['process'];
        $pid = $process->start();
        if ($pid <= 0) {
            throw new RuntimeException('拉起业务 server 失败:process start failed');
        }

        $spec['pid'] = $pid;
        unset($spec['process']);
        return $spec;
    }

    public function processPid(array $spec): int {
        /** @var Process|null $process */
        $process = $spec['process'] ?? null;
        if (!$process instanceof Process) {
            return 0;
        }
        return (int)($process->pid ?? 0);
    }

    protected function buildCommand(string $app, string $env, string $role, int $port, int $rpcPort, string $src, array $extra): array {
        $command = [
            PHP_BINARY,
            SCF_ROOT . '/boot',
            'gateway_upstream',
            'start',
            "-app={$app}",
            "-env={$env}",
            "-role={$role}",
            "-port={$port}",
        ];

        if ($src === 'dir') {
            $command[] = '-dir';
        } elseif ($src === 'phar') {
            $command[] = '-phar';
        }

        foreach ($this->sanitizeExtraFlags($extra) as $arg) {
            if (!is_string($arg) || $arg === '') {
                continue;
            }
            $command[] = $arg;
        }

        if ($rpcPort > 0) {
            $command[] = '-rport=' . $rpcPort;
        }

        return $command;
    }

    protected function sanitizeExtraFlags(array $extra): array {
        return array_values(array_filter($extra, static function ($arg) {
            if (!is_string($arg) || $arg === '') {
                return false;
            }
            return !str_starts_with($arg, '-rport=');
        }));
    }

    public function stop(array $instance, int $graceSeconds = 5): void {
        $metadata = (array)($instance['metadata'] ?? []);
        if (($metadata['managed'] ?? false) !== true) {
            return;
        }
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        $rpcPort = (int)($metadata['rpc_port'] ?? ($instance['rpc_port'] ?? 0));
        $pid = (int)($metadata['pid'] ?? 0);
        $this->requestGracefulShutdown($host, $port);
        if ($pid > 0) {
            @Process::kill($pid, SIGTERM);
        }

        $deadline = microtime(true) + max(1, $graceSeconds);
        while (microtime(true) < $deadline) {
            $httpAlive = $port > 0 && $this->isListening($host, $port, 0.2);
            $rpcAlive = $rpcPort > 0 && $this->isListening($host, $rpcPort, 0.2);
            $pidAlive = $pid > 0 && @Process::kill($pid, 0);
            if (!$httpAlive && !$rpcAlive && !$pidAlive) {
                return;
            }
            usleep(200000);
        }

        if ($pid > 0 && @Process::kill($pid, 0)) {
            @Process::kill($pid, SIGKILL);
        }
        if ($port > 0 && $this->isListening($host, $port, 0.2)) {
            CoreServer::killProcessByPort($port);
        }
        if ($rpcPort > 0 && $this->isListening($host, $rpcPort, 0.2)) {
            CoreServer::killProcessByPort($rpcPort);
        }
    }

    protected function requestGracefulShutdown(string $host, int $port, float $timeoutSeconds = 1.0): bool {
        if ($port <= 0) {
            return false;
        }

        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $timeoutSeconds,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return false;
        }

        stream_set_timeout($socket, max(1, (int)ceil($timeoutSeconds)));
        $request = "POST " . self::GRACEFUL_SHUTDOWN_PATH . " HTTP/1.1\r\n"
            . "Host: {$host}:{$port}\r\n"
            . "Connection: close\r\n"
            . "Content-Length: 0\r\n"
            . "X-Gateway-Control: shutdown\r\n\r\n";
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);

        return is_string($response) && str_contains($response, ' 200 ');
    }

    public function waitUntilReady(string $host, int $port, int $timeoutSeconds = 20, int $intervalMs = 200): bool {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        while (microtime(true) < $deadline) {
            if ($this->isListening($host, $port, 0.2)) {
                return true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        return false;
    }

    public function waitUntilServicesReady(string $host, int $port, int $rpcPort = 0, int $timeoutSeconds = 20, int $intervalMs = 200): bool {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $logged = false;
        while (microtime(true) < $deadline) {
            $httpReady = $this->isListening($host, $port, 0.2);
            $rpcReady = $rpcPort <= 0 || $this->isListening($host, $rpcPort, 0.2);
            if ($httpReady && $rpcReady) {
                if ($logged) {
                    $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                    echo date('m-d H:i:s') . " 【Gateway】业务实例端口就绪: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
                }
                return true;
            }
            if (!$logged) {
                $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
                echo date('m-d H:i:s') . " 【Gateway】等待业务实例端口就绪: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
                $logged = true;
            }
            usleep(max(50, $intervalMs) * 1000);
        }
        if ($logged) {
            $rpcInfo = $rpcPort > 0 ? ", RPC:{$rpcPort}" : '';
            echo date('m-d H:i:s') . " 【Gateway】等待业务实例端口就绪超时: {$host}:{$port}{$rpcInfo}" . PHP_EOL;
        }
        return false;
    }
}
