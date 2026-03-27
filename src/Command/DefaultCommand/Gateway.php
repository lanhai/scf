<?php

namespace Scf\Command\DefaultCommand;

require_once dirname(__DIR__, 2) . '/Server/Proxy/CliBootstrap.php';

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Config;
use Scf\Core\Env;
use Scf\Server\Proxy\CliBootstrap;
use Swoole\Process;

class Gateway implements CommandInterface {

    public function commandName(): string {
        return 'gateway';
    }

    public function desc(): string {
        return '代理网关服务';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('start', '启动代理网关');
        $commandHelp->addAction('stop', '停止代理网关');
        $commandHelp->addAction('reload', '重启业务平面');
        $commandHelp->addAction('restart', '重启代理网关');
        $commandHelp->addAction('status', '查看代理网关状态');
        $commandHelp->addActionOpt('-app', '应用目录名');
        $commandHelp->addActionOpt('-env', '运行环境, 例如 dev');
        $commandHelp->addActionOpt('-port', 'Gateway 监听端口');
        $commandHelp->addActionOpt('-rpc_port', 'Gateway 对外 RPC 监听端口, 默认沿用应用 rpc_port');
        $commandHelp->addActionOpt('-host', 'Gateway 监听地址');
        $commandHelp->addActionOpt('-upstream_host', '业务 server 地址, 默认 127.0.0.1');
        $commandHelp->addActionOpt('-upstream_port', '业务 server 端口, 不传则自动分配');
        $commandHelp->addActionOpt('-upstream_rpc_port', '业务 server 内部 RPC 端口, 由 Gateway 转发到此端口, 不传则自动分配');
        $commandHelp->addActionOpt('-upstream_version', 'upstream 版本标签');
        $commandHelp->addActionOpt('-upstream_role', 'upstream 节点角色, 默认继承当前 Gateway 角色');
        $commandHelp->addActionOpt('-spawn_upstream', '是否自动启动业务 server, 默认 on');
        $commandHelp->addActionOpt('-reuse_upstream', '是否复用已存在的 upstream 端口, 默认 off');
        return $commandHelp;
    }

    public function exec(): ?string {
        $action = (string)(Manager::instance()->getArg(0) ?: 'start');
        if (in_array($action, ['start', 'stop', 'reload', 'restart', 'status'], true)) {
            Env::initialize(MODE_CGI);
        }
        if ($action === 'stop') {
            return $this->stop();
        }
        if ($action === 'reload') {
            return $this->control('reload');
        }
        if ($action === 'restart') {
            return $this->control('restart');
        }
        if ($action === 'status') {
            return $this->status();
        }
        if ($action === 'start') {
            CliBootstrap::bootedRun();
            return null;
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    protected function stop(): string {
        $this->markLoopStop();
        $app = (string)(Manager::instance()->getOpt('app') ?: getenv('APP_DIR') ?: 'app');
        $role = (string)(Manager::instance()->getOpt('role') ?: getenv('SERVER_ROLE') ?: 'master');
        $pidFile = dirname(SCF_ROOT) . '/var/' . $app . '_gateway_' . $role . '.pid';
        if (!is_file($pidFile)) {
            return 'Gateway 进程不存在';
        }
        $pid = (int)file_get_contents($pidFile);
        if ($pid <= 0 || !Process::kill($pid, 0)) {
            @unlink($pidFile);
            return 'Gateway 进程不存在';
        }
        Process::kill($pid, SIGTERM);
        return '已发送 Gateway 停止信号:' . $pid;
    }

    protected function control(string $command): string {
        $port = $this->resolveGatewayPort();
        if ($port <= 0) {
            return 'Gateway 端口未配置';
        }
        $result = $this->requestLocalGateway($port, '/_gateway/internal/command', 'POST', [
            'command' => $command,
        ]);
        if ($result['status'] !== 200) {
            return 'Gateway 指令发送失败:' . ($result['body'] ?: ('HTTP ' . $result['status']));
        }
        if ($command === 'restart') {
            return '已发送 Gateway 重启指令';
        }
        if ($command === 'reload') {
            return '已发送 Gateway 业务平面重启指令';
        }
        return '已发送 Gateway 指令:' . $command;
    }

    protected function status(): string {
        $port = $this->resolveGatewayPort();
        if ($port <= 0) {
            return 'Gateway 端口未配置';
        }
        $result = $this->requestLocalGateway($port, '/_gateway/healthz');
        if ($result['status'] !== 200) {
            return 'Gateway 不在线';
        }
        return 'Gateway 在线';
    }

    protected function requestLocalGateway(int $port, string $path, string $method = 'GET', array $payload = []): array {
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            2.0,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return ['status' => 0, 'body' => $errstr ?: 'connect failed'];
        }

        stream_set_timeout($socket, 2);
        $body = $payload ? json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : '';
        $request = "{$method} {$path} HTTP/1.1\r\n"
            . "Host: 127.0.0.1:{$port}\r\n"
            . "X-Gateway-Internal: 1\r\n"
            . "Connection: close\r\n";
        if ($method === 'POST') {
            $request .= "Content-Type: application/json\r\n";
            $request .= "Content-Length: " . strlen($body) . "\r\n";
        }
        $request .= "\r\n" . $body;
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);

        if (!is_string($response) || $response === '') {
            return ['status' => 0, 'body' => 'empty response'];
        }
        $status = 0;
        if (preg_match('#^HTTP/\\S+\\s+(\\d{3})#', $response, $matches)) {
            $status = (int)$matches[1];
        }
        $parts = explode("\r\n\r\n", $response, 2);
        return [
            'status' => $status,
            'body' => $parts[1] ?? '',
        ];
    }

    protected function markLoopStop(): void {
        $app = (string)(Manager::instance()->getOpt('app') ?: getenv('APP_DIR') ?: 'app');
        $role = (string)(Manager::instance()->getOpt('role') ?: getenv('SERVER_ROLE') ?: 'master');
        $flagFile = dirname(SCF_ROOT) . '/var/' . $app . '_gateway_' . $role . '.stop';
        @file_put_contents($flagFile, (string)time());
    }

    protected function resolveGatewayPort(): int {
        $port = (int)(Manager::instance()->getOpt('port') ?: SERVER_PORT);
        if ($port > 0) {
            return $port;
        }
        $serverConfig = Config::server();
        return (int)($serverConfig['port'] ?? 0);
    }
}
