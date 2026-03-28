<?php

namespace Scf\Command\DefaultCommand;

require_once dirname(__DIR__, 2) . '/Server/Proxy/CliBootstrap.php';

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Config;
use Scf\Core\Env;
use Scf\Server\Proxy\LocalIpc;
use Scf\Server\Proxy\CliBootstrap;
use Swoole\Process;

/**
 * Gateway CLI 命令入口。
 *
 * 该命令类只负责把外部 `./server gateway ...` / `./server start ...`
 * 风格的指令映射到 gateway 控制面动作，避免调用方直接感知本地 IPC 或内部 HTTP。
 */
class Gateway implements CommandInterface {

    /**
     * 返回该 CLI 命令的注册名。
     *
     * @return string 命令名
     */
    public function commandName(): string {
        return 'gateway';
    }

    /**
     * 返回该 CLI 命令的简短说明。
     *
     * @return string 命令描述
     */
    public function desc(): string {
        return '代理网关服务';
    }

    /**
     * 构建 gateway 命令帮助信息。
     *
     * @param Help $commandHelp 帮助信息容器，方法会向其中追加动作和参数说明
     * @return Help 返回同一个帮助对象，便于调用方继续链式处理
     */
    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('start', '启动代理网关');
        $commandHelp->addAction('stop', '停止代理网关');
        $commandHelp->addAction('reload', '重启业务平面');
        $commandHelp->addAction('restart', '重启代理网关');
        $commandHelp->addAction('restart_crontab', '重启 Gateway 排程子进程');
        $commandHelp->addAction('restart_redisqueue', '重启 Gateway Redis 队列子进程');
        $commandHelp->addAction('status', '查看代理网关状态');
        $commandHelp->addActionOpt('-app', '应用目录名');
        $commandHelp->addActionOpt('-env', '运行环境, 例如 dev');
        $commandHelp->addActionOpt('-port', 'Gateway 监听端口');
        $commandHelp->addActionOpt('-rpc_port', 'Gateway 对外 RPC 监听端口, 默认沿用应用 rpc_port');
        $commandHelp->addActionOpt('-host', 'Gateway 监听地址');
        $commandHelp->addActionOpt('-control_port', 'Gateway 控制面监听端口, tcp模式下默认自动偏移');
        $commandHelp->addActionOpt('-upstream_host', '业务 server 地址, 默认 127.0.0.1');
        $commandHelp->addActionOpt('-upstream_port', '业务 server 端口, 不传则自动分配');
        $commandHelp->addActionOpt('-upstream_rpc_port', '业务 server 内部 RPC 端口, 由 Gateway 转发到此端口, 不传则自动分配');
        $commandHelp->addActionOpt('-upstream_version', 'upstream 版本标签');
        $commandHelp->addActionOpt('-upstream_role', 'upstream 节点角色, 默认继承当前 Gateway 角色');
        $commandHelp->addActionOpt('-spawn_upstream', '是否自动启动业务 server, 默认 on');
        $commandHelp->addActionOpt('-reuse_upstream', '是否复用已存在的 upstream 端口, 默认 off');
        return $commandHelp;
    }

    /**
     * 执行 gateway CLI 命令。
     *
     * @return string|null 返回命令执行提示，`start` 分支会直接进入 bootstrap 流程并返回 null
     */
    public function exec(): ?string {
        $action = (string)(Manager::instance()->getArg(0) ?: 'start');
        if (in_array($action, ['start', 'stop', 'reload', 'restart', 'restart_crontab', 'restart_redisqueue', 'status'], true)) {
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
        if ($action === 'restart_crontab') {
            return $this->control('restart_crontab');
        }
        if ($action === 'restart_redisqueue') {
            return $this->control('restart_redisqueue');
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

    /**
     * 直接向 gateway master 发送停止信号。
     *
     * @return string 停止处理结果
     */
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

    /**
     * 通过本地控制面转发 gateway 内部命令。
     *
     * 优先走 LocalIpc，只有本地 socket 不可用时才回退到控制面 HTTP，
     * 保证控制指令尽量走本机轻量通道。
     *
     * @param string $command 需要下发的 gateway 内部命令
     * @return string 命令发送结果描述
     */
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
        if ($command === 'restart_crontab') {
            return '已发送 Gateway Crontab 子进程重启指令';
        }
        if ($command === 'restart_redisqueue') {
            return '已发送 Gateway RedisQueue 子进程重启指令';
        }
        return '已发送 Gateway 指令:' . $command;
    }

    /**
     * 查询 gateway 控制面健康状态。
     *
     * @return string 在线状态描述
     */
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

    /**
     * 向本地 gateway 控制面发送请求。
     *
     * 常用控制动作会优先使用 Unix Domain Socket，避免依赖 HTTP 监听是否已经对外开放。
     *
     * @param int $port gateway 监听端口
     * @param string $path 请求路径
     * @param string $method HTTP 方法
     * @param array $payload 请求体数据
     * @return array{status:int,body:string} 标准化后的响应结构
     */
    protected function requestLocalGateway(int $port, string $path, string $method = 'GET', array $payload = []): array {
        $ipcAction = match ([$method, $path]) {
            ['POST', '/_gateway/internal/command'] => 'gateway.command',
            ['GET', '/_gateway/internal/console/subscription'] => 'gateway.console.subscription',
            ['GET', '/_gateway/healthz'] => 'gateway.health',
            default => '',
        };
        if ($ipcAction !== '') {
            $ipcResponse = LocalIpc::request(LocalIpc::gatewaySocketPath($port), $ipcAction, $payload, 2.0);
            if (is_array($ipcResponse)) {
                return [
                    'status' => (int)($ipcResponse['status'] ?? 0),
                    'body' => json_encode($ipcResponse['data'] ?? ['message' => (string)($ipcResponse['message'] ?? '')], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
                ];
            }
        }
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

    /**
     * 给 server loop 写入停止标记，避免外层守护循环马上再拉起当前 gateway。
     *
     * @return void
     */
    protected function markLoopStop(): void {
        $app = (string)(Manager::instance()->getOpt('app') ?: getenv('APP_DIR') ?: 'app');
        $role = (string)(Manager::instance()->getOpt('role') ?: getenv('SERVER_ROLE') ?: 'master');
        $flagFile = dirname(SCF_ROOT) . '/var/' . $app . '_gateway_' . $role . '.stop';
        @file_put_contents($flagFile, (string)time());
    }

    /**
     * 解析 gateway 业务入口端口。
     *
     * @return int gateway 业务监听端口
     */
    protected function resolveGatewayPort(): int {
        $port = (int)(Manager::instance()->getOpt('port') ?: SERVER_PORT);
        if ($port > 0) {
            return $port;
        }
        $serverConfig = Config::server();
        return (int)($serverConfig['port'] ?? 0);
    }
}
