<?php

namespace Scf\Server;

use Scf\Cache\Redis;
use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Component;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Log;
use Scf\Core\Result;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Core\Table\ServerNodeTable;
use Scf\Database\Exception\NullPool;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Server\DashboardAuth;
use Scf\Server\Task\CrontabManager;
use Scf\Util\Date;
use Scf\Util\File;
use Swlib\Saber\WebSocket;
use Swlib\SaberGM;
use Swoole\Coroutine;
use Swoole\Coroutine\System;
use Throwable;
use Swlib\Http\Exception\RequestException;

class Manager extends Component {

    /**
     * @var array 服务器列表
     */
    protected array $servers = [];

    public function _init(): void {
        parent::_init();
    }

    /**
     * 获取master节点host
     * @return string
     */
    public function getMasterHost(): string {
        $host = Runtime::instance()->get('_MASTER_HOST_');
        if (!$host) {
            $appConfig = Config::get('app', []);
            $masterHost = $appConfig['master_host'] ?? Config::get('app.app.master_host', '127.0.0.1');
            $host = App::isMaster() ? '127.0.0.1' : $masterHost;
            $host = $this->normalizeMasterHost($host, App::isMaster());
            Runtime::instance()->set('_MASTER_HOST_', $host);
        }
        return $host;
    }

    /**
     * 归一化 slave 连接 master 时使用的目标地址。
     *
     * 这里需要同时兼容两类部署：
     * 1. 本机/内网直连控制面：应自动补上 gateway 控制面端口；
     * 2. 通过 nginx 域名入口访问 master：不应擅自补 `10580`，否则会绕过
     *    反向代理，直接去访问一个外部并未暴露的控制面端口。
     *
     * 因此只有“本机自连”或“明确写了 IP/localhost 但未带端口”时，才自动补
     * 控制面端口；普通域名默认保留原样，让它继续走 nginx 暴露出来的入口。
     *
     * @param string $host 配置中的 master_host 原始值。
     * @param bool $localMaster 当前是否为 master 本机自连场景。
     * @return string 归一化后的连接目标。
     */
    protected function normalizeMasterHost(string $host, bool $localMaster = false): string {
        $host = trim($host);
        if ($host === '') {
            return $localMaster ? ('127.0.0.1:' . $this->resolveLocalMasterPort()) : '127.0.0.1';
        }

        $parsed = parse_url(str_contains($host, '://') ? $host : 'tcp://' . $host);
        if ($parsed === false) {
            return $host;
        }

        $hostPart = $parsed['host'] ?? null;
        if (!$hostPart) {
            return $host;
        }

        $port = (int)($parsed['port'] ?? 0);
        if ($port <= 0 && $localMaster) {
            $port = $this->resolveLocalMasterPort();
        } elseif ($port <= 0 && $this->shouldAppendGatewayPortForMasterHost($hostPart)) {
            if (MASTER_PORT) {
                $port = $this->resolveGatewayControlPort((int)MASTER_PORT);
            } else {
                $port = $this->resolveGatewayControlPort((int)(Config::server()['port'] ?? 0));
            }
        }

        $parsed['host'] = $hostPart;
        $parsed['port'] = $port > 0 ? $port : null;
        return $this->buildTargetFromParts($parsed, isset($parsed['scheme']) && $parsed['scheme'] !== 'tcp'
            ? ((string)$parsed['scheme'] . '://')
            : '');
    }

    /**
     * 判断一个 master_host 在未显式带端口时，是否应该自动补 gateway 控制面端口。
     *
     * 只有直连型地址才需要补端口：
     * - IP 地址
     * - localhost / master 这类容器内/内网主机名
     *
     * 对外部域名默认不补端口，让它继续走 nginx 暴露出来的 80/443 入口。
     *
     * @param string $hostPart 解析后的 host 部分。
     * @return bool 需要自动补控制面端口时返回 true。
     */
    protected function shouldAppendGatewayPortForMasterHost(string $hostPart): bool {
        $hostPart = strtolower(trim($hostPart));
        if ($hostPart === '') {
            return false;
        }
        if (filter_var($hostPart, FILTER_VALIDATE_IP) !== false) {
            return true;
        }
        return in_array($hostPart, ['localhost', 'master'], true);
    }

    protected function resolveLocalMasterPort(): int {
        $dashboardPort = (int)(Runtime::instance()->dashboardPort() ?: 0);
        if ($dashboardPort > 0) {
            return $dashboardPort;
        }
        return $this->resolveGatewayControlPort((int)(MASTER_PORT ?: Runtime::instance()->httpPort()));
    }

    protected function resolveGatewayControlPort(int $businessPort): int {
        if ($businessPort <= 0) {
            return 0;
        }
        $serverConfig = Config::server();
        $configPort = (int)($serverConfig['port'] ?? $businessPort);
        $configuredControlPort = (int)($serverConfig['gateway_control_port'] ?? 0);
        if ($configuredControlPort > 0) {
            $offset = max(1, $configuredControlPort - $configPort);
            return $businessPort + $offset;
        }
        return $businessPort + 1000;
    }

    /**
     * 连接master节点
     * @return WebSocket
     */
    public function getMasterSocketConnection(): WebSocket {
        $socketHost = $this->ensureMasterSocketPath($this->getMasterHost());
        $startedAt = time();
        $attempt = 0;
        $retryDelay = 1;
        while (true) {
            $attempt++;
            try {
                $socket = SaberGM::websocket($this->buildInternalSocketUrl($socketHost, 'node-' . APP_NODE_ID));
                if ($this->wsHandshakeEstablished($socket)) {
                    if ($attempt > 1) {
                        Console::success("【Server】已连接到master节点[{$socketHost}]，重试次数:{$attempt}", false);
                    }
                    return $socket;
                }
                $cli = $this->getWsClient($socket);
                $status = $cli->statusCode ?? 'null';
                $err = $cli->errCode ?? 'null';
                $this->logMasterConnectionRetry($socketHost, "握手未就绪: status={$status} err={$err}", $startedAt, $attempt, $retryDelay);
            } catch (RequestException $throwable) {
                $this->logMasterConnectionRetry($socketHost, '连接失败:' . $throwable->getMessage(), $startedAt, $attempt, $retryDelay);
            }
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep($retryDelay);
            } else {
                sleep($retryDelay);
            }
            $retryDelay = min($retryDelay * 2, 30);
        }
    }

    /**
     * 输出 slave 连接 master 的重试日志。
     *
     * 首次部署或 master 先于 slave 尚未完全就绪时，短时间内连不上控制面是正常现象。
     * 这里在启动宽限期内只输出“等待 master 就绪”的提示，避免生产首次部署时
     * 被误解为真实故障；只有持续超时后才升级为 warning。
     *
     * @param string $socketHost 目标 master socket 地址
     * @param string $reason 最近一次失败原因
     * @param int $startedAt 本轮连接开始时间
     * @param int $attempt 当前重试次数
     * @param int $retryDelay 下一轮重试等待秒数
     * @return void
     */
    protected function logMasterConnectionRetry(string $socketHost, string $reason, int $startedAt, int $attempt, int $retryDelay): void {
        $elapsed = max(0, time() - $startedAt);
        $message = "【Server】等待master节点[{$socketHost}]就绪: {$reason}, attempt={$attempt}, retry_in={$retryDelay}s, elapsed={$elapsed}s";
        if ($elapsed < 60) {
            Console::info($message, false);
            return;
        }
        Console::warning($message, false);
    }

    /**
     * 构建内部 websocket 地址。
     *
     * 这里保留显式协议的语义：
     * - `http://` -> `ws://`
     * - `https://` -> `wss://`
     * - `ws://` / `wss://` 原样沿用
     * 未显式指定协议时，仍默认 `ws://`。
     *
     * @param string $socketHost 可带协议头的 host[:port][/path]。
     * @param string $subject 内部鉴权 token 的 subject。
     * @return string 完整 websocket URL。
     */
    public function buildInternalSocketUrl(string $socketHost, string $subject): string {
        $parts = parse_url(str_contains($socketHost, '://') ? $socketHost : ('ws://' . $socketHost));
        if ($parts === false) {
            $query = http_build_query([
                'token' => DashboardAuth::createInternalSocketToken($subject)
            ]);
            $separator = str_contains($socketHost, '?') ? '&' : '?';
            return 'ws://' . $socketHost . $separator . $query;
        }

        $scheme = strtolower((string)($parts['scheme'] ?? 'ws'));
        $scheme = match ($scheme) {
            'https' => 'wss',
            'http' => 'ws',
            'wss' => 'wss',
            default => 'ws',
        };
        parse_str((string)($parts['query'] ?? ''), $queryParams);
        $queryParams['token'] = DashboardAuth::createInternalSocketToken($subject);

        $authority = trim((string)($parts['host'] ?? ''));
        if ($authority === '') {
            $authority = trim((string)($parts['path'] ?? ''), '/');
            $parts['path'] = '';
        }
        $port = (int)($parts['port'] ?? 0);
        if ($port > 0) {
            $authority .= ':' . $port;
        }
        $path = (string)($parts['path'] ?? '');
        if ($path === '' || $path === '/') {
            $path = '/dashboard.socket';
        }

        return $scheme . '://' . $authority . $path . '?' . http_build_query($queryParams);
    }

    /**
     * 确保 master 连接目标始终落到 dashboard websocket 路径。
     *
     * `master_host` 可能带前缀路径，例如 `/cp`。这里不能只在 path 为空时才补
     * `/dashboard.socket`，否则 slave 会把页面入口路径当成 websocket 握手路径。
     *
     * @param string $socketHost 原始 master 目标。
     * @return string
     */
    protected function ensureMasterSocketPath(string $socketHost): string {
        $parts = parse_url(str_contains($socketHost, '://') ? $socketHost : ('ws://' . $socketHost));
        if ($parts === false) {
            return rtrim($socketHost, '/') . '/dashboard.socket';
        }

        $path = (string)($parts['path'] ?? '');
        if ($path === '' || $path === '/') {
            $path = '/dashboard.socket';
        } elseif (!str_ends_with($path, '/dashboard.socket')) {
            $path = rtrim($path, '/') . '/dashboard.socket';
        }
        $parts['path'] = $path;

        return $this->buildTargetFromParts($parts, isset($parts['scheme']) ? ((string)$parts['scheme'] . '://') : '');
    }

    /**
     * 依据 parse_url 结果重建目标地址。
     *
     * @param array<string, mixed> $parts parse_url 的返回结果。
     * @param string $schemePrefix 协议前缀，例如 `ws://`。
     * @return string
     */
    protected function buildTargetFromParts(array $parts, string $schemePrefix = ''): string {
        $host = trim((string)($parts['host'] ?? ''));
        if ($host === '') {
            $host = trim((string)($parts['path'] ?? ''));
            $parts['path'] = '';
        }
        $target = $schemePrefix . $host;
        $port = (int)($parts['port'] ?? 0);
        if ($port > 0) {
            $target .= ':' . $port;
        }
        $path = (string)($parts['path'] ?? '');
        if ($path !== '') {
            $target .= $path;
        }
        $query = (string)($parts['query'] ?? '');
        if ($query !== '') {
            $target .= '?' . $query;
        }
        $fragment = (string)($parts['fragment'] ?? '');
        if ($fragment !== '') {
            $target .= '#' . $fragment;
        }
        return $target;
    }

    /**
     * 向单一节点发送指令
     * @param string $command
     * @param string $host
     * @param array $params
     * @param string $commander
     * @return Result
     */
    public function sendCommandToNode(string $command, string $host, array $params = [], string $commander = 'main'): Result {
        if ($commander !== 'main') {
            $socket = $this->getMasterSocketConnection();
            $socket->push(JsonHelper::toJson(['event' => 'send_command_to_node', 'data' => ['command' => $command, 'host' => $host, 'params' => $params]]));
            $reply = $socket->recv(30);
            if ($reply === false || $reply->data == '') {
                $socket->close();
                return Result::error('指令发送超时');
            }
            $result = JsonHelper::recover($reply->data);
            $socket->close();
            if (!$result['success']) {
                return Result::error($result['message']);
            }
            return Result::success($result['message']);
        }
        if ($host == SERVER_HOST && App::isMaster()) {
            $host = 'localhost';
        }
        $node = ServerNodeTable::instance()->get($host);
        if (!$node) {
            return Result::error('节点不存在:' . $host);
        }
        $server = \Scf\Server\Http::server();
        if (!$server) {
            return Result::error('服务器未初始化');
        }
        if (!$server->exist($node['socket_fd']) || !$server->isEstablished($node['socket_fd'])) {
            return Result::error('节点不在线');
        }
        $server->push($node['socket_fd'], JsonHelper::toJson(['event' => 'command', 'data' => [
            'command' => $command,
            'params' => $params
        ]]));
        return Result::success();
    }

    /**
     * 向所有节点发送指令
     * @param string $command
     * @param array $params
     * @return int
     */
    public function sendCommandToAllNodeClients(string $command, array $params = []): int {
        $server = \Scf\Server\Http::server();
        $nodes = ServerNodeTable::instance()->rows();
        $successed = 0;
        if ($nodes) {
            foreach ($nodes as $node) {
                if ($node['role'] == NODE_ROLE_MASTER) {
                    //跳过master节点
                    continue;
                }
                if ($server->isEstablished($node['socket_fd'])) {
                    $server->push($node['socket_fd'], JsonHelper::toJson(['event' => 'command', 'data' => [
                        'command' => $command,
                        'params' => $params
                    ]])) and $successed++;
                } else {
                    $server->close($node['socket_fd']);
                    $this->removeNodeClient($node['socket_fd']);
                }
            }
            $successed and Console::log("【Server】已向" . Color::cyan($successed) . "个子节点发送命令：{$command}");
        } else {
            Console::log("【Server】没有可用的节点");
        }
        return $successed;
    }

    /**
     * 添加节点客户端
     * @param $fd
     * @param string $host
     * @param string $role
     * @return bool
     */
    public function addNodeClient($fd, string $host, string $role): bool {
        if ($host == SERVER_HOST && App::isMaster()) {
            $host = 'localhost';
        }
        return ServerNodeTable::instance()->set($host, [
            'host' => $host,
            'socket_fd' => $fd,
            'connect_time' => time(),
            'role' => $role
        ]);
    }

    /**
     * 移除节点客户端
     * @param $fd
     * @return bool
     */
    public function removeNodeClient($fd): bool {
        $nodes = ServerNodeTable::instance()->rows();
        $deleted = false;
        foreach ($nodes as $node) {
            if ($node['socket_fd'] == $fd) {
                ServerNodeTable::instance()->delete($node['host']);
                $deleted = true;
            }
        }
        return $deleted;
    }

    /**
     * 向所有控制面板连接发送消息
     * @param string $message
     * @return void
     */
    public function sendMessageToAllDashboardClients(string $message): void {
        $nodes = Runtime::instance()->get('DASHBOARD_CLIENTS') ?: [];
        if ($nodes) {
            $server = \Scf\Server\Http::server();
            $changed = false;
            foreach ($nodes as $fd) {
                if (!$server->isEstablished($fd) || !$server->push($fd, $message)) {
                    $server->close($fd);
                    $nodes = array_diff($nodes, [$fd]);
                    $changed = true;
                }
            }
            if ($changed) {
                // 仅在移除了客户端时才回写更新，避免无意义的写入
                Runtime::instance()->set('DASHBOARD_CLIENTS', array_values($nodes));
            }
        }
    }

    /**
     * 添加控制面板客户端
     * @param $fd
     * @return bool
     */
    public function addDashboardClient($fd): bool {
        $nodes = Runtime::instance()->get('DASHBOARD_CLIENTS') ?: [];
        if (!in_array($fd, $nodes)) {
            $nodes[] = $fd;
            Runtime::instance()->set('DASHBOARD_CLIENTS', $nodes);
        }
        return true;
    }

    /**
     * 移除控制面板客户端
     * @param $fd
     * @return bool
     */
    public function removeDashboardClient($fd): bool {
        $nodes = Runtime::instance()->get('DASHBOARD_CLIENTS') ?: [];
        if (in_array($fd, $nodes)) {
            $nodes = array_diff($nodes, [$fd]);
            Runtime::instance()->set('DASHBOARD_CLIENTS', array_values($nodes));
        }
        return true;
    }

    /**
     * 取出 Saber WebSocket 底层 client（不同版本可能有 getClient 方法；否则用反射）
     */
    protected function getWsClient(WebSocket $ws) {
        if (method_exists($ws, 'getClient')) {
            return $ws->getClient();
        }
        $ref = new \ReflectionObject($ws);
        if ($ref->hasProperty('client')) {
            $prop = $ref->getProperty('client');
            //$prop->setAccessible(true);
            return $prop->getValue($ws);
        }
        return null;
    }

    /**
     * 判断 Saber WebSocket 是否已完成握手升级。
     *
     * 这个判定只用于“初次建连”阶段，要求状态码为 `101`，确保连接真正升级为
     * websocket；已建立连接后的存活判定请使用 `wsConnected`。
     *
     * @param WebSocket $ws
     * @return bool
     */
    protected function wsHandshakeEstablished(WebSocket $ws): bool {
        $cli = $this->getWsClient($ws);
        if (!$cli) {
            return false;
        }
        $connected = property_exists($cli, 'connected') && (bool)$cli->connected;
        $status = property_exists($cli, 'statusCode') ? (int)$cli->statusCode : 0;
        return $connected && $status === 101;
    }

    /**
     * 判断 Saber WebSocket 在“已建连阶段”是否仍可用。
     *
     * 子进程长轮询 `recv(timeout)` 时，底层 client 可能临时写入 EAGAIN/ETIMEDOUT。
     * 这些错误码只表示“当前无消息”，不能直接视为断链；否则 heartbeat/cluster
     * 会因为空轮询而频繁重连。
     */
    protected function wsConnected(WebSocket $ws): bool {
        $cli = $this->getWsClient($ws);
        if (!$cli) {
            return false;
        }
        $connected = property_exists($cli, 'connected') && (bool)$cli->connected;
        $errCode = property_exists($cli, 'errCode') ? (int)$cli->errCode : 0;
        if (!$connected) {
            return false;
        }
        if ($errCode === 0) {
            return true;
        }
        return $this->isTransientSocketErrCode($errCode);
    }

    /**
     * 判断 WebSocket client 的 errCode 是否为“可忽略的瞬态错误”。
     *
     * @param int $errCode 底层 socket 错误码。
     * @return bool true 表示仅是超时/暂时无数据，不代表连接断开。
     */
    protected function isTransientSocketErrCode(int $errCode): bool {
        if ($errCode <= 0) {
            return false;
        }
        $transientCodes = [11, 60, 110];
        defined('SOCKET_EAGAIN') and $transientCodes[] = (int)SOCKET_EAGAIN;
        defined('SOCKET_EWOULDBLOCK') and $transientCodes[] = (int)SOCKET_EWOULDBLOCK;
        defined('SOCKET_ETIMEDOUT') and $transientCodes[] = (int)SOCKET_ETIMEDOUT;
        return in_array($errCode, array_values(array_unique($transientCodes)), true);
    }

    /**
     * 仅在握手成功的情况下 push，失败返回 false
     */
    protected function wsSafePush(WebSocket $ws, string $data): bool {
        if (!$this->wsConnected($ws)) {
            return false;
        }
        try {
            $ws->push($data);
            return true;
        } catch (Throwable $e) {
            return false;
        }
    }

    /**
     * 对外暴露 Saber WebSocket 当前是否仍处于握手成功且连接存活状态。
     *
     * cluster / heartbeat 这类长连接子进程在 recv timeout 时，需要区分
     * “当前仅无消息”与“底层连接已经断开”。这里复用框架内部已有的 wsConnected
     * 判定，避免每个子进程各自再做一套反射判断。
     *
     * @param WebSocket $ws
     * @return bool
     */
    public function isSocketConnected(WebSocket $ws): bool {
        return $this->wsConnected($ws);
    }

    /**
     * 所有节点状态
     * @return array
     */
    public function getStatus(): array {
        $servers = $this->getServers();
        $master = 0;
        $slave = 0;
        foreach ($servers as $s) {
            if ($s['role'] == NODE_ROLE_MASTER) {
                $master++;
            } else {
                $slave++;
            }
        }
        $appInfo = App::info()->toArray();
        $logger = Log::instance();
        return [
            'event' => 'server_status',
            'info' => [...$appInfo,
                'master' => $master,
                'slave' => $slave,
            ],
            'servers' => $servers,
            'logs' => [
                'error' => [
                    'total' => $logger->count('error', date('Y-m-d')),
                    'list' => []
                ],
                'info' => [
                    'total' => $logger->count('info', date('Y-m-d')),
                    'list' => []
                ],
                'slow' => [
                    'total' => $logger->count('slow', date('Y-m-d')),
                    'list' => []
                ]
            ]
        ];
    }

    /**
     * 获取所有节点的指纹
     * @param bool $online
     * @return array
     */
    public function serverFingerPrints(bool $online = true): array {
        return array_map(function ($value) {
            return $value['fingerprint'];
        }, $this->getServers($online));
    }

    /**
     * 根据节点指纹查找节点信息
     * @param $fingerprint
     * @param bool $online
     * @return array|null
     */
    public function getNodeByFingerprint($fingerprint, bool $online = false): ?array {
        $target = ArrayHelper::findColumn($this->getServers($online), 'fingerprint', $fingerprint);
        return $target ?: null;
    }

    /**
     * 获取节点列表
     * @param bool $onlineOnly
     * @return array
     */
    public function getServers(bool $onlineOnly = true): array {
        if (ENV_MODE == MODE_CGI) {
            $nodes = ServerNodeStatusTable::instance()->rows();
        } else {
            $dashboardPort = (int)(Runtime::instance()->dashboardPort() ?: File::read(SERVER_DASHBOARD_PORT_FILE));
            if ($dashboardPort <= 0) {
                Console::error('dashboard port not found');
                return [];
            }
            $client = Http::create('http://localhost:' . $dashboardPort . '/nodes');
            $result = $client->get();
            if ($result->hasError()) {
                Console::error($result->getMessage());
                return [];
            }
            $nodes = $result->getData('data');
        }
        $list = [];
        if ($nodes) {
            foreach ($nodes as $node) {
                $node['online'] = true;
                if (time() - $node['heart_beat'] > 20) {
                    $node['online'] = false;
                    if ($onlineOnly) {
                        continue;
                    }
                }
                $list[] = $node;
            }
            try {
                ArrayHelper::multisort($list, 'role');
            } catch (Throwable) {
                return $list;
            }
        }
        return $list;
    }
}
