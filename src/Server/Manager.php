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
use Scf\Server\Task\CrontabManager;
use Scf\Util\Date;
use Scf\Util\File;
use Swlib\Saber\WebSocket;
use Swlib\SaberGM;
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
            $host = Config::get('app')['master_host'] ?? '127.0.0.1';
            if (App::isMaster()) {
                $host = '127.0.0.1';
            }
            if (filter_var($host, FILTER_VALIDATE_IP) !== false) {
                $port = MASTER_PORT ?: Runtime::instance()->httpPort();
                $host = $host . ':' . $port;
            }
            Runtime::instance()->set('_MASTER_HOST_', $host);
        }
        return $host;
    }

    /**
     * 连接master节点
     * @return WebSocket
     */
    public function getMasterSocketConnection(): WebSocket {
        $socketHost = $this->getMasterHost();
        if (!str_contains($socketHost, ':')) {
            $socketHost .= '/dashboard.socket';
        }
        try {
            $socket = SaberGM::websocket('ws://' . $socketHost . '?username=node-' . APP_NODE_ID . '&password=' . md5(App::authKey()));
            if (!$this->wsConnected($socket)) {
                $cli = $this->getWsClient($socket);
                $status = $cli->statusCode ?? 'null';
                $err = $cli->errCode ?? 'null';
                Console::warning("【Server】与master节点[{$socketHost}]握手失败: status={$status} err={$err}", false);
                sleep(5);
                return $this->getMasterSocketConnection();
            }
            return $socket;
        } catch (RequestException $throwable) {
            Console::warning("【Server】连接master节点[{$socketHost}]失败:" . $throwable->getMessage(), false);
            sleep(5);
            return $this->getMasterSocketConnection();
        }
    }

    /**
     * 推送控制台日志到master节点
     * @param $log
     * @return void
     */
    public function pushConsoleLog($log): void {
        $socketHost = $this->getMasterHost();
        $client = Http::create("http://{$socketHost}/console.socket");
        $client->post([
            'message' => $log,
            'host' => App::isMaster() ? 'master' : SERVER_HOST
        ]);
        $client->close();
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
     * 判断 Saber WebSocket 是否握手成功（HTTP 101 + connected=true + 无错误码）
     */
    protected function wsConnected(WebSocket $ws): bool {
        $cli = $this->getWsClient($ws);
        if (!$cli) return false;
        $connected = property_exists($cli, 'connected') && (bool)$cli->connected;
        $status = property_exists($cli, 'statusCode') ? (int)$cli->statusCode : 0;
        $errCode = property_exists($cli, 'errCode') ? (int)$cli->errCode : 0;
        return $connected && $status === 101 && $errCode === 0;
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
            $client = Http::create(Dashboard::host() . '/nodes');
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