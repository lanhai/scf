<?php

namespace Scf\Server;

use Scf\Cache\Redis;
use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Component;
use Scf\Core\Config;
use Scf\Core\Console;
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
            $socket = SaberGM::websocket('ws://' . $socketHost . '?username=node-' . SERVER_NODE_ID . '&password=' . md5(App::authKey()));
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
        $client = \Scf\Client\Http::create("http://{$socketHost}/console.socket");
        $client->post([
            'message' => $log,
            'host' => App::isMaster() ? 'master' : SERVER_HOST
        ]);
        $client->close();
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
        return [
            'event' => 'server_status',
            'info' => [...$appInfo,
                'master' => $master,
                'slave' => $slave,
            ],
            'servers' => $servers,
            'logs' => [
                'error' => [
                    'total' => $this->countLog('error', date('Y-m-d')),
                    'list' => []
                ],
                'info' => [
                    'total' => $this->countLog('info', date('Y-m-d')),
                    'list' => []
                ],
                'slow' => [
                    'total' => $this->countLog('slow', date('Y-m-d')),
                    'list' => []
                ]
            ]
        ];
    }

    /**
     * 读取本地日志
     * @param $type
     * @param $day
     * @param $start
     * @param $size
     * @param ?string $subDir
     * @return ?array
     */
    public function getLog($type, $day, $start, $size, string $subDir = null): ?array {
        if ($subDir) {
            $dir = APP_LOG_PATH . '/' . $type . '/' . $subDir . '/';
        } else {
            $dir = APP_LOG_PATH . '/' . $type . '/';
        }
        $fileName = $dir . $day . '.log';
        if (!file_exists($fileName)) {
            return [];
        }
        if ($start < 0) {
            $size = abs($start);
            $start = 0;
        }
        clearstatcache();
        $logs = [];
        // 使用 tac 命令倒序读取文件，然后用 sed 命令读取指定行数
        $command = sprintf(
            'tac %s | sed -n %d,%dp',
            escapeshellarg($fileName),
            $start + 1,
            $start + $size
        );
        $result = System::exec($command);
        if ($result === false) {
            return [];
        }
        $lines = explode("\n", $result['output']);
        foreach ($lines as $line) {
            if (trim($line) && ($log = JsonHelper::is($line) ? JsonHelper::recover($line) : $line)) {
                $logs[] = $log;
            }
        }
        return $logs;
    }

    /**
     * 统计日志
     * @param string $type
     * @param string $day
     * @param string|null $subDir
     * @return int
     */
    public function countLog(string $type, string $day, string $subDir = null): int {
        if ($subDir) {
            $dir = APP_LOG_PATH . '/' . $type . '/' . $subDir . '/';
        } else {
            $dir = APP_LOG_PATH . '/' . $type . '/';
        }
        $fileName = $dir . $day . '.log';
        return $this->countFileLines($fileName);
    }

    /**
     * 记录日志
     * @param string $type
     * @param mixed $message
     * @return bool|int
     */
    public function addLog(string $type, mixed $message): bool|int {
        try {
            //TODO 日志推送到master节点
            $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
            if ($masterDB instanceof NullPool) {
                if (!IS_HTTP_SERVER) {
                    //日志本地化
                    if ($type == 'crontab') {
                        $dir = APP_LOG_PATH . '/' . $type . '/' . $message['task'] . '/';
                        $content = $message['message'];
                    } else {
                        $dir = APP_LOG_PATH . '/' . $type . '/';
                        $content = $message;
                    }
                    $fileName = $dir . date('Y-m-d', strtotime(Date::today())) . '.log';
                    if (!is_dir($dir)) {
                        mkdir($dir, 0775, true);
                    }
                    File::write($fileName, !is_string($content) ? JsonHelper::toJson($content) : $content, true);
                }
                return false;
            }
            $queueKey = "_LOGS_" . $type;
            return $masterDB->lPush($queueKey, [
                'day' => Date::today(),
                'message' => $message
            ]);
        } catch (Throwable) {
            return false;
        }
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
        //$masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
        //$this->servers = $masterDB->sMembers(App::id() . ':nodes') ?: [];
        if (SERVER_MODE == MODE_CGI) {
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
//                $key = App::id() . ':node:' . $id;
//                if (!$node = $masterDB->get($key)) {
//                    continue;
//                }
                $node['online'] = true;
                if (time() - $node['heart_beat'] > 20) {
                    $node['online'] = false;
                    if ($onlineOnly) {
                        continue;
                    }
                }
                $node['tasks'] = CrontabManager::allStatus();
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

    /**
     * 统计日志文件行数
     * @param $file
     * @return int
     */
    protected function countFileLines($file): int {
        $line = 0; //初始化行数
        if (file_exists($file)) {
            $output = trim(System::exec("wc -l " . escapeshellarg($file))['output']);
            $arr = explode(' ', $output);
            $line = (int)$arr[0];
        }
        return $line;
    }
}