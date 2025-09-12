<?php

namespace Scf\Server;

use Scf\Cache\Redis;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Component;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Runtime;
use Scf\Database\Exception\NullPool;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Server\Struct\Node;
use Scf\Server\Task\CrontabManager;
use Scf\Util\Date;
use Scf\Util\File;
use Swlib\Saber\WebSocket;
use Swlib\SaberGM;
use Swoole\Coroutine;
use Swoole\Coroutine\System;
use Swoole\WebSocket\Server;
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

    public static function clearAllSocketClients(): bool {
        Runtime::instance()->delete('DASHBOARD_CLIENTS');
        return Runtime::instance()->delete('NODE_CLIENTS');
    }

    /**
     * 获取master节点host
     * @return string
     */
    public function getMasterHost(): string {
        $host = Runtime::instance()->get('_MASTER_HOST_');
        if (!$host) {
            $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
            $key = App::id() . ':node:master';
            if (!$node = $masterDB->get($key)) {
                sleep(3);
                return $this->getMasterHost();
            }
            $hostIsIp = filter_var($node['ip'], FILTER_VALIDATE_IP) !== false;
            if ($hostIsIp) {
                $host = $node['ip'] . ':' . $node['port'];
            } else {
                $host = $node['ip'];
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
                Console::warning("【Server】连接master节点[{$socketHost}]握手失败: status={$status} err={$err}", false);
                sleep(10);
                return $this->getMasterSocketConnection();
            }
            return $socket;
        } catch (RequestException $throwable) {
            Console::warning("【Server】连接master节点[{$socketHost}]失败:" . $throwable->getMessage(), false);
            sleep(10);
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
     * @return void
     */
    public function sendCommandToAllNodeClients(string $command, array $params = []): void {
        $server = Http::server();
        $nodes = Runtime::instance()->get('NODE_CLIENTS') ?: [];
        if ($nodes) {
            $successed = 0;
            $changed = false;
            foreach ($nodes as $index => $fd) {
                if ($server->exist($fd) && $server->isEstablished($fd) && $server->push($fd, JsonHelper::toJson(['event' => 'command', 'data' => [
                        'command' => $command,
                        'params' => $params
                    ]]))) {
                    $successed++;
                } else {
                    unset($nodes[$index]);
                    $changed = true;
                }
            }
            if ($changed) {
                Runtime::instance()->set('NODE_CLIENTS', $nodes);
            }
            Console::log("【Server】已向" . Color::cyan($successed) . "个节点发送命令：{$command}");
        }
    }

    /**
     * 添加节点客户端
     * @param $fd
     * @return bool
     */
    public function addNodeClient($fd): bool {
        $nodes = Runtime::instance()->get('NODE_CLIENTS') ?: [];
        if (!in_array($fd, $nodes)) {
            $nodes[] = $fd;
            Runtime::instance()->set('NODE_CLIENTS', $nodes);
        }
        return true;
    }

    /**
     * 移除节点客户端
     * @param $fd
     * @return bool
     */
    public function removeNodeClient($fd): bool {
        $nodes = Runtime::instance()->get('NODE_CLIENTS') ?: [];
        if (in_array($fd, $nodes)) {
            $nodes = array_diff($nodes, [$fd]);
            Runtime::instance()->set('NODE_CLIENTS', $nodes);
        }
        return true;
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
     * 向所有控制面板连接发送消息
     * @param string $message
     * @return void
     */
    public function sendMessageToAllDashboardClients(string $message): void {
        $nodes = Runtime::instance()->get('DASHBOARD_CLIENTS') ?: [];
        if ($nodes) {
            $server = Http::server();
            $changed = false;
            foreach ($nodes as $index => $fd) {
                if (!$server->exist($fd) || !$server->isEstablished($fd) || !$server->push($fd, $message)) {
                    unset($nodes[$index]);
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
     * 移除控制面板客户端
     * @param $fd
     * @return bool
     */
    public function removeDashboardClient($fd): bool {
        $nodes = Runtime::instance()->get('DASHBOARD_CLIENTS') ?: [];
        if (in_array($fd, $nodes)) {
            $nodes = array_diff($nodes, [$fd]);
            Runtime::instance()->set('DASHBOARD_CLIENTS', $nodes);
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
        } catch (\Throwable $e) {
            return false;
        }
    }

    /**
     * 更新
     * @param $id
     * @param $updateKey
     * @param $value
     * @return bool
     */
    public function update($id, $updateKey, $value): bool {
        $key = App::id() . '-node-' . $id;
        $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
        if ($node = $masterDB->get($key)) {
            $node['framework_build_version'] = $node['framework_build_version'] ?? '--';
            $node['framework_update_ready'] = $node['framework_update_ready'] ?? false;
            $node = Node::factory($node);
            $node->heart_beat = time();
            $node->$updateKey = $value;
            return $masterDB->set($key, $node->toArray());
        }
        return false;
    }

    /**
     * 心跳
     * @param Server $server
     * @param Node $node
     * @return bool
     */
    public function heartbeat(Server $server, Node $node): bool {
        $profile = App::profile();
        $node->app_version = $profile->version;
        $node->public_version = $profile->public_version ?: '--';
        $node->heart_beat = time();
        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
        $node->framework_update_ready = file_exists(SCF_ROOT . '/build/update.pack');
        $node->tables = ATable::list();
        $node->restart_times = Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0;
        $node->stack_useage = memory_get_usage(true);
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->server_stats = $server->stats();
        //$node->tasks = CrontabManager::allStatus();
        $node->mysql_execute_count = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
        $node->http_request_reject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
        $node->http_request_count = Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0;
        $node->http_request_count_current = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
        $node->http_request_count_today = Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0;
        $node->http_request_processing = Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0;
        $nodeId = App::isMaster() ? 'master' : $node->id;
        $key = $profile->appid . ':node:' . $nodeId;
        $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
        //App::isMaster() and $masterDB->set($profile->appid . '_MASTER_PORT', Runtime::instance()->httpPort(), -1);
        return $masterDB->set($key, $node->asArray(), 60);
    }

    /**
     * 节点报到
     * @param Node $node
     * @return string|bool
     * @throws Exception
     */
    public function register(Node $node): string|bool {
        if (!$node->validate()) {
            throw new Exception("节点设置错误:" . $node->getError());
        }
        $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
        $profile = App::profile();
        $nodeId = App::isMaster() ? 'master' : $node->id;
        if (!$masterDB->sIsMember($profile->appid . ':nodes', $nodeId)) {
            $masterDB->sAdd($profile->appid . ':nodes', $nodeId);
        }
        $key = $profile->appid . ':node:' . $nodeId;
        //App::isMaster() and $masterDB->set($profile->appid . '_MASTER_PORT', Runtime::instance()->httpPort(), -1);
        return $masterDB->set($key, $node->toArray(), 60);
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
            if ($s['role'] == 'master') {
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
     * 根据指纹移除节点
     * @param $fingerprint
     * @return bool
     */
    public function removeNodeByFingerprint($fingerprint): bool {
        $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
        $node = $this->getNodeByFingerprint($fingerprint);
        if ($node) {
            if (time() - $node['heart_beat'] < 30) {
                return false;
            }
            $key = App::id() . '-node-' . $node['id'];
            try {
                $masterDB->sRemove(App::id() . ':nodes', $node['id']);
                $masterDB->delete($key);
                return true;
            } catch (Throwable $exception) {
                Console::log(Color::red("【" . $key . "】" . "删除失败:" . $exception->getMessage()), false);
            }
        }
        return false;
    }

    /**
     * 获取节点列表
     * @param bool $online
     * @return array
     */
    public function getServers(bool $online = true): array {
        $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
        $this->servers = $masterDB->sMembers(App::id() . ':nodes') ?: [];
        $list = [];
        if ($this->servers) {
            foreach ($this->servers as $id) {
                $key = App::id() . ':node:' . $id;
                if (!$node = $masterDB->get($key)) {
                    continue;
                }
                $node['online'] = true;
                if (time() - $node['heart_beat'] > 20 && $online) {
                    $node['online'] = false;
                    continue;
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