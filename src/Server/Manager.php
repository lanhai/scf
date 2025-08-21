<?php

namespace Scf\Server;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Cache\Redis;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Component;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\ATable;
use Scf\Database\Exception\NullPool;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Server\Struct\Node;
use Scf\Server\Task\Crontab;
use Scf\Server\Task\CrontabManager;
use Scf\Util\Date;
use Scf\Util\File;
use Swlib\SaberGM;
use Swoole\Coroutine;
use Swoole\Coroutine\System;
use Swoole\WebSocket\Server;
use Throwable;

class Manager extends Component {

    /**
     * @var array 服务器列表
     */
    protected array $servers = [];

    public function _init(): void {
        parent::_init();
    }

    /**
     * @return string
     */
    public function getNodeId(): string {
        return App::id() . '-node-' . SERVER_NODE_ID;
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
        $key = $profile->appid . '-node-' . SERVER_NODE_ID;
        return Redis::pool($this->_config['service_center_server'] ?? 'main')->set($key, $node->toArray(), -1);
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
        if (!$masterDB->sIsMember($profile->appid . '-nodes', $node->id)) {
            $masterDB->sAdd($profile->appid . '-nodes', $node->id);
        }
        $key = $profile->appid . '-node-' . $node->id;
        return $masterDB->set($key, $node->toArray(), -1);
    }

    /**
     * @return array
     */
    #[ArrayShape(['event' => "string", 'info' => "array", 'servers' => "array", 'framework_update_ready' => "boolean", 'logs' => "array[]"])]
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
     * 获取master节点
     * @return ?Node
     */
    public function getMasterNode(): ?Node {
        $servers = $this->getServers();
        /** @var Node $master */
        $master = null;
        foreach ($servers as $item) {
            $item['framework_build_version'] = $item['framework_build_version'] ?? '--';
            $item['framework_update_ready'] = $item['framework_update_ready'] ?? false;
            $node = Node::factory($item);
            if ($node->role == 'master') {
                $master = $node;
                break;
            }
        }
        return $master;
    }

    /**
     * 重启排程任务
     * @return bool
     */
    public function restart(): bool {
        $master = $this->getMasterNode();
        if (is_null($master)) {
            return false;
        }
        if (SERVER_HOST_IS_IP) {
            $socketHost = $master->ip . ':' . $master->socketPort;
        } else {
            $socketHost = $master->socketPort . '.' . SERVER_HOST;
        }
        try {
            $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
            $websocket->push('reload');
        } catch (Throwable $exception) {
            Console::log(Color::red("【" . $socketHost . "】" . "连接失败:" . $exception->getMessage()), false);
            return false;
        }
        return true;
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
            if (time() - $node['heart_beat'] < 5) {
                return false;
            }
            $key = App::id() . '-node-' . $node['id'];
            try {
                $masterDB->sRemove(App::id() . '-nodes', $node['id']);
                $masterDB->delete($key);
                return true;
            } catch (Throwable $exception) {
                Console::log(Color::red("【" . $key . "】" . "删除失败:" . $exception->getMessage()), false);
            }
        }
        return false;
    }

    /**
     * 获取服务器列表
     * @param bool $online
     * @return array
     */
    public function getServers(bool $online = true): array {
        $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
        $this->servers = $masterDB->sMembers(App::id() . '-nodes') ?: [];
        $list = [];
        if ($this->servers) {
            foreach ($this->servers as $id) {
                $key = App::id() . '-node-' . $id;
                if (!$node = $masterDB->get($key)) {
                    continue;
                }
                $node['online'] = true;
                if (time() - $node['heart_beat'] > 5 && $online) {
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