<?php

namespace Scf\Server;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Cache\MasterDB;
use Scf\Command\Color;
use Scf\Core\App;
use Scf\Core\Component;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Core\Key;
use Scf\Core\Table\Counter;
use Scf\Core\Table\ATable;
use Scf\Helper\ArrayHelper;
use Scf\Server\Struct\Node;
use Scf\Server\Task\Crontab;
use Scf\Util\Date;
use Swlib\SaberGM;
use Swoole\Coroutine;
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
        if ($node = MasterDB::get($key)) {
            $node['framework_build_version'] = $node['framework_build_version'] ?? '--';
            $node['framework_update_ready'] = $node['framework_update_ready'] ?? false;
            $node = Node::factory($node);
            $node->heart_beat = time();
            $node->$updateKey = $value;
            return MasterDB::set($key, $node->toArray());
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
        //$node->tasks = Crontab::instance()->stats();
        $node->mysql_execute_count = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
        $node->http_request_reject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
        $node->http_request_count = Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0;
        $node->http_request_count_current = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
        $node->http_request_count_today = Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0;
        $node->http_request_processing = Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0;
        $key = $profile->appid . '-node-' . SERVER_NODE_ID;
//        if (!MasterDB::sIsMember($profile->appid . '-nodes', $node->id)) {
//            MasterDB::sAdd($profile->appid . '-nodes', $node->id);
//        }
        return MasterDB::set($key, $node->toArray());
    }

    /**
     * 节点报到
     * @param Node $node
     * @return string|bool
     * @throws Exception
     */
    public function report(Node $node): string|bool {
        if (!$node->validate()) {
            throw new Exception("节点设置错误:" . $node->getError());
        }
        $profile = App::profile();
        if (!MasterDB::sIsMember($profile->appid . '-nodes', $node->id)) {
            MasterDB::sAdd($profile->appid . '-nodes', $node->id);
        }
        $key = $profile->appid . '-node-' . $node->id;
        return MasterDB::set($key, $node->toArray());
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
                    'total' => MasterDB::countLog('error', date('Y-m-d')),
                    'list' => [],// MasterDB::getLog('error', date('Y-m-d'), 0, 1)
                ],
                'info' => [
                    'total' => MasterDB::countLog('info', date('Y-m-d')),
                    'list' => [],// MasterDB::getLog('info', date('Y-m-d'), 0, 1)
                ],
                'slow' => [
                    'total' => MasterDB::countLog('slow', date('Y-m-d')),
                    'list' => [],// MasterDB::getLog('slow', date('Y-m-d'), 0, 1)
                ]
            ]
        ];
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
        $node = $this->getNodeByFingerprint($fingerprint);
        if ($node) {
            if (time() - $node['heart_beat'] < 5) {
                return false;
            }
            $key = App::id() . '-node-' . $node['id'];
            try {
                MasterDB::sRemove(App::id() . '-nodes', $node['id']);
                MasterDB::delete($key);
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
        $this->servers = MasterDB::sMembers(App::id() . '-nodes') ?: [];
        $list = [];
        if ($this->servers) {
            foreach ($this->servers as $id) {
                $key = App::id() . '-node-' . $id;
                if (!$node = MasterDB::get($key)) {
                    continue;
                }
                $node['online'] = true;
                if (time() - $node['heart_beat'] > 5 && $online) {
                    $node['online'] = false;
                    //MasterDB::sRemove(App::id() . '-nodes', $node['id']);
                    //MasterDB::delete($key);
                    continue;
                }
                $node['tasks'] = Crontab::instance()->getList();
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