<?php

namespace Scf\Server;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Core\App;
use Scf\Core\Component;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Cache\MasterDB;
use Scf\Helper\ArrayHelper;
use Scf\Command\Color;
use Scf\Server\Runtime\Table;
use Scf\Server\Struct\Node;
use Scf\Server\Table\Counter;
use Scf\Server\Table\Runtime;
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
        $node->app_version = App::version();
        $node->public_version = App::publicVersion() ?: '--';
        $node->heart_beat = time();
        $node->tables = Table::list();
        $node->restart_times = Runtime::instance()->get('restart_times') ?: 0;
        $node->stack_useage = memory_get_usage(true);
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->server_stats = $server->stats();
        //$node->tasks = Crontab::instance()->stats();
        $node->mysql_execute_count = Counter::instance()->get('_MYSQL_EXECUTE_COUNT_' . (time() - 1)) ?: 0;
        $node->http_request_reject = Counter::instance()->get('_REQUEST_REJECT_') ?: 0;
        $node->http_request_count = Counter::instance()->get('_REQUEST_COUNT_') ?: 0;
        $node->http_request_count_current = Counter::instance()->get('_REQUEST_COUNT_' . (time() - 1)) ?: 0;
        $node->http_request_count_today = Counter::instance()->get('_REQUEST_COUNT_' . Date::today()) ?: 0;
        $node->http_request_processing = Counter::instance()->get('_REQUEST_PROCESSING_') ?: 0;
        $key = App::id() . '-node-' . SERVER_NODE_ID;
        if (!MasterDB::sIsMember(App::id() . '-nodes', $node->id)) {
            MasterDB::sAdd(App::id() . '-nodes', $node->id);
        }
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
        if (!MasterDB::sIsMember(App::id() . '-nodes', $node->id)) {
            MasterDB::sAdd(App::id() . '-nodes', $node->id);
        }
        $key = App::id() . '-node-' . $node->id;
        return MasterDB::set($key, $node->toArray());
    }

    /**
     * @return array
     */
    #[ArrayShape(['event' => "string", 'info' => "array", 'servers' => "array", 'logs' => "array[]"])]
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
                    'list' => MasterDB::getLog('error', date('Y-m-d'), 0, 2)
                ],
                'info' => [
                    'total' => MasterDB::countLog('info', date('Y-m-d')),
                    'list' => MasterDB::getLog('info', date('Y-m-d'), 0, 2)
                ],
                'slow' => [
                    'total' => MasterDB::countLog('slow', date('Y-m-d')),
                    'list' => MasterDB::getLog('slow', date('Y-m-d'), 0, 2)
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
     * 获取所有在线节点的指纹
     * @return array
     */
    public function serverFingerPrints(): array {
        return array_map(function ($value) {
            return $value['fingerprint'];
        }, $this->getServers());
    }

    /**
     * 根据节点指纹查找节点信息
     * @param $fingerprint
     * @return array|null
     */
    public function getNodeByFingerprint($fingerprint): ?array {
        $target = ArrayHelper::findColumn($this->getServers(), 'fingerprint', $fingerprint);
        return $target ?: null;
    }

    /**
     * 获取服务器列表
     * @return array
     */
    public function getServers(): array {
        $this->servers = MasterDB::sMembers(App::id() . '-nodes') ?: [];
        $list = [];
        if ($this->servers) {
            foreach ($this->servers as $id) {
                $key = App::id() . '-node-' . $id;
                if (!$node = MasterDB::get($key)) {
                    continue;
                }
                $node['online'] = true;
                if (time() - $node['heart_beat'] >= 5) {
                    $node['online'] = false;
                    MasterDB::sRemove(App::id() . '-nodes', $node['id']);
                    MasterDB::delete($key);
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