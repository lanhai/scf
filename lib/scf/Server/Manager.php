<?php

namespace Scf\Server;

use JetBrains\PhpStorm\ArrayShape;
use Scf\Core\Component;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Mode\Web\Updater;
use Scf\Runtime\MasterDB;
use Scf\Runtime\Table;
use Scf\Server\Struct\NodeStruct;
use Scf\Server\Table\Counter;
use Scf\Server\Table\Runtime;
use Swoole\Coroutine;

class Manager extends Component {

    /**
     * @var array 服务器列表
     */
    protected array $servers = [];
    /**
     * @var string
     */
    protected string $lkey = APP_ID . '-nodes';

    public function _init() {
        parent::_init();
    }

    /**
     * 节点报到
     * @param NodeStruct $node
     * @return string|null
     * @throws Exception
     */
    public function report(NodeStruct $node): ?string {
        $version = Updater::instance()->getLocalVersion();
        $node->app_version = $version['version'];
        $node->scf_version = SCF_VERSION;
        $node->heart_beat = time();
        $node->tables = Table::list();
        $node->restart_times = Runtime::instance()->get('restart_times') ?: 0;
        $node->stack_useage = Coroutine::getStackUsage();
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->tasks = Crontab::instance()->stats();
        $node->server_run_mode = APP_RUN_MODE;
        if (!$node->validate()) {
            throw new Exception("节点设置错误:" . $node->getError());
        }
        if (!$this->getServers() || !in_array($node->id, $this->servers)) {
            MasterDB::lPush($this->lkey, $node->id);
        }
        $key = APP_ID . '-node-' . $node->id;
        return MasterDB::set($key, $node->toArray());
    }

    /**
     * @return string
     */
    public function getNodeId(): string {
        return APP_ID . '-node-' . SERVER_NODE_ID;
    }

    /**
     * 更新
     * @param $id
     * @param $updateKey
     * @param $value
     * @return bool
     */
    public function update($id, $updateKey, $value): bool {
        $key = APP_ID . '-node-' . $id;
        if ($node = MasterDB::get($key)) {
            $node = NodeStruct::factory($node);
            $node->heart_beat = time();
            $node->$updateKey = $value;
            return MasterDB::set($key, $node->toArray());
        }
        return false;
    }

    /**
     * 心跳
     * @return bool
     */
    public function heartbeat(): bool {
        $key = APP_ID . '-node-' . SERVER_NODE_ID;
        $version = Updater::instance()->getLocalVersion();
        if ($nodeInfo = MasterDB::get($key)) {
            $node = NodeStruct::factory($nodeInfo);
            $node->app_version = $version['version'];
            $node->scf_version = SCF_VERSION;
            $node->heart_beat = time();
            $node->tables = Table::list();
            $node->restart_times = Runtime::instance()->get('restart_times') ?: 0;
            $node->stack_useage = memory_get_usage(true);
            $node->threads = count(Coroutine::list());
            $node->thread_status = Coroutine::stats();
            $node->tasks = Crontab::instance()->stats();
            $node->http_request_count = Counter::instance()->get('_REQUEST_COUNT_');
            return MasterDB::set($key, $node->toArray());
        }
        return false;
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
        return [
            'event' => 'server_status',
            'info' => [
                'appid' => $servers[0]['appid'],
                'name' => $servers[0]['name'],
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
                ]
            ]
        ];
    }

    /**
     * 获取服务器列表
     * @return array
     */
    public function getServers(): array {
        MasterDB::init();
        $this->servers = MasterDB::lAll($this->lkey) ?: [];
        $list = [];
        if ($this->servers) {
            foreach ($this->servers as $id) {
                $key = APP_ID . '-node-' . $id;
                if (!$node = MasterDB::get($key)) {
                    continue;
                }
                $list[] = $node;
            }
        }
        return $list;
    }
}