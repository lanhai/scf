<?php

namespace Scf\Server\Gateway;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Core\Table\GatewayCommandHistoryTable;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Core\Table\ServerNodeTable;
use Scf\Helper\JsonHelper;
use Scf\Server\Manager;
use Swoole\Coroutine\Channel;
use Swoole\Timer;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;

/**
 * Gateway 集群心跳与节点同步职责。
 *
 * 职责边界：
 * - 负责节点连接管理、心跳快照组包、节点状态表刷新与集群消息分发；
 * - 不负责 dashboard 协议入口与业务指标展示拼装。
 *
 * 架构位置：
 * - 位于 GatewayServer 的 cluster 协调层；
 * - 连接 node websocket 事件与 ServerNodeStatusTable 内存态。
 *
 * 设计意图：
 * - 将 cluster tick 与 heartbeat 语义拆分维护，
 *   保证节点新鲜度展示与心跳周期统计互不覆盖。
 */
trait GatewayClusterHeartbeatTrait {
    /**
     * 处理来自集群节点 socket 的上报事件。
     *
     * @param Server $server Gateway socket server
     * @param Frame $frame 当前消息帧
     * @return void
     */
    protected function handleNodeSocketMessage(Server $server, Frame $frame): void {
        if (!JsonHelper::is($frame->data)) {
            return;
        }

        $data = JsonHelper::recover($frame->data);
        $event = (string)($data['event'] ?? '');
        switch ($event) {
            case 'slave_node_report':
                $host = (string)($data['data']['host'] ?? $data['data'] ?? '');
                $role = (string)($data['data']['role'] ?? NODE_ROLE_SLAVE);
                $reportIp = (string)($data['data']['ip'] ?? '');
                if ($host === '') {
                    return;
                }
                $existingNode = ServerNodeTable::instance()->get($host);
                $previousFd = (int)($existingNode['socket_fd'] ?? 0);
                if ($this->addNodeClient($frame->fd, $host, $role)) {
                    $server->push($frame->fd, JsonHelper::toJson(['event' => 'slave_node_report_response', 'data' => $frame->fd]));
                    $this->pushConsoleSubscription($frame->fd);
                    $peerCount = 0;
                    foreach (ServerNodeTable::instance()->rows() as $node) {
                        if (($node['role'] ?? '') !== NODE_ROLE_MASTER) {
                            $peerCount++;
                        }
                    }
                    $nodeLabel = "node_id={$host}, role={$role}, fd={$frame->fd}, peers={$peerCount}";
                    if ($reportIp !== '') {
                        $nodeLabel .= ", ip={$reportIp}";
                    }
                    if ($previousFd > 0 && $previousFd !== $frame->fd) {
                        Console::warning("【GatewayCluster】节点重连加入: {$nodeLabel}, previous_fd={$previousFd}");
                    } else {
                        Console::success("【GatewayCluster】节点加入: {$nodeLabel}");
                    }
                    $this->pushDashboardStatus();
                } else {
                    $failedLabel = "node_id={$host}, role={$role}, fd={$frame->fd}";
                    if ($reportIp !== '') {
                        $failedLabel .= ", ip={$reportIp}";
                    }
                    Console::warning("【GatewayCluster】节点加入失败: {$failedLabel}");
                }
                break;
            case 'node_heart_beat':
                $host = (string)($data['data']['host'] ?? '');
                $status = (array)($data['data']['status'] ?? []);
                if ($host !== '' && $status) {
                    $previousStatus = (array)(ServerNodeStatusTable::instance()->get($host) ?: []);

                    // Linux 排程的最近同步状态由独立 socket 事件即时回传，
                    // 普通节点心跳不能把这段运行态覆盖掉，否则 dashboard
                    // 刷新后会出现“已安装/已启用，但最近同步又变成空”的假象。
                    if (!isset($status['linux_crontab_sync']) && isset($previousStatus['linux_crontab_sync'])) {
                        $status['linux_crontab_sync'] = $previousStatus['linux_crontab_sync'];
                    }
                    if (!isset($status['linux_crontab_installed']) && isset($previousStatus['linux_crontab_installed'])) {
                        $status['linux_crontab_installed'] = $previousStatus['linux_crontab_installed'];
                    }

                    // 兼容旧版 slave 心跳还没稳定带 env 的情况，避免节点弹窗里环境列反复掉成空值。
                    if (empty($status['env']) && !empty($previousStatus['env'])) {
                        $status['env'] = $previousStatus['env'];
                    }

                    ServerNodeStatusTable::instance()->set($host, $status);
                }
                if ($server->isEstablished($frame->fd)) {
                    $server->push($frame->fd, '::pong');
                }
                break;
            case 'node_update_state':
                $payload = (array)($data['data'] ?? []);
                $taskId = (string)($payload['task_id'] ?? '');
                $host = (string)($payload['host'] ?? '');
                if ($taskId !== '' && $host !== '') {
                    Runtime::instance()->set($this->nodeUpdateTaskStateKey($taskId, $host), $payload);
                }
                $commandId = trim((string)($payload['command_id'] ?? ''));
                if ($commandId !== '') {
                    $this->acceptSlaveCommandFeedback([
                        'command_id' => $commandId,
                        'command' => (string)($payload['command'] ?? 'appoint_update'),
                        'host' => $host,
                        'state' => (string)($payload['state'] ?? ''),
                        'message' => (string)($payload['message'] ?? ''),
                        'error' => (string)($payload['error'] ?? ''),
                        'updated_at' => (int)($payload['updated_at'] ?? time()),
                        'result' => (array)$payload,
                    ]);
                }
                Manager::instance()->sendMessageToAllDashboardClients(JsonHelper::toJson([
                    'event' => 'node_update_state',
                    'data' => $payload,
                ]));
                if (!empty($payload['message'])) {
                    Console::info((string)$payload['message'], false);
                }
                break;
            case 'slave_command_feedback':
                $payload = (array)($data['data'] ?? []);
                $this->acceptSlaveCommandFeedback($payload);
                break;
            case 'linux_crontab_sync_state':
                $payload = (array)($data['data'] ?? []);
                $this->acceptLinuxCrontabSyncState($payload);
                break;
            case 'linux_crontab_installed_state':
                $payload = (array)($data['data'] ?? []);
                $this->acceptLinuxCrontabInstalledState($payload);
                break;
            case 'console_log':
                $this->acceptConsolePayload((array)($data['data'] ?? []));
                break;
            default:
                break;
        }
    }

    protected function acceptLinuxCrontabSyncState(array $payload): void {
        $host = (string)($payload['host'] ?? '');
        if ($host === '') {
            return;
        }

        $syncState = [
            'state' => (string)($payload['state'] ?? ''),
            'message' => (string)($payload['message'] ?? ''),
            'error' => (string)($payload['error'] ?? ''),
            'item_count' => (int)($payload['item_count'] ?? 0),
            'sync' => (array)($payload['sync'] ?? []),
            'updated_at' => (int)($payload['updated_at'] ?? time()),
        ];
        Runtime::instance()->set($this->nodeLinuxCrontabSyncStateKey($host), $syncState);

        $status = (array)($payload['status'] ?? []);
        if (!$status) {
            $status = (array)(ServerNodeStatusTable::instance()->get($host) ?: []);
        }
        if ($status) {
            $status['linux_crontab_sync'] = $syncState;
            ServerNodeStatusTable::instance()->set($host, $status);
        }

        if ($syncState['message'] !== '') {
            if ($syncState['state'] === 'failed') {
                Console::warning($syncState['message'], false);
            } else {
                Console::info($syncState['message'], false);
            }
        }

        $this->pushDashboardStatus();
    }

    protected function nodeLinuxCrontabSyncStateKey(string $host): string {
        // Swoole\Table 的 row key 长度有限制（常见上限 64 字节），
        // 直接拼接较长 node_id 会触发 "key is too long" 并导致写入失败。
        // 这里统一用定长 hash，保证 Runtime key 可写且跨进程读写一致。
        return 'linux_crontab_sync_state:' . md5($host);
    }

    /**
     * 接收并落盘 slave 回报的“本机系统 crontab 快照”。
     *
     * @param array<string, mixed> $payload
     * @return void
     */
    protected function acceptLinuxCrontabInstalledState(array $payload): void {
        $host = (string)($payload['host'] ?? '');
        if ($host === '') {
            return;
        }

        $installedState = [
            'state' => (string)($payload['state'] ?? ''),
            'message' => (string)($payload['message'] ?? ''),
            'error' => (string)($payload['error'] ?? ''),
            'updated_at' => (int)($payload['updated_at'] ?? time()),
            'snapshot' => (array)($payload['snapshot'] ?? []),
            'request_id' => trim((string)($payload['request_id'] ?? '')),
        ];
        Runtime::instance()->set($this->nodeLinuxCrontabInstalledStateKey($host), $installedState);

        $status = (array)($payload['status'] ?? []);
        if (!$status) {
            $status = (array)(ServerNodeStatusTable::instance()->get($host) ?: []);
        }
        if ($status) {
            $status['linux_crontab_installed'] = $installedState;
            ServerNodeStatusTable::instance()->set($host, $status);
        }

        if ($installedState['message'] !== '') {
            if ($installedState['state'] === 'failed') {
                Console::warning($installedState['message'], false);
            } else {
                Console::info($installedState['message'], false);
            }
        }

        $this->pushDashboardStatus();
    }

    protected function nodeLinuxCrontabInstalledStateKey(string $host): string {
        return 'linux_crontab_installed_state:' . md5($host);
    }

    /**
     * 查询 slave 指令历史（分页）。
     *
     * @param int $page 页码
     * @param int $size 每页数量
     * @param string $host host 过滤
     * @param string $state 状态过滤
     * @return array{page:int,size:int,total:int,list:array<int,array<string,mixed>>,summary:array<string,int>}
     */
    public function dashboardCommandHistory(int $page = 1, int $size = 20, string $host = '', string $state = ''): array {
        return GatewayCommandHistoryTable::instance()->paginate($page, $size, $host, $state);
    }

    /**
     * 汇总 slave 指令历史状态。
     *
     * @return array{total:int,accepted:int,running:int,success:int,failed:int,pending:int,other:int}
     */
    protected function slaveCommandHistorySummary(): array {
        return GatewayCommandHistoryTable::instance()->summary();
    }

    /**
     * 生成一条稳定且可追踪的 slave 指令 id。
     *
     * @param string $command 指令名
     * @param string $host 目标 host
     * @return string
     */
    protected function createSlaveCommandId(string $command, string $host): string {
        return 'cmd_' . dechex(time()) . '_' . substr(md5($command . '|' . $host . '|' . uniqid('', true)), 0, 16);
    }

    /**
     * 过滤可落盘的命令参数，避免把过大的 payload 直接写入历史记录。
     *
     * @param array<string, mixed> $params 原始命令参数
     * @return array<string, mixed>
     */
    protected function sanitizeSlaveCommandHistoryParams(array $params): array {
        $clean = $params;
        if (isset($clean['command_id'])) {
            unset($clean['command_id']);
        }
        $encoded = json_encode($clean, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if (!is_string($encoded) || strlen($encoded) <= 2048) {
            return $clean;
        }
        return [
            '_truncated' => true,
            '_raw_size' => strlen($encoded),
        ];
    }

    /**
     * 过滤可落盘的命令结果，避免回执 payload 过大写爆历史表行。
     *
     * @param array<string, mixed> $result 原始结果
     * @return array<string, mixed>
     */
    protected function sanitizeSlaveCommandHistoryResult(array $result): array {
        $encoded = json_encode($result, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if (!is_string($encoded) || strlen($encoded) <= 4096) {
            return $result;
        }
        return [
            '_truncated' => true,
            '_raw_size' => strlen($encoded),
        ];
    }

    /**
     * 新增一条 slave 指令历史，并推送实时更新到 dashboard。
     *
     * @param array<string, mixed> $record 指令记录
     * @return array<string, mixed>
     */
    protected function appendSlaveCommandHistoryRecord(array $record): array {
        $saved = GatewayCommandHistoryTable::instance()->append($record);
        $this->pushSlaveCommandHistoryUpdate('created', $saved);
        return $saved;
    }

    /**
     * 根据 command_id 更新指令历史并广播到 dashboard。
     *
     * @param string $commandId 指令 id
     * @param array<string, mixed> $updates 更新字段
     * @return array<string, mixed>|null
     */
    protected function updateSlaveCommandHistoryRecord(string $commandId, array $updates): ?array {
        $updated = GatewayCommandHistoryTable::instance()->updateByCommandId($commandId, $updates);
        if (is_array($updated)) {
            $this->pushSlaveCommandHistoryUpdate('updated', $updated);
        }
        return $updated;
    }

    /**
     * 接收并落盘 slave 端的指令回执。
     *
     * @param array<string, mixed> $payload 回执数据
     * @return void
     */
    protected function acceptSlaveCommandFeedback(array $payload): void {
        $commandId = trim((string)($payload['command_id'] ?? ''));
        if ($commandId === '') {
            return;
        }
        $host = trim((string)($payload['host'] ?? ''));
        $command = trim((string)($payload['command'] ?? ''));
        $state = trim((string)($payload['state'] ?? ''));
        $message = trim((string)($payload['message'] ?? ''));
        $error = trim((string)($payload['error'] ?? ''));
        $record = $this->updateSlaveCommandHistoryRecord($commandId, [
            'host' => $host,
            'command' => $command,
            'state' => $state === '' ? 'running' : $state,
            'message' => $message,
            'error' => $error,
            'result' => $this->sanitizeSlaveCommandHistoryResult((array)($payload['result'] ?? [])),
            'updated_at' => (int)($payload['updated_at'] ?? time()),
        ]);
        if ($record !== null) {
            return;
        }

        $this->appendSlaveCommandHistoryRecord([
            'command_id' => $commandId,
            'command' => $command,
            'host' => $host,
            'state' => $state === '' ? 'running' : $state,
            'message' => $message,
            'error' => $error,
            'params' => [],
            'result' => $this->sanitizeSlaveCommandHistoryResult((array)($payload['result'] ?? [])),
            'source' => 'feedback_orphan',
            'created_at' => (int)($payload['updated_at'] ?? time()),
            'updated_at' => (int)($payload['updated_at'] ?? time()),
        ]);
    }

    /**
     * 向 dashboard 广播指令历史更新事件。
     *
     * @param string $action 更新类型 created/updated
     * @param array<string, mixed> $record 指令记录
     * @return void
     */
    protected function pushSlaveCommandHistoryUpdate(string $action, array $record): void {
        Manager::instance()->sendMessageToAllDashboardClients(JsonHelper::toJson([
            'event' => 'command_history_update',
            'data' => [
                'action' => $action,
                'record' => $record,
                'summary' => $this->slaveCommandHistorySummary(),
            ],
        ]));
    }

    /**
     * 组装本机 heartbeat 快照并叠加业务运行态。
     *
     * @param array<string, mixed> $status heartbeat 基础状态
     * @return array<string, mixed>
     */
    protected function buildGatewayHeartbeatStatus(array $status): array {
        $previousLocalStatus = (array)(ServerNodeStatusTable::instance()->get('localhost') ?: []);
        // heartbeat 进程会周期性整包覆写 localhost 状态。
        // cluster_tick_at 由 1s cluster tick 维护，不能在 heartbeat 写回时丢失。
        // 这里显式保留上一轮 cluster tick，确保 dashboard 始终能拿到该字段。
        $status['cluster_tick_at'] = (int)($previousLocalStatus['cluster_tick_at'] ?? 0);
        if ($status['cluster_tick_at'] <= 0) {
            $status['cluster_tick_at'] = time();
        }

        // 心跳子进程的某一轮 tick 可能恰好与 gateway shutdown 交错。
        // 即便上层定时器入口已经做过一次守卫，这里仍需在真正拼装业务 overlay 之前
        // 再检查一次，避免进入 dashboardUpstreams() 时撞上 shutdown 窗口里的 upstream IPC。
        if ($this->shouldSkipGatewayBusinessOverlayDuringShutdown()) {
            return array_replace_recursive($status, [
                'name' => 'Gateway',
                'script' => 'gateway',
                'ip' => SERVER_HOST,
                'port' => $this->port,
                'role' => SERVER_ROLE,
                'server_run_mode' => APP_SRC_TYPE,
                'proxy_mode_label' => 'gateway_proxy',
                'manager_pid' => (int)($this->server->manager_pid ?? $this->serverManagerPid),
                'master_pid' => (int)($this->server->master_pid ?? $this->serverMasterPid),
                'fingerprint' => APP_FINGERPRINT . ':gateway',
            ]);
        }
        // heartbeat 只采当前 active upstream 的运行态，避免“全量并发探测”拖慢心跳循环。
        // 运行态优先走本机 IPC 实时抓取；当 IPC 短暂超时，再回退到本进程最近缓存。
        $status = $this->composeGatewayNodeRuntimeStatus($status, $this->heartbeatBusinessUpstreams());
        return array_replace_recursive($status, [
            'name' => 'Gateway',
            'script' => 'gateway',
            'ip' => SERVER_HOST,
            'port' => $this->port,
            'role' => SERVER_ROLE,
            'server_run_mode' => APP_SRC_TYPE,
            'proxy_mode_label' => 'gateway_proxy',
            'manager_pid' => (int)($this->server->manager_pid ?? $this->serverManagerPid),
            'master_pid' => (int)($this->server->master_pid ?? $this->serverMasterPid),
            'fingerprint' => APP_FINGERPRINT . ':gateway',
        ]);
    }

    protected function heartbeatBusinessUpstreams(): array {
        $snapshot = $this->currentGatewayStateSnapshot();
        $activeVersion = (string)($snapshot['active_version'] ?? '');
        $plans = [];

        foreach (($snapshot['generations'] ?? []) as $generation) {
            if (!is_array($generation)) {
                continue;
            }
            $generationVersion = (string)($generation['version'] ?? '');
            $generationStatus = (string)($generation['status'] ?? '');
            if ($activeVersion !== '') {
                if ($generationVersion !== $activeVersion) {
                    continue;
                }
            } elseif ($generationStatus !== 'active') {
                continue;
            }
            foreach (($generation['instances'] ?? []) as $instance) {
                if (!is_array($instance)) {
                    continue;
                }
                $metadata = (array)($instance['metadata'] ?? []);
                if (($metadata['managed'] ?? false) !== true) {
                    continue;
                }
                $host = (string)($instance['host'] ?? '127.0.0.1');
                $port = (int)($instance['port'] ?? 0);
                if ($host === '' || $port <= 0) {
                    continue;
                }
                $plans[] = [
                    'generation' => $generation,
                    'instance' => $instance,
                ];
            }
        }

        if (!$plans) {
            return [];
        }

        $nodes = [];
        foreach ($plans as $plan) {
            $generation = (array)($plan['generation'] ?? []);
            $instance = (array)($plan['instance'] ?? []);
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            if ($host === '' || $port <= 0) {
                continue;
            }
            $runtimeStatus = $this->fetchUpstreamInternalStatusSync(
                $host,
                $port,
                self::INTERNAL_UPSTREAM_STATUS_PATH,
                1.0
            );
            if ($runtimeStatus) {
                $this->instanceManager->updateInstanceRuntimeStatus($host, $port, $runtimeStatus);
            } else {
                $runtimeStatus = $this->instanceManager->instanceRuntimeStatus($host, $port, 30);
            }
            $nodes[] = $this->buildUpstreamNode($generation, $instance, $runtimeStatus, true);
        }

        return $nodes;
    }

    /**
     * 刷新 localhost 在节点状态表中的快照。
     *
     * @return void
     */
    protected function refreshLocalGatewayNodeStatus(): void {
        ServerNodeStatusTable::instance()->set('localhost', $this->buildGatewayClusterNode());
    }

    protected function buildRemoteGatewayNodes(): array {
        $nodes = [];
        foreach (ServerNodeStatusTable::instance()->rows() as $host => $status) {
            if ($host === 'localhost' || !is_array($status)) {
                continue;
            }
            $status['online'] = (time() - (int)($status['heart_beat'] ?? 0)) <= 20;
            $nodes[] = $status;
        }
        return $nodes;
    }

    /**
     * 构建 cluster tick 维度的 localhost 节点信息。
     *
     * @return array<string, mixed>
     */
    protected function buildGatewayClusterNode(): array {
        $previousNode = (array)(ServerNodeStatusTable::instance()->get('localhost') ?: []);
        $node = $this->buildGatewayNode();
        $node['host'] = 'localhost';
        $node['id'] = APP_NODE_ID;
        $node['appid'] = App::id() ?: 'scf_app';
        // cluster tick 是 1s 控制面刷新，不应覆盖 heartbeat 的 5s 语义时间戳。
        // 这里单独记录 cluster tick 时间，并保留 heartbeat 原值供 dashboard 计算 HB 延迟。
        $node['cluster_tick_at'] = time();
        $node['heart_beat'] = (int)($node['heart_beat'] ?? ($previousNode['heart_beat'] ?? 0));
        $node['master_pid'] = (int)($this->server->master_pid ?? $this->serverMasterPid);
        $node['manager_pid'] = (int)($this->server->manager_pid ?? $this->serverManagerPid);
        $node['fingerprint'] = APP_FINGERPRINT;
        return $node;
    }

    protected function addNodeClient(int $fd, string $host, string $role): bool {
        return ServerNodeTable::instance()->set($host, [
            'host' => $host,
            'socket_fd' => $fd,
            'connect_time' => time(),
            'role' => $role,
        ]);
    }

    protected function removeNodeClient(int $fd): bool {
        $deleted = false;
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if ((int)($node['socket_fd'] ?? 0) !== $fd) {
                continue;
            }
            $host = (string)$node['host'];
            $role = (string)($node['role'] ?? NODE_ROLE_SLAVE);
            ServerNodeTable::instance()->delete($host);
            ServerNodeStatusTable::instance()->delete($host);
            Console::warning("【GatewayCluster】节点断开移除: host={$host}, role={$role}, fd={$fd}");
            $deleted = true;
        }
        if ($deleted) {
            $this->pushDashboardStatus();
        }
        return $deleted;
    }

    protected function pruneDisconnectedNodeClients(): void {
        foreach (ServerNodeTable::instance()->rows() as $node) {
            $fd = (int)($node['socket_fd'] ?? 0);
            if ($fd > 0 && $this->server->exist($fd) && $this->server->isEstablished($fd)) {
                continue;
            }
            $this->removeNodeClient($fd);
        }
    }

    protected function hasConnectedNode(string $host): bool {
        return ServerNodeTable::instance()->exist($this->normalizeNodeHost($host));
    }

    protected function sendCommandToNodeClient(string $command, string $host, array $params = []): Result {
        $normalized = $this->normalizeNodeHost($host);
        $commandId = trim((string)($params['command_id'] ?? ''));
        if ($commandId === '') {
            $commandId = $this->createSlaveCommandId($command, $normalized);
        }
        $dispatchParams = $params;
        $dispatchParams['command_id'] = $commandId;
        $node = ServerNodeTable::instance()->get($normalized);
        if (!$node) {
            $this->appendSlaveCommandHistoryRecord([
                'command_id' => $commandId,
                'command' => $command,
                'host' => $normalized,
                'state' => 'failed',
                'message' => '节点不存在，命令未下发',
                'error' => 'node_not_found',
                'params' => $this->sanitizeSlaveCommandHistoryParams($dispatchParams),
                'result' => [],
                'source' => 'single',
                'created_at' => time(),
                'updated_at' => time(),
            ]);
            return Result::error('节点不存在:' . $host, data: [
                'command_id' => $commandId,
                'host' => $normalized,
            ]);
        }
        $fd = (int)($node['socket_fd'] ?? 0);
        if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
            $this->removeNodeClient($fd);
            $this->appendSlaveCommandHistoryRecord([
                'command_id' => $commandId,
                'command' => $command,
                'host' => $normalized,
                'state' => 'failed',
                'message' => '节点不在线，命令未下发',
                'error' => 'node_offline',
                'params' => $this->sanitizeSlaveCommandHistoryParams($dispatchParams),
                'result' => [],
                'source' => 'single',
                'created_at' => time(),
                'updated_at' => time(),
            ]);
            return Result::error('节点不在线', data: [
                'command_id' => $commandId,
                'host' => $normalized,
            ]);
        }
        $this->appendSlaveCommandHistoryRecord([
            'command_id' => $commandId,
            'command' => $command,
            'host' => $normalized,
            'state' => 'accepted',
            'message' => '指令已下发，等待 slave 回执',
            'params' => $this->sanitizeSlaveCommandHistoryParams($dispatchParams),
            'result' => [],
            'source' => 'single',
            'created_at' => time(),
            'updated_at' => time(),
        ]);
        $pushed = $this->server->push($fd, JsonHelper::toJson(['event' => 'command', 'data' => [
            'command' => $command,
            'params' => $dispatchParams,
        ]]));
        if (!$pushed) {
            $this->updateSlaveCommandHistoryRecord($commandId, [
                'state' => 'failed',
                'message' => '指令下发失败',
                'error' => 'socket_push_failed',
                'updated_at' => time(),
            ]);
            return Result::error('节点命令下发失败');
        }
        return Result::success([
            'command_id' => $commandId,
            'host' => $normalized,
        ]);
    }

    protected function sendCommandToAllNodeClients(string $command, array $params = []): int {
        $success = 0;
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                continue;
            }
            $fd = (int)($node['socket_fd'] ?? 0);
            if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
                $this->removeNodeClient($fd);
                continue;
            }
            $host = (string)($node['host'] ?? '');
            if ($host === '') {
                continue;
            }
            $commandId = $this->createSlaveCommandId($command, $host);
            $dispatchParams = $params;
            $dispatchParams['command_id'] = $commandId;
            $this->appendSlaveCommandHistoryRecord([
                'command_id' => $commandId,
                'command' => $command,
                'host' => $host,
                'state' => 'accepted',
                'message' => '指令已下发，等待 slave 回执',
                'params' => $this->sanitizeSlaveCommandHistoryParams($dispatchParams),
                'result' => [],
                'source' => 'broadcast',
                'created_at' => time(),
                'updated_at' => time(),
            ]);
            if ($this->server->push($fd, JsonHelper::toJson(['event' => 'command', 'data' => [
                'command' => $command,
                'params' => $dispatchParams,
            ]]))) {
                $success++;
            } else {
                $this->updateSlaveCommandHistoryRecord($commandId, [
                    'state' => 'failed',
                    'message' => '指令下发失败',
                    'error' => 'socket_push_failed',
                    'updated_at' => time(),
                ]);
            }
        }
        return $success;
    }

    protected function connectedSlaveHosts(): array {
        $hosts = [];
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                continue;
            }
            $fd = (int)($node['socket_fd'] ?? 0);
            if ($fd <= 0 || !$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
                continue;
            }
            $hosts[] = (string)$node['host'];
        }
        return array_values(array_unique($hosts));
    }

    protected function waitForNodeUpdateSummary(string $taskId, array $hosts, int $timeout): array {
        if (!$hosts) {
            return [
                'finished' => true,
                'success' => 0,
                'failed_nodes' => [],
                'pending_hosts' => [],
            ];
        }

        $summary = $this->summarizeNodeUpdateTask($taskId, $hosts);
        if ($summary['finished']) {
            return $summary;
        }

        $waitCh = new Channel(1);
        $round = 1;
        Timer::tick(5000, function (int $timerId) use ($taskId, $hosts, $timeout, &$summary, &$round, $waitCh) {
            $summary = $this->summarizeNodeUpdateTask($taskId, $hosts);
            if ($summary['finished'] || $round >= max(1, (int)($timeout / 5))) {
                Timer::clear($timerId);
                $waitCh->push(true);
            }
            $round++;
        });
        $waitCh->pop($timeout + 3);
        return $summary;
    }

    protected function summarizeNodeUpdateTask(string $taskId, array $hosts): array {
        $success = 0;
        $failedNodes = [];
        $pendingHosts = [];
        foreach ($hosts as $host) {
            $state = Runtime::instance()->get($this->nodeUpdateTaskStateKey($taskId, $host));
            if (!$state) {
                $pendingHosts[] = $host;
                continue;
            }
            $current = $state['state'] ?? '';
            if ($current === 'success') {
                $success++;
                continue;
            }
            if ($current === 'failed') {
                $failedNodes[] = [
                    'host' => $host,
                    'error' => $state['error'] ?? '',
                    'message' => $state['message'] ?? '',
                    'updated_at' => $state['updated_at'] ?? 0,
                ];
                continue;
            }
            $pendingHosts[] = $host;
        }
        return [
            'finished' => empty($pendingHosts),
            'success' => $success,
            'failed_nodes' => $failedNodes,
            'pending_hosts' => $pendingHosts,
        ];
    }

    protected function clearNodeUpdateTaskStates(string $taskId, array $hosts): void {
        foreach ($hosts as $host) {
            Runtime::instance()->delete($this->nodeUpdateTaskStateKey($taskId, $host));
        }
    }

    protected function nodeUpdateTaskStateKey(string $taskId, string $host): string {
        return 'NODE_UPDATE_TASK:' . md5($taskId . ':' . $host);
    }

    protected function normalizeNodeHost(string $host): string {
        if (in_array($host, ['localhost', '127.0.0.1'], true)) {
            return 'localhost';
        }
        return $host;
    }
}
