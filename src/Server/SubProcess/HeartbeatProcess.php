<?php

namespace Scf\Server\SubProcess;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\ATable;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Core\Table\SocketConnectionTable;
use Scf\Helper\JsonHelper;
use Scf\Server\LinuxCrontab\LinuxCrontabManager;
use Scf\Server\Manager;
use Scf\Server\Gateway\ConsoleRelay;
use Scf\Server\Struct\Node;
use Scf\Util\Date;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Process;
use Swoole\Timer;
use Throwable;
use function Co\run;

/**
 * Heartbeat 子进程运行逻辑。
 *
 * 责任边界：
 * - 维护 slave_node_report / node_heart_beat / command 接收主链路。
 */
class HeartbeatProcess extends AbstractRuntimeProcess {
    protected const REMOTE_APPOINT_UPDATE_TIMEOUT_SECONDS = 300;

    /**
     * 最近一次构建完成的节点状态快照。
     *
     * linux_crontab_sync 需要在命令执行后立即回报“最新状态 + 同步结果”，
     * 这里复用最近一次心跳构建结果，避免再起一套重复采样链路。
     *
     * @var array<string, mixed>
     */
    protected array $lastNodeStatusPayload = [];

    /**
     * @param int $masterPid
     * @param int $managerPid
     * @return Process
     */
    public function create(int $masterPid, int $managerPid): Process {
        return new Process(function (Process $process) use ($masterPid, $managerPid) {
            run(function () use ($process, $masterPid, $managerPid) {
                $this->call('mark_gateway_sub_process_context');
                App::mount();
                Runtime::instance()->set(Key::RUNTIME_HEARTBEAT_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_HEARTBEAT_PROCESS_HEARTBEAT_AT, time());
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【Heatbeat】心跳进程PID:" . $process->pid, false);
                }
                MemoryMonitor::start('Heatbeat');
                $node = Node::factory();
                $node->appid = APP_ID;
                $node->id = APP_NODE_ID;
                $node->name = APP_DIR_NAME;
                $node->ip = SERVER_HOST;
                $node->fingerprint = APP_FINGERPRINT;
                $node->port = Runtime::instance()->httpPort();
                $node->socketPort = Runtime::instance()->dashboardPort() ?: Runtime::instance()->httpPort();
                $node->started = (int)$this->call('server_started_at');
                $node->restart_times = 0;
                $node->master_pid = $masterPid;
                $node->manager_pid = $managerPid;
                $node->swoole_version = swoole_version();
                $node->cpu_num = swoole_cpu_num();
                $node->stack_useage = Coroutine::getStackUsage();
                $node->scf_version = SCF_COMPOSER_VERSION;
                $node->server_run_mode = APP_SRC_TYPE;
                $nodeHost = APP_NODE_ID;
                while (true) {
                    Runtime::instance()->set(Key::RUNTIME_HEARTBEAT_PROCESS_HEARTBEAT_AT, time());
                    $socket = Manager::instance()->getMasterSocketConnection();
                    $socket->push(JsonHelper::toJson(['event' => 'slave_node_report', 'data' => [
                        'host' => $nodeHost,
                        'ip' => SERVER_HOST,
                        'role' => SERVER_ROLE
                    ]]));
                    $pingTimerId = Timer::tick(1000 * 5, function () use ($socket, &$node, $nodeHost) {
                        if ((bool)$this->call('should_skip_heartbeat_status_build')) {
                            MemoryMonitor::updateUsage('Heatbeat');
                            return;
                        }
                        $payload = $this->buildAndCacheNodeStatusPayload($node);
                        if ($node->role == NODE_ROLE_MASTER) {
                            ServerNodeStatusTable::instance()->set('localhost', $payload);
                            $socket->push('::ping');
                        } else {
                            $socket->push(JsonHelper::toJson(['event' => 'node_heart_beat', 'data' => [
                                'host' => $nodeHost,
                                'status' => $payload
                            ]]));
                        }
                        MemoryMonitor::updateUsage('Heatbeat');
                    });
                    while (true) {
                        Runtime::instance()->set(Key::RUNTIME_HEARTBEAT_PROCESS_HEARTBEAT_AT, time());
                        $processSocket = $process->exportSocket();
                        $cmd = $processSocket->recv(timeout: 0.1);
                        if ($cmd == 'shutdown') {
                            Timer::clear($pingTimerId);
                            Console::warning('【Heatbeat】服务器已关闭,终止心跳', false);
                            $socket->close();
                            MemoryMonitor::stop();
                            Runtime::instance()->set(Key::RUNTIME_HEARTBEAT_PROCESS_HEARTBEAT_AT, 0);
                            Runtime::instance()->set(Key::RUNTIME_HEARTBEAT_PID, 0);
                            $this->call('exit_coroutine_runtime');
                            return;
                        }
                        $reply = $socket->recv(1.0);
                        if ($reply === false) {
                            if (!Manager::instance()->isSocketConnected($socket)) {
                                Timer::clear($pingTimerId);
                                try {
                                    $socket->close();
                                } catch (Throwable) {
                                }
                                Console::warning('【Heatbeat】与master节点连接已断开,准备重连', false);
                                break;
                            }
                            continue;
                        }
                        if ($reply) {
                            if (isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                                $socket->push('', WEBSOCKET_OPCODE_PONG);
                                continue;
                            }
                            if (!empty($reply->data) && $reply->data !== "::pong") {
                                if (JsonHelper::is($reply->data)) {
                                    $data = JsonHelper::recover($reply->data);
                                    $event = $data['event'] ?? 'unknow';
                                    if ($event == 'command') {
                                        $command = (string)($data['data']['command'] ?? '');
                                        $params = (array)($data['data']['params'] ?? []);
                                        if (!$this->handleRemoteCommand($command, $params, $socket, $node)) {
                                            Console::warning("【Heatbeat】Command '$command' is not supported", false);
                                        }
                                    } elseif ($event == 'slave_node_report_response') {
                                        $masterHost = Manager::instance()->getMasterHost();
                                        Console::success('【Heatbeat】已与master[' . $masterHost . ']建立连接,客户端ID:' . $data['data'], false);
                                    } elseif ($event == 'console_subscription') {
                                        ConsoleRelay::setRemoteSubscribed((bool)($data['data']['enabled'] ?? false));
                                    } else {
                                        Console::info("【Heatbeat】收到master消息:" . $reply->data, false);
                                    }
                                } else {
                                    Console::info("【Heatbeat】收到master消息:" . $reply->data, false);
                                }
                            }
                        }
                    }
                }
            });
        }, false, SOCK_DGRAM);
    }

    /**
     * 刷新节点动态运行态并构建心跳上报 payload。
     *
     * @param Node $node 当前节点运行态对象
     * @return array<string, mixed>
     */
    protected function buildAndCacheNodeStatusPayload(Node $node): array {
        $node->role = SERVER_ROLE;
        $node->app_version = App::version() ?: (App::info()?->toArray()['version'] ?? App::profile()->version);
        $node->public_version = App::publicVersion() ?: (App::info()?->toArray()['public_version'] ?? (App::profile()->public_version ?: '--'));
        $node->framework_build_version = FRAMEWORK_BUILD_VERSION;
        $node->heart_beat = time();
        $node->framework_update_ready = function_exists('scf_framework_update_ready') && scf_framework_update_ready();
        $node->tables = ATable::list();
        $node->restart_times = Counter::instance()->get(Key::COUNTER_SERVER_RESTART) ?: 0;
        $node->stack_useage = memory_get_usage(true);
        $node->threads = count(Coroutine::list());
        $node->thread_status = Coroutine::stats();
        $node->server_stats = Runtime::instance()->get('SERVER_STATS') ?: [];
        $node->server_stats['long_connection_num'] = SocketConnectionTable::instance()->count();
        $node->mysql_execute_count = Counter::instance()->get(Key::COUNTER_MYSQL_PROCESSING . (time() - 1)) ?: 0;
        $node->http_request_reject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
        $node->http_request_count = Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0;
        $node->http_request_count_current = Counter::instance()->get(Key::COUNTER_REQUEST . (time() - 1)) ?: 0;
        $node->http_request_count_today = Counter::instance()->get(Key::COUNTER_REQUEST . Date::today()) ?: 0;
        $node->http_request_processing = Counter::instance()->get(Key::COUNTER_REQUEST_PROCESSING) ?: 0;
        $node->memory_usage = MemoryMonitor::sum();
        $node->tasks = LinuxCrontabManager::nodeTasks(false);
        $payload = (array)$this->call('build_node_status_payload', $node);
        $this->lastNodeStatusPayload = $payload;
        return $payload;
    }

    /**
     * 处理来自 master 的远端控制命令。
     *
     * @param string $command 命令名称
     * @param array<string, mixed> $params 命令参数
     * @param object $socket 当前 slave->master websocket 连接
     * @param Node $node 当前节点运行态对象
     * @return bool 命令是否已识别并处理
     */
    protected function handleRemoteCommand(string $command, array $params, object $socket, Node $node): bool {
        if ($command !== '') {
            Console::info("【GatewayCluster】收到master指令: command={$command}", false);
        }
        switch ($command) {
            case 'shutdown':
                $socket->push("【" . SERVER_HOST . "】start shutdown");
                $this->call('trigger_shutdown');
                return true;
            case 'reload':
                $socket->push("【" . SERVER_HOST . "】start reload");
                $this->call('trigger_reload');
                return true;
            case 'restart':
                $socket->push("【" . SERVER_HOST . "】start restart");
                $this->call('trigger_restart');
                return true;
            case 'restart_redisqueue':
                if ((bool)$this->call('dispatch_gateway_internal_command', 'restart_redisqueue', [], 1.0)) {
                    $socket->push("【" . SERVER_HOST . "】start redisqueue restart");
                    return true;
                }
                return false;
            case 'subprocess_restart':
                $name = trim((string)($params['name'] ?? ''));
                $restartResult = (array)$this->call('restart_managed_processes', $name === '' ? [] : [$name]);
                $socket->push("【" . SERVER_HOST . "】" . (string)($restartResult['message'] ?? '子进程重启已提交'));
                return true;
            case 'subprocess_stop':
                $name = trim((string)($params['name'] ?? ''));
                $stopResult = (array)$this->call('stop_managed_processes', $name === '' ? [] : [$name]);
                $socket->push("【" . SERVER_HOST . "】" . (string)($stopResult['message'] ?? '子进程停止已提交'));
                return true;
            case 'subprocess_restart_all':
                $restartAllResult = (array)$this->call('restart_all_managed_processes');
                $socket->push("【" . SERVER_HOST . "】" . (string)($restartAllResult['message'] ?? '全部子进程重启已提交'));
                return true;
            case 'linux_crontab_sync':
                try {
                    $result = LinuxCrontabManager::applyReplicationPayload((array)($params['config'] ?? []));
                    Console::success("【LinuxCrontab】已同步本地排程配置: items=" . (int)($result['item_count'] ?? 0), false);
                    $this->reportLinuxCrontabSyncState($socket, 'success', [
                        'message' => "【" . SERVER_HOST . "】Linux 排程已同步: items=" . (int)($result['item_count'] ?? 0),
                        'item_count' => (int)($result['item_count'] ?? 0),
                        'sync' => (array)($result['sync'] ?? []),
                    ], $node);
                } catch (Throwable $throwable) {
                    Console::warning("【LinuxCrontab】同步排程配置失败:" . $throwable->getMessage(), false);
                    $this->reportLinuxCrontabSyncState($socket, 'failed', [
                        'message' => "【" . SERVER_HOST . "】Linux 排程同步失败:" . $throwable->getMessage(),
                        'error' => $throwable->getMessage(),
                    ], $node);
                }
                return true;
            case 'appoint_update':
                $taskId = (string)($params['task_id'] ?? '');
                $type = (string)($params['type'] ?? '');
                $version = (string)($params['version'] ?? '');
                $statePayload = [
                    'event' => 'node_update_state',
                    'data' => [
                        'task_id' => $taskId,
                        // 升级状态回报必须和 slave_node_report / heartbeat 使用同一 host 标识，
                        // 否则 master 汇总 task 状态时会把这个 slave 误判成一直 pending。
                        'host' => APP_NODE_ID,
                        'type' => $type,
                        'version' => $version,
                        'updated_at' => time(),
                    ]
                ];
                $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                    'data' => [
                        'state' => 'running',
                        'message' => "【" . SERVER_HOST . "】开始更新 {$type} => {$version}",
                    ]
                ])));
                $result = $this->executeRemoteAppointUpdate($type, $version, $taskId);
                $reportedState = (string)(($result['data'] ?? [])['master']['state'] ?? '');
                if (!empty($result['ok']) && $reportedState !== 'failed') {
                    $finalState = $reportedState === 'pending' ? 'pending' : 'success';
                    $finalMessage = $finalState === 'pending'
                        ? "【" . SERVER_HOST . "】版本更新已完成，等待重启生效:{$type} => {$version}"
                        : "【" . SERVER_HOST . "】版本更新成功:{$type} => {$version}";
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => $finalState,
                            'message' => $finalMessage,
                            'updated_at' => time(),
                        ]
                    ])));
                    $socket->push($finalMessage);
                } else {
                    $error = (string)($result['message'] ?? '') ?: App::getLastUpdateError() ?: '未知原因';
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => 'failed',
                            'error' => $error,
                            'message' => "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}",
                            'updated_at' => time(),
                        ]
                    ])));
                    $socket->push("【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}");
                }
                return true;
            default:
                // 远端集群命令属于强约束控制面协议：只允许白名单。
                // 明确业务流程下不能再走“未知命令兜底分发”，否则命令会在错误进程上下文
                // 被执行，造成重启链路状态漂移（例如 registry/实例状态不一致）。
                if ($command !== '') {
                    Console::warning("【GatewayCluster】未支持的远端命令，已拒绝: command={$command}", false);
                }
                return false;
        }
    }

    /**
     * 在 slave 节点上执行远端下发的指定版本升级。
     *
     * @param string $type 升级类型
     * @param string $version 目标版本
     * @param string $taskId 集群升级任务 id
     * @return array<string, mixed>
     */
    protected function executeRemoteAppointUpdate(string $type, string $version, string $taskId): array {
        if ((bool)$this->call('has_process', 'GatewayBusinessCoordinator')) {
            Console::info("【GatewayCluster】转交业务编排执行升级: task={$taskId}, type={$type}, version={$version}", false);
            $response = (array)$this->call('request_gateway_internal_command', 'appoint_update_remote', [
                'task_id' => $taskId,
                'type' => $type,
                'version' => $version,
            ], self::REMOTE_APPOINT_UPDATE_TIMEOUT_SECONDS + 5);
            if (empty($response['ok'])) {
                return [
                    'ok' => false,
                    'message' => (string)($response['message'] ?? 'Gateway 内部升级命令执行失败'),
                    'data' => [],
                    'updated_at' => time(),
                ];
            }
            return [
                'ok' => true,
                'message' => (string)($response['message'] ?? 'success'),
                'data' => (array)($response['data'] ?? []),
                'updated_at' => time(),
            ];
        }

        if (App::appointUpdateTo($type, $version)) {
            return [
                'ok' => true,
                'message' => 'success',
                'data' => [],
                'updated_at' => time(),
            ];
        }

        return [
            'ok' => false,
            'message' => App::getLastUpdateError() ?: '未知原因',
            'data' => [],
            'updated_at' => time(),
        ];
    }

    /**
     * 将 Linux 排程同步结果即时回报给 master gateway。
     *
     * @param object $socket 当前 slave -> master 的 websocket 连接
     * @param string $state success / failed
     * @param array<string, mixed> $payload 额外状态字段
     * @param Node $node 当前节点运行态对象
     * @return void
     */
    protected function reportLinuxCrontabSyncState(object $socket, string $state, array $payload, Node $node): void {
        $status = $this->lastNodeStatusPayload;
        if (!$status) {
            $status = $this->buildAndCacheNodeStatusPayload($node);
        }
        $status['linux_crontab_sync'] = [
            'state' => $state,
            'message' => (string)($payload['message'] ?? ''),
            'error' => (string)($payload['error'] ?? ''),
            'updated_at' => time(),
            'item_count' => (int)($payload['item_count'] ?? 0),
            'sync' => (array)($payload['sync'] ?? []),
        ];
        try {
            $socket->push(JsonHelper::toJson([
                'event' => 'linux_crontab_sync_state',
                'data' => [
                    'host' => APP_NODE_ID,
                    'state' => $state,
                    'message' => (string)($payload['message'] ?? ''),
                    'error' => (string)($payload['error'] ?? ''),
                    'item_count' => (int)($payload['item_count'] ?? 0),
                    'sync' => (array)($payload['sync'] ?? []),
                    'updated_at' => time(),
                    'status' => $status,
                ],
            ]));
        } catch (Throwable) {
        }
    }
}
