<?php

namespace Scf\Server\SubProcess;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\SecondWindowCounter;
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
                $node->env = SERVER_RUN_ENV ?: 'production';
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
        $node->env = SERVER_RUN_ENV ?: 'production';
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
        $node->mysql_execute_count = SecondWindowCounter::mysqlCountOfSecond(time() - 1);
        $node->http_request_reject = Counter::instance()->get(Key::COUNTER_REQUEST_REJECT_) ?: 0;
        $node->http_request_count = Counter::instance()->get(Key::COUNTER_REQUEST) ?: 0;
        $node->http_request_count_current = SecondWindowCounter::requestCountOfSecond(time() - 1);
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
        $commandId = trim((string)($params['command_id'] ?? ''));
        if ($commandId === '') {
            $commandId = 'legacy_' . uniqid('cmd_', true);
        }
        switch ($command) {
            case 'shutdown':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】收到 shutdown 指令");
                $socket->push("【" . SERVER_HOST . "】start shutdown");
                $this->call('trigger_shutdown');
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'success', "【" . SERVER_HOST . "】shutdown 指令已执行");
                return true;
            case 'reload':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】收到 reload 指令");
                $socket->push("【" . SERVER_HOST . "】start reload");
                $this->call('trigger_reload');
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'success', "【" . SERVER_HOST . "】reload 指令已执行");
                return true;
            case 'restart':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】收到 restart 指令");
                $socket->push("【" . SERVER_HOST . "】start restart");
                $this->call('trigger_restart');
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'success', "【" . SERVER_HOST . "】restart 指令已执行");
                return true;
            case 'restart_redisqueue':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】开始重启 RedisQueue");
                $restartResult = (array)$this->call('restart_managed_processes', ['RedisQueue']);
                if (!empty($restartResult['ok'])) {
                    $socket->push("【" . SERVER_HOST . "】start redisqueue restart");
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'success', (string)($restartResult['message'] ?? 'RedisQueue 重启已提交'));
                } else {
                    $error = (string)($restartResult['message'] ?? 'dispatch failed');
                    $socket->push("【" . SERVER_HOST . "】redisqueue restart failed:" . $error);
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'failed', "【" . SERVER_HOST . "】RedisQueue 重启失败", $error);
                }
                return true;
            case 'subprocess_restart':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】开始重启子进程");
                $name = trim((string)($params['name'] ?? ''));
                $restartResult = (array)$this->call('restart_managed_processes', $name === '' ? [] : [$name]);
                $resultMessage = (string)($restartResult['message'] ?? '子进程重启已提交');
                $socket->push("【" . SERVER_HOST . "】" . $resultMessage);
                if (!empty($restartResult['ok'])) {
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'success', $resultMessage);
                } else {
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'failed', "【" . SERVER_HOST . "】子进程重启失败", $resultMessage);
                }
                return true;
            case 'subprocess_stop':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】开始停止子进程");
                $name = trim((string)($params['name'] ?? ''));
                $stopResult = (array)$this->call('stop_managed_processes', $name === '' ? [] : [$name]);
                $resultMessage = (string)($stopResult['message'] ?? '子进程停止已提交');
                $socket->push("【" . SERVER_HOST . "】" . $resultMessage);
                if (!empty($stopResult['ok'])) {
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'success', $resultMessage);
                } else {
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'failed', "【" . SERVER_HOST . "】子进程停止失败", $resultMessage);
                }
                return true;
            case 'subprocess_restart_all':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】开始重启全部子进程");
                $restartAllResult = (array)$this->call('restart_all_managed_processes');
                $resultMessage = (string)($restartAllResult['message'] ?? '全部子进程重启已提交');
                $socket->push("【" . SERVER_HOST . "】" . $resultMessage);
                if (!empty($restartAllResult['ok'])) {
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'success', $resultMessage);
                } else {
                    $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'failed', "【" . SERVER_HOST . "】全部子进程重启失败", $resultMessage);
                }
                return true;
            case 'linux_crontab_sync':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】开始同步 Linux 排程");
                try {
                    $result = LinuxCrontabManager::applyReplicationPayload((array)($params['config'] ?? []));
                    $sync = (array)($result['sync'] ?? []);
                    $itemCount = (int)($result['item_count'] ?? 0);
                    $managedLineCount = (int)($sync['managed_line_count'] ?? 0);
                    $enabledCount = (int)($sync['enabled_count'] ?? 0);
                    $applicableEnabledCount = (int)($sync['applicable_enabled_count'] ?? 0);
                    $scopeSkippedCount = (int)($sync['scope_skipped_count'] ?? 0);
                    $expressionSkippedCount = (int)($sync['expression_skipped_count'] ?? 0);
                    $currentEnv = trim((string)($sync['current_env'] ?? ''));
                    $currentRole = trim((string)($sync['current_role'] ?? ''));
                    $skipped = (bool)($sync['skipped'] ?? false);
                    $reason = trim((string)($sync['reason'] ?? ''));
                    $noManagedLines = !$skipped && $enabledCount > 0 && $managedLineCount === 0;
                    $state = 'success';
                    $error = '';
                    if ($skipped) {
                        $state = 'failed';
                        $error = $reason !== '' ? $reason : 'sync_skipped';
                    } elseif ($noManagedLines) {
                        $state = 'failed';
                        if ($scopeSkippedCount > 0) {
                            $error = 'scope_mismatch';
                        } elseif ($expressionSkippedCount > 0) {
                            $error = 'empty_schedule_expression';
                        } else {
                            $error = 'managed_lines_empty';
                        }
                    }

                    $message = "【" . SERVER_HOST . "】Linux 排程同步结果: items={$itemCount}, enabled={$enabledCount}, applicable={$applicableEnabledCount}, managed_lines={$managedLineCount}";
                    if ($scopeSkippedCount > 0) {
                        $message .= ", scope_skipped={$scopeSkippedCount}";
                    }
                    if ($expressionSkippedCount > 0) {
                        $message .= ", expression_skipped={$expressionSkippedCount}";
                    }
                    if ($currentEnv !== '') {
                        $message .= ", current_env={$currentEnv}";
                    }
                    if ($currentRole !== '') {
                        $message .= ", current_role={$currentRole}";
                    }
                    if ($skipped) {
                        $message .= $reason !== '' ? ", skipped={$reason}" : ', skipped=1';
                    }
                    if ($noManagedLines) {
                        $message .= ", failed={$error}";
                    }

                    if ($state === 'failed') {
                        Console::warning("【LinuxCrontab】同步本地排程失败: {$message}", false);
                    } else {
                        Console::success("【LinuxCrontab】已同步本地排程配置: {$message}", false);
                    }

                    $this->reportLinuxCrontabSyncState($socket, $state, [
                        'message' => $message,
                        'error' => $error,
                        'item_count' => (int)($result['item_count'] ?? 0),
                        'sync' => $sync,
                    ], $node);
                    $this->reportRemoteCommandFeedback(
                        $socket,
                        $commandId,
                        $command,
                        $state === 'failed' ? 'failed' : 'success',
                        $message,
                        $state === 'failed' ? $error : '',
                        ['sync' => $sync]
                    );
                } catch (Throwable $throwable) {
                    Console::warning("【LinuxCrontab】同步排程配置失败:" . $throwable->getMessage(), false);
                    $this->reportLinuxCrontabSyncState($socket, 'failed', [
                        'message' => "【" . SERVER_HOST . "】Linux 排程同步失败:" . $throwable->getMessage(),
                        'error' => $throwable->getMessage(),
                    ], $node);
                    $this->reportRemoteCommandFeedback(
                        $socket,
                        $commandId,
                        $command,
                        'failed',
                        "【" . SERVER_HOST . "】Linux 排程同步失败",
                        $throwable->getMessage()
                    );
                }
                return true;
            case 'linux_crontab_installed_snapshot':
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】开始上报 Linux 排程安装快照");
                $requestId = trim((string)($params['request_id'] ?? ''));
                try {
                    $snapshot = (new LinuxCrontabManager())->installedSnapshot();
                    $this->reportLinuxCrontabInstalledState($socket, 'success', [
                        'message' => "【" . SERVER_HOST . "】Linux 排程本机快照已回报",
                        'snapshot' => $snapshot,
                        'request_id' => $requestId,
                    ], $node);
                    $this->reportRemoteCommandFeedback(
                        $socket,
                        $commandId,
                        $command,
                        'success',
                        "【" . SERVER_HOST . "】Linux 排程本机快照已回报",
                        '',
                        ['request_id' => $requestId]
                    );
                } catch (Throwable $throwable) {
                    $this->reportLinuxCrontabInstalledState($socket, 'failed', [
                        'message' => "【" . SERVER_HOST . "】Linux 排程本机快照获取失败:" . $throwable->getMessage(),
                        'error' => $throwable->getMessage(),
                        'request_id' => $requestId,
                    ], $node);
                    $this->reportRemoteCommandFeedback(
                        $socket,
                        $commandId,
                        $command,
                        'failed',
                        "【" . SERVER_HOST . "】Linux 排程本机快照获取失败",
                        $throwable->getMessage(),
                        ['request_id' => $requestId]
                    );
                }
                return true;
            case 'appoint_update':
                $taskId = (string)($params['task_id'] ?? '');
                $type = (string)($params['type'] ?? '');
                $version = (string)($params['version'] ?? '');
                $this->reportRemoteCommandFeedback($socket, $commandId, $command, 'running', "【" . SERVER_HOST . "】开始更新 {$type} => {$version}");
                $statePayload = [
                    'event' => 'node_update_state',
                    'data' => [
                        'command_id' => $commandId,
                        'command' => $command,
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
                    $this->reportRemoteCommandFeedback(
                        $socket,
                        $commandId,
                        $command,
                        $finalState,
                        $finalMessage,
                        '',
                        (array)($result['data'] ?? [])
                    );
                } else {
                    $error = (string)($result['message'] ?? '') ?: App::getLastUpdateError() ?: '未知原因';
                    $businessRaw = (array)(($result['data'] ?? [])['business_command_raw'] ?? []);
                    $internalError = trim((string)($businessRaw['message'] ?? ''));
                    if ($internalError !== '' && !str_contains($error, $internalError)) {
                        $error .= " ({$internalError})";
                    }
                    $socket->push(JsonHelper::toJson(array_replace_recursive($statePayload, [
                        'data' => [
                            'state' => 'failed',
                            'error' => $error,
                            'message' => "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}",
                            'updated_at' => time(),
                        ]
                    ])));
                    $socket->push("【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}");
                    $this->reportRemoteCommandFeedback(
                        $socket,
                        $commandId,
                        $command,
                        'failed',
                        "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version}",
                        $error,
                        (array)($result['data'] ?? [])
                    );
                }
                return true;
            default:
                // 远端集群命令属于强约束控制面协议：只允许白名单。
                // 明确业务流程下不能再走“未知命令兜底分发”，否则命令会在错误进程上下文
                // 被执行，造成重启链路状态漂移（例如 registry/实例状态不一致）。
                if ($command !== '') {
                    Console::warning("【GatewayCluster】未支持的远端命令，已拒绝: command={$command}", false);
                }
                $this->reportRemoteCommandFeedback(
                    $socket,
                    $commandId,
                    $command,
                    'failed',
                    "【" . SERVER_HOST . "】未支持的远端命令",
                    'unsupported_command'
                );
                return false;
        }
    }

    /**
     * 向 master 上报 slave 指令生命周期回执。
     *
     * 回执事件与 node_update_state 分离，避免升级场景和普通控制命令复用同一结构时
     * 互相覆盖字段语义；master 会以 command_id 为主键更新历史状态。
     *
     * @param object $socket 当前 slave -> master 的 websocket 连接
     * @param string $commandId 指令唯一 id
     * @param string $command 指令名
     * @param string $state accepted/running/success/failed/pending
     * @param string $message 指令状态说明
     * @param string $error 错误原因
     * @param array<string, mixed> $result 额外结果 payload
     * @return void
     */
    protected function reportRemoteCommandFeedback(
        object $socket,
        string $commandId,
        string $command,
        string $state,
        string $message,
        string $error = '',
        array $result = []
    ): void {
        $commandId = trim($commandId);
        if ($commandId === '') {
            return;
        }
        $command = trim($command);
        if ($command === '') {
            $command = 'unknown';
        }

        try {
            $socket->push(JsonHelper::toJson([
                'event' => 'slave_command_feedback',
                'data' => [
                    'command_id' => $commandId,
                    'command' => $command,
                    'host' => APP_NODE_ID,
                    'state' => trim($state) ?: 'running',
                    'message' => $message,
                    'error' => $error,
                    'updated_at' => time(),
                    'result' => $result,
                ],
            ]));
        } catch (Throwable) {
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
            $response = (array)$this->call('request_gateway_business_command', 'appoint_update', [
                'task_id' => $taskId,
                'type' => $type,
                'version' => $version,
            ], self::REMOTE_APPOINT_UPDATE_TIMEOUT_SECONDS + 5);
            if (empty($response['ok'])) {
                $raw = (array)$response;
                $errorDetail = trim((string)($raw['message'] ?? ''));
                $message = (string)($response['message'] ?? 'Gateway 业务编排命令执行失败');
                if ($errorDetail !== '' && !str_contains($message, $errorDetail)) {
                    $message .= "({$errorDetail})";
                }
                return [
                    'ok' => false,
                    'message' => $message,
                    'data' => [
                        'business_command_raw' => $raw,
                    ],
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

    /**
     * 将 Linux 排程“本机已安装快照”即时回报给 master gateway。
     *
     * @param object $socket 当前 slave -> master 的 websocket 连接
     * @param string $state success / failed
     * @param array<string, mixed> $payload 额外状态字段
     * @param Node $node 当前节点运行态对象
     * @return void
     */
    protected function reportLinuxCrontabInstalledState(object $socket, string $state, array $payload, Node $node): void {
        $status = $this->lastNodeStatusPayload;
        if (!$status) {
            $status = $this->buildAndCacheNodeStatusPayload($node);
        }
        $installedState = [
            'state' => $state,
            'message' => (string)($payload['message'] ?? ''),
            'error' => (string)($payload['error'] ?? ''),
            'updated_at' => time(),
            'snapshot' => (array)($payload['snapshot'] ?? []),
            'request_id' => trim((string)($payload['request_id'] ?? '')),
        ];
        $status['linux_crontab_installed'] = $installedState;
        try {
            $pushed = $socket->push(JsonHelper::toJson([
                'event' => 'linux_crontab_installed_state',
                'data' => [
                    'host' => APP_NODE_ID,
                    ...$installedState,
                    'status' => $status,
                ],
            ]));
            if ($pushed === false) {
                Console::warning('【LinuxCrontab】本机排程快照回报失败: socket disconnected', false);
            }
        } catch (Throwable) {
        }
    }
}
