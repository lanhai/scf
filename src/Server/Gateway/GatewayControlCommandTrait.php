<?php

namespace Scf\Server\Gateway;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Result;
use Scf\Core\Table\Counter;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Helper\JsonHelper;
use Scf\Server\LinuxCrontab\LinuxCrontabManager;
use Scf\Server\Manager;
use Swoole\Coroutine;

/**
 * Gateway 控制命令与升级编排职责。
 *
 * 职责边界：
 * - 负责 dashboard 命令解析、本地/远端下发、回执等待与升级状态机驱动；
 * - 不承担底层进程生命周期和 dashboard 传输层协议实现。
 *
 * 架构位置：
 * - 位于 GatewayServer 控制平面的命令编排层；
 * - 上接 dashboard API，下接 business coordinator / node socket 分发链路。
 *
 * 设计意图：
 * - 保持命令语义、request_id 与状态机文案稳定，
 *   通过纯职责迁移降低 GatewayServer 主类复杂度。
 */
trait GatewayControlCommandTrait {
    /**
     * gateway-only 模式下不再暴露内置 crontab 子进程列表。
     *
     * @return array<int, mixed>
     */
    public function dashboardCrontabs(): array {
        return [];
    }

    /**
     * 向所有在线 slave 广播 Linux crontab 配置快照。
     *
     * @return array{target_count:int,accepted_count:int}
     */
    public function replicateLinuxCrontabConfigToSlaveNodes(): array {
        $targets = $this->connectedSlaveHosts();
        if (!$targets) {
            return [
                'target_count' => 0,
                'accepted_count' => 0,
            ];
        }

        $manager = new LinuxCrontabManager();
        $accepted = 0;
        $targetCount = count($targets);

        foreach ($targets as $host) {
            $nodeStatus = (array)(ServerNodeStatusTable::instance()->get($host) ?: []);
            $targetEnv = trim((string)($nodeStatus['env'] ?? ''));

            // 按节点 env 定向下发配置，避免“收到命令但因 env 不匹配导致 managed_lines=0”。
            // 若当前节点尚未上报 env（兼容旧版本），回退为“下发全部 slave 配置”。
            $payload = $manager->replicationPayload(NODE_ROLE_SLAVE, $targetEnv !== '' ? $targetEnv : null);
            $result = $this->sendCommandToNodeClient('linux_crontab_sync', (string)$host, [
                'config' => $payload,
            ]);
            if (!$result->hasError()) {
                $accepted++;
            }
        }

        Console::info("【LinuxCrontab】已广播排程配置到 slave 节点: accepted={$accepted}, targets={$targetCount}", false);

        return [
            'target_count' => $targetCount,
            'accepted_count' => $accepted,
        ];
    }

    /**
     * Dashboard 控制命令统一入口。
     *
     * @param string $command 命令名
     * @param string $host 目标 host / node_id
     * @param array<string, mixed> $params 命令参数
     * @return Result
     */
    public function dashboardCommand(string $command, string $host, array $params = []): Result {
        Console::info("【Gateway】Dashboard命令: {$command}, host={$host}" . $this->formatDashboardParamsLog($params));
        $resolvedNodeHost = $this->resolveGatewayCommandNodeHost($host);
        if ($resolvedNodeHost === 'localhost') {
            $result = $this->dispatchLocalGatewayCommand($command, $params);
        } elseif ($resolvedNodeHost !== null) {
            $result = $this->hasConnectedNode($resolvedNodeHost)
                ? $this->sendCommandToNodeClient($command, $resolvedNodeHost, $params)
                : Result::error('节点不在线');
        } else {
            $result = match ($command) {
                'restart', 'reload' => $this->restartUpstreamsByHost($host),
                'shutdown' => $this->shutdownUpstreamsByHost($host),
                'appoint_update' => $this->dashboardUpdate((string)($params['type'] ?? ''), (string)($params['version'] ?? '')),
                default => Result::error('暂不支持的命令:' . $command),
            };
        }
        if ($result->hasError()) {
            Console::warning("【Gateway】Dashboard命令失败: {$command}, host={$host}, error=" . $result->getMessage());
        } else {
            Console::success("【Gateway】Dashboard命令完成: {$command}, host={$host}");
        }
        return $result;
    }

    protected function isLocalGatewayHost(string $host): bool {
        $host = trim($host);
        if ($host === '') {
            return false;
        }
        return in_array($host, ['localhost', '127.0.0.1'], true);
    }

    protected function resolveGatewayCommandNodeHost(string $host): ?string {
        $host = trim($host);
        if ($host === '') {
            return null;
        }
        if ($this->isLocalGatewayHost($host)) {
            return 'localhost';
        }
        if ($this->hasConnectedNode($host)) {
            return $this->normalizeNodeHost($host);
        }
        foreach (ServerNodeStatusTable::instance()->rows() as $nodeKey => $status) {
            if ($nodeKey === 'localhost' || !is_array($status)) {
                continue;
            }
            if ($nodeKey === $host || (string)($status['ip'] ?? '') === $host) {
                return (string)$nodeKey;
            }
        }
        return null;
    }

    protected function dispatchLocalGatewayCommand(string $command, array $params = []): Result {
        $execution = $this->resolveGatewayControlCommandExecution($command, $params);
        $afterWrite = $execution['after_write'] ?? null;
        if (is_callable($afterWrite)) {
            $afterWrite();
        }
        return $execution['result'];
    }

    /**
     * 解析 Gateway 控制命令并生成执行计划。
     *
     * @param string $command 命令名
     * @param array<string, mixed> $params 命令参数
     * @return array<string, mixed>
     */
    protected function resolveGatewayControlCommandExecution(string $command, array $params = []): array {
        $command = trim($command);
        if ($command === '') {
            return [
                'result' => Result::error('命令不能为空'),
                'internal_error_status' => 400,
            ];
        }

        switch ($command) {
            case 'reload':
                return [
                    'result' => $this->dispatchGatewayBusinessCommand('reload', [], false, 0, '业务实例与业务子进程已开始重启'),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'reload_gateway':
                $reservation = $this->reserveGatewayReload();
                if (!$reservation['accepted']) {
                    return [
                        'result' => Result::error($reservation['message']),
                        'internal_error_status' => 409,
                    ];
                }
                return [
                    'result' => Result::success($reservation['message']),
                    'internal_success_status' => 200,
                    'internal_message' => $reservation['message'],
                    'after_write' => $reservation['scheduled']
                        ? function () use ($reservation): void {
                            $this->scheduleReservedGatewayReload((bool)$reservation['restart_managed_upstreams']);
                        }
                        : null,
                ];
            case 'restart_redisqueue':
                if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
                    return [
                        'result' => Result::error('RedisQueue 子进程未启用'),
                        'internal_error_status' => 409,
                    ];
                }
                $restart = function (): void {
                    $this->restartGatewayRedisQueueProcess();
                };
                return [
                    'result' => Result::success('RedisQueue 子进程已开始重启'),
                    'internal_success_status' => 200,
                    'internal_message' => 'redisqueue process restart started',
                    'after_write' => $restart,
                ];
            case 'subprocess_restart':
                if (!$this->subProcessManager) {
                    return [
                        'result' => Result::error('SubProcessManager 未初始化'),
                        'internal_error_status' => 409,
                    ];
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->restartManagedProcesses($target === '' ? [] : [$target]);
                return [
                    'result' => $result['ok'] ? Result::success($result['message']) : Result::error($result['message']),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'subprocess_stop':
                if (!$this->subProcessManager) {
                    return [
                        'result' => Result::error('SubProcessManager 未初始化'),
                        'internal_error_status' => 409,
                    ];
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->stopManagedProcesses($target === '' ? [] : [$target]);
                return [
                    'result' => $result['ok'] ? Result::success($result['message']) : Result::error($result['message']),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'subprocess_restart_all':
                if (!$this->subProcessManager) {
                    return [
                        'result' => Result::error('SubProcessManager 未初始化'),
                        'internal_error_status' => 409,
                    ];
                }
                $result = $this->subProcessManager->restartAllManagedProcesses();
                return [
                    'result' => $result['ok'] ? Result::success($result['message']) : Result::error($result['message']),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'restart':
                if ($this->gatewayShutdownScheduled) {
                    return [
                        'result' => Result::error('Gateway 已进入关闭流程，无法再发起重启'),
                        'internal_error_status' => 409,
                    ];
                }
                // restart 默认走“先回收业务实例，再重启 gateway 控制面”。
                // 仅当显式传入 preserve_managed_upstreams=true 时才保留 upstream。
                $preserveManagedUpstreams = (bool)($params['preserve_managed_upstreams'] ?? false);
                return [
                    'result' => Result::success($preserveManagedUpstreams ? 'Gateway 控制面已开始重启' : 'Gateway 与业务实例已开始重启'),
                    'internal_success_status' => 200,
                    'internal_message' => $preserveManagedUpstreams ? 'gateway control-plane restart started' : 'gateway full restart started',
                    'after_write' => function () use ($preserveManagedUpstreams): void {
                        $this->scheduleGatewayShutdown($preserveManagedUpstreams);
                    },
                ];
            case 'shutdown':
                return [
                    'result' => Result::success('Gateway 已开始关闭'),
                    'internal_success_status' => 200,
                    'internal_message' => 'gateway shutdown started',
                    'after_write' => function (): void {
                        $this->scheduleGatewayShutdown();
                    },
                ];
            case 'appoint_update':
                return [
                    'result' => $this->dashboardUpdate((string)($params['type'] ?? ''), (string)($params['version'] ?? '')),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            case 'appoint_update_remote':
                return [
                    'result' => $this->executeRemoteSlaveAppointUpdate(
                        (string)($params['task_id'] ?? ''),
                        (string)($params['type'] ?? ''),
                        (string)($params['version'] ?? '')
                    ),
                    'internal_error_status' => 409,
                    'internal_success_status' => 200,
                ];
            default:
                return [
                    'result' => Result::error('暂不支持的命令:' . $command),
                    'internal_error_status' => 400,
                ];
        }
    }

    protected function gatewayBusinessCommandResultKey(string $requestId): string {
        // Swoole\Table 的 key 长度上限较小，不能直接拼接带高精度 uniqid 的 request id。
        // 这里统一压缩成稳定长度摘要，避免滚动升级/安装等同步等待链路在写回结果时
        // 因 key 过长触发 warning，进而让 worker 端一直等到超时。
        return 'gateway_business_result:' . md5($requestId);
    }

    protected function upstreamSupervisorCommandResultKey(string $requestId): string {
        return 'upstream_supervisor:' . md5($requestId);
    }

    protected function waitForUpstreamSupervisorCommandResult(string $requestId, int $timeoutSeconds = 120): Result {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $resultKey = $this->upstreamSupervisorCommandResultKey($requestId);
        while (microtime(true) < $deadline) {
            $payload = Runtime::instance()->get($resultKey);
            if (is_array($payload) && array_key_exists('ok', $payload)) {
                Runtime::instance()->delete($resultKey);
                $message = (string)($payload['message'] ?? ($payload['ok'] ? 'success' : 'failed'));
                $data = (array)($payload['data'] ?? []);
                return !empty($payload['ok'])
                    ? Result::success($data ?: $message)
                    : Result::error($message, 'SERVICE_ERROR', $data);
            }
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(0.1);
            } else {
                usleep(100000);
            }
        }
        Runtime::instance()->delete($resultKey);
        return Result::error('UpstreamSupervisor 执行超时');
    }

    protected function dispatchUpstreamSupervisorCommand(
        string $action,
        array $params = [],
        bool $waitForResult = false,
        int $timeoutSeconds = 120
    ): Result {
        if (!$this->upstreamSupervisor) {
            return Result::error('UpstreamSupervisor 未启用');
        }
        if ($this->gatewayLeaseEpoch() <= 0) {
            return Result::error('Gateway owner epoch 未初始化');
        }
        $payload = array_merge($params, [
            'action' => $action,
            'owner_epoch' => $this->gatewayLeaseEpoch(),
        ]);
        if (!$waitForResult) {
            return $this->upstreamSupervisor->sendCommand($payload)
                ? Result::success('accepted')
                : Result::error('命令投递失败');
        }
        $requestId = uniqid('upstream_supervisor_', true);
        Runtime::instance()->delete($this->upstreamSupervisorCommandResultKey($requestId));
        $payload['request_id'] = $requestId;
        if (!$this->upstreamSupervisor->sendCommand($payload)) {
            Runtime::instance()->delete($this->upstreamSupervisorCommandResultKey($requestId));
            return Result::error('命令投递失败');
        }
        return $this->waitForUpstreamSupervisorCommandResult($requestId, $timeoutSeconds);
    }

    protected function waitForGatewayBusinessCommandResult(string $requestId, int $timeoutSeconds = 30): Result {
        $deadline = microtime(true) + max(1, $timeoutSeconds);
        $resultKey = $this->gatewayBusinessCommandResultKey($requestId);
        Console::info("【Gateway】等待业务编排结果: request_id={$requestId}, timeout={$timeoutSeconds}s");
        while (microtime(true) < $deadline) {
            $payload = Runtime::instance()->get($resultKey);
            if (is_array($payload) && array_key_exists('ok', $payload)) {
                Runtime::instance()->delete($resultKey);
                $message = (string)($payload['message'] ?? ($payload['ok'] ? 'success' : 'failed'));
                $data = (array)($payload['data'] ?? []);
                Console::info("【Gateway】收到业务编排结果: request_id={$requestId}, ok=" . (!empty($payload['ok']) ? 'yes' : 'no') . ", message={$message}");
                return !empty($payload['ok'])
                    ? Result::success($data ?: $message)
                    : Result::error($message, 'SERVICE_ERROR', $data);
            }
            // 这条等待链会被 dashboard/internal HTTP 协程直接调用，必须使用协程友好休眠，
            // 否则在 Swoole worker 内会触发 “all coroutines are asleep - deadlock”。
            if (Coroutine::getCid() > 0) {
                Coroutine::sleep(0.1);
            } else {
                usleep(100000);
            }
        }
        Runtime::instance()->delete($resultKey);
        Console::warning("【Gateway】业务编排结果等待超时: request_id={$requestId}, timeout={$timeoutSeconds}s");
        return Result::error('Gateway 业务编排子进程执行超时');
    }

    protected function dispatchGatewayBusinessCommand(
        string $command,
        array $params = [],
        bool $waitForResult = false,
        int $timeoutSeconds = 30,
        ?string $acceptedMessage = null
    ): Result {
        if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('GatewayBusinessCoordinator')) {
            return Result::error('Gateway 业务编排子进程未启用');
        }
        // reload 是纯异步命令，入口（dashboard/filewatch/local ipc）只需要“可达且可执行”保证。
        // 这里统一走 Runtime 信号通道，让业务编排子进程在自身 tick 中消费并执行，
        // 避免依赖 worker->manager->child 的瞬时 pipe 写入结果导致“投递成功但未执行”。
        if (!$waitForResult && $command === 'reload') {
            $signal = [
                'token' => uniqid('gateway_reload_', true),
                'queued_at' => time(),
            ];
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK, $signal);
            Console::info(
                "【Gateway】投递业务编排命令: command={$command}, wait=no, mode=runtime_signal, token="
                . (string)($signal['token'] ?? '')
            );
            return Result::success($acceptedMessage ?: 'accepted');
        }
        $requestId = '';
        if ($waitForResult) {
            $requestId = uniqid('gateway_business_', true);
            Runtime::instance()->delete($this->gatewayBusinessCommandResultKey($requestId));
            $params['request_id'] = $requestId;
            Console::info("【Gateway】投递业务编排命令: command={$command}, request_id={$requestId}, wait=yes, timeout={$timeoutSeconds}s");
        } else {
            Console::info("【Gateway】投递业务编排命令: command={$command}, wait=no");
        }
        if (!$this->subProcessManager->sendCommand($command, $params, ['GatewayBusinessCoordinator'])) {
            return Result::error('Gateway 业务编排命令投递失败');
        }
        if (!$waitForResult) {
            return Result::success($acceptedMessage ?: 'accepted');
        }
        return $this->waitForGatewayBusinessCommandResult($requestId, $timeoutSeconds);
    }

    protected function dispatchGatewayBusinessCommandAsync(string $command, array $params = []): string|false {
        if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('GatewayBusinessCoordinator')) {
            return false;
        }
        $requestId = uniqid('gateway_business_', true);
        Runtime::instance()->delete($this->gatewayBusinessCommandResultKey($requestId));
        $params['request_id'] = $requestId;
        $token = $this->enqueueGatewayBusinessRuntimeCommand($command, $params);
        if ($token === false) {
            Runtime::instance()->delete($this->gatewayBusinessCommandResultKey($requestId));
            Console::error("【Gateway】异步投递业务编排命令失败: command={$command}, request_id={$requestId}, reason=runtime_queue_write_failed");
            return false;
        }
        Console::info("【Gateway】异步投递业务编排命令: command={$command}, request_id={$requestId}, mode=runtime_queue, token={$token}");
        return $requestId;
    }

    protected function enqueueGatewayBusinessRuntimeCommand(string $command, array $params = []): string|false {
        $token = uniqid('gateway_business_runtime_', true);
        $queue = Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE);
        $items = (is_array($queue) && is_array($queue['items'] ?? null)) ? (array)$queue['items'] : [];
        $items[] = [
            'token' => $token,
            'command' => $command,
            'params' => $params,
            'queued_at' => time(),
        ];
        // 控制面命令是低频事件，队列保留最近 32 条足够覆盖并发场景，
        // 同时避免 Runtime 表值无限膨胀。
        if (count($items) > 32) {
            $items = array_slice($items, -32);
        }
        $written = Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COMMAND_QUEUE, [
            'items' => array_values($items),
            'updated_at' => time(),
        ]);
        if (!$written) {
            Console::error(
                "【Gateway】业务编排命令入队失败: command={$command}, token={$token}, runtime_count="
                . Runtime::instance()->count() . ", runtime_size=" . Runtime::instance()->size() . ", queued_items=" . count($items)
            );
            return false;
        }
        return $token;
    }

    protected function consumeFallbackBusinessReloadSignal(): bool {
        $signal = Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK);
        if (!is_array($signal)) {
            return false;
        }
        $token = trim((string)($signal['token'] ?? ''));
        if ($token === '') {
            Runtime::instance()->delete(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK);
            return false;
        }
        if ($token === $this->lastFallbackBusinessReloadToken) {
            return false;
        }
        $this->lastFallbackBusinessReloadToken = $token;
        Runtime::instance()->delete(Key::RUNTIME_GATEWAY_BUSINESS_RELOAD_FALLBACK);
        return true;
    }

    protected function handleGatewayBusinessCommand(string $command, array $params = []): array {
        return match ($command) {
            'reload' => [
                'ok' => true,
                'message' => 'gateway business reload started',
                'data' => $this->restartGatewayBusinessPlane(false),
            ],
            'appoint_update' => $this->buildGatewayBusinessCommandResult(
                $this->executeLocalDashboardUpdate(
                    (string)($params['task_id'] ?? ''),
                    (string)($params['type'] ?? ''),
                    (string)($params['version'] ?? '')
                )
            ),
            default => [
                'ok' => false,
                'message' => '暂不支持的业务编排命令:' . $command,
                'data' => [],
            ],
        };
    }

    protected function buildGatewayBusinessCommandResult(Result $result): array {
        return [
            'ok' => !$result->hasError(),
            'message' => (string)$result->getMessage(),
            'data' => (array)($result->getData() ?: []),
        ];
    }

    public function dashboardInstall(string $key, string $role = 'master'): Result {
        $key = trim($key);
        $role = trim($role) ?: 'master';
        if ($key === '') {
            return Result::error('安装秘钥不能为空');
        }
        return $this->dispatchUpstreamSupervisorCommand('install', [
            'key' => $key,
            'role' => $role,
            'plans' => $this->managedUpstreamPlans,
        ], true, 300);
    }

    public function dashboardUpdate(string $type, string $version): Result {
        $type = trim($type);
        $version = trim($version);
        if ($type === '' || $version === '') {
            return Result::error('更新类型和版本号不能为空');
        }
        if ($type === 'framework' && !FRAMEWORK_IS_PHAR) {
            return Result::error('当前为源码模式,框架在线升级不可用');
        }
        if ($type === 'framework' && function_exists('scf_framework_update_ready') && scf_framework_update_ready()) {
            return Result::error('正在等待升级,请重启服务器');
        }

        Console::info("【Gateway】开始执行升级: type={$type}, version={$version}");
        $taskId = uniqid('gateway_update_', true);
        $slaveHosts = $this->connectedSlaveHosts();
        $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
        $this->logUpdateStage($taskId, $type, $version, 'dispatch_cluster', ['slaves' => count($slaveHosts)]);
        if ($slaveHosts) {
            $this->sendCommandToAllNodeClients('appoint_update', [
                'type' => $type,
                'version' => $version,
                'task_id' => $taskId,
            ]);
        }

        $this->emitLocalNodeUpdateState($taskId, $type, $version, 'running', "【" . SERVER_HOST . "】开始更新 {$type} => {$version}");
        $requestId = $this->dispatchGatewayBusinessCommandAsync('appoint_update', [
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
        ]);
        if ($requestId === false) {
            $error = 'Gateway 业务编排子进程未启用';
            $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
            Console::error("【Gateway】升级失败: type={$type}, version={$version}, error={$error}");
            return Result::error($error);
        }

        Coroutine::create(function () use ($requestId, $taskId, $type, $version, $slaveHosts): void {
            $localResult = $this->waitForGatewayBusinessCommandResult($requestId, self::DASHBOARD_UPDATE_LOCAL_TIMEOUT_SECONDS);
            $localData = (array)($localResult->getData() ?: []);
            if ($localResult->hasError() && !$localData) {
                $error = (string)($localResult->getMessage() ?: '更新失败');
                $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
                $this->emitLocalNodeUpdateState($taskId, $type, $version, 'failed', "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}", $error);
                Console::error("【Gateway】升级失败: type={$type}, version={$version}, error={$error}");
                return;
            }

            $restartSummary = (array)($localData['restart_summary'] ?? [
                'success_count' => 0,
                'failed_nodes' => [],
            ]);
            $upstreamRollout = (array)($localData['upstream_rollout'] ?? [
                'attempted' => $type !== 'public',
                'success' => count((array)($restartSummary['failed_nodes'] ?? [])) === 0,
                'success_count' => (int)($restartSummary['success_count'] ?? 0),
                'failed_count' => count((array)($restartSummary['failed_nodes'] ?? [])),
                'failed_nodes' => (array)($restartSummary['failed_nodes'] ?? []),
            ]);
            $upstreamRollout['failed_nodes'] = array_values((array)($upstreamRollout['failed_nodes'] ?? []));
            $upstreamRollout['failed_count'] = (int)($upstreamRollout['failed_count'] ?? count($upstreamRollout['failed_nodes']));
            $upstreamRollout['success_count'] = (int)($upstreamRollout['success_count'] ?? 0);
            $upstreamRollout['attempted'] = (bool)($upstreamRollout['attempted'] ?? ($type !== 'public'));
            $upstreamRollout['success'] = (bool)($upstreamRollout['success'] ?? ($upstreamRollout['failed_count'] === 0));
            if (!empty($localData['iterate_business_processes']) && !$localResult->hasError()) {
                $this->logUpdateStage($taskId, $type, $version, 'iterate_business_processes');
                $this->iterateGatewayBusinessProcesses();
            }

            $this->logUpdateStage($taskId, $type, $version, 'wait_cluster_result');
            $summary = $this->waitForNodeUpdateSummary($taskId, $slaveHosts, self::DASHBOARD_UPDATE_CLUSTER_TIMEOUT_SECONDS);
            $pendingHosts = [];
            $masterState = (string)($localData['master']['state'] ?? ($localResult->hasError() ? 'failed' : 'success'));
            $masterError = (string)($localData['master']['error'] ?? ($localResult->hasError() ? (string)$localResult->getMessage() : ''));
            $totalNodes = max(1, count($slaveHosts) + 1);
            if ($masterState === 'pending') {
                $pendingHosts[] = SERVER_HOST;
            }
            $gatewayPendingRestart = (bool)($localData['gateway_pending_restart'] ?? ($type === 'framework' && $masterState === 'pending'));

            $payload = [
                'task_id' => $taskId,
                'type' => $type,
                'version' => $version,
                'total_nodes' => $totalNodes,
                'success_count' => $summary['success'] + ($masterState === 'success' ? 1 : 0),
                'failed_count' => count($summary['failed_nodes']) + count($restartSummary['failed_nodes']) + ($masterState === 'failed' ? 1 : 0),
                'pending_count' => count($summary['pending_hosts']) + count($pendingHosts),
                'failed_nodes' => array_merge($summary['failed_nodes'], $restartSummary['failed_nodes']),
                'pending_hosts' => array_merge($summary['pending_hosts'], $pendingHosts),
                'master' => [
                    'host' => SERVER_HOST,
                    'state' => $masterState,
                    'error' => $masterError,
                ],
                'upstream_rollout' => $upstreamRollout,
                'gateway_pending_restart' => $gatewayPendingRestart,
            ];
            $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
            $this->pushDashboardStatus();
            if ($payload['failed_nodes']) {
                Console::warning("【Gateway】升级完成但存在失败实例: type={$type}, version={$version}, success={$payload['success_count']}, failed=" . count($payload['failed_nodes']));
            } else {
                Console::success("【Gateway】升级完成: type={$type}, version={$version}, success={$payload['success_count']}");
            }
            $this->logUpdateStage($taskId, $type, $version, 'completed', [
                'success' => (int)$payload['success_count'],
                'failed' => (int)$payload['failed_count'],
                'pending' => (int)$payload['pending_count'],
            ]);

            if ($payload['failed_nodes']) {
                $error = $masterError !== '' ? $masterError : '部分节点升级失败';
                $this->emitLocalNodeUpdateState(
                    $taskId,
                    $type,
                    $version,
                    'failed',
                    "【" . SERVER_HOST . "】版本更新失败:{$type} => {$version},原因:{$error}",
                    $error,
                    [
                        'upstream_rollout' => $upstreamRollout,
                        'gateway_pending_restart' => $gatewayPendingRestart,
                    ]
                );
                return;
            }

            if ($masterState === 'pending') {
                $this->emitLocalNodeUpdateState(
                    $taskId,
                    $type,
                    $version,
                    'pending',
                    "【" . SERVER_HOST . "】业务实例升级结果:成功{$upstreamRollout['success_count']},失败{$upstreamRollout['failed_count']}，Gateway等待重启生效:{$type} => {$version}",
                    '',
                    [
                        'upstream_rollout' => $upstreamRollout,
                        'gateway_pending_restart' => $gatewayPendingRestart,
                    ]
                );
                return;
            }

            $this->emitLocalNodeUpdateState(
                $taskId,
                $type,
                $version,
                'success',
                "【" . SERVER_HOST . "】版本更新成功:{$type} => {$version}",
                '',
                [
                    'upstream_rollout' => $upstreamRollout,
                    'gateway_pending_restart' => $gatewayPendingRestart,
                ]
            );
        });

        $this->logUpdateStage($taskId, $type, $version, 'accepted', [
            'slaves' => count($slaveHosts),
            'request_id' => $requestId,
        ]);
        return Result::success([
            'accepted' => true,
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
            'message' => '升级任务已开始，请留意节点状态',
            'slave_count' => count($slaveHosts),
        ]);
    }

    protected function emitLocalNodeUpdateState(string $taskId, string $type, string $version, string $state, string $message, string $error = '', array $extra = []): void {
        $payload = [
            'task_id' => $taskId,
            'host' => APP_NODE_ID,
            'type' => $type,
            'version' => $version,
            'state' => $state,
            'message' => $message,
            'error' => $error,
            'updated_at' => time(),
        ];
        if ($extra) {
            $payload = array_merge($payload, $extra);
        }
        Runtime::instance()->set($this->nodeUpdateTaskStateKey($taskId, APP_NODE_ID), $payload);
        Manager::instance()->sendMessageToAllDashboardClients(JsonHelper::toJson([
            'event' => 'node_update_state',
            'data' => $payload,
        ]));
    }

    protected function executeLocalDashboardUpdate(string $taskId, string $type, string $version): Result {
        $type = trim($type);
        $version = trim($version);
        if ($type === '' || $version === '') {
            return Result::error('更新类型和版本号不能为空');
        }
        $taskId !== '' && $this->logUpdateStage($taskId, $type, $version, 'apply_local_package');
        Console::info("【Gateway】开始执行本地升级: task={$taskId}, type={$type}, version={$version}");
        if (!App::appointUpdateTo($type, $version, false)) {
            $error = App::getLastUpdateError() ?: '更新失败';
            Console::warning("【Gateway】本地升级失败: task={$taskId}, type={$type}, version={$version}, error={$error}");
            return Result::error($error, 'SERVICE_ERROR', [
                'restart_summary' => [
                    'success_count' => 0,
                    'failed_nodes' => [],
                ],
                'master' => [
                    'host' => SERVER_HOST,
                    'state' => 'failed',
                    'error' => $error,
                ],
            ]);
        }
        Console::info("【Gateway】本地包应用完成: task={$taskId}, type={$type}, version={$version}");

        $restartSummary = [
            'success_count' => 0,
            'failed_nodes' => [],
        ];
        if ($type !== 'public') {
            $taskId !== '' && $this->logUpdateStage($taskId, $type, $version, 'rolling_upstreams');
            Console::info("【Gateway】开始滚动升级业务实例: task={$taskId}, type={$type}, version={$version}");
            $restartSummary = $this->rollingUpdateManagedUpstreams($type, $version);
            Console::info("【Gateway】滚动升级业务实例结束: task={$taskId}, success=" . (int)($restartSummary['success_count'] ?? 0) . ", failed=" . count((array)($restartSummary['failed_nodes'] ?? [])));
        }
        if ($type !== 'public' && $restartSummary['success_count'] > 0 && !$restartSummary['failed_nodes']) {
            Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        }
        // framework 升级后 gateway 控制面只进入 pending restart，
        // 不在这一步迭代 gateway 托管子进程，避免控制面过早命中新框架包。
        $iterateBusinessProcesses = $type === 'app' && !$restartSummary['failed_nodes'];

        $master = [
            'host' => SERVER_HOST,
            'state' => $type === 'framework' ? 'pending' : ($restartSummary['failed_nodes'] ? 'failed' : 'success'),
            'error' => $type === 'framework'
                ? 'Gateway 需重启后才会加载新框架版本'
                : ($restartSummary['failed_nodes'] ? '部分业务实例升级失败' : ''),
        ];
        $upstreamFailedNodes = array_values((array)($restartSummary['failed_nodes'] ?? []));
        $upstreamRollout = [
            'attempted' => $type !== 'public',
            'success' => $type === 'public' ? true : count($upstreamFailedNodes) === 0,
            'success_count' => (int)($restartSummary['success_count'] ?? 0),
            'failed_count' => count($upstreamFailedNodes),
            'failed_nodes' => $upstreamFailedNodes,
        ];
        $payload = [
            'restart_summary' => $restartSummary,
            'master' => $master,
            'iterate_business_processes' => $iterateBusinessProcesses,
            'upstream_rollout' => $upstreamRollout,
            'gateway_pending_restart' => $type === 'framework',
        ];
        if ($restartSummary['failed_nodes']) {
            return Result::error('部分业务实例升级失败', 'SERVICE_ERROR', $payload);
        }
        Console::success("【Gateway】本地升级执行完成: task={$taskId}, type={$type}, version={$version}, state={$master['state']}");
        return Result::success($payload);
    }

    protected function executeRemoteSlaveAppointUpdate(string $taskId, string $type, string $version): Result {
        $type = trim($type);
        $version = trim($version);
        if ($type === '' || $version === '') {
            return Result::error('更新类型和版本号不能为空');
        }

        Console::info("【Gateway】开始执行远端升级: task={$taskId}, type={$type}, version={$version}");
        $requestId = $this->dispatchGatewayBusinessCommandAsync('appoint_update', [
            'task_id' => $taskId,
            'type' => $type,
            'version' => $version,
        ]);
        if ($requestId === false) {
            return Result::error('Gateway 业务编排子进程未启用');
        }

        $localResult = $this->waitForGatewayBusinessCommandResult($requestId, self::DASHBOARD_UPDATE_LOCAL_TIMEOUT_SECONDS);
        $localData = (array)($localResult->getData() ?: []);
        if (!empty($localData['iterate_business_processes']) && !$localResult->hasError()) {
            $this->iterateGatewayBusinessProcesses();
        }
        return $localResult;
    }

    protected function logUpdateStage(string $taskId, string $type, string $version, string $stage, array $extra = []): void {
        $parts = [
            "task={$taskId}",
            "type={$type}",
            "version={$version}",
            "stage={$stage}",
        ];
        foreach ($extra as $key => $value) {
            $parts[] = $key . '=' . (is_scalar($value) ? (string)$value : JsonHelper::toJson($value));
        }
        Console::info('【Gateway】升级状态机: ' . implode(', ', $parts));
    }

    protected function iterateGatewayBusinessProcesses(): void {
        $processNames = $this->managedGatewayBusinessProcessNames();
        if (!$processNames) {
            return;
        }
        Console::info('【Gateway】开始迭代业务子进程: ' . implode(', ', $processNames));
        $this->subProcessManager->iterateBusinessProcesses();
    }

    protected function managedGatewayBusinessProcessNames(): array {
        if (!$this->subProcessManager) {
            return [];
        }

        $processNames = [];
        if ($this->subProcessManager->hasProcess('RedisQueue')) {
            $processNames[] = 'RedisQueue';
        }
        return $processNames;
    }

    protected function restartGatewayRedisQueueProcess(): bool {
        if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
            Console::warning('【Gateway】RedisQueue 子进程未启用，跳过重启');
            return false;
        }
        Console::info('【Gateway】开始重启 RedisQueue 子进程');
        $this->subProcessManager->iterateRedisQueueProcess();
        $this->pushDashboardStatus();
        return true;
    }

    protected function restartGatewayBusinessPlane(bool $emitDashboardStatus = true): array {
        Console::info('【Gateway】开始重启业务平面');
        Counter::instance()->incr(Key::COUNTER_SERVER_RESTART);
        $summary = $this->restartManagedUpstreams();
        if ($emitDashboardStatus) {
            $this->pushDashboardStatus();
        }
        if ($summary['failed_nodes']) {
            Console::warning(
                "【Gateway】业务平面重启存在失败: success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                . ', ' . $this->managedRestartFailureDetails($summary)
            );
        } else {
            Console::success("【Gateway】业务平面重启完成: success={$summary['success_count']}");
        }
        return $summary;
    }

    protected function managedRestartFailureDetails(array $summary, ?array $oldPlans = null): string {
        $failedNodes = array_values(array_filter((array)($summary['failed_nodes'] ?? []), 'is_array'));
        $failedReasonParts = [];
        foreach (array_slice($failedNodes, 0, 5) as $node) {
            $host = trim((string)($node['host'] ?? $node['node'] ?? 'unknown'));
            $reason = trim((string)($node['error'] ?? $node['reason'] ?? $node['message'] ?? 'unknown'));
            if ($host === '') {
                $host = 'unknown';
            }
            if ($reason === '') {
                $reason = 'unknown';
            }
            $failedReasonParts[] = "{$host}:{$reason}";
        }
        if (count($failedNodes) > 5) {
            $failedReasonParts[] = '...+' . (count($failedNodes) - 5) . ' more';
        }
        $failedReasons = $failedReasonParts ? implode(' | ', $failedReasonParts) : 'none';

        return 'reasons=' . $failedReasons . ', old_upstream=' . $this->managedRestartOldUpstreamDiagnostics($oldPlans);
    }

    protected function managedRestartOldUpstreamDiagnostics(?array $plans = null): string {
        $candidates = [];
        if (is_array($plans) && $plans) {
            $candidates = $plans;
        } else {
            $snapshot = $this->instanceManager->snapshot();
            foreach ((array)($snapshot['generations'] ?? []) as $generation) {
                $status = (string)($generation['status'] ?? '');
                if (!in_array($status, ['active', 'draining'], true)) {
                    continue;
                }
                foreach ((array)($generation['instances'] ?? []) as $instance) {
                    $metadata = (array)($instance['metadata'] ?? []);
                    if (($metadata['managed'] ?? false) !== true) {
                        continue;
                    }
                    $port = (int)($instance['port'] ?? 0);
                    if ($port <= 0) {
                        continue;
                    }
                    $candidates[] = [
                        'version' => (string)($instance['version'] ?? $generation['version'] ?? ''),
                        'host' => (string)($instance['host'] ?? '127.0.0.1'),
                        'port' => $port,
                        'rpc_port' => (int)($metadata['rpc_port'] ?? 0),
                        'metadata' => $metadata,
                    ];
                }
            }
        }

        if (!$candidates) {
            return 'none';
        }

        $dedup = [];
        foreach ($candidates as $plan) {
            if (!is_array($plan)) {
                continue;
            }
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            if ($port <= 0) {
                continue;
            }
            $version = (string)($plan['version'] ?? '');
            $key = $version . '@' . $host . ':' . $port;
            $dedup[$key] = $plan;
        }
        $plans = array_values($dedup);
        if (!$plans) {
            return 'none';
        }

        $parts = [];
        foreach (array_slice($plans, 0, 3) as $plan) {
            $host = (string)($plan['host'] ?? '127.0.0.1');
            $port = (int)($plan['port'] ?? 0);
            $version = (string)($plan['version'] ?? '');
            $runtime = $this->fetchUpstreamRuntimeStatus($plan);
            $runtimeSource = 'live';
            if ($runtime === []) {
                $cached = $this->instanceManager->instanceRuntimeStatus($host, $port, 15);
                if ($cached) {
                    $runtime = $cached;
                    $runtimeSource = 'cache';
                } else {
                    $runtimeSource = 'unavailable';
                }
            }
            $runtimeAvailable = $runtime !== [];
            $gatewayWs = $this->instanceManager->gatewayConnectionCountFor($version, $host, $port);
            $serverConnectionNum = $runtimeAvailable ? (int)(($runtime['server_stats']['connection_num'] ?? 0)) : -1;
            $httpProcessing = $runtimeAvailable ? (int)($runtime['http_request_processing'] ?? 0) : -1;
            $rpcProcessing = $runtimeAvailable ? (int)($runtime['rpc_request_processing'] ?? 0) : -1;
            $queueProcessing = $runtimeAvailable ? (int)($runtime['redis_queue_processing'] ?? 0) : -1;
            $crontabBusy = $runtimeAvailable ? (int)($runtime['crontab_busy'] ?? 0) : -1;
            $mysqlInflight = $runtimeAvailable ? (int)($runtime['mysql_inflight'] ?? 0) : -1;
            $redisInflight = $runtimeAvailable ? (int)($runtime['redis_inflight'] ?? 0) : -1;
            $outboundHttpInflight = $runtimeAvailable ? (int)($runtime['outbound_http_inflight'] ?? 0) : -1;

            $parts[] = "{$host}:{$port}(v={$version},runtime={$runtimeSource},ws={$gatewayWs},conn="
                . ($serverConnectionNum >= 0 ? (string)$serverConnectionNum : 'n/a')
                . ",http=" . ($httpProcessing >= 0 ? (string)$httpProcessing : 'n/a')
                . ",rpc=" . ($rpcProcessing >= 0 ? (string)$rpcProcessing : 'n/a')
                . ",queue=" . ($queueProcessing >= 0 ? (string)$queueProcessing : 'n/a')
                . ",crontab=" . ($crontabBusy >= 0 ? (string)$crontabBusy : 'n/a')
                . ",mysql=" . ($mysqlInflight >= 0 ? (string)$mysqlInflight : 'n/a')
                . ",redis=" . ($redisInflight >= 0 ? (string)$redisInflight : 'n/a')
                . ",outbound_http=" . ($outboundHttpInflight >= 0 ? (string)$outboundHttpInflight : 'n/a')
                . ')';
        }
        if (count($plans) > 3) {
            $parts[] = '...+' . (count($plans) - 3) . ' more';
        }
        return implode(' | ', $parts);
    }

    protected function handleGatewayProcessCommand(string $command, array $params, object $socket): bool {
        switch ($command) {
            case 'shutdown':
                $socket->push("【" . SERVER_HOST . "】start shutdown");
                $this->scheduleGatewayShutdown();
                return true;
            case 'reload':
                $result = $this->dispatchGatewayBusinessCommand('reload', [], false, 0, 'gateway business reload started');
                $socket->push($result->hasError()
                    ? "【" . SERVER_HOST . "】business reload failed:" . $result->getMessage()
                    : "【" . SERVER_HOST . "】start business reload");
                return true;
            case 'restart_redisqueue':
                if (!$this->subProcessManager || !$this->subProcessManager->hasProcess('RedisQueue')) {
                    $socket->push("【" . SERVER_HOST . "】RedisQueue process unavailable");
                    return true;
                }
                $this->restartGatewayRedisQueueProcess();
                $socket->push("【" . SERVER_HOST . "】start redisqueue restart");
                return true;
            case 'subprocess_restart':
                if (!$this->subProcessManager) {
                    $socket->push("【" . SERVER_HOST . "】SubProcessManager unavailable");
                    return true;
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->restartManagedProcesses($target === '' ? [] : [$target]);
                $socket->push("【" . SERVER_HOST . "】" . $result['message']);
                return true;
            case 'subprocess_stop':
                if (!$this->subProcessManager) {
                    $socket->push("【" . SERVER_HOST . "】SubProcessManager unavailable");
                    return true;
                }
                $target = trim((string)($params['name'] ?? ''));
                $result = $this->subProcessManager->stopManagedProcesses($target === '' ? [] : [$target]);
                $socket->push("【" . SERVER_HOST . "】" . $result['message']);
                return true;
            case 'subprocess_restart_all':
                if (!$this->subProcessManager) {
                    $socket->push("【" . SERVER_HOST . "】SubProcessManager unavailable");
                    return true;
                }
                $result = $this->subProcessManager->restartAllManagedProcesses();
                $socket->push("【" . SERVER_HOST . "】" . $result['message']);
                return true;
            case 'restart':
                if ($this->gatewayShutdownScheduled) {
                    $socket->push("【" . SERVER_HOST . "】gateway restart failed: gateway shutting down");
                    return true;
                }
                $socket->push("【" . SERVER_HOST . "】start restart");
                // 远端 restart 默认按 full restart 执行，先下线 upstream 再重启 gateway。
                $preserveManagedUpstreams = (bool)($params['preserve_managed_upstreams'] ?? false);
                $this->scheduleGatewayShutdown($preserveManagedUpstreams);
                return true;
            case 'appoint_update':
                // slave 收到 master 转发的 appoint_update 时，不能在 cluster 协调进程里
                // 再走一遍 dashboardUpdate()。那条入口会把当前节点当成“发起升级的 gateway”，
                // 重新进入 dispatch_cluster/accepted 状态机，日志里就会出现
                // `stage=dispatch_cluster, slaves=0`，同时真正的本地升级执行链反而被绕开。
                //
                // 这里显式回退给 SubProcessManager 的内置 appoint_update 分支处理，由它统一：
                // 1. 转交 GatewayBusinessCoordinator 执行本地升级；
                // 2. 在 slave 本机完成 rolling upstream / 业务子进程迭代；
                // 3. 把 running/success/failed/pending 回报给 master。
                return false;
            default:
                return false;
        }
    }

    protected function restartUpstreamsByHost(string $host): Result {
        $plans = $this->matchedPlansByHost($host);
        if (!$plans) {
            return Result::error('未找到可重启的业务实例:' . $host);
        }

        Console::info("【Gateway】开始重启业务实例: host={$host}, count=" . count($plans));
        $summary = $this->restartManagedUpstreams($plans);
        $this->pushDashboardStatus();
        if ($summary['failed_nodes']) {
            Console::warning(
                "【Gateway】业务实例重启部分失败: host={$host}, success={$summary['success_count']}, failed=" . count($summary['failed_nodes'])
                . ', ' . $this->managedRestartFailureDetails($summary, $plans)
            );
            return Result::error('部分业务实例重启失败', 'SERVICE_ERROR', $summary);
        }
        Console::success("【Gateway】业务实例重启完成: host={$host}, success={$summary['success_count']}");
        return Result::success('已重启 ' . $summary['success_count'] . ' 个业务实例');
    }

    protected function shutdownUpstreamsByHost(string $host): Result {
        $plans = $this->matchedPlansByHost($host);
        if (!$plans) {
            return Result::error('未找到可关闭的业务实例:' . $host);
        }

        Console::info("【Gateway】开始关闭业务实例: host={$host}, count=" . count($plans));
        foreach ($plans as $plan) {
            $this->stopManagedPlan($plan);
        }
        $this->pushDashboardStatus();
        Console::success("【Gateway】业务实例已关闭: host={$host}, count=" . count($plans));
        return Result::success('已关闭 ' . count($plans) . ' 个业务实例');
    }

    protected function formatDashboardParamsLog(array $params): string {
        if (!$params) {
            return '';
        }
        $json = json_encode($params, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        return $json === false ? '' : ", params={$json}";
    }
}
