<?php

namespace Scf\Server\SubProcess;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Server\Manager;
use Scf\Util\MemoryMonitor;
use Swoole\Coroutine;
use Swoole\Process;
use Throwable;
use function Co\run;

/**
 * GatewayClusterCoordinator 子进程运行逻辑。
 *
 * 责任边界：
 * - master: 推进 gateway_cluster_tick 管理事件。
 * - slave: 维护轻量桥接通道并转发 console_log。
 */
class GatewayClusterCoordinatorProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        return new Process(function (Process $process) {
            run(function () use ($process) {
                $this->call('mark_gateway_sub_process_context');
                App::mount();
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_TRACE_SNAPSHOT, '');
                $this->call('touch_managed_heartbeat', Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT, time(), 'GatewayClusterCoordinator');
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【GatewayCluster】集群协调PID:" . $process->pid, false);
                }
                if (App::isMaster()) {
                    $this->runMasterLoop($process);
                    return;
                }
                $this->runSlaveLoop($process);
            });
        });
    }

    /**
     * @param Process $process
     * @return void
     */
    protected function runMasterLoop(Process $process): void {
        MemoryMonitor::start('GatewayCluster');
        $processSocket = $process->exportSocket();
        $lastTickAt = 0;
        while (true) {
            $loopStartedAt = microtime(true);
            $this->call('update_gateway_cluster_trace_snapshot', 'master.loop.start', [
                'pid' => (int)$process->pid,
            ]);
            if (!Runtime::instance()->serverIsAlive()) {
                Console::warning('【GatewayCluster】服务器已关闭,结束运行', false);
                MemoryMonitor::stop();
                $this->call('exit_coroutine_runtime');
                return;
            }
            $now = time();
            $this->call('touch_managed_heartbeat', Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT, $now, 'GatewayClusterCoordinator');
            $this->call('update_gateway_cluster_trace_snapshot', 'master.loop.heartbeat_touched', [
                'now' => $now,
            ]);
            if ($now !== $lastTickAt && Runtime::instance()->serverIsReady()) {
                $lastTickAt = $now;
                $pipeStartedAt = microtime(true);
                $this->call('update_gateway_cluster_trace_snapshot', 'master.loop.pipe_tick.start');
                $this->call('send_gateway_pipe_message', 'gateway_cluster_tick', []);
                $this->call('update_gateway_cluster_trace_snapshot', 'master.loop.pipe_tick.done', [
                    'cost_ms' => round((microtime(true) - $pipeStartedAt) * 1000, 2),
                ]);
            }
            $recvStartedAt = microtime(true);
            $this->call('update_gateway_cluster_trace_snapshot', 'master.loop.process_recv.start', ['timeout' => 0.1]);
            $socketPayload = $processSocket->recv(timeout: 0.1);
            $message = is_string($socketPayload) ? $socketPayload : '';
            $this->call('update_gateway_cluster_trace_snapshot', 'master.loop.process_recv.done', [
                'cost_ms' => round((microtime(true) - $recvStartedAt) * 1000, 2),
                'has_message' => $message !== '' ? 1 : 0,
                'is_shutdown' => $message !== '' && str_contains($message, 'shutdown') ? 1 : 0,
            ]);
            if ($message !== '' && str_contains($message, 'shutdown')) {
                Console::warning('【GatewayCluster】收到shutdown,安全退出', false);
                MemoryMonitor::stop();
                $this->call('exit_coroutine_runtime');
                return;
            }
            $this->call('update_gateway_cluster_trace_snapshot', 'master.loop.sleep', [
                'loop_cost_ms' => round((microtime(true) - $loopStartedAt) * 1000, 2),
            ]);
            Coroutine::sleep(0.1);
        }
    }

    /**
     * @param Process $process
     * @return void
     */
    protected function runSlaveLoop(Process $process): void {
        MemoryMonitor::start('GatewayCluster');
        $processSocket = $process->exportSocket();
        while (true) {
            $this->call('update_gateway_cluster_trace_snapshot', 'slave.connect.start', [
                'pid' => (int)$process->pid,
            ]);
            if (!Runtime::instance()->serverIsAlive()) {
                Console::warning('【GatewayCluster】服务器已关闭,结束运行', false);
                MemoryMonitor::stop();
                return;
            }
            try {
                $socket = Manager::instance()->getMasterSocketConnection();
                $this->call('update_gateway_cluster_trace_snapshot', 'slave.connect.ready');
                $lastKeepaliveAt = 0;
                while (true) {
                    $loopStartedAt = microtime(true);
                    $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.start');
                    if (!Runtime::instance()->serverIsAlive()) {
                        try {
                            $socket->close();
                        } catch (Throwable) {
                        }
                        Console::warning('【GatewayCluster】服务器已关闭,结束运行', false);
                        MemoryMonitor::stop();
                        $this->call('exit_coroutine_runtime');
                        return;
                    }
                    $this->call('touch_managed_heartbeat', Key::RUNTIME_GATEWAY_CLUSTER_COORDINATOR_HEARTBEAT_AT, time(), 'GatewayClusterCoordinator');
                    $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.heartbeat_touched');
                    if ((time() - $lastKeepaliveAt) >= 15) {
                        try {
                            $socket->push('::ping');
                            $lastKeepaliveAt = time();
                        } catch (Throwable $throwable) {
                            $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.keepalive.failed', [
                                'error' => $throwable->getMessage(),
                            ]);
                            try {
                                $socket->close();
                            } catch (Throwable) {
                            }
                            Console::warning('【GatewayCluster】keepalive 发送失败,准备重连:' . $throwable->getMessage(), false);
                            break;
                        }
                    }
                    $processRecvStartedAt = microtime(true);
                    $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.process_recv.start', ['timeout' => 0.1]);
                    $message = $processSocket->recv(timeout: 0.1);
                    $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.process_recv.done', [
                        'cost_ms' => round((microtime(true) - $processRecvStartedAt) * 1000, 2),
                        'has_message' => is_string($message) && $message !== '' ? 1 : 0,
                        'is_shutdown' => $message === 'shutdown' ? 1 : 0,
                    ]);
                    if ($message === 'shutdown') {
                        try {
                            $socket->close();
                        } catch (Throwable) {
                        }
                        Console::warning('【GatewayCluster】收到shutdown,安全退出', false);
                        MemoryMonitor::stop();
                        $this->call('exit_coroutine_runtime');
                        return;
                    }
                    if (is_string($message) && $message !== '' && StringHelper::isJson($message)) {
                        $payload = JsonHelper::recover($message);
                        $command = (string)($payload['command'] ?? '');
                        $params = (array)($payload['params'] ?? []);
                        if ($command === 'console_log') {
                            $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.console_log.forward.start');
                            $this->pushSocketMessage($socket, [
                                'event' => 'console_log',
                                'data' => [
                                    'host' => APP_NODE_ID,
                                    ...$params,
                                ]
                            ], 'console_log');
                            $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.console_log.forward.done');
                        }
                    }
                    $reply = $socket->recv(0.1);
                    if ($reply === false) {
                        if (!Manager::instance()->isSocketConnected($socket)) {
                            try {
                                $socket->close();
                            } catch (Throwable) {
                            }
                            Console::warning('【GatewayCluster】与master gateway连接已断开,准备重连', false);
                            break;
                        }
                    } elseif ($reply && isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                        $socket->push('', WEBSOCKET_OPCODE_PONG);
                    }
                    $this->call('update_gateway_cluster_trace_snapshot', 'slave.loop.end', [
                        'loop_cost_ms' => round((microtime(true) - $loopStartedAt) * 1000, 2),
                    ]);
                    MemoryMonitor::updateUsage('GatewayCluster');
                }
            } catch (Throwable $throwable) {
                if (!Runtime::instance()->serverIsAlive()) {
                    MemoryMonitor::stop();
                    $this->call('exit_coroutine_runtime');
                    return;
                }
                Console::warning("【GatewayCluster】与master gateway连接失败:" . $throwable->getMessage(), false);
            }
            Coroutine::sleep(1);
        }
    }

    /**
     * @param object $socket
     * @param array<string, mixed> $payload
     * @param string $label
     * @return void
     */
    protected function pushSocketMessage(object $socket, array $payload, string $label): void {
        $encoded = JsonHelper::toJson($payload);
        try {
            $pushed = $socket->push($encoded);
        } catch (Throwable $throwable) {
            throw new \RuntimeException("push({$label}) failed: " . $throwable->getMessage(), 0, $throwable);
        }
        if ($pushed === false) {
            throw new \RuntimeException("push({$label}) failed: socket disconnected");
        }
    }
}
