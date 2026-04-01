<?php

namespace Scf\Server\SubProcess;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Swoole\Process;
use function Co\run;

/**
 * GatewayHealthMonitor 子进程运行逻辑。
 *
 * 责任边界：
 * - 推进 active upstream 健康检查 tick。
 */
class GatewayHealthMonitorProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        return new Process(function (Process $process) {
            run(function () use ($process) {
                $this->call('mark_gateway_sub_process_context');
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT, time());
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_HEALTH_MONITOR_TRACE_SNAPSHOT, '');
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【GatewayHealth】健康检查PID:" . $process->pid, false);
                }
                $socket = $process->exportSocket();
                while (true) {
                    $loopStartedAt = microtime(true);

                    $aliveCheckStartedAt = microtime(true);
                    $serverAlive = Runtime::instance()->serverIsAlive();
                    $this->call('trace_heartbeat_step', 'GatewayHealth.loop.server_alive', $aliveCheckStartedAt, [
                        'alive' => $serverAlive ? 1 : 0,
                    ]);
                    if (!$serverAlive) {
                        Console::warning('【GatewayHealth】服务器已关闭,结束运行');
                        $this->call('exit_coroutine_runtime');
                        break;
                    }

                    $touchStartedAt = microtime(true);
                    $this->call('touch_managed_heartbeat', Key::RUNTIME_GATEWAY_HEALTH_MONITOR_HEARTBEAT_AT, time(), 'GatewayHealthMonitor');
                    $this->call('trace_heartbeat_step', 'GatewayHealth.loop.touch_heartbeat', $touchStartedAt);

                    $readyCheckStartedAt = microtime(true);
                    $serverReady = Runtime::instance()->serverIsReady();
                    $appReady = App::isReady();
                    $this->call('trace_heartbeat_step', 'GatewayHealth.loop.ready_check', $readyCheckStartedAt, [
                        'server_ready' => $serverReady ? 1 : 0,
                        'app_ready' => $appReady ? 1 : 0,
                    ]);
                    if ($serverReady && $appReady) {
                        $healthTickStartedAt = microtime(true);
                        $this->call('run_gateway_health_tick');
                        $this->call('trace_heartbeat_step', 'GatewayHealth.loop.run_health_tick', $healthTickStartedAt);
                    }

                    $recvStartedAt = microtime(true);
                    $message = $socket->recv(timeout: 1);
                    $this->call('trace_heartbeat_step', 'GatewayHealth.loop.pipe_recv', $recvStartedAt, [
                        'is_shutdown' => $message === 'shutdown' ? 1 : 0,
                        'has_message' => is_string($message) && $message !== '' ? 1 : 0,
                    ]);
                    if ($message === 'shutdown') {
                        Console::warning('【GatewayHealth】收到shutdown,安全退出');
                        $this->call('exit_coroutine_runtime');
                        break;
                    }
                    $this->call('trace_heartbeat_step', 'GatewayHealth.loop.total', $loopStartedAt);
                }
            });
        });
    }
}
