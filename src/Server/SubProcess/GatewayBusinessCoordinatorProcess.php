<?php

namespace Scf\Server\SubProcess;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Swoole\Process;
use function Co\run;

/**
 * GatewayBusinessCoordinator 子进程运行逻辑。
 *
 * 责任边界：
 * - 承接业务编排命令，并驱动业务编排周期 tick。
 */
class GatewayBusinessCoordinatorProcess extends AbstractRuntimeProcess {
    /**
     * @return Process
     */
    public function create(): Process {
        return new Process(function (Process $process) {
            run(function () use ($process) {
                $this->call('mark_gateway_sub_process_context');
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID, (int)$process->pid);
                Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT, time());
                if (!(bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING) ?? false)) {
                    Console::info("【GatewayBusiness】业务编排PID:" . $process->pid, false);
                }
                $lastTickAt = 0;
                $socket = $process->exportSocket();
                while (true) {
                    if (!Runtime::instance()->serverIsAlive()) {
                        Console::warning('【GatewayBusiness】服务器已关闭,结束运行');
                        $this->call('exit_coroutine_runtime');
                        break;
                    }
                    $ownerPid = (int)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_PID) ?? 0);
                    if ($ownerPid > 0 && $ownerPid !== (int)$process->pid) {
                        Console::warning("【GatewayBusiness】检测到新实例接管(owner_pid={$ownerPid})，当前实例退出");
                        $this->call('exit_coroutine_runtime');
                        break;
                    }
                    $now = time();
                    Runtime::instance()->set(Key::RUNTIME_GATEWAY_BUSINESS_COORDINATOR_HEARTBEAT_AT, $now);
                    if ($now !== $lastTickAt && Runtime::instance()->serverIsReady()) {
                        $lastTickAt = $now;
                        $this->call('run_gateway_business_tick');
                    }

                    $message = $socket->recv(timeout: 1);
                    if ($message === 'shutdown') {
                        Console::warning('【GatewayBusiness】收到shutdown,安全退出');
                        $this->call('exit_coroutine_runtime');
                        break;
                    }
                    if (is_string($message) && $message !== '') {
                        if (!StringHelper::isJson($message)) {
                            continue;
                        }
                        $payload = JsonHelper::recover($message);
                        $command = trim((string)($payload['command'] ?? ''));
                        if ($command === '') {
                            continue;
                        }
                        $this->call('run_gateway_business_command', $command, (array)($payload['params'] ?? []), 'pipe', '');
                        continue;
                    }

                    $queued = $this->call('dequeue_gateway_business_runtime_command');
                    if (is_array($queued)) {
                        $this->call(
                            'run_gateway_business_command',
                            (string)$queued['command'],
                            (array)$queued['params'],
                            'runtime_queue',
                            (string)($queued['token'] ?? '')
                        );
                    }
                }
            });
        });
    }
}
