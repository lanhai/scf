<?php

namespace Scf\Server\SubProcess;

use Scf\Core\App;
use Scf\Core\Console;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Server\Manager;
use Scf\Util\MemoryMonitor;
use Swoole\Process;
use Throwable;
use function Co\run;

/**
 * ConsolePush 子进程运行逻辑。
 *
 * 责任边界：
 * - 维持到 master 的 websocket 通道，并把本地 console payload 转发给 master。
 */
class ConsolePushProcess extends AbstractRuntimeProcess {
    /**
     * 创建 ConsolePush 子进程定义。
     *
     * @return Process
     */
    public function create(): Process {
        return new Process(function (Process $process) {
            App::mount();
            Console::info("【ConsolePush】控制台消息推送PID:" . $process->pid, false);
            MemoryMonitor::start('ConsolePush');
            run(function () use ($process) {
                while (true) {
                    $masterSocket = Manager::instance()->getMasterSocketConnection();
                    while (true) {
                        $masterSocket->push('::ping');
                        $reply = $masterSocket->recv(5);
                        if ($reply && isset($reply->opcode) && $reply->opcode === WEBSOCKET_OPCODE_PING) {
                            $masterSocket->push('', WEBSOCKET_OPCODE_PONG);
                        }
                        if (($reply === false || !$reply || $reply->data === '' || $reply->data === '::pong')
                            && !Manager::instance()->isSocketConnected($masterSocket)) {
                            try {
                                $masterSocket->close();
                            } catch (Throwable) {
                            }
                            Console::warning('【ConsolePush】与master节点连接已断开', false);
                            break;
                        }
                        $processSocket = $process->exportSocket();
                        $msg = $processSocket->recv(timeout: 30);
                        if ($msg) {
                            if (StringHelper::isJson($msg)) {
                                $payload = JsonHelper::recover($msg);
                                $masterSocket->push(JsonHelper::toJson(['event' => 'console_log', 'data' => [
                                    'host' => SERVER_ROLE == NODE_ROLE_MASTER ? 'master' : SERVER_HOST,
                                    ...$payload
                                ]]));
                            } elseif ($msg == 'shutdown') {
                                $masterSocket->close();
                                Console::warning('【ConsolePush】管理进程退出,结束推送', false);
                                MemoryMonitor::stop();
                                return;
                            }
                        }
                        MemoryMonitor::updateUsage('ConsolePush');
                    }
                }
            });
        });
    }
}
