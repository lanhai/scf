<?php

namespace Scf\Server\Listener;

use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Table\Runtime;
use Scf\Core\Table\ServerNodeStatusTable;
use Scf\Core\Table\ServerNodeTable;
use Scf\Core\Table\SocketConnectionTable;
use Scf\Core\Table\SocketRouteTable;
use Scf\Helper\JsonHelper;
use Scf\Mode\Socket\Connection;
use Scf\Server\DashboardAuth;
use Scf\Server\Http;
use Scf\Server\Manager;
use Scf\Util\Time;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\Timer;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;
use Throwable;

class SocketListener extends Listener {


    /** 防止同一 fd 并发 push 造成阻塞 */
    private static array $pushing = [];

    /**
     * 接收消息
     */
    protected function onMessage(Server $server, Frame $frame): void {
        //$key = 'WORKER_MEMORY_USAGE:' . $server->worker_id + 1;
        //$lastUsage = Runtime::instance()->get($key) ?: 0;

        if ($route = SocketConnectionTable::instance()->route($frame->fd)) {
            $this->callRoute($route, $frame->fd, Connection::EVENT_MESSAGE, $frame->data, server: $server);
            return;
        }
        if (JsonHelper::is($frame->data)) {
            $data = JsonHelper::recover($frame->data);
            switch ($data['event']) {
                case 'console_log':
                    $time = $data['data']['time'] ?? date('m-d H:i:s') . "." . substr(Time::millisecond(), -3);
                    $message = $data['data']['message'] ?? "";
                    $host = $data['data']['host'];
                    Manager::instance()->sendMessageToAllDashboardClients(JsonHelper::toJson(['event' => 'console', 'message' => ['data' => $message], 'time' => $time, 'node' => $host]));
                    break;
                case 'send_command_to_node':
                    $command = $data['data']['command'];
                    $host = $data['data']['host'];
                    $params = $data['data']['params'] ?? [];
                    $sendResult = Manager::instance()->sendCommandToNode($command, $host, $params);
                    if ($sendResult->hasError()) {
                        $server->push($frame->fd, JsonHelper::toJson([
                            'success' => false,
                            'message' => $sendResult->getMessage()
                        ]));
                    } else {
                        $server->push($frame->fd, JsonHelper::toJson([
                            'success' => true,
                            'message' => '发送成功'
                        ]));
                    }
                    break;
                case 'appoint_update':
                    $taskId = $data['data']['task_id'] ?? uniqid('update_', true);
                    $frameFd = $frame->fd;
                    $runUpdateTask = function (bool $replyToRequester = true) use ($data, $server, $frameFd, $taskId) {
                        $slaveHosts = $this->connectedSlaveHosts($server);
                        $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
                        Manager::instance()->sendCommandToAllNodeClients('appoint_update', [
                            'type' => $data['data']['type'],
                            'version' => $data['data']['version'],
                            'task_id' => $taskId,
                        ]);
                        $timeout = $data['data']['timeout'] ?? 300;
                        $masterUpdateDone = false;
                        $masterUpdateSuccess = false;
                        $masterUpdateError = null;
                        Coroutine::create(function () use ($data, &$masterUpdateDone, &$masterUpdateSuccess, &$masterUpdateError) {
                            $masterUpdateSuccess = App::appointUpdateTo($data['data']['type'], $data['data']['version'], false);
                            if (!$masterUpdateSuccess) {
                                $masterUpdateError = App::getLastUpdateError() ?: '未知原因';
                            }
                            $masterUpdateDone = true;
                        });
                        $slaveSuccessCount = 0;
                        $round = 1;
                        $waitCh = new Channel(1);
                        Timer::tick(1000 * 5, function ($timerId) use ($taskId, $slaveHosts, $timeout, &$slaveSuccessCount, &$round, $waitCh, &$masterUpdateDone) {
                            $summary = $this->summarizeNodeUpdateTask($taskId, $slaveHosts);
                            $slaveSuccessCount = $summary['success'];
                            if (($summary['finished'] && $masterUpdateDone) || $round >= ($timeout / 5)) {
                                Timer::clear($timerId);
                                if (!$waitCh->isEmpty()) { /* no-op */
                                }
                                $waitCh->push(true);
                            }
                            $round++;
                        });
                        $waitCh->pop($timeout + 3);
                        if ($slaveSuccessCount) {
                            Console::success("【Server】{$slaveSuccessCount} 个子节点更新完成，版本号:{$data['data']['version']}");
                        }
                        $summary = $this->summarizeNodeUpdateTask($taskId, $slaveHosts);
                        if ($summary['failed_nodes']) {
                            $failedDetails = array_map(static function ($item) {
                                $error = $item['error'] ?? '';
                                return $item['host'] . ($error ? '(' . $error . ')' : '');
                            }, $summary['failed_nodes']);
                            Console::warning("【Server】以下节点升级失败:" . implode('; ', $failedDetails));
                        }
                        if ($summary['pending_hosts']) {
                            Console::warning("【Server】以下节点升级超时未完成:" . implode(',', $summary['pending_hosts']));
                        }
                        $masterState = 'pending';
                        if ($masterUpdateSuccess) {
                            $masterState = 'success';
                        } elseif ($masterUpdateDone) {
                            $masterState = 'failed';
                            Console::warning("【Server】当前节点升级失败:{$data['data']['type']} => {$data['data']['version']},原因:{$masterUpdateError}");
                        } else {
                            Console::warning("【Server】当前节点升级超时未完成:{$data['data']['type']} => {$data['data']['version']}");
                        }
                        $responsePayload = [
                            'task_id' => $taskId,
                            'type' => $data['data']['type'],
                            'version' => $data['data']['version'],
                            'total_nodes' => count($slaveHosts) + 1,
                            'success_count' => $summary['success'] + ($masterState === 'success' ? 1 : 0),
                            'failed_count' => count($summary['failed_nodes']) + ($masterState === 'failed' ? 1 : 0),
                            'pending_count' => count($summary['pending_hosts']) + ($masterState === 'pending' ? 1 : 0),
                            'failed_nodes' => $summary['failed_nodes'],
                            'pending_hosts' => $summary['pending_hosts'],
                            'master' => [
                                'host' => SERVER_HOST,
                                'state' => $masterState,
                                'error' => $masterState === 'failed' ? $masterUpdateError : '',
                            ]
                        ];
                        $this->clearNodeUpdateTaskStates($taskId, $slaveHosts);
                        if ($replyToRequester && $server->exist($frameFd) && $server->isEstablished($frameFd)) {
                            $server->push($frameFd, JsonHelper::toJson($responsePayload));
                        }
                        if ($masterUpdateSuccess && $data['data']['type'] !== 'public') {
                            App::scheduleUpdateReload($data['data']['type']);
                        }
                    };
                    if (!empty($data['data']['async'])) {
                        if ($server->exist($frameFd) && $server->isEstablished($frameFd)) {
                            $server->push($frameFd, JsonHelper::toJson([
                                'accepted' => true,
                                'task_id' => $taskId,
                                'type' => $data['data']['type'],
                                'version' => $data['data']['version'],
                                'message' => '升级任务已开始，请通过节点状态或控制台查看进度',
                            ]));
                        }
                        Coroutine::create(function () use ($runUpdateTask) {
                            $runUpdateTask(false);
                        });
                    } else {
                        $runUpdateTask();
                    }
                    break;
                case 'restartAll':
                    Manager::instance()->sendCommandToAllNodeClients('shutdown');
                    try {
                        Http::instance()->shutdown();
                    } catch (Throwable $e) {
                        Console::warning($e->getMessage());
                    }
                    break;
                case 'reloadAll':
                    Manager::instance()->sendCommandToAllNodeClients('restart');
                    try {
                        Http::instance()->reload();
                    } catch (Throwable $e) {
                        Console::warning($e->getMessage());
                    }
                    break;
                //推送服务器运行状态数据到控制面板
                case 'server_status':
                    $this->subscribeServerStatus($frame->fd, $server);
                    break;
                //节点报道
                case 'slave_node_report':
                    $host = $data['data']['host'] ?? $data['data'];
                    $role = $data['data']['role'] ?? NODE_ROLE_SLAVE;
                    if (Manager::instance()->addNodeClient($frame->fd, $host, $role)) {
                        $server->push($frame->fd, JsonHelper::toJson(['event' => 'slave_node_report_response', 'data' => $frame->fd]));
                    } else {
                        $server->push($frame->fd, JsonHelper::toJson(['event' => 'message', 'data' => "节点报道失败!客户端ID:" . $frame->fd]));
                    }
                    $nodes = ServerNodeTable::instance()->rows();
                    Console::info("【Server】{$host} 节点加入,客户端连接ID:{$frame->fd},累计节点:" . count($nodes));
                    break;
                case 'node_heart_beat':
                    $host = $data['data']['host'];
                    $status = $data['data']['status'];
                    ServerNodeStatusTable::instance()->set($host, $status);
                    $server->push($frame->fd, "::pong");
                    break;
                case 'node_update_state':
                    $payload = $data['data'] ?? [];
                    $taskId = $payload['task_id'] ?? '';
                    $host = $payload['host'] ?? '';
                    if ($taskId && $host) {
                        Runtime::instance()->set($this->nodeUpdateTaskStateKey($taskId, $host), $payload);
                    }
                    Manager::instance()->sendMessageToAllDashboardClients(JsonHelper::toJson([
                        'event' => 'node_update_state',
                        'data' => $payload,
                    ]));
                    if (!empty($payload['message'])) {
                        Console::info($payload['message'], false);
                    }
                    break;
                default:
                    $server->push($frame->fd, "不支持的事件");
                    break;
            }
        } else {
            switch ($frame->data) {
                case 'version':
                    $version = Updater::instance()->getVersion();
                    $server->push($frame->fd, JsonHelper::toJson($version));
                    break;
                case 'ping':
                case '::ping':
                    $server->push($frame->fd, "::pong");
                    break;
                default:
                    Console::info($frame->data, false);
                    //$server->push($frame->fd, "message received:" . $frame->data);
                    break;
            }
        }
//        Event::defer(function () use ($key, $lastUsage, $frame) {
//            $usage = memory_get_usage();
//            $usageMb = round($usage / 1048576, 2);
//            if ($usageMb - $lastUsage >= 2) {
//                Log::instance()->setModule("server")->info("{$key} [socket:{$frame->fd}]当前内存使用：{$usageMb}MB,较上次增长：" . round($usageMb - $lastUsage, 2) . "MB", false);
//            }
//            Runtime::instance()->set($key, $usageMb);
//        });
    }

    private function connectedSlaveHosts(Server $server): array {
        $hosts = [];
        foreach (ServerNodeTable::instance()->rows() as $node) {
            if (($node['role'] ?? '') === NODE_ROLE_MASTER) {
                continue;
            }
            if (!$server->isEstablished($node['socket_fd'])) {
                continue;
            }
            $hosts[] = $node['host'];
        }
        return array_values(array_unique($hosts));
    }

    private function summarizeNodeUpdateTask(string $taskId, array $hosts): array {
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

    private function clearNodeUpdateTaskStates(string $taskId, array $hosts): void {
        foreach ($hosts as $host) {
            Runtime::instance()->delete($this->nodeUpdateTaskStateKey($taskId, $host));
        }
    }

    private function nodeUpdateTaskStateKey(string $taskId, string $host): string {
        return 'NODE_UPDATE_TASK:' . md5($taskId . ':' . $host);
    }

    protected function callRoute(string $route, int $fd, string $event, string $message = "", ?Request $request = null, ?Server $server = null): bool {
        $router = SocketRouteTable::instance()->get($route);
        $cls = $router['class'];
        try {
            /** @var Connection $obj */
            $obj = $cls::instance($server ?: Http::server(), $fd);
        } catch (Throwable $e) {
            Console::warning("Socket route instance error: {$e->getMessage()}");
            return false;
        }
        try {
            if ($event == Connection::EVENT_DISCONNECT) {
                return $obj->onDisconnect();
            } elseif ($event == Connection::EVENT_AUTH) {
                return $obj->setRequest($request)->auth();
            } elseif ($event == Connection::EVENT_CONNECT) {
                return $obj->onConnect();
            } elseif ($event == Connection::EVENT_MESSAGE) {
                if ($message == "ping" || $message == "::ping") {
                    $server = $server ?: Http::server();
                    return $server->push($fd, "::pong");
                }
                $obj->setEvent($event);
                return $obj->setMessage($message)->onMessage();
            } else {
                Console::warning("Socket route call {$cls} failed: unknow event");
            }
        } catch (Throwable $e) {
            Console::warning("Socket route call {$cls} failed: {$e->getMessage()}");
        }
        return false;
    }

    /**
     * 握手
     * @param Request $request
     * @param Response $response
     * @return false|void
     */
    protected function onHandshake(Request $request, Response $response) {
        if (!Runtime::instance()->serverIsReady()) {
            $response->status(503);
            $response->header('Content-Type', 'application/json;charset=utf-8');
            $response->end(JsonHelper::toJson([
                'errCode' => 'SERVICE_UNAVAILABLE',
                'message' => '服务重启中,请稍后重试',
                'data' => ''
            ]));
            return false;
        }
        $uri = $request->server['request_uri'] ?? '/';
        $connectionRoute = '';
        if ($uri !== '/' && SocketRouteTable::instance()->has($uri)) {
            if (!$this->callRoute($uri, $request->fd, Connection::EVENT_AUTH, request: $request)) {
                $response->status(403);
                goto end;
            }
            $connectionRoute = $uri;
            Event::defer(function () use ($request, $uri) {
                if (Http::server()->isEstablished($request->fd)) {
                    $this->callRoute($uri, $request->fd, Connection::EVENT_CONNECT);
                }
            });
            goto upgrade;
        }
        $token = $request->get['token'] ?? '';
        if (!$token) {
            $response->status(403);
            goto end;
        }
        if (!DashboardAuth::validateSocketToken($token)) {
            $response->status(403);
            goto end;
        }
        upgrade:
        //websocket握手连接算法验证
        $secWebSocketKey = $request->header['sec-websocket-key'];
        $patten = '#^[+/0-9A-Za-z]{21}[AQgw]==$#';
        if (0 === preg_match($patten, $secWebSocketKey) || 16 !== strlen(base64_decode($secWebSocketKey))) {
            $response->end();
            return false;
        }
        $key = base64_encode(
            sha1(
                $request->header['sec-websocket-key'] . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11',
                true
            )
        );
        $headers = [
            'Upgrade' => 'websocket',
            'Connection' => 'Upgrade',
            'Sec-WebSocket-Accept' => $key,
            'Sec-WebSocket-Version' => '13',
        ];
        // WebSocket connection to 'ws://127.0.0.1:9502/'
        // failed: Error during WebSocket handshake:
        // Response must not include 'Sec-WebSocket-Protocol' header if not present in request: websocket
        if (isset($request->header['sec-websocket-protocol'])) {
            $headers['Sec-WebSocket-Protocol'] = $request->header['sec-websocket-protocol'];
        }
        foreach ($headers as $key => $val) {
            $response->header($key, $val);
        }
        SocketConnectionTable::instance()->remember($request->fd, Http::server()->worker_id + 1, $connectionRoute);
        $response->status(101);
        end:
        $response->end();
    }

    /**
     * WebSocket连接打开事件,只有未设置handshake回调时此回调才生效
     * @param Server $server
     * @param Request $request
     * @return void
     */
    protected function onOpen(Server $server, Request $request): void {
        $server->push($request->fd, "未授权的请求");
        $server->close($request->fd);
    }

    public function onConnect(Server $server, $fd): void {

    }

    /**
     * 连接关闭
     * @param Server $server
     * @param $fd
     * @return void
     */
    protected function onClose(Server $server, $fd): void {
        $connection = SocketConnectionTable::instance()->connection($fd);
        if ($connection) {
            SocketConnectionTable::instance()->delete($fd);
            $route = $connection['route'] ?? '';
            if ($route) {
                $this->callRoute($route, $fd, Connection::EVENT_DISCONNECT);
            }
        }
        Manager::instance()->removeNodeClient($fd);
        Manager::instance()->removeDashboardClient($fd);
    }

    protected function subscribeServerStatus($fd, Server $server): void {
        Manager::instance()->addDashboardClient($fd);
        Timer::tick(1000, function ($id) use ($server, $fd) {
            if (!Runtime::instance()->serverIsAlive()) {
                $server->close($fd);
                Timer::clear($id);
                return;
            }
            $status = Manager::instance()->getStatus();
            if ($server->exist($fd) && $server->isEstablished($fd)) {
                // 避免同一 fd 在上一次发送未完成时再次 push，导致协程互等
                if (!empty(self::$pushing[$fd])) {
                    return; // 跳过本轮，等下次
                }
                self::$pushing[$fd] = true;
                Event::defer(function () use ($server, $fd, $status, $id) {
                    try {
                        // push 可能因发送缓冲区拥堵而 yield，这里放到下一轮事件循环中执行避免阻塞 tick 回调
                        $ok = $server->push($fd, JsonHelper::toJson($status));
                        if ($ok === false) {
                            // 发送失败，关闭连接并停止推送
                            if ($server->exist($fd)) {
                                $server->close($fd);
                            }
                            Timer::clear($id);
                        }
                    } catch (Throwable) {
                        if ($server->exist($fd)) {
                            $server->close($fd);
                        }
                        Timer::clear($id);
                    } finally {
                        unset(self::$pushing[$fd]);
                    }
                });
            } else {
                Manager::instance()->removeDashboardClient($fd);
                Timer::clear($id);
            }
            unset($status);
        });
    }
}
