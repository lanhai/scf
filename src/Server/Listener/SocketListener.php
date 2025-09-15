<?php

namespace Scf\Server\Listener;

use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Helper\JsonHelper;
use Scf\Server\Http;
use Scf\Server\Manager;
use Scf\Util\Auth;
use Swoole\Event;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\Timer;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;
use Throwable;

class SocketListener extends Listener {

    /**
     * 接收消息
     */
    protected function onMessage(Server $server, Frame $frame): void {
        if (JsonHelper::is($frame->data)) {
            $data = JsonHelper::recover($frame->data);
            switch ($data['event']) {
                case 'appoint_update':
                    $finishCount = Manager::instance()->sendCommandToAllNodeClients('appoint_update', [
                        'type' => $data['data']['type'],
                        'version' => $data['data']['version'],
                    ]);
                    //等待所有节点升级完成
                    if ($data['data']['type'] == 'app') {
                        $finishCount = 0;
                        $round = 1;
                        Timer::tick(1000 * 5, function ($timerId) use ($data, &$finishCount, &$round) {
                            $finish = true;
                            $nodes = Manager::instance()->getServers();
                            if ($nodes) {
                                foreach ($nodes as $node) {
                                    if ($node['app_version'] !== $data['data']['version']) {
                                        $finish = false;
                                    } else {
                                        $finishCount++;
                                    }
                                }
                            }
                            if ($finish || $round >= 12 * 10) {
                                Timer::clear($timerId);
                            }
                            $round++;
                        });
                        Event::wait();
                        Console::success("【Server】{$finishCount} 个节点应用更新完成，版本号:{$data['data']['version']}");
                    }
                    if (App::appointUpdateTo($data['data']['ty'], $data['data']['version'])) {
                        $finishCount++;
                    }
                    $server->push($frame->fd, $finishCount);
                    break;
                case 'restartAll':
                    Manager::instance()->sendCommandToAllNodeClients('restart');
                    try {
                        Http::instance()->reload();
                    } catch (Throwable $e) {
                        Console::warning($e->getMessage());
                    }
                    break;
                //推送服务器运行状态数据到控制面板
                case 'server_status':
                    Manager::instance()->addDashboardClient($frame->fd);
                    Timer::tick(1000, function ($id) use ($server, $frame) {
                        $status = Manager::instance()->getStatus();
                        if ($server->exist($frame->fd) && $server->isEstablished($frame->fd)) {
                            try {
                                $server->push($frame->fd, JsonHelper::toJson($status));
                            } catch (Throwable) {
                                $server->close($frame->fd);
                                Timer::clear($id);
                            }
                        } else {
                            Manager::instance()->removeDashboardClient($frame->fd);
                            Timer::clear($id);
                        }
                    });
                    break;
                default:
                    $server->push($frame->fd, "不支持的事件");
                    break;
            }
        } else {
            switch ($frame->data) {
                case 'slave-node-report':
                    if (Manager::instance()->addNodeClient($frame->fd)) {
                        $server->push($frame->fd, JsonHelper::toJson(['event' => 'message', 'data' => "节点报道成功!客户端ID:" . $frame->fd]));
                    } else {
                        $server->push($frame->fd, JsonHelper::toJson(['event' => 'message', 'data' => "节点报道失败!客户端ID:" . $frame->fd]));
                    }
                    break;
                case 'version':
                    $version = Updater::instance()->getVersion();
                    $server->push($frame->fd, JsonHelper::toJson($version));
                    break;
                case '::ping':
                    $server->push($frame->fd, "::pong");
                    break;
                default:
                    Console::info($frame->data, false);
                    //$server->push($frame->fd, "message received:" . $frame->data);
                    break;
            }
        }
    }


    /**
     * 握手
     * @param Request $request
     * @param Response $response
     * @return false|void
     */
    protected function onHandshake(Request $request, Response $response) {
        $password = $request->get['password'] ?? '';
        $token = $request->get['token'] ?? '';
        if ((!$password || $password != md5(App::authKey())) && (!$token || strlen(Auth::decode($token)) != 10)) {
            $response->status(403);
            $response->end();
        } else {
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
            $response->status(101);
            $response->end();
            $fd = $request->fd;
            Event::defer(function () use ($fd) {
                Http::server()->push($fd, JsonHelper::toJson(['event' => 'welcome', 'data' => [
                    'time' => date('Y-m-d H:i:s'),
                    'host' => SERVER_HOST
                ]]));
            });
        }
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

    /**
     *连接关闭
     * @param Server $server
     * @param $fd
     * @return void
     */
    protected function onClose(Server $server, $fd): void {
        if ($server->isEstablished($fd)) {
            Manager::instance()->removeNodeClient($fd);
            Manager::instance()->removeDashboardClient($fd);
        }
    }
}