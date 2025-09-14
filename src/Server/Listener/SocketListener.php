<?php

namespace Scf\Server\Listener;

use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Exception;
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
     * @throws Exception
     */
    protected function onMessage(Server $server, Frame $frame): void {
        if (JsonHelper::is($frame->data)) {
            $data = JsonHelper::recover($frame->data);
            switch ($data['event']) {
                case 'appoint_update':
                    Manager::instance()->sendCommandToAllNodeClients('appoint_update', [
                        'type' => $data['data']['type'],
                        'version' => $data['data']['version'],
                    ]);
                    $server->push($frame->fd, "success");
                    break;
                case 'restartAll':
                    Manager::instance()->sendCommandToAllNodeClients('restart');
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
                case 'update':
                    $server->push($frame->fd, "开始执行更新");
                    if (App::forceUpdate()) {
                        $server->push($frame->fd, "更新成功");
                    } else {
                        $server->push($frame->fd, "更新失败");
                    }
                    $server->disconnect($frame->fd);
                    break;
                case str_starts_with($frame->data, 'appoint_update')://指定更新
                    $data = explode(":", $frame->data)[1];
                    $arr = explode("|", $data);
                    $type = $arr[0];
                    $version = $arr[1];
                    if (App::appointUpdateTo($type, $version)) {
                        $server->exist($frame->fd) && $server->isEstablished($frame->fd) and $server->push($frame->fd, "版本更新成功:{$type}=>{$version}");
                    } else {
                        $server->exist($frame->fd) && $server->isEstablished($frame->fd) and $server->push($frame->fd, "版本更新失败:{$type}=>{$version}");
                    }
                    $server->exist($frame->fd) && $server->isEstablished($frame->fd) and $server->disconnect($frame->fd);
                    break;
                case '::ping':
                    $server->push($frame->fd, "::pong");
                    break;
                default:
                    Console::info($frame->data, false);
                    $server->push($frame->fd, "message received:" . $frame->data);
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