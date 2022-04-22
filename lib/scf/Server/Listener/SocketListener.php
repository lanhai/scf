<?php

namespace Scf\Server\Listener;

use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Log;
use Scf\Mode\Web\Updater;
use Scf\Server\Http;
use Scf\Server\Manager;
use Scf\Server\Struct\NodeStruct;
use Scf\Util\Time;
use Swlib\SaberGM;
use Swoole\Coroutine;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;
use Swlib\Http\Exception\RequestException;

class SocketListener extends Listener {

    /**
     * 接收消息
     * @throws Exception
     */
    protected function onMessage(Server $server, Frame $frame) {
        if (JsonHelper::is($frame->data)) {
            $data = JsonHelper::recover($frame->data);
            switch ($data['event']) {
                case 'server_status':
                    $server->tick(1000, function ($id) use ($server, $frame) {
                        if ($server->exist($frame->fd)) {
                            $status = Manager::instance()->getStatus();
                            $server->push($frame->fd, JsonHelper::toJson($status));
                        } else {
                            $server->clearTimer($id);
                        }
                    });
                    Coroutine::create(function () use ($frame, $server) {
                        $servers = Manager::instance()->getServers();
                        foreach ($servers as $node) {
                            if (!$node) {
                                continue;
                            }
                            $node = NodeStruct::factory($node);
                            if (time() - $node->heart_beat >= 3) {
                                continue;
                            }
                            try {
                                $websocket = SaberGM::websocket('ws://' . $node->ip . ':' . $node->socketPort . '?username=manager&password=' . md5(APP_AUTH_KEY));
                                Coroutine::create(function () use ($websocket, $node, $frame, $server) {
                                    $websocket->push('log_subscribe');
                                    while ($server->exist($frame->fd)) {
                                        if ($reply = $websocket->recv(3)) {
                                            if (!$reply->data) {
                                                Console::log("日志监听连接已断开 from " . $node->ip, false);
                                                break;
                                            } else {
                                                if (!$server->exist($frame->fd)) {
                                                    break;
                                                }
                                                $reply->data = Log::filter($reply->data);
                                                $server->push($frame->fd, JsonHelper::toJson(['event' => 'console', 'message' => $reply, 'time' => date('m-d H:i:s') . "." . substr(Time::millisecond(), -3), 'node' => $node->ip]));
                                                //Console::log(Color::green($reply) . " from " . $node->ip, false);
                                            }
                                        }
                                        Coroutine::sleep(0.1);
                                    }
                                    Coroutine::defer(function () use ($websocket, $frame) {
                                        $websocket->close();
                                        unset($websocket);
                                        //Console::log('#' . $frame->fd . Color::yellow('断开连接'));
                                    });
                                });
                            } catch (RequestException $exception) {
                                Console::log(Color::red($node->ip . ":" . $node->socketPort . "连接失败:" . $exception->getMessage()), false);
                            }
                        }
                    });
                    break;
                default:
                    $server->push($frame->fd, "不支持的事件");
                    break;
            }
        } else {
            switch ($frame->data) {
                case 'log_subscribe':
                    Console::subscribe($frame->fd);
                    $server->push($frame->fd, "日志订阅成功!会话ID:" . $frame->fd);
                    break;
                case 'reload':
                    $server->push($frame->fd, "start reload");
                    self::$server->reload();
                    break;
                case 'version':
                    $version = Updater::instance()->getVersion();
                    $server->push($frame->fd, JsonHelper::toJson($version));
                    break;
                case 'update':
                    $server->push($frame->fd, "开始执行更新");
                    if (Http::instance()->forceUpdate()) {
                        //$server->push($frame->fd, "更新成功");
                    } else {
                        $server->push($frame->fd, "更新失败");
                    }
                    break;
                default:
                    $server->push($frame->fd, "message received:" . $frame->data);
                    break;
            }
        }
    }

    /**
     *连接关闭
     * @param Server $server
     * @param $fd
     * @return void
     */
    protected function onClose(Server $server, $fd) {
        if ($server->isEstablished($fd)) {
            Console::unsubscribe($fd);
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
        if ((!$password || $password != md5(APP_AUTH_KEY)) && !$token) {
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
            foreach ($headers as $key => $val) {
                $response->header($key, $val);
            }
            // WebSocket connection to 'ws://127.0.0.1:9502/'
            // failed: Error during WebSocket handshake:
            // Response must not include 'Sec-WebSocket-Protocol' header if not present in request: websocket
            if (isset($request->header['sec-websocket-protocol'])) {
                $headers['Sec-WebSocket-Protocol'] = $request->header['sec-websocket-protocol'];
            }
            $response->status(101);
            $response->end();
            $fd = $request->fd;
            Http::master()->defer(function () use ($fd) {
                self::$server->push($fd, JsonHelper::toJson(['event' => 'welcome', 'time' => date('Y-m-d H:i:s')]));
            });
        }
    }

}