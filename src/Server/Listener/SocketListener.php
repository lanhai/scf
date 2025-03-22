<?php

namespace Scf\Server\Listener;

use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Core\Log;
use Scf\Helper\JsonHelper;
use Scf\Command\Color;
use Scf\Server\Http;
use Scf\Server\Manager;
use Scf\Server\Struct\Node;
use Scf\Util\Auth;
use Scf\Util\Time;
use Swlib\SaberGM;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Http\Request;
use Swoole\Http\Response;
use Swoole\Timer;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server;
use Swlib\Http\Exception\RequestException;
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
                case 'restartAll':
                    Console::log("【Server】" . Color::yellow("收到重启服务器指令"));
                    $servers = Manager::instance()->getServers();
                    foreach ($servers as $node) {
                        if (!$node) {
                            continue;
                        }
                        $node['framework_build_version'] = $node['framework_build_version'] ?? '--';
                        $node['framework_update_ready'] = $node['framework_update_ready'] ?? false;
                        $node = Node::factory($node);
                        if (time() - $node->heart_beat >= 3) {
                            continue;
                        }
                        try {
                            if (SERVER_HOST_IS_IP) {
                                $socketHost = $node->ip . ':' . $node->socketPort;
                            } else {
                                $socketHost = $node->socketPort . '.' . $node->ip . '/dashboard.socket';
                            }
                            $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
                            Coroutine::create(function () use ($websocket, $node, $frame, $server) {
                                $websocket->push('reload');
                                Coroutine::defer(function () use ($websocket, $frame) {
                                    $websocket->close();
                                    unset($websocket);
                                });
                            });
                        } catch (RequestException $exception) {
                            Console::log(Color::red($socketHost . "连接失败:" . $exception->getMessage()), false);
                        }
                    }
                    break;
                //推送服务器运行状态数据
                case 'server_status':
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
                            Timer::clear($id);
                        }
                    });
                    //节点控制台消息转发推送
                    Coroutine::create(function () use ($frame, $server) {
                        $servers = Manager::instance()->getServers();
                        foreach ($servers as $node) {
                            if (!$node) {
                                continue;
                            }
                            $node['framework_build_version'] = $node['framework_build_version'] ?? '--';
                            $node['framework_update_ready'] = $node['framework_update_ready'] ?? false;
                            $node = Node::factory($node);
                            if (time() - $node->heart_beat >= 3) {
                                continue;
                            }
                            try {
                                if (SERVER_HOST_IS_IP) {
                                    $socketHost = $node->ip . ':' . $node->socketPort;
                                } else {
                                    $socketHost = $node->socketPort . '.' . $node->ip . '/dashboard.socket';
                                }
                                $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
                                Coroutine::create(function () use ($websocket, $node, $frame, $server) {
                                    $websocket->push('log_subscribe');
                                    while ($server->exist($frame->fd)) {
                                        if (!$server->isEstablished($frame->fd)) {
                                            break;
                                        }
                                        if ($reply = $websocket->recv(5)) {
                                            if (!$reply->data) {
                                                Console::log("日志监听连接已断开 from " . $node->ip, false);
                                                break;
                                            } else {
                                                if (!$server->exist($frame->fd)) {
                                                    break;
                                                }
                                                $reply->data = Log::filter($reply->data);
                                                $server->push($frame->fd, JsonHelper::toJson(['event' => 'console', 'message' => $reply, 'time' => date('m-d H:i:s') . "." . substr(Time::millisecond(), -3), 'node' => $node->ip]));
                                            }
                                        }
                                        Coroutine::sleep(0.1);
                                    }
                                    Coroutine::defer(function () use ($websocket, $frame) {
                                        $websocket->close();
                                        unset($websocket);
                                    });
                                });
                            } catch (RequestException $exception) {
                                Console::log(Color::red($socketHost . "连接失败:" . $exception->getMessage()), false);
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
                    Http::server()->reload();
                    break;
                case 'version':
                    $version = Updater::instance()->getVersion();
                    $server->push($frame->fd, JsonHelper::toJson($version));
                    break;
                case 'update':
                    $server->push($frame->fd, "开始执行更新");
                    if (Http::instance()->forceUpdate()) {
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
                    if (Http::instance()->appointUpdateTo($type, $version)) {
                        $server->exist($frame->fd) && $server->isEstablished($frame->fd) and $server->push($frame->fd, "版本更新成功:{$type}=>{$version}");
                    } else {
                        $server->exist($frame->fd) && $server->isEstablished($frame->fd) and $server->push($frame->fd, "版本更新失败:{$type}=>{$version}");
                    }
                    $server->exist($frame->fd) && $server->isEstablished($frame->fd) and $server->disconnect($frame->fd);
                    break;
                default:
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
                Http::server()->push($fd, JsonHelper::toJson(['event' => 'welcome', 'time' => date('Y-m-d H:i:s')]));
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
            Console::unsubscribe($fd);
        }
    }
}