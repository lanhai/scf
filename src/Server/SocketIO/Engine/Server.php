<?php

declare(strict_types=1);

namespace Scf\Server\SocketIO\Engine;

use Scf\Server\SocketIO\Engine\Payload\ChannelPayload;
use Scf\Server\SocketIO\Engine\Payload\ConfigPayload;
use Scf\Server\SocketIO\Engine\Payload\HttpResponsePayload;
use Scf\Server\SocketIO\Engine\Payload\PollingPayload;
use Scf\Server\SocketIO\Engine\Transport\Xhr;
use Scf\Server\SocketIO\Enum\Message\TypeEnum;
use Scf\Server\SocketIO\Event\EventPool;
use Scf\Server\SocketIO\Storage\Table\ListenerSessionTable;
use Scf\Server\SocketIO\Storage\table\ListenerTable;
use Scf\Server\SocketIO\Storage\table\SessionListenerTable;
use Scf\Server\SocketIO\Parser\WebSocket\Packet;
use Scf\Server\SocketIO\Parser\WebSocket\PacketPayload;
use Swoole\WebSocket\Server as WebSocketServer;
use Swoole\WebSocket\Frame as WebSocketFrame;
use Swoole\Http\Request as HttpRequest;
use Swoole\Http\Response as HttpResponse;

/**
 * Class Server
 *
 * @package Scf\Server\SocketIO\Engine
 */
class Server
{
    /** @var WebSocketServer */
    protected $server;

    /** @var array */
    protected $serverEvents = [
        'workerStart', 'request', 'open', 'message', 'close'
    ];

    /** @var Callable */
    private $callback;

    /** @var \Scf\Server\SocketIO\Server */
    private $socketIOServer;

    /**
     * Server constructor.
     *
     * @param int $port
     * @param ConfigPayload $configPayload
     * @param callable $callback
     * @param \Scf\Server\SocketIO\Server $socketIOServer
     */
    public function __construct(int $port, ConfigPayload $configPayload, Callable $callback, \Scf\Server\SocketIO\Server $socketIOServer) {

        $this->callback = $callback;

        $this->socketIOServer = $socketIOServer;

        $this->server = new WebSocketServer("0.0.0.0", $port);

        $this->server->set([
            'open_http_protocol' => true,
            'worker_num' => $configPayload->getWorkerNum() ?? 2,
            'daemonize' => $configPayload->getDaemonize() ?? 0
        ]);

        foreach ($this->serverEvents as $event) {
            $method = 'on' . ucfirst($event);
            if (method_exists($this, $method)) {
                $this->server->on($event, [$this, $method]);
            }
        }

        $this->server->start();
    }

    /**
     * block event listening
     */
    public function onWorkerStart()
    {
        $callback = $this->callback;
        $callback($this->socketIOServer);
    }

    /**
     * @param HttpRequest $request
     * @param HttpResponse $response
     *
     * @throws \Exception
     */
    public function onRequest(HttpRequest $request, HttpResponse $response)
    {
        if ($request->server['request_uri'] === '/socket.io/') {
            $eio = intval($request->get['EIO']) ?? 0;
            $t = $request->get['t'] ?? '';
            $transport = $request->get['transport'] ?? '';
            $sid = $request->get['sid'] ?? '';

            switch ($request->server['request_method']) {
                case 'GET':
                    $pollingPayload = new PollingPayload();
                    $pollingPayload
                        ->setHeaders($request->header)
                        ->setEio($eio)
                        ->setSid($sid)
                        ->setT($t)
                        ->setTransport($transport);

                    $polling = new Xhr();
                    $responsePayload = $polling->handleGet($pollingPayload);
                    break;

                case 'POST':
                    $pollingPayload = new PollingPayload();
                    $pollingPayload
                        ->setHeaders($request->header)
                        ->setRequestPayload($request->rawContent())
                        ->setEio($eio)
                        ->setT($t)
                        ->setTransport($transport)
                        ->setSid($sid);

                    $polling = new Xhr();
                    $responsePayload = $polling->handlePost($pollingPayload);
                    break;

                default:
                    $responsePayload = new HttpResponsePayload();
                    $responsePayload->setStatus(405)->setHtml('method not found');
                    break;
            }
        } else {
            $responsePayload = new HttpResponsePayload();
            $responsePayload->setStatus(404)->setHtml('uri not found');
        }

        $response->status($responsePayload->getStatus());

        if (!empty($responsePayload->getHeader())) {
            foreach ($responsePayload->getHeader() as $key => $value) {
                $response->setHeader($key, strval($value));
            }
        }

        if (!empty($responsePayload->getCookie())) {
            foreach ($responsePayload->getCookie() as $key => $value) {
                $response->setCookie($key, $value);
            }
        }

        if (!empty($responsePayload->getHtml())) {
            $response->end($responsePayload->getHtml());
        }

        if (!empty($responsePayload->getChunkData())) {
            $response->write($responsePayload->getChunkData());
        }

        return;
    }

    /**
     * @param WebSocketServer $server
     * @param HttpRequest $request
     *
     * @throws \Exception
     */
    public function onOpen(WebSocketServer $server, HttpRequest $request)
    {
        echo "server: handshake success with fd{$request->fd}\n";

        if ($request->server['request_uri'] === '/socket.io/') {
            $eio = intval($request->get['EIO']) ?? 0;
            $transport = $request->get['transport'] ?? '';
            $sid = $request->get['sid'] ?? '';

            if ($eio == 3 && $transport == 'websocket' && !empty($sid)) {
                SessionListenerTable::getInstance()->push($sid, $request->fd);
                ListenerSessionTable::getInstance()->push(strval($request->fd), $sid);

                $this->produceEvent($server, '/', 'connection', $request->fd);
            } else {
                echo "invalid params\n";
                $server->close($request->fd);
            }
        } else {
            echo "illegal uri\n";
            $server->close($request->fd);
        }
    }

    /**
     * @param WebSocketServer $server
     * @param WebSocketFrame $frame
     *
     * @throws \Exception
     */
    public function onMessage(WebSocketServer $server, WebSocketFrame $frame)
    {
        echo "websocket\n";
        var_dump($frame->data);

        $packetPayload = Packet::decode($frame->data);

        switch ($packetPayload->getType()) {
            case TypeEnum::PING:
                $message = $packetPayload->getMessage();
                $server->push($frame->fd, TypeEnum::PONG . $message);
                break;

            case TypeEnum::MESSAGE:
                $this->handleEvent($server, $frame->fd, $packetPayload);
                break;

            case TypeEnum::UPGRADE:
                $server->push($frame->fd, TypeEnum::NOOP);
                break;

            default:
                $server->push($frame->fd, 'unknown message or wrong packet');
                break;
        }
    }

    /**
     * @param WebSocketServer $server
     * @param int $fd
     *
     * @throws \Exception
     */
    public function onClose(WebSocketServer $server, int $fd)
    {
        $sid = ListenerSessionTable::getInstance()->pop(strval($fd));
        $fd = SessionListenerTable::getInstance()->pop($sid);

        ListenerTable::getInstance()->pop(strval($fd));

        $this->produceEvent($server, '/', 'disconnect', $fd);

        echo "client closed sid: {$sid}, fd: {$fd}\n";
    }

    /**
     * @param WebSocketServer $server
     * @param int $fd
     * @param PacketPayload $packetPayload
     *
     * @throws \Exception
     */
    private function handleEvent(WebSocketServer $server, int $fd, PacketPayload $packetPayload)
    {
        $namespace = $packetPayload->getNamespace();
        $eventName = $packetPayload->getEvent();

        ListenerTable::getInstance()->push(strval($fd));

        $this->produceEvent($server, $namespace, $eventName, $fd, $packetPayload->getMessage());
    }

    /**
     * @param WebSocketServer $server
     * @param string $namespace
     * @param string $event
     * @param int $fd
     * @param string $message
     */
    private function produceEvent(WebSocketServer $server, string $namespace, string $event, int $fd, string $message = '')
    {
        $eventPayload = EventPool::getInstance()->get($namespace, $event);

        $chan = $eventPayload->getChan();

        if (!is_null($chan)) {

            $channelPayload = new ChannelPayload();
            $channelPayload
                ->setWebSocketServer($server)
                ->setFd($fd)
                ->setMessage($message);

            go(function () use ($chan, $channelPayload) {
                $chan->push($channelPayload);
            });
        } else {
            echo "EventPool not found this namespace[{$namespace}] and event[{$event}]\n";
        }
    }
}