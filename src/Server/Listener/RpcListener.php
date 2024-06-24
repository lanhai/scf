<?php

namespace Scf\Server\Listener;

use Scf\Helper\JsonHelper;
use Scf\Rpc\Manager;
use Scf\Rpc\Protocol\Protocol;
use Scf\Rpc\Protocol\Request;
use Scf\Rpc\Protocol\Response;
use Scf\Rpc\Service\AbstractService;
use Swoole\WebSocket\Server;

class RpcListener extends Listener {
    protected int $maxPackageSize = 1024 * 1024 * 8;

    protected function onConnect(Server $server, $clientId) {
        //$client = $server->getClientInfo($clientId);
        //Console::log('【Server】新连接:' . $client['remote_ip']);
    }

    protected function onReceive(Server $server, $fd, $reactor_id, $data): void {
        $header = substr($data, 0, 4);
        $response = new Response();
        if (strlen($header) != 4) {
            $response->setStatus($response::STATUS_PACKAGE_READ_TIMEOUT);
            $response->setMsg("数据包接收超时");
            $this->reply($server, $fd, $response);
            return;
        }
        //判断数据包大小
        $allLength = Protocol::packDataLength($header);
        if ($allLength > $this->maxPackageSize) {
            $response->setStatus($response::STATUS_ILLEGAL_PACKAGE);
            $response->setMsg("数据包长度不合法");
            $this->reply($server, $fd, $response);
            return;
        }
        $data = Protocol::unpack($data);
        if (strlen($data) != $allLength) {
            $response->setStatus($response::STATUS_PACKAGE_READ_TIMEOUT);
            $response->setMsg("数据包接收超时");
            $this->reply($server, $fd, $response);
            return;
        }
        $request = JsonHelper::recover($data);
        if (!is_array($request)) {
            $response->setStatus($response::STATUS_ILLEGAL_PACKAGE);
            $response->setMsg("数据包内容不合法");
            $this->reply($server, $fd, $response);
            return;
        }
        $request = new Request($request);
        $serviceManager = Manager::instance();
        $serviceList = $serviceManager->getServiceRegisterArray();
        try {
            if (!$request->getService()) {
                $response->setMsg("非法请求");
                $response->setStatus($response::STATUS_SERVICE_NOT_EXIST);
            } elseif (isset($serviceList[$request->getService()])) {
                /** @var AbstractService $service */
                //克隆模式，否则如果定义了成员属性会发生协程污染
                $service = clone $serviceList[$request->getService()];
                $service->__exec($request, $response);
            } else {
                $response->setMsg("不存在的服务");
                $response->setStatus($response::STATUS_SERVICE_NOT_EXIST);
            }
        } catch (\Throwable) {
            $response->setStatus(Response::STATUS_SERVICE_ERROR);
        }
        $this->reply($server, $fd, $response);
    }

    protected function reply(Server $clientSocket, $fd, Response $response): void {
        $str = JsonHelper::toJson($response);
        $str = Protocol::pack($str);
        $clientSocket->send($fd, $str);
        $clientSocket->close($fd);
    }

}