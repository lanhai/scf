<?php


namespace Scf\Rpc;


use Scf\Rpc\Client\RequestContext;
use Scf\Rpc\Protocol\Request;
use Scf\Rpc\Protocol\Response;
use Scf\Rpc\Network\TcpClient;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;

class Client {
    private array $requestContext = [];
    /** @var callable|null */
    private $onSuccess;
    /** @var callable|null */
    private $onFail;
    /** @var mixed */
    private mixed $clientArg;

    protected int $maxPackageSize = 1024 * 1024 * 20;


    public function addRequest(string $requestPath, ?int $serviceVersion = null): RequestContext {
        $req = new RequestContext();
        $req->setRequestPath($requestPath);
        $req->setServiceVersion($serviceVersion);
        $this->requestContext[] = $req;
        return $req;
    }

    /**
     * @param callable|null $onSuccess
     * @return Client
     */
    public function setOnSuccess(?callable $onSuccess): Client {
        $this->onSuccess = $onSuccess;
        return $this;
    }

    /**
     * @param callable|null $onFail
     * @return Client
     */
    public function setOnFail(?callable $onFail): Client {
        $this->onFail = $onFail;
        return $this;
    }

    public function exec(float $timeout = 3.0): int {
        $start = time();
        $channel = new Channel(256);
        /** @var RequestContext $requestContext */
        foreach ($this->requestContext as $requestContext) {
            Coroutine::create(function () use ($requestContext, $channel, $timeout) {
                $requestPath = $requestContext->getRequestPath();
                $requestPaths = explode('.', $requestPath);
                $service = array_shift($requestPaths);
                $module = array_shift($requestPaths);
                $action = array_shift($requestPaths);
                $node = $requestContext->getServiceNode();
                $res = new Response();
                if (!$node) {
                    $res->setStatus(Response::STATUS_NOT_AVAILABLE_NODE);
                } else {
                    if (empty($node)) {
                        $res->setStatus(Response::STATUS_NOT_AVAILABLE_NODE);
                    } else {
                        $requestContext->setServiceNode($node);
                        $pack = new Request();
                        $pack->setAppid($requestContext->getClientAppid());
                        $pack->setSign($requestContext->getSign());
                        $pack->setClientIp($requestContext->getClientIp());
                        $pack->setService($service);
                        $pack->setModule($module);
                        $pack->setAction($action);
                        $pack->setArg($requestContext->getArg());
                        $pack->setRequestUUID($requestContext->getRequestUUID());
                        $pack->setClientArg($requestContext->getArg());

                        $client = new TcpClient($this->maxPackageSize, $timeout);
                        if (!$client->connect($node)) {
                            $res->setStatus(Response::STATUS_CONNECT_TIMEOUT);
                        } else {
                            $client->sendRequest($pack);
                            $res = $client->recv();
                        }
                    }
                }
                $channel->push([
                    'context' => $requestContext,
                    'response' => $res
                ]);
            });
        }
        $all = count($this->requestContext);
        $left = $timeout;
        while ((time() < $start + $timeout) && $all > 0) {
            $t = microtime(true);
            $ret = $channel->pop($left + 0.01);
            if ($ret) {
                $all--;
                $this->execCallback($ret['response'], $ret['context']);
            }
            $left = $left - (microtime(true) - $t);
            if ($left < 0 || $all <= 0) {
                break;
            }
        }
        return $all;
    }

    /**
     * @return mixed
     */
    public function getClientArg(): mixed {
        return $this->clientArg;
    }

    /**
     * @param mixed $clientArg
     */
    public function setClientArg(mixed $clientArg): void {
        $this->clientArg = $clientArg;
    }


    private function execCallback(Response $response, RequestContext $context): void {
        //失败状态监测
        $failStatus = [
            Response::STATUS_CONNECT_TIMEOUT,
            Response::STATUS_SERVER_TIMEOUT,
            Response::STATUS_SERVICE_SHUTDOWN,
            Response::STATUS_SERVICE_ERROR
        ];
        if (in_array($response->getStatus(), $failStatus)) {
            //TODO 将节点状态设为不可用
        }
        if ($response->getStatus() === Response::STATUS_OK) {
            $call = $context->getOnSuccess();
            $clientCall = $this->onSuccess;
        } else {
            $call = $context->getOnFail();
            $clientCall = $this->onFail;
        }
        if (is_callable($clientCall)) {
            call_user_func($clientCall, $response, $context);
        }

        if (is_callable($call)) {
            call_user_func($call, $response, $context);
        }
    }
}