<?php

namespace Scf\Rpc\Service;

use Scf\Core\Env;
use Scf\Core\Result;
use Scf\Rpc\Protocol\Request;
use Scf\Rpc\Protocol\Response;
use Scf\Server\Worker\ProcessLife;
use Throwable;

abstract class AbstractServiceModule {
    /** @var Request */
    private Request $request;
    /** @var Response */
    private Response $response;


    /** @var array $allowMethodReflections */
    protected array $allowMethodReflections = [];

    public function __construct() {
        $forbidList = [
            '__exec', '__destruct',
            '__clone', '__construct', '__call',
            '__callStatic', '__get', '__set',
            '__isset', '__unset', '__sleep',
            '__wakeup', '__toString', '__invoke',
            '__set_state', '__clone', '__debugInfo',
            'onRequest'
        ];

        $refClass = new \ReflectionClass(static::class);
        $refMethods = $refClass->getMethods(\ReflectionMethod::IS_PUBLIC);
        foreach ($refMethods as $refMethod) {
            if ((!in_array($refMethod->getName(), $forbidList)) && (!$refMethod->isStatic())) {
                $this->allowMethodReflections[$refMethod->getName()] = $refMethod;
            }
        }
    }

//    abstract public function moduleName(): string;

    protected function request(): Request {
        return $this->request;
    }

    protected function response(): Response {
        return $this->response;
    }

    protected function onRequest(Request $request): bool {
        return true;
    }

    protected function afterRequest(Request $request): void {

    }

    /**
     * @throws Throwable
     */
    protected function onException(Throwable $throwable) {
        throw $throwable;
    }

    protected function actionNotFound(Request $request): void {
        $this->response()->setStatus(Response::STATUS_ACTION_NOT_EXIST);
    }


    abstract protected function onServiceError($msg, $code = 503, mixed $data = ""): void;

    abstract protected function onServiceSuccess(mixed $data = ""): void;

    public function __exec(Request $request, Response $response): void {
        $this->request = $request;
        $this->response = $response;
        try {
            if ($this->onRequest($request) !== false) {
                $action = $request->getAction();
                if (isset($this->allowMethodReflections[$action])) {
                    /** @var Result $result */
                    $args = $request->getArg();
                    if ($args) {
                        $result = $this->$action(...$args);
                    } else {
                        $result = $this->$action();
                    }
                    $responseData = [
                        'errCode' => $result->getErrCode(),
                        'data' => $result->getData(),
                        'message' => $result->getMessage()
                    ];
                    $logger = ProcessLife::instance();
                    Env::isDev() and $responseData['debug'] = $logger->requestDebugInfo();
                    $this->onServiceSuccess($responseData);

                } else {
                    $this->actionNotFound($request);
                }
            }
        } catch (Throwable $throwable) {
            $this->onException($throwable);
        } finally {
            try {
                $this->afterRequest($request);
            } catch (Throwable $throwable) {
                $this->onException($throwable);
            }
        }
    }
}