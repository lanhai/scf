<?php


namespace Scf\Rpc\Service;


use Scf\Rpc\Protocol\Request;
use Scf\Rpc\Protocol\Response;

abstract class AbstractService {
    private array $modules = [];
    /** @var Request */
    private Request $request;
    /** @var Response */
    private Response $response;


    abstract function serviceName(): string;

    protected function request(): Request {
        return $this->request;
    }

    protected function response(): Response {
        return $this->response;
    }


    public function addModule(AbstractServiceModule $module): AbstractService {
        $classMap = explode("\\", get_class($module));
        $moduleName = array_pop($classMap);
        $this->modules[$moduleName] = $module;
        return $this;
    }

    protected function onRequest(Request $request): bool {
        return true;
    }

    /**
     * @throws \Throwable
     */
    protected function onException(\Throwable $throwable) {
        throw $throwable;
    }

    protected function moduleNotFound(Request $request): void {
        $this->response()->setStatus(Response::STATUS_MODULE_NOT_EXIST);
    }


    protected function afterRequest(Request $request): void {

    }

    public function __exec(Request $request, Response $response): void {
        $this->request = $request;
        $this->response = $response;
        try {
            if ($this->onRequest($request) !== false) {
                $module = $this->modules[$request->getModule()] ?? null;
                if ($module instanceof AbstractServiceModule) {
                    //克隆模式，否则如果定义了成员属性会发生协程污染
                    $module = clone $module;
                    $module->__exec($request, $response);
                } else {
                    $this->moduleNotFound($request);
                }
            }
        } catch (\Throwable $throwable) {
            $this->onException($throwable);
        } finally {
            try {
                $this->afterRequest($request);
            } catch (\Throwable $throwable) {
                $this->onException($throwable);
            }
        }
    }

}