<?php

namespace Scf\Mode\Rpc;

use Scf\Rpc\Protocol\Request;
use Scf\Rpc\Service\AbstractServiceModule;

abstract class Controller extends AbstractServiceModule {

    protected function actionNotFound(Request $request): void {
        if ($request->getAction() == '__document__') {
            $this->onDocument($request);
        } else {
            $this->response()->setStatus(404)->setMsg('方法[' . $request->getAction() . ']不存在');
        }
    }

    protected function onServiceError($msg, $code = 503, mixed $data = ""): void {
        $this->response()->setResult($data);
        $this->response()->setStatus(503)->setMsg($msg);
    }

    protected function onServiceSuccess(mixed $data = ""): void {
        $this->response()->setResult($data);
    }

    protected function onException(\Throwable $throwable): void {
        $this->response()->setStatus(500)->setMsg($throwable->getMessage());
    }

    protected function onDocument(Request $request): void {
        $cls = static::class;
        try {
            $document = Document::instance()->create($cls);
            $this->response()->setResult($document);
        } catch (\Throwable $exception) {
            $this->response()->setStatus(503)->setResult($exception->getMessage());
        }
    }
}