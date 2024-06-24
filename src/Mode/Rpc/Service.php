<?php

namespace Scf\Mode\Rpc;

use Scf\Helper\JsonHelper;
use Scf\Rpc\Protocol\Request;
use Scf\Rpc\Service\AbstractService;
use Scf\Util\Auth;

abstract class Service extends AbstractService {
    protected function onRequest(Request $request): bool {
        $sign = $request->getSign();
        if (!$sign) {
            $this->response()->setStatus(403)->setMsg('非法访问');
            return false;
        }
        //TODO  使用RSA算法验证签名
        $signData = Auth::decode($sign, $request->getAppid());
        if (!$signData) {
            $this->response()->setStatus(403)->setMsg('验签失败');
            return false;
        }
        $signData = JsonHelper::recover($signData);
        $appid = $signData['appid'] ?? null;
        $requestTime = $signData['time'] ?? 0;
        if ($appid != $request->getAppid() || (time() - $requestTime) > 60 || ($requestTime - time()) > 60) {
            $this->response()->setStatus(403)->setMsg('签名错误');
            return false;
        }
        \Scf\Mode\Web\App::instance()->start();
        return true;
    }

    public static function create(): static {
        return new static();
    }

}