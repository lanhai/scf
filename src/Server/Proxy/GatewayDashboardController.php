<?php

namespace Scf\Server\Proxy;

use Scf\Core\Result;
use Scf\Mode\Web\Request;
use Scf\Server\Controller\DashboardController;

class GatewayDashboardController extends DashboardController {

    public function __construct(
        protected GatewayServer $gateway
    ) {
    }

    public function hydrateDashboardSession(?string $token, string $currentUser = 'system'): static {
        $this->token = (string)$token;
        $this->currentUser = $currentUser;
        $this->loginUser['token'] = $token;
        return $this;
    }

    public function actionCommand(): Result {
        Request::post([
            'command' => Request\Validator::required('命令不能为空'),
            'host' => Request\Validator::required('节点不能为空'),
            'params'
        ])->assign($command, $host, $params);

        return $this->gateway->dashboardCommand((string)$command, (string)$host, (array)($params ?: []));
    }

    public function actionNodes(): Result {
        return Result::success($this->gateway->dashboardNodes());
    }

    public function actionServer(): Result {
        return Result::success(
            $this->gateway->dashboardServerStatus(
                $this->token,
                (string)(Request::header('host') ?: ''),
                (string)(Request::header('referer') ?: '')
            )
        );
    }

    public function actionUpdate(): Result {
        Request::post([
            'type' => Request\Validator::required('更新类型错误'),
            'version' => Request\Validator::required('版本号不能为空')
        ])->assign($type, $version);

        return $this->gateway->dashboardUpdate((string)$type, (string)$version);
    }

    public function actionCrontabs(): Result {
        return Result::success($this->gateway->dashboardCrontabs());
    }
}
