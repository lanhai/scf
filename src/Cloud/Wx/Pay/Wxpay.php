<?php

namespace Scf\Cloud\Wx\Pay;

use Scf\Client\Http;
use Scf\Core\App;
use Scf\Core\Component;
use Scf\Core\Result;
use Scf\Helper\XmlHelper;

class Wxpay extends Component {
    protected string $apiHost = 'https://api.mch.weixin.qq.com';
    protected array $gateway = [
        'transfer' => '/mmpaymkttransfers/promotion/transfers',
        'unifiedorder' => '/pay/unifiedorder'
    ];

    /**
     * @param WxpayParameterBuilder $builder
     * @return Result
     */
    public function request(WxpayParameterBuilder $builder): Result {
        $action = $builder->getAction();
        if (!method_exists($this, $action)) {
            return Result::error('不支持的方法:' . $action, 'UNSUPPORT_METHOD');
        }
        return $this->$action($builder);
    }

    /**
     * 预交易下单
     * @param WxpayParameterBuilder $builder
     * @return Result
     */
    protected function unifiedorder(WxpayParameterBuilder $builder): Result {
        return $this->http($builder, $this->gateway['unifiedorder']);
    }

    /**
     * 请求转账
     * @param WxpayParameterBuilder $builder
     * @return Result
     */
    protected function transfer(WxpayParameterBuilder $builder): Result {
        return $this->http($builder, $this->gateway['transfer']);
    }

    /**
     * 提交HTTP请求
     * @param WxpayParameterBuilder $builder
     * @param string|array $path
     * @return Result
     */
    protected function http(WxpayParameterBuilder $builder, string|array $path): Result {
        $apiHost = App::isDevEnv() ? 'https://api.weixin.lkyapp.com/pay' : $this->apiHost;
        $pem = $builder->getPem();
        $client = Http::create($apiHost . $path, certificate: $pem);
        $request = $client->XPost($builder->getParams());
        if ($request->hasError()) {
            return Result::error($request->getMessage(), $request->getErrCode());
        }
        $result = XmlHelper::toArray($request->getData());
        $returnCode = $result['return_code'] ?? "FAIL";
        $returnMsg = $result['return_msg'] ?? "未知错误";
        if ($returnCode !== 'SUCCESS') {
            return Result::error("请求微信支付失败:" . $returnMsg, $returnCode, $result);
        }
        $resultCode = $result['result_code'] ?? "FAIL";
        $resultMsg = $result['err_code_des'] ?? "未知错误";
        if ($resultCode !== 'SUCCESS') {
            return Result::error("微信支付业务处理失败:" . $resultMsg, $result['err_code'] ?? 'UNKNOW_ERROR', $result);
        }
        return Result::success($result);
    }
}