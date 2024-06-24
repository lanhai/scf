<?php

namespace Scf\Cloud\Wx\Pay;

use Scf\Client\Http;
use Scf\Core\Component;
use Scf\Core\Result;
use Scf\Helper\XmlHelper;

/**
 * 微信支付请求组件
 * 单例模式,勿使用属性参数!
 */
class Wxpay extends Component {
    protected array $gateway = [
        'transfer_v2' => 'https://api.mch.weixin.qq.com/mmpaymkttransfers/promotion/transfers',
        'unifiedorder_v2' => 'https://api.mch.weixin.qq.com/pay/unifiedorder'
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
        $client = Http::create($this->gateway['unifiedorder_v2']);
        return $this->http($builder, $client);
    }

    /**
     * 请求转账
     * @param WxpayParameterBuilder $builder
     * @return Result
     */
    protected function transfer(WxpayParameterBuilder $builder): Result {
        if ($builder->getVersion() == 'v3') {
            return Result::error('不支持的方法');
        }
        $pem = $builder->getPem();
        $client = Http::create($this->gateway['transfer_v2'], certificate: $pem);
        return $this->http($builder, $client);
    }

    /**
     * 提交HTTP请求
     * @param WxpayParameterBuilder $builder
     * @param Http $client
     * @return Result
     */
    protected function http(WxpayParameterBuilder $builder, Http $client): Result {
        $request = $client->XPost($builder->getParams());
        if ($request->hasError()) {
            return Result::error($request->getMessage(), 'REQUEST_ERROR');
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