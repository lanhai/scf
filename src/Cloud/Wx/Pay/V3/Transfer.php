<?php

namespace Scf\Cloud\Wx\Pay\V3;

use Scf\Cloud\Wx\Pay\V3\Response\TransferResponse;
use Scf\Cloud\Wx\Pay\Wxpay;

class Transfer extends Api {
    protected string $path = '/v3/fund-app/mch-transfer/transfer-bills';


    public function request(): TransferResponse {
        return TransferResponse::getResponse($this->_request(), 'create');
    }


    /**
     * @var string
     * @required true|商户appid不能为空
     * @rule string,max:32|商户appid长度不能大于32位
     */
    public string $appid;

    /**
     * @var string
     * @required true|商户单号不能为空
     * @rule string,max:32|商户单号长度不能大于32位
     */
    public string $out_bill_no;

    /**
     * @var string
     * @required true|转账场景ID不能为空
     * @rule string,max:32|转账场景ID长度不能大于32位
     * @default string:1005
     */
    public string $transfer_scene_id;

    /**
     * @var string
     * @required true|收款用户openid不能为空
     * @rule string,max:64|收款用户openid长度不能大于64位
     */
    public string $openid;

    /**
     * @var int
     * @required true|转账金额不能为空
     * @rule int,min:10|转账金额不能小于0.1元
     * @rule int,max:1000000|转账金额不能大于1万元
     */
    public int $transfer_amount;

    /**
     * @var string
     * @required true|转账备注不能为空
     * @rule string,max:32|转账备注长度不能大于32位
     */
    public string $transfer_remark;
    /**
     * @var array
     * @required true|转账场景报备信息不能为空
     * @rule array|转账场景报备信息格式错误
     */
    public array $transfer_scene_report_infos;

    /**
     * @var ?string
     * @rule method:create_notify_url|通知回调地址格式错误
     * @default string:''
     */
    public ?string $notify_url;

    /**
     * @var ?string
     * @rule string,max:32|用户收款感知不能大于32位
     */
    public ?string $user_recv_perception;
    /**
     * @var ?string
     * @rule string,max:32|收款用户姓名长度不能大于32位
     */
    public ?string $user_name;

    protected function create_notify_url(): bool {
        if (empty($this->out_bill_no)) {
            return false;
        }
        $this->notify_url = Wxpay::instance()->getConfig('trsansfer_notify_url') . '/' . $this->out_bill_no . '/';
        return true;
    }

}