<?php

namespace Scf\Cloud\Wx\Pay\V3;

use Scf\Cloud\Wx\Pay\V3\Response\TransferResponse;

class TransferQuery extends Api {
    protected string $method = 'GET';
    protected string $path = '/v3/fund-app/mch-transfer/transfer-bills/transfer-bill-no/{transfer_bill_no}';
    /**
     * @var string
     * @required true|微信转账单号不能为空
     * @rule string,max:64|微信转账单号不能大于64位
     */
    public string $transfer_bill_no;

    public function request(): TransferResponse {
        return TransferResponse::getResponse($this->_request(), 'query');
    }
}