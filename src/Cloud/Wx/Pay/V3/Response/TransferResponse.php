<?php

namespace Scf\Cloud\Wx\Pay\V3\Response;


class TransferResponse extends Base {

    /**
     * @var ?string
     * @required true|商户单号为空
     * @rule string,max:32|商户单号长度大于32位
     */
    public ?string $out_bill_no;

    /**
     * @var ?string
     * @required true|微信单号为空
     * @rule string,max:64|微信单号长度大于64位
     */
    public ?string $transfer_bill_no;
    /**
     * @var ?string
     * @required true|转账状态为空
     * @rule string,max:32|转账状态长度大于32位
     */
    public ?string $state;

    /**
     * @var ?string
     * @required true|转账时间为空
     * @rule string,max:32|转账时间长度大于32位
     */
    public ?string $create_time;

    /**
     * @var ?string
     * @required[query] true|更新时间为空
     * @rule string,max:32|更新时间长度大于32位
     */
    public ?string $update_time;

    /**
     * @var int|null
     * @required[query] true|转账金额为空
     */
    public ?int $transfer_amount;

    /**
     * @var string|null
     */
    public ?string $openid;

    /**
     * @var ?string
     */
    public ?string $package_info;

    /**
     * @var ?string
     */
    public ?string $fail_reason;
}