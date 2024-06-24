<?php

namespace Scf\Cloud\Wx\Pay;

use Scf\Core\Coroutine\Component;
use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Helper\StringHelper;
use Scf\Util\File;
use Scf\Util\Random;
use Scf\Util\Sn;
use Throwable;

class WxpayParameterBuilder extends Component {
    /**
     * API版本
     * @var string
     */
    protected string $_version = 'v3';
    protected string $_appid = "";
    protected string $_mchId = "";
    protected string $_openid = "";
    protected string $_key = "";
    protected ?array $pem = null;
    protected array $params = [];
    protected string $action;

    /**
     * 创建转账参数
     * @param string $appid
     * @param string $version
     * @param array|null $pem
     * @return static
     */
    public static function build(string $appid, string $version = 'v3', ?array $pem = null): static {
        $obj = self::instance()->version($version)->appid($appid);
        if (!is_null($pem)) {
            $caFilePath = APP_PATH . 'tmp/wxpay/pem/' . $appid . '/';
            if (!is_dir($caFilePath)) {
                mkdir($caFilePath, 0777, true);
            }
            $certPEM = $caFilePath . 'cert.pem';
            $keyPEM = $caFilePath . 'key.pem';
            if (!empty($pem['cert']) && (file_exists($certPEM) || File::write($certPEM, $pem['cert']))) {
                $obj->pemCert($certPEM);
            }
            if (!empty($pem['key']) && (file_exists($keyPEM) || File::write($keyPEM, $pem['key']))) {
                $obj->pemKey($keyPEM);
            }
        }
        return $obj;
    }

    /**
     * 预交易下单
     * @param int $amount
     * @param string $body
     * @param string $ip
     * @param string $notifyUrl
     * @param string|null $paySn
     * @return Result
     */
    public function unifiedorder(int $amount, string $body, string $ip, string $notifyUrl, ?string $paySn = null): Result {
        $this->action = 'unifiedorder';
        if (!$this->_mchId) {
            return Result::error('缺少参数:mchId');
        }
        $paySn = $paySn ?: Sn::create('T');
        $this->params = [];
        $nonceStr = Random::character(16);
        $this->setParameter('appid', $this->_appid);
        $this->setParameter('mch_id', $this->_mchId);
        $this->setParameter('nonce_str', $nonceStr);
        $this->setParameter('body', $body);
        $this->setParameter('out_trade_no', $paySn);
        $this->_openid and $this->setParameter('openid', $this->_openid);
        $this->setParameter('total_fee', $amount);
        $this->setParameter('spbill_create_ip', $ip);
        $this->setParameter('notify_url', $notifyUrl);
        $this->setParameter('trade_type', 'NATIVE');
        if (!$sign = $this->createSign()) {
            return Result::error('签名生成失败');
        }
        $this->setParameter('sign', $sign); //签名
        return Result::success();
    }

    /**
     * 生成转账签名
     * @param int $amount
     * @param string $desc
     * @param string|null $paySn
     * @return Result
     */
    public function transfer(int $amount, string $desc = "系统转账", ?string $paySn = null): Result {
        $this->action = 'transfer';
        if (!$this->_key) {
            return Result::error('缺少参数:key');
        }
        if ($this->_version == 'v3') {
            return Result::error('V3暂不支持');
        } else {
            if (empty($this->pem['cert']) || empty($this->pem['key'])) {
                return Result::error('缺少API操作证书');
            }
            if (!$this->_mchId) {
                return Result::error('缺少参数:mchId');
            }
            if (!$this->_openid) {
                return Result::error('缺少参数:openid');
            }
            $paySn = $paySn ?: Sn::create('T');
            $this->params = [];
            $nonceStr = Random::character(16);
            $this->setParameter('mch_appid', $this->_appid);
            $this->setParameter('mchid', $this->_mchId);
            $this->setParameter('nonce_str', $nonceStr);
            $this->setParameter('partner_trade_no', $paySn);
            $this->setParameter('openid', $this->_openid);
            $this->setParameter('check_name', 'NO_CHECK');
            $this->setParameter('amount', $amount);
            $this->setParameter('desc', $desc);
            //$this->setParameter('spbill_create_ip', get_client_ip());
            if (!$sign = $this->createSign()) {
                return Result::error('签名生成失败');
            }
            $this->setParameter('sign', $sign); //签名
        }
        return Result::success();
    }

    /**
     * 设置支付秘钥
     * @param string $key
     * @return $this
     */
    public function key(string $key): static {
        $this->_key = $key;
        return $this;
    }

    /**
     * 设置用户openid
     * @param string $openid
     * @return $this
     */
    public function openid(string $openid): static {
        $this->_openid = $openid;
        return $this;
    }

    /**
     * 设置微信支付商户ID
     * @param string $mchId
     * @return $this
     */
    public function mchId(string $mchId): static {
        $this->_mchId = $mchId;
        return $this;
    }

    /**
     * 设置appid
     * @param string $appid
     * @param array|null $pem
     * @return $this
     */
    protected function appid(string $appid, ?array $pem = null): static {
        $this->_appid = $appid;
        return $this;
    }

    /**
     * 设置版本号
     * @param string $v
     * @return $this
     */
    public function version(string $v): static {
        $this->_version = $v;
        return $this;
    }

    /**
     * 设置操作
     * @param string $action
     * @return $this
     */
    public function action(string $action): static {
        $this->action = $action;
        return $this;
    }

    public function pemCert($cert): static {
        $this->pem['cert'] = $cert;
        return $this;
    }

    public function pemKey($key): static {
        $this->pem['key'] = $key;
        return $this;
    }

    /**
     * 设置签名参数值
     * @param string $parameter 字段名
     * @param string $parameterValue 字段值
     */
    protected function setParameter(string $parameter, string $parameterValue): void {
        $this->params[StringHelper::trim($parameter)] = StringHelper::trim($parameterValue);
    }

    /**
     * 获取指定参数值
     * @param string $parameter 参数字段名
     * @return string
     */
    protected function getParameter(string $parameter): string {
        return $this->params[$parameter];
    }

    public function getAction(): string {
        return $this->action;
    }

    public function getParams(): array {
        return $this->params;
    }

    public function getVersion(): string {
        return $this->_version;
    }

    /**
     * @return array|string[]
     */
    public function getPem(): array {
        return $this->pem;
    }

    /**
     * 生成统一支付接口的支付签名
     * @return string
     */
    protected function createSign(): string {
        $bizParameters = $this->params;
        try {
            ksort($bizParameters);
            $bizString = ArrayHelper::toQueryMap($bizParameters, false) . "&key=" . $this->_key;
            $bizString = md5($bizString);
            return strtoupper($bizString);
        } catch (Throwable) {
            return false;
        }
    }
}