<?php

namespace Scf\Cloud\Wx\Pay\V3;

use RuntimeException;
use Scf\Client\Http;
use Scf\Core\App;
use Scf\Core\Result;
use Scf\Core\Struct;
use Scf\Helper\JsonHelper;
use Scf\Util\File;
use Scf\Util\Random;
use Throwable;

abstract class Api extends Struct {
    protected string $method = 'POST';
    protected string $gateway = 'https://api.mch.weixin.qq.com';
    protected string $path = '';
    protected string $merchantId;
    protected array $pem = [
        'key' => '',
        'cert' => ''
    ];

    abstract public function request();

    /**
     * 初始化接口
     * @param string $merchantId 微信商户号
     * @param string $pemKey 证书key
     * @param string $pemCert 证书cert
     * @return static
     */
    public static function create(string $merchantId, string $pemKey, string $pemCert): static {
        $api = static::factory();
        $api->setPem($merchantId, $pemKey, $pemCert);
        return $api;
    }

    /**
     * 提交HTTP请求
     * @return Result
     */
    protected function _request(): Result {
        if (!$this->validate()) {
            return Result::error("请求参数验证失败:" . $this->getError(), 'REQUEST_VALIDATE_ERROR');
        }
        $gateway = App::isDevEnv() ? 'https://api.weixin.lkyapp.com/pay' : $this->gateway;
        try {
            $auth = $this->createSign();
        } catch (Throwable $e) {
            return Result::error('签名失败:' . $e->getMessage(), 'SIGN_ERROR');
        }
        $client = Http::create($gateway . $this->path);
        $client->setHeader('Authorization', $auth['authorization']);
        $client->setHeader('Accept', 'application/json');
        $client->setHeader('User-Agent', 'Chrome/49.0.2587.3');
        if (strtoupper($this->method) == 'POST') {
            $request = $client->JPost($this->asArray(true));
        } else {
            $request = $client->get();
        }
        $client->close();
        if ($request->hasError()) {
            $responseBody = $client->getBody();
            if (JsonHelper::is($responseBody)) {
                $responseBody = JsonHelper::recover($responseBody);
            }
            return Result::error($responseBody['message'] ?? $request->getMessage(), $responseBody['code'] ?? 'REQUEST_ERROR', $responseBody);
        }
        return Result::success($request->getData());
    }

    /**
     * 设置商户证书
     * @param string $merchantId
     * @param string $key
     * @param string $cert
     * @return $this
     */
    protected function setPem(string $merchantId, string $key, string $cert): static {
        $this->merchantId = $merchantId;
        $caFilePath = APP_PATH . '/tmp/wxpay/pem/' . $merchantId;
        if (!is_dir($caFilePath)) {
            mkdir($caFilePath, 0777, true);
        }
        $keyPEM = $caFilePath . '/key.pem';
        if ($key && (file_exists($keyPEM) || File::write($keyPEM, $key))) {
            $this->pem['key'] = $keyPEM;
        }
        $certPEM = $caFilePath . '/cert.pem';
        if ($cert && (file_exists($certPEM) || File::write($certPEM, $cert))) {
            $this->pem['cert'] = $certPEM;
        }
        return $this;
    }


    /**
     * 构建签名
     * @return array
     * @throws RuntimeException
     */
    protected function createSign(): array {
        if (empty($this->pem['key']) || empty($this->pem['cert'])) {
            throw new RuntimeException('缺少商户证书');
        }
        $body = $this->replacePathParams();
        $body = $body ? JsonHelper::toJson($this->asArray(true)) : "";
        $timestamp = time();
        $nonce = Random::character(32);
        $message = strtoupper($this->method) . "\n" .
            $this->path . "\n" .
            $timestamp . "\n" .
            $nonce . "\n" .
            $body . "\n";
        $privateKey = file_get_contents($this->pem['key']);
        $signature = '';
        $result = openssl_sign($message, $signature, $privateKey, OPENSSL_ALGO_SHA256);
        if (!$result) {
            throw new RuntimeException('Failed to generate signature');
        }
        $signatureBase64 = base64_encode($signature);
        $serialNo = $this->getSerialNo();
        $authorization = sprintf(
            'WECHATPAY2-SHA256-RSA2048 mchid="%s",nonce_str="%s",timestamp="%s",serial_no="%s",signature="%s"',
            $this->merchantId,
            $nonce,
            $timestamp,
            $serialNo,
            $signatureBase64
        );
        return [
            'authorization' => $authorization,
            'timestamp' => (string)$timestamp,
            'nonce_str' => $nonce,
        ];
    }

    /**
     * 替换path参数
     * @return array
     */
    protected function replacePathParams(): array {
        $body = $this->asArray(true);
        $path = $this->path;
        preg_match_all('/\{([a-zA-Z0-9_]+)\}/', $path, $matches);
        $pathParams = $matches[1] ?? [];
        if ($pathParams) {
            foreach ($pathParams as $param) {
                if (isset($body[$param])) {
                    $this->path = str_replace('{' . $param . '}', $body[$param], $this->path);
                    unset($body[$param]);
                }
            }
        }
        return $body;
    }

    /**
     * 获取证书编码
     * @return string
     */
    protected function getSerialNo(): string {
        $certData = $this->pem['cert'] ?? '';
        if (empty($certData)) {
            throw new RuntimeException('Certificate data is empty.');
        }
        if (file_exists($certData)) {
            $certData = file_get_contents($certData);
            if ($certData === false) {
                throw new RuntimeException('Failed to read certificate file.');
            }
        }
        $cert = @openssl_x509_read($certData);
        if ($cert === false) {
            $errorMsg = '';
            while ($msg = openssl_error_string()) {
                $errorMsg .= $msg . "; ";
            }
            throw new RuntimeException('Invalid certificate: ' . $errorMsg);
        }
        $certInfo = openssl_x509_parse($cert);
        if (isset($certInfo['serialNumberHex'])) {
            return strtoupper($certInfo['serialNumberHex']);
        }
        return strtoupper(dechex($certInfo['serialNumber']));
    }
}