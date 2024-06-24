<?php

namespace Scf\Mode\Web;

use Scf\Core\Config;
use Scf\Util\Sn;

/**
 * 简易webservice服务
 */
class SimpleWs extends Controller {
    protected array $headers = [];
    protected array $clientApps = [];
    protected array $appConfig = [];

    protected string|null $appid = null;
    protected string|null $sign = null;
    protected string|null $token = null;
    protected string|null $mpAppid = null;
    protected string|null $sessionKey = null;
    protected string|null $client = null;
    protected string|null $version = null;

    public function _init() {
        $config = Config::get('simple_webservice');
        $this->clientApps = $config['apps'] ?? [];
        $this->_valid();
        $this->appid = $this->headers['appid'] ?? null;
        $this->token = $this->headers['token'] ?? null;
        $this->mpAppid = $this->headers['mpappid'] ?? null;
        $this->sessionKey = $this->headers['sessionkey'] ?? null;
        $this->client = $this->headers['client'] ?? null;
        $this->version = $this->headers['version'] ?? null;
    }

    /**
     * 验证接口请求是否合法
     */
    protected function _valid() {
        $this->headers = $this->request()->header();
        $appid = !empty($this->headers['appid']) ? $this->headers['appid'] : '';
        $sign = !empty($this->headers['sign']) ? $this->headers['sign'] : '';
        $timestamp = !empty($this->headers['timestamp']) ? $this->headers['timestamp'] : '';
        $randStr = !empty($this->headers['rand']) ? $this->headers['rand'] : '';
        //查询商户
        if (!$appid || !isset($this->clientApps[$appid])) {
            $this->error('appid错误:'.$appid, 'APPID_NOT_EXIST');
        } elseif (!$sign) {
            $this->error('签名不能为空', 'SIGN_REQUIERD');
        } elseif (!$timestamp) {
            $this->error('unix时间戳不能为空', 'TIMESTAMP_REQUIRED');
        } elseif (!$randStr) {
            $this->error('随机字符串不能为空', 'RANDSTR_REQUIRED');
        }
        $this->appConfig = $this->clientApps[$appid];
        if ($sign != $this->_getSign($appid, $timestamp, $randStr)) {
            if (Lifetime::isDevEnv()) {
                $this->error('签名错误', 'SIGN_ERROR', [
                    'appid' => $appid,
                    'timestamp' => $timestamp,
                    'rand' => $randStr,
                    'appkey' => $this->appConfig['key'],
                    'sign' => $this->_getSign($appid, $timestamp, $randStr)
                ]);
            } else {
                $this->error('签名错误', 'SIGN_ERROR');
            }
        }
        //检查签名是否被使用过
        if (time() - $timestamp > 30 || $timestamp - time() > 30) {
            $this->error('请同步客户端时间,当前服务器时间:' . date('Y-m-d H:i:s') . '(' . time() . '),请求时间:' . date('Y-m-d H:i:s', $timestamp) . '(' . $timestamp . ')', 'SIGN_EXPIRED', ['server_time' => time(), 'client_time' => $timestamp]);
        }
    }

    /**
     *测试环境自动签名
     * @param $appId
     * @param $appkey
     * @return array
     */
    protected function _debugSign($appId, $appkey): array {
        $timestamp = time();
        $randStr = Sn::create_uuid();
        $sign = md5($appId . md5($appkey) . $timestamp . $randStr);
        return compact('timestamp', 'randStr', 'sign');
    }

    /**
     * 获取签名
     * @param $appId
     * @param $timestamp
     * @param $randStr
     * @return string
     */
    protected function _getSign($appId, $timestamp, $randStr): string {
        $this->sign = md5($appId . md5($this->appConfig['key']) . $timestamp . $randStr);
        return $this->sign;
    }
}