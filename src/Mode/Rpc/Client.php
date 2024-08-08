<?php

namespace Scf\Mode\Rpc;

use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Result;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Request;
use Scf\Rpc\Client as TcpClient;
use Scf\Rpc\Protocol\Response;
use Scf\Rpc\Server\ServiceNode;
use Scf\Util\Arr;
use Scf\Util\Auth;
use Scf\Util\Random;

class Client {
    /**
     *  配置项
     * @var array
     */
    protected array $_config = [];
    /**
     * @var array
     */
    private static array $_instances = [];

    protected static array $_nodes = [];

    protected array $_server = [];

    /**
     * 构造器
     * @param array $config 配置项
     */
    public function __construct(array $config = [], $server = []) {
        if (!$this->_config) {
            //合并配置
            $this->_config = Arr::merge($this->_config, $config);
        } else {
            //覆盖配置
            foreach ($config as $k => $c) {
                if (isset($this->_config[$k])) {
                    $this->_config[$k] = $c;
                }
            }
        }
        $this->_server = $server;
    }

    /**
     * 获取单利
     * @param array|null $conf
     * @return static
     */
    final public static function instance(array $conf = null): static {
        $class = static::class;
        $instanceName = $class . ($conf['server'] ?? '');
        if (!isset(self::$_instances[$instanceName])) {
            $_configs = Config::get('rpc');
            $insanceConfig = $_configs['client']['services'][$class] ?? [];
            $config = is_array($conf) ? Arr::merge($insanceConfig, $conf) : $insanceConfig;
            self::$_instances[$instanceName] = new $class($config, $_configs['client']['servers'][$config['server']]);
        }
        return self::$_instances[$instanceName];
    }

    /**
     * @return Result
     */
    public function __document__(): Result {
        return $this->exec(...func_get_args());
    }


    /**
     * 执行rpc请求
     * @return Result
     */
    protected function exec(): Result {
        $params = func_get_args();
        $dbt = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 2);
        $action = $dbt[1]['function'] ?? null;
        $serviceName = str_replace("/", ".", $this->_config['service']) . '.' . $action;
        $appid = $this->_config['appid'] ?? App::id();
        $client = new TcpClient();
        $ctx = $client->addRequest($serviceName);
        $node = $this->getNode($serviceName);
        $node->setVersion(1);
        $ctx->setServiceNode($node);
        $ctx->setRequestAppid($appid);
        $ctx->setRequestIp(Request::ip() ?: SERVER_HOST);
        $ctx->setArg($params);
        //todo 签名使用rsa算法
        $signData = [
            Random::character(10) => Random::character(10),
            'appid' => $appid,
            'time' => time()
        ];
        $sign = Auth::encode(JsonHelper::toJson($signData), $appid);
        $ctx->setSign($sign);
        $ctx->setOnSuccess(function ($response) use (&$ret) {
            $ret = $response->__toString();
        });
        $ctx->setOnFail(function (Response $response) use (&$ret) {
            $ret = $response->__toString();
        });
        $client->exec();

        $result = $ret ? JsonHelper::recover($ret) : [];
        $code = $result['status'] ?? 500;
        $msg = $result['msg'] ?? '服务端系统错误,请确实服务端是否启动';
        if ((int)$code === 0) {
            if (isset($result['result']['errCode']) && isset($result['result']['message']) && isset($result['result']['data'])) {
                return Result::factory($result['result']);
            }
            return Result::success($result['result'] ?? "");
        }
        //App::isDevEnv() and Log::instance()->error('[' . $serviceName . ']服务请求错误:' . $msg);
        return match ((int)$code) {
            403 => Result::error('[' . $serviceName . ']无权访问,请检查签名', $code, $msg),
            500 => Result::error('[' . $serviceName . ']请求失败,请稍后重试', $code, $msg),
            1001 => Result::error('[' . $serviceName . ']获取不到服务端可用节点', $code, $msg),
            1002 => Result::error('[' . $serviceName . ']连接服务端节点超时', $code, $msg),
            1003 => Result::error('[' . $serviceName . ']服务端响应超时', $code, $msg),
            2001 => Result::error('[' . $serviceName . ']服务端读取客户端请求数据包超时', $code, $msg),
            2002 => Result::error('[' . $serviceName . ']发送的数据包不合法', $code, $msg),
            3000 => Result::error('[' . $serviceName . ']节点不可用', 503, $msg),
            3001 => Result::error('[' . $serviceName . ']服务不存在', 404, $msg),
            3002 => Result::error('[' . $serviceName . ']服务模块不存在', 404, $msg),
            default => Result::error('[' . $serviceName . ']系统错误', 500, $msg),
        };
    }

    private function getNode($service): ServiceNode {
        $nodeKey = md5($this->_server['host'] . ':' . $this->_server['port'] . $service);
        if (!isset(self::$_nodes[$nodeKey])) {
            $node = new ServiceNode();
            $node->setIp($this->_server['host']);
            $node->setPort($this->_server['port']);
            $node->setService($service);
            self::$_nodes[$nodeKey] = $node;
        }
        return self::$_nodes[$nodeKey];
    }

    /**
     * @return array
     */
    public function getConfig(): array {
        return [
            'config' => $this->_config,
            'server' => $this->_server
        ];
    }
}