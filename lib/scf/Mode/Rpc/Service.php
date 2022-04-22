<?php

/**
 * 连客云集成框架
 * User: linkcloud
 * Date: 14-7-4
 * Time: 上午11:25
 */

namespace HuiYun\Mode\Rpc;

//导入类库
use HuiYun\Component\Log;
use HuiYun\Core\Output;
use HuiYun\Core\Result;
use HuiYun\Mode\Web\View;
use stdClass;

abstract class Service {
    protected $allowMethodList = '';
    protected $crossDomain = false;
    protected $P3P = false;
    protected $get = true;
    /**
     * @var bool 是否调试
     * @rule test
     */
    protected $debug = false;

    public function __construct() {
        if (get_action()) {
            if (get_action() == 'document') {
                //生成文档
                try {
                    $document = [
                        'name' => camel2lower(get_controller()),
                        'langue' => 'php',
                        'debug' => 'https://dev.xyobd.com/cp/api/rpc/?service=' . camel2lower(get_controller()),
                        'gateway' => ['dev' => get_host() . '/' . camel2lower(get_module()) . '/' . camel2lower(get_controller()) . '/', 'release' => 'https://api.xyobd.com/' . camel2lower(get_module()) . '/' . camel2lower(get_controller()) . '/']
                    ];
                    echo json_encode(array_merge($document, \HuiYun\Component\Rpc::document($this)), JSON_UNESCAPED_UNICODE);
                    exit;
                } catch (\Exception $exception) {
                    exit($exception->getMessage());
                }
            } else {
                ob_get_clean();
                send_http_status(404);
                if (!empty($_SERVER['HTTP_USER_AGENT'])) {
                    $view = new View();
                    $view->display('/common/error_404');
                }
                exit();
            }
        } else {
//            if (!empty($_SERVER['HTTP_USER_AGENT'])) {
//                $view = new View();
//                $view->display('/common/error_404');
//                exit();
//            }
        }
        //实例化HproseHttpServer
        $methods = get_class_methods($this);
        foreach ($methods as $k => $m) {
            if ($m[0] === '_') {
                unset($methods[$k]);
            }
        }

        try {
            $server = new \Hprose\Http\Server();
            $server->addMethods($methods, $this);
            if (DEBUG || $this->debug) {
                $server->setDebugEnabled(true);
            }
            //Hprose设置
            $server->setCrossDomainEnabled($this->crossDomain);
            $server->setP3PEnabled($this->P3P);
            $server->setGetEnabled($this->get);
            //增加通讯加密过滤器
            $server->setFilter(new RpcOutputDecryptFilter('default'));
            $this->_init();
//            $server->onSendError = function ($error, \stdClass $context) {
//                send_http_status(500);
//                exit('发生错误');
//                throw new \Exception('发生错误:');
//            };
            //启动server
            $server->onAfterInvoke = function ($name, &$args, $byref, &$result, \stdClass $context) {
                $millisecond = get_millisecond();
                $millisecond = str_pad($millisecond, 3, '0', STR_PAD_RIGHT);
                $log = [
                    'date' => date('Y-m-d H:i:s.') . $millisecond,
                    'client' => get_client_ip(),
                    'gateway' => get_host() . '/' . camel2lower(get_module()) . '/' . camel2lower(get_controller()),
                    'method' => $name,
                    'params' => $args,
                    'result' => $result
                ];
                Log::instance()->publish('rpc_response', $log);
            };
            $server->start();
        } catch (\Exception $exception) {
            exit($exception->getMessage());
        }
    }

    protected function _init() {

    }

    public function _view() {
        return null;
    }

    /**
     * 获取结果
     * @param $result
     * @return Result
     */
    protected function result($result) {
        $result = new Result($result);
        $result->setPattern('arr');
        return $result;
    }

    /**
     * 获取输出器对象
     * @param string $pattern
     * @return Output
     */
    protected function output($pattern = 'arr') {
        if (!$this->_output) {
            $this->_output = new Output();
        }
        if (is_array($pattern)) {
            $this->_output->setPattern('arr');
            if (isset($pattern['errCode']) && isset($pattern['message']) && isset($pattern['data'])) {
                return $this->_output->output($pattern['errCode'], $pattern['message'], $pattern['data']);
            } else {
                return $this->_output->success($pattern);
            }
        } else {
            $this->_output->setPattern($pattern);
        }
        return $this->_output;
    }
}

class RpcOutputDecryptFilter implements \Hprose\Filter {
    protected $client;

    public function __construct($client) {
        $this->client = $client;
    }

    public function inputFilter($data, stdClass $context) {
        //客户端请求的数据
        return $data;
    }

    public function outputFilter($data, stdClass $context) {
        //加密向客户端发送的数据
        return \HuiYun\Component\Rpc::instance()->rsaEncrype($data, $this->client);
    }

}