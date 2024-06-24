<?php
/**
 * Created by PhpStorm.
 * User: lanhai
 * Date: 2017/9/11
 * Time: 上午11:10
 */

namespace Scf\Core;

use JetBrains\PhpStorm\ArrayShape;
use ReflectionException;

class Output {
    protected array $_errCodes = [];
    protected array $_errCodesIndex = [];
    private static array $_data = [
        'errCode' => 0,
        'message' => '',
        'data' => [],
    ];

    /**
     * @throws ReflectionException
     */
    public function addErrCode($class): void {
        $refl = new \ReflectionClass($class);
        $this->_errCodes = array_merge($this->_errCodes, $refl->getConstants());
        if ($this->_errCodes) {
            $index = [];
            foreach ($this->_errCodes as $k => $v) {
                $index[md5($v)] = $k;
            }
            $this->_errCodesIndex = $index;
        }
    }

    public function showErrCode(): array {
        return $this->_errCodes;
    }

    /**
     * 错误返回
     * @param string $msg 提示信息 留空默认为系统错误
     * @param int|string $code 错误码
     * @param array|string $data 扩展数据
     * @return array
     */
    public function error(string $msg = '', int|string $code = 'SERVICE_ERROR', array|string $data = ''): array {
        $indexKey = md5($msg);
        if (!$msg) {
            return $this->output($code, '系统繁忙,请重试', $data);
        } else {
            if (is_array($msg) && !empty($msg['errCode'])) {
                $code = $msg['errCode'];
                $data = $msg['data'];
                $msg = $msg['message'];
            }
            if (isset($this->_errCodesIndex[$indexKey])) {
                return $this->output($this->_errCodesIndex[$indexKey], $msg, $data);
            }
            return $this->output($code, $msg, $data);
        }
    }

    /**
     * 成功返回
     * @param mixed $data
     * @return array
     */
    public function success(mixed $data = ''): array {
        return $this->output(0, 'success', $data);
    }

    /**
     * 标准格式化返回/输出
     * @param $errCode
     * @param $message
     * @param array|string $data
     * @return array
     */
    #[ArrayShape(['errCode' => "", 'message' => "", 'data' => "array|string", 'timestamp' => "int"])]
    protected function output($errCode, $message, array|string $data = ''): array {
        return [
            'errCode' => $errCode,
            'message' => $message,
            'data' => $data,
            'timestamp' => time()
        ];
    }
}