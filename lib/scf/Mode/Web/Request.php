<?php

namespace Scf\Mode\Web;

use Scf\Core\RequestInstance;
use Scf\Helper\JsonHelper;
use Scf\Helper\ObjectHelper;
use Scf\Mode\Web\Request\Verify;
use Scf\Mode\Web\Request\Assigner;
use Swoole\Coroutine;

class Request {
    use RequestInstance;

    const REQUIRED = '__REQUIRED__';
    const POST = 'POST';
    const GET = 'GET';

    /**
     * @var array 服务器环境信息
     */
    protected array $_SERVER = [];
    /**
     * @var array get参数
     */
    protected array $_GET = [];
    /**
     * @var array post 参数
     */
    protected array $_POST = [];
    /**
     * @var array cookie
     */
    protected array $_COOKIE = [];
    /**
     * @var array 文件
     */
    protected array $_FILES = [];
    /**
     * @var array header
     */
    protected array $_HEADER = [];
    /**
     * @var string
     */
    protected string $_CONTENT;
    /**
     * @var \Swoole\Http\Request
     */
    protected \Swoole\Http\Request $_request;

    protected int $cid = -1;

    /**
     * @param array $map
     * @return Assigner
     */
    public static function get(array $map = []): Assigner {
        return Assigner::factory($map)->setMedhod();
    }

    /**
     * @param array $map
     * @return Assigner
     */
    public static function post(array $map = []): Assigner {
        return Assigner::factory($map)->setMedhod('post');
    }

    /**
     * @param array $map
     * @return Assigner
     */
    public static function all(array $map = []): Assigner {
        return Assigner::factory($map)->setMedhod('all');
    }

    /**
     * @param $key
     * @return array|string
     */
    public static function header($key = null): array|string {
        return static::instance()->_header($key);
    }

    /**
     * @param $key
     * @return array|string
     */
    public static function server($key = null): array|string {
        return static::instance()->_server($key);
    }

    /**
     * @return string
     */
    public static function getActionName(): string {
        return static::instance()->_getActionName();
    }

    /**
     * 返回controller name
     * @return string
     */
    public static function getControllerName(): string {
        return static::instance()->_getControllerName();
    }

    /**
     * @return bool
     */
    public static function isPost(): bool {
        return static::instance()->_isPost();
    }

    /**
     * @param string|null $key
     * @return array|mixed|string
     */
    public static function cookie(string $key = null): mixed {
        return static::instance()->_cookie($key);
    }

    /**
     * 返回action name
     * @return string
     */
    public function _getActionName(): string {
        return Router::instance()->getAction();
    }

    /**
     * 返回controller name
     * @return string
     */
    public function _getControllerName(): string {
        return Router::instance()->getController();
    }

    /**
     * 判断是否是POST请求
     * @return bool
     */
    public function _isPost(): bool {
        return strtoupper($this->server('request_method')) == 'POST';

    }

    /**
     * 读取cookie内容
     * @param string|null $key
     * @return array|mixed|string
     */
    public function _cookie(string $key = null): mixed {
        return $key ? $this->_COOKIE[$key] ?? '' : $this->_COOKIE;
    }

    protected function _valid($key, $val, $message) {
        if ($message instanceof Verify) {
            $isValid = $message->isValid($val);
            $message = $message->getMessage();
        } else {
            $isValid = !is_null($val) && $val != '';
        }
        !$isValid and Response::interrupt(is_null($message) ? '参数:' . $key . '不能为空' : $message, 'PARAMS_NOT_VAILD');
    }

    /**
     * 获取get参数
     * @param null $key
     * @param mixed|null $default
     * @param mixed|null|Verify $message
     * @return string|array|null
     */
    public function _get($key = null, mixed $default = null, mixed $message = null): null|string|array {
        if (is_null($key)) {
            return $this->_GET;
        }
        if ($default == self::REQUIRED) {
            $val = $this->_GET[$key] ?? null;
            $this->_valid($key, $val, $message);
        }
//        if (empty($this->_GET[$key]) && $default == self::REQUIRED) {
//            Response::interrupt(is_null($message) ? '参数:' . $key . '不能为空' : $message);
//        }
        return !isset($this->_GET[$key]) || is_null($this->_GET[$key]) || $this->_GET[$key] == '' ? $default : $this->_GET[$key];
    }

    /**
     * 获取post参数
     * @param null $key
     * @param mixed|null $default
     * @param mixed|null|Verify $message
     * @return string|array|null
     */
    public function _post($key = null, mixed $default = null, mixed $message = null): null|string|array {
        if (is_null($key)) {
            return $this->_POST;
        }
        if ($default == self::REQUIRED) {
            $val = $this->_POST[$key] ?? null;
            $this->_valid($key, $val, $message);
        }
//        if (empty($this->_POST[$key]) && $default == self::REQUIRED) {
//            Response::interrupt(is_null($message) ? '参数:' . $key . '不能为空' : $message);
//        }
        return !isset($this->_POST[$key]) || is_null($this->_POST[$key]) || $this->_POST[$key] == '' ? $default : $this->_POST[$key];
    }

    /**
     * 获取REQUEST参数
     * @param $key
     * @param mixed|null $default
     * @param mixed|null $message
     * @return array|mixed|null
     */
    public function _all($key = null, mixed $default = null, mixed $message = null): mixed {
        $request = [...$this->_GET, ...$this->_POST];
        if (is_null($key)) {
            return $request;
        }
        if ($default == self::REQUIRED) {
            $val = $request[$key] ?? null;
            $this->_valid($key, $val, $message);
        }
//        if (empty($request[$key]) && $default == self::REQUIRED) {
//            Response::interrupt(is_null($message) ? '参数:' . $key . '不能为空' : $message);
//        }
        return !isset($request[$key]) || is_null($request[$key]) || $request[$key] == '' ? $default : $request[$key];
    }

    /**
     * 返回服务器参数信息
     * @param null $key
     * @return string|array
     */
    public function _server($key = null): string|array {
        return $key ? $this->_SERVER[$key] ?? '' : $this->_SERVER;
    }

    /**
     * 获取HTTP头信息
     * @param $key
     * @return string|array
     */
    public function _header($key = null): string|array {
        return $key ? $this->_HEADER[$key] ?? '' : $this->_HEADER;
    }

    /**
     * 获取POST的原始数据
     * @return string
     */
    public function content(): string {
        return $this->_CONTENT;
    }

    public function debug() {
        var_dump($this->_request);
    }

    /**
     * 判断是否ajax请求
     * @return bool
     */
    public function isAjax(): bool {
        return strtolower(($this->_SERVER["http_x_requested_with"] ?? '') == 'xmlhttprequest' || str_contains(strtolower($this->_HEADER['accept'] ?? ''), 'json') != false);
    }

    /**
     * @param \Swoole\Http\Request $request
     * @return void
     */
    public function register(\Swoole\Http\Request $request): void {
        $this->cid = Coroutine::getCid();
        $this->_request = $request;
        $request = ObjectHelper::toArray($request);
        $this->_COOKIE = $request['cookie'] ?: [];
        $this->_HEADER = $request['header'] ?: [];
        $this->_FILES = $request['files'] ?: [];
        $this->_SERVER = $request['server'] ?: [];
        $this->_GET = $request['get'] ?: [];
        $this->_CONTENT = $this->_request->getContent();
        $this->_POST = $this->isJsonContent() ? JsonHelper::is($this->_CONTENT) ? JsonHelper::recover($this->_CONTENT) : [] : ($request['post'] ?: []);
    }

    /**
     * 获得本次请求协程ID
     * @return int
     */
    public function getCid(): int {
        return $this->cid;
    }

    /**
     * 判断是否提交的json数据
     * @return bool
     */
    public function isJsonContent(): bool {
        $contentType = $this->_request->header['content-type'] ?? 'application/x-www-form-urlencoded';
        return $contentType == 'application/json';
    }
}