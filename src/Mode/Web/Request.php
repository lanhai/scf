<?php

namespace Scf\Mode\Web;

use Scf\Core\Config;
use Scf\Core\Traits\ProcessLifeSingleton;
use Scf\Helper\JsonHelper;
use Scf\Helper\ObjectHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Request\Assigner;
use Scf\Mode\Web\Request\Validator;
use Scf\Server\Worker\ProcessLife;
use Scf\Util\Arr;
use Swoole\Coroutine;

class Request {
    use ProcessLifeSingleton;

    const REQUIRED = '__REQUIRED__';
    const VALIDATOR = '__VALIDATOR__';
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
     * @return string
     */
    public static function id(): string {
        return ProcessLife::id();
    }

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

    public static function files(): array {
        return static::instance()->_files();
    }

    /**
     * @param array $map
     * @return Assigner
     */
    public static function all(array $map = []): Assigner {
        return Assigner::factory($map)->setMedhod('all');
    }

    public static function proto(): string|null {
        return Request::header('x-forwarded-proto') ?: 'http';
    }

    public static function host(): string|null {
        return self::header('host') ?: 'locahost';
    }

    public static function ip(): array|string {
        return self::header('x-real-ip') ?: self::server('remote_addr');
    }

    public static function url(): string|null {
        return self::proto() . '://' . self::host();
    }

    public static function uri(): string|null {
        return self::server('request_uri');
    }

    public static function raw(): string {
        return static::instance()->content();
    }

    /**
     * @return bool|string
     */
    public static function isRpc(): bool|string {
        $path = self::server('request_uri');
        if (!$path) {
            return false;
        }
        $arr = explode("/", $path);
        if (count($arr) < 5 || $arr[1] !== 'rpc.service') {
            return false;
        }
        return StringHelper::lower2camel($arr[2]) . "." . StringHelper::lower2camel($arr[3]) . "." . $arr[4];
    }

    /**
     * @param $key
     * @return array|string
     */
    public static function header($key = null): array|string {
        return static::instance()->_header($key);
    }

    /**
     * 来源
     * @return string|null
     */
    public static function referer(): string|null {
        return static::instance()->_header('referer') ?? null;
    }

    /**
     * @return bool
     */
    public static function inWechat(): bool {
        return str_contains(self::header('user-agent'), 'MicroMessenger');
    }

    /**
     * @return string
     */
    public static function path(): string {
        return static::instance()->_server('path_info') ?? '/';
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
     * @return bool
     */
    public static function isAjax(): bool {
        return static::instance()->_isAjax();
    }

    /**
     * @param string|null $key
     * @return array|mixed|string
     */
    public static function cookie(string $key = null): mixed {
        return static::instance()->_cookie($key);
    }

    /**
     * @param $path
     * @return string
     */
    public static function resetPath($path): string {
        return static::instance()->_resetPath($path);
    }

    /**
     * 获取path片段值
     * @param int $offset
     * @return mixed|null
     */
    public static function fragment(int $offset): mixed {
        return Router::instance()->getFragment($offset - 1);
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
     * 返回module name
     * @return string
     */
    public function _getModuleName(): string {
        return Router::instance()->getModule();
    }

    /**
     * 判断是否是POST请求
     * @return bool
     */
    public function _isPost(): bool {
        return strtoupper($this->server('request_method')) == 'POST';

    }

    public function _files(): array {
        return $this->_FILES;

    }

    /**
     * 读取cookie内容
     * @param string|null $key
     * @return array|mixed|string
     */
    public function _cookie(string $key = null): mixed {
        return $key ? $this->_COOKIE[$key] ?? '' : $this->_COOKIE;
    }

    protected function _valid($key, $val, $message): void {
        if ($message instanceof Validator) {
            $isValid = $message->isValid($val);
            $message = $message->getMessage();
        } else {
            $isValid = !is_null($val) && $val != '';
        }
        !$isValid and Response::interrupt(is_null($message) ? '参数:' . $key . '不能为空' : $message, 'PARAMS_NOT_VAILD', status: 200);
    }

    /**
     * 获取get参数
     * @param null $key
     * @param mixed $default
     * @return string|array|null
     */
    public function _get($key = null, mixed $default = null): null|string|array {
        if (is_null($key)) {
            return $this->filter($this->_GET);
        }
        $filter = null;
        if ($default instanceof Validator) {
            $val = $this->_GET[$key] ?? null;
            $filter = $default->getFilter() == Validator::_CODE ? $default->getFilter() : null;
            $this->_valid($key, $val, $default);
        }
        $input = !isset($this->_GET[$key]) || is_null($this->_GET[$key]) || $this->_GET[$key] == '' ? $default : $this->_GET[$key];
        return $this->filter($input, $filter);
    }

    /**
     * 获取post参数
     * @param null $key
     * @param mixed $default
     * @return string|array|null
     */
    public function _post($key = null, mixed $default = null): null|string|array {
        if (is_null($key)) {
            return $this->filter($this->_POST);
        }
        $filter = null;
        if ($default instanceof Validator) {
            $val = $this->_POST[$key] ?? null;
            $filter = $default->getFilter() == Validator::_CODE ? $default->getFilter() : null;
            $this->_valid($key, $val, $default);
        }
        $input = !isset($this->_POST[$key]) || is_null($this->_POST[$key]) || $this->_POST[$key] == '' ? $default : $this->_POST[$key];
        return $this->filter($input, $filter);
    }

    /**
     * 获取REQUEST参数
     * @param $key
     * @param mixed $default
     * @return string|array|null
     */
    public function _all($key = null, mixed $default = null): string|array|null {
        $request = [...$this->_GET, ...$this->_POST];
        if (is_null($key)) {
            return $this->filter($request);
        }
        $filter = null;
        if ($default instanceof Validator) {
            $val = $request[$key] ?? null;
            $filter = $default->getFilter() == Validator::_CODE ? $default->getFilter() : null;
            $this->_valid($key, $val, $default);
        }
        $input = !isset($request[$key]) || $request[$key] == '' ? $default : $request[$key];
        return $this->filter($input, $filter);
    }

    protected function filter($data, $filter = null): null|string|array {
        is_array($data) && array_walk_recursive($data, 'self::filter_exp');
        if ($data instanceof Validator || is_null($data) || is_bool($data)) {
            return null;
        }
        $filters = $filter ?? Config::get('default_filter');
        if ($filters) {
            if (is_string($filters)) {
                //代码白名单字段跳过过滤
                if ($filters === Validator::_CODE) {
                    return $data;
                }
                $filters = explode(',', $filters);
            } elseif (is_int($filters)) {
                $filters = [$filters];
            }
            foreach ($filters as $filter) {
                if (function_exists($filter)) {
                    $data = is_array($data) ? Arr::map($filter, $data) : $filter($data ?? ""); // 参数过滤
                } else {
                    $data = filter_var($data, is_int($filter) ? $filter : filter_id($filter));
                    if (false === $data) {
                        return null;
                    }
                }
            }
        }
        return $data;
    }

    /**
     * 过滤表单中的表达式
     * @param $value
     */
    protected function filter_exp(&$value): void {
        if (is_string($value) && in_array(strtolower($value), ['exp', 'or'])) {
            $value .= ' ';
        }
    }

    /**
     * @param string $path
     * @return string
     */
    public function _resetPath(string $path): string {
        return $this->_SERVER['path_info'] = $path;
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

    /**
     * 判断是否ajax请求
     * @return bool
     */
    public function _isAjax(): bool {
        return strtolower(($this->_SERVER["http_x_requested_with"] ?? '') == 'xmlhttprequest' || str_contains(strtolower($this->_HEADER['accept'] ?? ''), 'json') != false || str_contains(strtolower($this->_HEADER['content-type'] ?? ""), 'json') != false);
    }

    public function isHttpRequest(): bool {
        return isset($this->_request);
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
        return strtolower($contentType) == 'application/json';
    }
}