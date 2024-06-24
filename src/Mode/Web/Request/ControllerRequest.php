<?php

namespace Scf\Mode\Web\Request;

use Scf\Core\Coroutine\Component;
use Scf\Mode\Web\Request;

class ControllerRequest extends Component {
    /**
     * 返回action name
     * @return string
     */
    public function getActionName(): string {
        return Request::instance()->_getActionName();
    }

    /**
     * 返回controller name
     * @return string
     */
    public function getControllerName(): string {
        return Request::instance()->_getControllerName();
    }

    /**
     * 判断是否是POST请求
     * @return bool
     */
    public function isPost(): bool {
        return Request::instance()->_isPost();
    }

    /**
     * 读取cookie内容
     * @param string|null $key
     * @return array|mixed|string
     */
    public function cookie(string $key = null): mixed {
        return Request::instance()->_cookie($key);
    }

    /**
     * @param $key
     * @return array|string
     */
    public function header($key = null): array|string {
        return Request::instance()->_header($key);
    }

    /**
     * @param $key
     * @return array|string
     */
    public function server($key = null): array|string {
        return Request::instance()->_server($key);
    }

    /**
     * @param $key
     * @param mixed|null $default
     * @param mixed|null $message
     * @return array|string|null
     */
    public function get($key = null, mixed $default = null, mixed $message = null): array|string|null {
        return Request::instance()->_get($key, $default, $message);
    }

    /**
     * @param $key
     * @param mixed|null $default
     * @param mixed|null $message
     * @return array|string|null
     */
    public function post($key = null, mixed $default = null, mixed $message = null): array|string|null {
        return Request::instance()->_post($key, $default, $message);
    }

    /**
     * @param $key
     * @param mixed|null $default
     * @param mixed|null $message
     * @return array|mixed|null
     */
    public function getAll($key = null, mixed $default = null, mixed $message = null): mixed {
        return Request::instance()->_all($key, $default, $message);
    }

}