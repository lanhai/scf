<?php


namespace Scf\Component;

use Scf\Core\Coroutine\Component;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;

class Cookie extends Component {

    protected array $_config = [
        'prefix' => null, // cookie 名称前缀
        'expire' => null, // cookie 保存时间
        'path' => '/', // cookie 保存路径
        'domain' => null, // cookie 有效域名
        'secure' => null, // SSL模式
        'httponly' => null, // httponly设置
    ];
    protected string $_prefix;

    protected function _init(): void {
        $this->_prefix = is_null($this->_config['prefix']) ? '' : $this->_config['prefix'];
        if (!is_null($this->_config['expire'])) {
            $this->_config['expire'] = time() + $this->_config['expire'];
        }
        if (!empty($config['httponly'])) {
            ini_set("session.cookie_httponly", 1);
        }
    }

    /**
     * @param $name
     * @param $default
     * @return array|mixed|string|null
     */
    public function get($name = null, $default = null): mixed {
        $_COOKIE = Request::cookie() ?: [];
        if (is_null($name)) {
            $cookie = [];
            if ($this->_prefix) {
                $plen = strlen($this->_prefix);
                foreach ($_COOKIE as $k => $v) {
                    if (0 === stripos($k, $this->_prefix)) {
                        $k = substr($k, 0, $plen);
                        $cookie[$k] = $v;
                    }
                }
            } else {
                return $_COOKIE;
            }
        } else {
            $name = str_replace('.', '_', $name);
            return $this->_prefix ?
                ($_COOKIE[$this->_prefix . $name] ?? $default) :
                ($_COOKIE[$name] ?? $default);
        }
        return $default;
    }

    /**
     * @param $name
     * @param $value
     * @param array $options lifetime/domain/path
     * @return void
     */
    public function set($name, $value, array $options = []): void {
        $name = str_replace('.', '_', $name);
        if ($this->_prefix) {
            $this->_set($this->_prefix . $name, $value, $options);
        } else {
            $this->_set($name, $value, $options);
        }
    }

    public function del($name): void {
        $name = $this->_prefix . $name;
        $this->_del($name);
    }

    public function clean(): void {
        if ($this->_prefix) {
            foreach ($_COOKIE as $key => $val) {
                if (0 === stripos($key, $this->_prefix)) {
                    $this->del($key);
                }
            }
        } else {
            foreach ($_COOKIE as $key => $val) {
                $this->del($key);
            }
        }
    }

    public function _destroy(): void {
        foreach ($_COOKIE as $key => $val) {
            $this->del($key);
        }
    }

    protected function _del($key): void {
        Response::instance()->setCookie($key, '');
    }

    protected function _set($key, $value, $options = []): void {
        $options = array_merge($this->_config, $options);
        Response::instance()->setCookie($key, $value, $options);
    }

}
