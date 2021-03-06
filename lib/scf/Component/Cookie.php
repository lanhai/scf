<?php


namespace Scf\Component;

use Scf\Core\App;
use Scf\Core\Component;

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

    protected function _init() {
        $this->_prefix = is_null($this->_config['prefix']) ? '' : $this->_config['prefix'];
        if (!is_null($this->_config['expire'])) {
            $this->_config['expire'] = time() + $this->_config['expire'];
        }
        if (!empty($config['httponly'])) {
            ini_set("session.cookie_httponly", 1);
        }
    }

    public function get($name = null, $default = null) {
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
                (isset($_COOKIE[$this->_prefix . $name]) ? $_COOKIE[$this->_prefix . $name] : $default) :
                (isset($_COOKIE[$name]) ? $_COOKIE[$name] : $default);
        }

        return $default;
    }

    public function getPrivate($name) {
        $v = $this->get($name);
        if (!is_null($v)) {
            $v = Cipher::instance()->decrypt(base64_decode($v));
        }

        return $v;
    }

    public function set($name, $value, $options = []) {
        $name = str_replace('.', '_', $name);
        if ($this->_prefix) {
            $this->_set($this->_prefix . $name, $value, $options);
        } else {
            $this->_set($name, $value, $options);
        }
    }

    public function setPrivate($name, $value, $options = []) {
        $value = base64_encode(Cipher::instance()->encrypt($value));
        $this->set($name, $value, $options);
    }

    public function del($name) {
        $name = $this->_prefix . $name;
        $this->_del($name);
    }

    public function clean() {
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

    public function destroy() {
        foreach ($_COOKIE as $key => $val) {
            $this->del($key);
        }
    }

    protected function _del($key) {
        setcookie($key, '', NOW_TIME - 3600, $this->_config['path'], $this->_config['domain']);
        unset($_COOKIE[$key]);
    }

    protected function _set($key, $value, $options = []) {
        $options = array_merge($this->_config, $options);
        setcookie($key, $value, $options['expire'], $options['path'], $options['domain'], $options['secure']);
        $_COOKIE[$key] = $value;
    }

}
