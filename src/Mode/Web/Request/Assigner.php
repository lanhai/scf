<?php

namespace Scf\Mode\Web\Request;

use Scf\Core\Traits\ComponentTrait;
use Scf\Mode\Web\Request;

class Assigner {
    use ComponentTrait;

    protected string $method = 'get';
    protected array $requiredParams = [];

    public function assign(&...$vars) {
        return call_user_func_array([$this, $this->method], $vars);
    }

    /**
     * 将获取参数打包
     * @param $data
     * @return false|mixed
     */
    public function pack(&$data = null): mixed {
        $data = call_user_func([$this, $this->method]);
        return $data;
    }

    public function require($map): static {
        $this->requiredParams = $map;
        return $this;
    }

    /**
     * @param string $method
     * @return $this
     */
    public function setMedhod(string $method = 'get'): static {
        $this->method = $method;
        return $this;
    }

    /**
     * @param ...$vars
     * @return array
     */
    protected function all(&...$vars): array {
        $data = [];
        $index = 0;
        if (!$this->_config) return $vars[0] = Request::instance()->_all();
        foreach ($this->_config as $key => $default) {
            if (is_numeric($key)) {
                $key = $default;
                $default = null;
            }
            $data[$key] = Request::instance()->_all($key, $default);
            $vars[$index] = $data[$key];
            $index++;
        }
        return $data;
    }

    /**
     * @param ...$vars
     * @return array
     */
    protected function get(&...$vars): array {
        $data = [];
        $index = 0;
        if (!$this->_config) return $vars[0] = Request::instance()->_get();
        foreach ($this->_config as $key => $default) {
            if (is_numeric($key)) {
                $key = $default;
                $default = null;
            }
            $data[$key] = Request::instance()->_get($key, $default);
            $vars[$index] = $data[$key];
            $index++;
        }
        return $data;
    }

    /**
     * @param ...$vars
     * @return array
     */
    protected function post(&...$vars): array {
        $data = [];
        $index = 0;
        if (!$this->_config) return $vars[0] = Request::instance()->_post();
        foreach ($this->_config as $key => $default) {
            if (is_numeric($key)) {
                $key = $default;
                $default = null;
            }
            $data[$key] = Request::instance()->_post($key, $default);
            $vars[$index] = $data[$key];
            $index++;
        }
        return $data;
    }

}