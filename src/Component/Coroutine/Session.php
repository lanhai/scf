<?php

namespace Scf\Component\Coroutine;

use Scf\Core\Coroutine\Component;
use Scf\Cache\Redis;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Util\Sn;

class Session extends Component {

    protected array $_config = [
        'prefix' => null, // SESSION名称前缀
        'name' => null,
        'path' => null,
        'domain' => null,
        'expire' => null,
        'use_trans_sid' => null,
        'use_cookies' => null,
        'cache_limiter' => null,
        'cache_expire' => null,
        'auto_start' => true,
    ];
    protected string $_prefix = '';
    protected ?string $sessionId = null;
    protected string $_id = "";

    protected function _init(): void {
        $this->_prefix = is_null($this->_config['prefix']) ? '' : $this->_config['prefix'];
        $sessionId = Request::cookie('_SESSIONID_');
        if (!$sessionId) {
            $sessionId = Sn::create_uuid();
            Response::instance()->setCookie('_SESSIONID_', $sessionId);
        }
        $this->_id = $sessionId;
        $this->sessionId = '_SESSION_' . $sessionId;
    }

    /**
     * 读取
     * @param string|null $name
     * @return mixed
     */
    public function get(string $name = null): mixed {
        if ($this->_prefix) {
            $name = $this->_prefix . $name;
        }
        if (is_null($name)) {
            $result = Redis::pool()->hgetAll($this->sessionId);
            //$result = MasterDB::hgetAll($this->sessionId);
        } else {
            $result = Redis::pool()->hget($this->sessionId, $name);
            //$result = MasterDB::hget($this->sessionId, $name);
        }
        if ($result) {
            Redis::pool()->expire($this->sessionId, $this->_config['expire'] ?: 1800);
            //MasterDB::expire($this->sessionId, $this->_config['expire'] ?: 1800);
        }
        return $result;
    }

    /**
     * 赋值
     * @param string $name
     * @param $value
     * @return bool|int
     */
    public function set(string $name, $value): bool|int {
        if ($this->_prefix) {
            $name = $this->_prefix . $name;
        }
        $set = Redis::pool()->hset($this->sessionId, $name, $value);
        Redis::pool()->expire($this->sessionId, $this->_config['expire'] ?: 1800);
        //$set = MasterDB::hset($this->sessionId, $name, $value);
        //MasterDB::expire($this->sessionId, $this->_config['expire'] ?: 1800);
        return $set;
    }

    /**
     * 删除一个session,不支持.语法
     * @param string $name
     * @return bool|int
     */
    public function del(string $name): bool|int {
        if ($this->_prefix) {
            $name = $this->_prefix . $name;
        }
        return Redis::pool()->hdel($this->sessionId, $name);
        //return MasterDB::hdel($this->sessionId, $name);
    }

    /**
     * 清空session
     */
    public function clean(): bool {
        return Redis::pool()->delete($this->sessionId);
        //return MasterDB::delete($this->sessionId);
    }

    /**
     * 开启session
     */
    public function start(): void {
        session_start();
    }

    public function id(): ?string {
        return $this->_id;
    }
}
