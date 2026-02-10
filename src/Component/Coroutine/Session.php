<?php

namespace Scf\Component\Coroutine;

use RuntimeException;
use Scf\Core\Coroutine\Component;
use Scf\Cache\Redis;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Util\Sn;

class Session extends Component {

    protected array $_config = [
        'prefix' => null, // SESSION名称前缀
        'path' => null,
        'domain' => null,
        'expire' => null,
        'cookie_name' => '_CID_',
        'cookie_secure' => false,
        'cookie_httponly' => true,
        'cookie_samesite' => 'Lax',
    ];
    protected string $_prefix = '';
    protected ?string $sessionId = null;
    protected string $_id = "";

    protected function _init(): void {
        $this->_prefix = is_null($this->_config['prefix']) ? '' : $this->_config['prefix'];
        $cookieName = $this->_config['cookie_name'];
        $sessionId = Request::cookie($cookieName);
        if (!$sessionId) {
            $sessionId = Sn::create_uuid();
            Response::instance()->setCookie(
                $cookieName,
                $sessionId,
                $this->_config
            );
        }
        $this->_id = $sessionId;
        $this->sessionId = 'session:' . $sessionId;
    }

    /**
     * 读取
     * @param string|null $name
     * @return mixed
     */
    public function get(string $name = null): mixed {
        if (is_null($name)) {
            throw new RuntimeException('Session::get(null) is forbidden, use all() explicitly.');
        }
        if ($this->_prefix) {
            $name = $this->_prefix . $name;
        }
        $result = Redis::pool()->hget($this->sessionId, $name);
        if ($result && str_contains($name, 'LOGIN_UID')) {
            Redis::pool()->expire($this->sessionId, $this->_config['expire'] ?: 1800);
        }
        return $result;
    }

    public function all(): array {
        $result = Redis::pool()->hgetAll($this->sessionId);
        if ($result) {
            Redis::pool()->expire($this->sessionId, $this->_config['expire'] ?: 1800);
        }
        return $result ?: [];
    }

    /**
     * 赋值
     * @param string $name
     * @param $value
     * @return bool|int
     */
    public function set(string $name, $value): bool|int {
        if ($value === null) {
            return $this->del($name);
        }
        if ($this->_prefix) {
            $name = $this->_prefix . $name;
        }
        $set = Redis::pool()->hset($this->sessionId, $name, $value);
        Redis::pool()->expire($this->sessionId, $this->_config['expire'] ?: 1800);
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
    }

    /**
     * 清空session
     */
    public function clean(): bool {
        return Redis::pool()->delete($this->sessionId);
    }

    public function id(): ?string {
        return $this->_id;
    }
}
