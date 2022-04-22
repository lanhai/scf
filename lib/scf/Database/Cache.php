<?php

namespace Scf\Database;

use Scf\Core\Component;

class Cache extends Component {

    protected Redis $driver;
    protected array $_config = [
        'driver' => 'redis',
        'lifetime' => 10,
    ];

    public function _init() {
        parent::_init();
        $this->driver = Redis::pool();
    }

    /**
     * @param $key
     * @param $value
     * @param $lifetime
     * @return bool
     */
    public function set($key, $value, $lifetime = null): bool {
        return $this->driver->set($key, $value, $lifetime ?: $this->_config['lifetime']);
    }

    /**
     * @param $key
     * @return mixed
     */
    public function get($key): mixed {
        return $this->driver->get($key);
    }

    /**
     * @param $key
     * @return bool
     */
    public function delete($key): bool {
        return $this->driver->delete($key);
    }

    /**
     * @return void
     */
    public function close() {
        $this->driver->close();
    }
}