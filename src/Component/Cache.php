<?php

namespace Scf\Component;

use Scf\Core\Component;
use Scf\Database\Exception\NullPool;
use Scf\Cache\Redis;

class Cache extends Component {

    protected Redis|NullPool $driver;
    protected array $_config = [
        'driver' => 'redis',
        'lifetime' => 10,
    ];

    public function _init(): void {
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
        return $this->driver->set($key, $value, $lifetime ?: $this->_config['lifetime']) ?? false;
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
    public function close(): void {
        $this->driver->close();
    }
}