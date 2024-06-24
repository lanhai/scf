<?php

namespace Scf\Database;

use Scf\Core\Coroutine;
use Scf\Core\Traits\ProcessLifeSingleton;

class Overrider {
    use ProcessLifeSingleton;

    protected ?array $_config = null;

    public function __construct($cid) {
        $this->ancestorCid = $cid;
        if ($this->ancestorCid !== -1) {
            Coroutine::defer(function () {
                $this->_config = null;
            });
        }
    }

    public function __destruct() {
        $this->_config = null;
    }

    public static function set($config): ?array {
        return static::instance()->_set($config);
    }

    public static function get(): ?array {
        return static::instance()->_get();
    }

    private function _set($config): ?array {
        $this->_config = $config;
        return $this->_config;
    }

    private function _get(): ?array {
        return $this->_config;
    }
}