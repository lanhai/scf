<?php

namespace Scf\Mode\Web;

use Scf\Core\CoroutineComponent;
use Scf\Database\Query;
use Scf\Mode\Web\Exception\AppException;
use Swoole\Coroutine;

abstract class ArComponent extends CoroutineComponent {
    protected static array $_AR_INSTANCE;

    /**
     * @param int $id
     * @return static
     */
    public static function lookup(int $id): static {
        $cid = Coroutine::getCid();
        if (!isset(static::$_AR_INSTANCE[$id . $cid])) {
            $cls = static::factory();
            if (!method_exists($cls, 'find')) {
                static::$_AR_INSTANCE[$id . $cid] = $cls;
            } else {
                static::$_AR_INSTANCE[$id . $cid] = $cls->find($id);
            }
            if ($cid > 0) {
                Coroutine::defer(function () use ($id, $cid) {
                    unset(static::$_AR_INSTANCE[$id . $cid]);
                });
            }
        }
        return static::$_AR_INSTANCE[$id . $cid];
    }

    abstract public function find($id): static;

    abstract public function ar();

    public function exist(): bool {
        return isset($this->_ar) && $this->_ar->exist();
    }
}