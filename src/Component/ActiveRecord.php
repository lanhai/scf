<?php

namespace Scf\Component;

use JetBrains\PhpStorm\Pure;
use Scf\Core\Coroutine\Component;
use Scf\Helper\JsonHelper;
use Swoole\Coroutine;

abstract class ActiveRecord extends Component {
    protected static array $_AR_INSTANCE;

    /**
     * @param mixed ...$args
     * @return static
     */
    public static function lookup(...$args): static {
        $cid = Coroutine::getCid();
        $instanceKey = md5(get_called_class() . JsonHelper::toJson($args)) . $cid;
        if (!isset(static::$_AR_INSTANCE[$instanceKey])) {
            $cls = static::factory();
            if (!method_exists($cls, 'find')) {
                static::$_AR_INSTANCE[$instanceKey] = $cls;
            } else {
                if (method_exists(static::class, 'query')) {
                    static::$_AR_INSTANCE[$instanceKey] = $cls->query(...$args);
                } else {
                    static::$_AR_INSTANCE[$instanceKey] = $cls->find($args[0]);
                }
            }
            if ($cid > 0) {
                Coroutine::defer(function () use ($instanceKey, $cid) {
                    unset(static::$_AR_INSTANCE[$instanceKey]);
                });
            }
        }
        return static::$_AR_INSTANCE[$instanceKey];
    }

    abstract public function find($id): static;

    abstract public function ar();

    #[Pure] public function exist(): bool {
        return isset($this->_ar) && $this->_ar->exist();
    }
}