<?php


namespace Scf\Core;


use Swoole\Coroutine;

/**
 * 单次http请求生命周期内的单例
 */
trait RequestInstance {
    private static array $instance = [];

    /**
     * @return static
     */
    public static function instance(): static {
        $cid = Coroutine::getCid();
        return self::getRequestInstance($cid);
    }

    /**
     * @param int $cid
     * @return static
     */
    private static function getRequestInstance(int $cid = 0): static {
        $cid = $cid ?: Coroutine::getCid();
        $parentInstance = self::getParentInstance($cid);
        if ($parentInstance) {
            return $parentInstance;
        }
        return self::create();
    }

    private static function getParentInstance($cid) {
        $cls = get_called_class();
        $pcid = Coroutine::getPcid($cid);
        if ($pcid == -1) {
            return false;
        }
        if (isset(self::$instance[$pcid . $cls])) {
            return self::$instance[$pcid . $cls];
        }
        return false;
        //return self::getParentInstance($pcid);
    }

    /**
     * 创建一个协程日志记录器
     * @return static
     */
    public static function create(): static {
        $cid = Coroutine::getCid();
        $cls = get_called_class();
        if (!isset(self::$instance[$cid . $cls])) {
            self::$instance[$cid . $cls] = new static();
            if ($cid > 0) {
                Coroutine::defer(function () use ($cid, $cls) {
                    unset(self::$instance[$cid . $cls]);
                });
            }
        }
        return self::$instance[$cid . $cls];
    }

    public function destroy(int $cid = null) {
        if ($cid === null) {
            $cid = Coroutine::getCid();
        }
        $cls = get_called_class();
        unset(self::$instance[$cid . $cls]);
    }
}