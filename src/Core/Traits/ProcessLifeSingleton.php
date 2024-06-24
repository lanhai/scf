<?php


namespace Scf\Core\Traits;

use Scf\Core\Context;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;

/**
 *顶级协程生命周期内的单例
 */
trait ProcessLifeSingleton {
    private static array $instance = [];
    private static array $parents = [];
    private array $parentsMap = [];
    private int $ancestorCid = 0;
    private ?Channel $channel = null;

    /**
     * 注意:在构造函数里不能进行任何会刷新缓冲期的操作,比如使用控制台打印(包含 flush)等操作,否则会导致当前实例自动销毁回收
     * @param $cid
     */
    public function __construct($cid) {
        $this->ancestorCid = $cid;
    }

    /**
     * @return static
     */
    public static function instance(): static {
        $cls = static::class;
        $instanceId = "_processing_instance_" . $cls;
        $cid = Coroutine::getCid();
        $pcid = Coroutine::getPcid();

        $outterCid = Context::get("outter_cid", $pcid > 0 ? $pcid : $cid, $pcid > 0 ? $pcid : $cid);
        !Context::has("outter_cid") and Context::set("outter_cid", $outterCid);
        $instance = Context::has($instanceId) ? Context::get($instanceId) : Context::get($instanceId, new static($outterCid), $pcid > 0 ? $pcid : $cid);
        !Context::has($instanceId) and Context::set($instanceId, $instance);
        return $instance;
    }

    /**
     * 获取请求祖先携程ID
     * @return int
     */
    public function getAncestorCid(): int {
        return $this->ancestorCid;
    }

    /**
     * @return bool
     */
    public function isAncestorThread(): bool {
        return $this->ancestorCid == Coroutine::getCid();
    }

//    public function __destruct() {
//        $cls = static::class;
//        Console::warning("{$cls} #{$this->ancestorCid} destruct");
//    }
}