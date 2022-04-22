<?php

namespace Scf\Mode\Web;

use Scf\Core\App;
use Scf\Core\CoroutineInstance;
use Scf\Util\Time;

class Thread extends App {
    use CoroutineInstance;

    /**
     * @var int 开始运行时间
     */
    protected int $_STARTED;
    /**
     * @var int 结束运行时间
     */
    protected int $_ENDED;

    /**
     * 线程开始运行时间
     * @return int
     */
    public function start(): int {
        $this->_STARTED = Time::millisecond();
        return $this->_STARTED;
    }

    /**
     * 线程运行耗时(ms)
     * @return int
     */
    public function consume(): int {
        $this->_ENDED = Time::millisecond();
        return $this->_ENDED - $this->_STARTED;
    }
}

