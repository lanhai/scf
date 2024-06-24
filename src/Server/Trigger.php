<?php
/**
 * Created by PhpStorm.
 * User: yf
 * Date: 2018/8/14
 * Time: 下午6:17
 */

namespace Scf\Server;


use Scf\Server\Trigger\DefaultTrigger;
use Scf\Server\Trigger\Location;
use Scf\Server\Trigger\ITigger;
use Scf\Core\Traits\Singleton;

class Trigger {
    use Singleton;

    private $trigger;

    private $onError;
    private $onException;

    function __construct(?ITigger $trigger = null) {
        if ($trigger == null) {
            $trigger = new DefaultTrigger();
        }
        $this->trigger = $trigger;
        $this->onError = new Event();
        $this->onException = new Event();
    }

    public function error($msg, int $errorCode = E_USER_ERROR, Location $location = null) {
        if ($location == null) {
            $location = $this->getLocation();
        }
        $this->trigger->error($msg, $errorCode, $location);
        $all = $this->onError->all();
        foreach ($all as $call) {
            call_user_func($call, $msg, $errorCode, $location);
        }
    }

    public function throwable(\Throwable $throwable) {
        $this->trigger->throwable($throwable);
        $all = $this->onException->all();
        foreach ($all as $call) {
            call_user_func($call, $throwable);
        }
    }

    public function onError(): Event {
        return $this->onError;
    }

    public function onException(): Event {
        return $this->onException;
    }

    private function getLocation(): Location {
        $location = new Location();
        $debugTrace = debug_backtrace();
        array_shift($debugTrace);
        $caller = array_shift($debugTrace);
        $location->setLine($caller['line']);
        $location->setFile($caller['file']);
        return $location;
    }
}