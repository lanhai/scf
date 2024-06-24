<?php


namespace Scf\Server\Trigger;


use Scf\Core\Log;

class DefaultTrigger implements ITigger {

    public function error($msg, int $errorCode = E_USER_ERROR, Location $location = null): void {
        if ($location == null) {
            $location = new Location();
            $debugTrace = debug_backtrace();
            $caller = array_shift($debugTrace);
            $location->setLine($caller['line']);
            $location->setFile($caller['file']);
        }
        Log::instance()->error("{$msg} at file:{$location->getFile()} line:{$location->getLine()}", $this->errorMapLogLevel($errorCode), 'trigger');
    }

    public function throwable(\Throwable $throwable): void {
        $msg = "{$throwable->getMessage()} at file:{$throwable->getFile()} line:{$throwable->getLine()}";
        Log::instance()->error($msg, 500, 'trigger');
    }

    private function errorMapLogLevel(int $errorCode): string {
        return match ($errorCode) {
            E_PARSE, E_ERROR, E_CORE_ERROR, E_COMPILE_ERROR, E_USER_ERROR => 'error',
            E_WARNING, E_USER_WARNING, E_COMPILE_WARNING, E_RECOVERABLE_ERROR => 'warning',
            E_NOTICE, E_USER_NOTICE, E_STRICT, E_DEPRECATED, E_USER_DEPRECATED => 'notice',
            default => 'info',
        };
    }
}