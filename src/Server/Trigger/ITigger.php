<?php


namespace Scf\Server\Trigger;


interface ITigger
{
    public function error($msg,int $errorCode = E_USER_ERROR,Location $location = null);
    public function throwable(\Throwable $throwable);
}