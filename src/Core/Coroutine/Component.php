<?php

namespace Scf\Core\Coroutine;

use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\CoroutineSingleton;

abstract class Component {
    use ComponentTrait, CoroutineSingleton;
}
