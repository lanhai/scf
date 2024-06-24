<?php

namespace Scf\Core;

use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\Singleton;

abstract class Component {
    use ComponentTrait, Singleton;
}
