<?php

namespace App\Demo\Controller;

use JetBrains\PhpStorm\Pure;
use Scf\Core\Result;
use Scf\Mode\Web\Controller;


class Index extends Controller {

    #[Pure] public function actionIndex() {
        return Result::raw('hello world');
    }
}