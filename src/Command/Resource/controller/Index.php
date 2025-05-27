<?php

namespace App\Demo\Controller;

use Scf\Core\Result;
use Scf\Mode\Web\Controller;


class Index extends Controller {

    public function actionIndex(): string {
        return Result::raw('hello world');
    }
}