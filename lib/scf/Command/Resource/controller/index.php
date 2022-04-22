<?php

namespace App\Demo\Controller;

use Scf\Mode\Web\Controller;


class Index extends Controller {

    public function actionIndex() {
        $this->_response->write('hello world!');
    }
}