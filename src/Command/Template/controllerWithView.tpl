<?php
/**
 * Created by Cli command.
 * User: System
 * Date: {date}
 */

namespace App\{ModuleName}\Controller;

class {ControllerName} extends Base {

    public function _init() {
         parent::_init();
    }

    public function actionIndex() {
         $this->display();
    }

    public function actionDebug(){
        $this->output()->success('ajax调试信息:当前时间:'.date('Y-m-d H:i:s',NOW_TIME));
    }

}
