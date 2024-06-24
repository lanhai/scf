<?php
/**
 * Created by Cli command.
 * User: System
 * Date: {date}
 */
namespace App\{ModuleName};

use HuiYun\Component\Debug;

class Module extends \HuiYun\Mode\Web\Module {

    public function run() {
        Debug::disable();
        parent::run();
    }

}