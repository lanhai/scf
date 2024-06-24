<?php
/**
 * Created by Cli command.
 * User: System
 * Date: {date}
 */

namespace App\{ModuleName}\Model;

use Common\Model\Curd;

class {ModelName} extends Curd {
    //protected $_dbName = '{ModuleName}';
    protected $_table = '{TableName}';

    public function doSomething(){
         return $this->_table;
    }
}