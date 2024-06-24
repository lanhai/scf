<?php
/**
 * Created by Cli command.
 * User: System
 * Date: {date}
 */

namespace App\{ModuleName}\Controller;

use HuiYun\Mode\Web\Controller;

class Base extends Controller {

    /**
     * 初始化
     */
    public function _init() {

    }

    /**
     * 展示错误
     * @param $msg
     * @param int $code
     */
    protected function _error($msg, $code = 0) {
        if ($this->request()->isAjax() || isset($_GET['token'])) {
            $this->output()->error($msg, $code);
        } else {
            $this->setTplVar('error', "[" . $code . "]" . $msg);
            $this->display('common/error');
            exit;
        }
    }
}