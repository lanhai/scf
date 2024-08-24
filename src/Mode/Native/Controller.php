<?php

/**
 * 控制器
 */

namespace Scf\Mode\Native;


abstract class Controller {

    protected array $_config = [];

    public function __construct($config = []) {
        $this->_config = $config;
        $this->_init();
    }

    /**
     * 初始化
     */
    protected function _init() {

    }
}