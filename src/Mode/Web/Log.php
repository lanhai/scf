<?php

namespace Scf\Mode\Web;

class Log extends \Scf\Core\Log {
    protected bool $debug = false;

    /**
     * 开启调试
     * @return void
     */
    public function enableDebug(): void {
        $this->debug = true;
    }

    /**
     * 检查调试信息是否开启
     * @return bool
     */
    public function isDebugEnable(): bool {
        return $this->debug;
    }

} 