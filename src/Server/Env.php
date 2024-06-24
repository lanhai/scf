<?php

namespace Scf\Server;

use Scf\Core\App;
use Swoole\Coroutine\System;
use Swoole\Event;
use function Co\run;

class Env {
    /**
     * @return bool 是否开发环境
     */
    public static function isDev(): bool {
        return APP_RUN_ENV == 'dev';
    }

    /**
     * 判断是否在docker容器中运行
     * @return bool
     */
    public static function inDocker(): bool {
        return SERVER_ENV == 'docker';
    }

    /**
     * 获取内网IP
     * @return ?string
     */
    public static function getIntranetIp(): ?string {
        $intranetIp = null;
        foreach (swoole_get_local_ip() as $ip) {
            $intranetIp = $ip;
            break;
        }
        if (!self::inDocker()) {
            return $intranetIp;
        }
        run(function () use (&$intranetIp) {
            //$osName = trim(System::exec('echo ${OS_NAME}')['output']);
            $hostIP = trim(System::exec('echo ${HOST_IP}')['output']);
            $mode = trim(System::exec('echo ${NETWORK_MODE}')['output']);
            if ($mode == NETWORK_MODE_GROUP) {
                $intranetIp = $hostIP ?: App::getServerIp()['intranet'];
            }
        });
        Event::wait();
        return $intranetIp;
    }
}