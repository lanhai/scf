<?php

namespace Scf\Core;

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
        if (self::inDocker()) {
            $intranetIp = App::getServerIp()['intranet'];
        } else {
            foreach (swoole_get_local_ip() as $ip) {
                $intranetIp = $ip;
            }
        }
        return $intranetIp;
    }
}