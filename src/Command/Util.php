<?php

namespace Scf\Command;


use Scf\Core\Console;

class Util {
    public static function fo() {
        return <<<LOGO
////////////////////////////////////////////////////////////////////
//                          _ooOoo_                               //
//                         o8888888o                              //
//                         88" . "88                              //
//                         (| ^_^ |)                              //
//                         O\  =  /O                              //
//                      ____/`---'\____                           //
//                    .'  \\|     |//  `.                         //
//                   /  \\|||  :  |||//  \                        //
//                  /  _||||| -:- |||||-  \                       //
//                  |   | \\\  -  /// |   |                       //
//                  | \_|  ''\---/''  |   |                       //
//                  \  .-\__  `-`  ___/-. /                       //
//                ___`. .'  /--.--\  `. . ___                     //
//            \  \ `-.   \_ __\ /__ _/   .-` /  /                 //
//      ========`-.____`-.___\_____/___.-`____.-'========         //
//                           `=---='                              //
//      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        //
//      佛祖保佑                永无BUG               永不宕机        //
////////////////////////////////////////////////////////////////////
LOGO;
    }

    public static function logo() {
        $version = Color::green('v1.0.1');
        return <<<LOGO
****************************************
*     ______      ______    _______    *
*   /  _____|    /  ____|  |  _____|   *
*  |  |____     /  /       | |_____    *
*   \_____ \   |  |        |  _____|   *
*   ______| |   \  \____   | |         *
*  |_______/     \______|  |_|         *
****************************************
LOGO;
    }

    static function displayItem($name, $value): string {
        if ($value === true) {
            $value = 'true';
        } else if ($value === false) {
            $value = 'false';
        } else if ($value === null) {
            $value = 'null';
        } else if (is_array($value)) {
            $value = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }
        return "\e[32m" . str_pad($name, 30, ' ', STR_PAD_RIGHT) . "\e[34m" . $value . "\e[0m";
    }

    public static function releaseResource($source, $destination, $confirm = false): void {
        $filename = basename($destination);
        clearstatcache();
        $replace = true;
        if (is_file($destination)) {
            echo Color::danger("{$filename} 已经存在, 是否覆盖? [ Y / N (默认) ] : ");
            $answer = strtolower(trim(strtoupper(fgets(STDIN))));
            if (!in_array($answer, ['y', 'yes'])) {
                $replace = false;
            }
        }
        if ($replace) {
            if ($confirm) {
                echo Color::danger("是否释放文件 {$filename}? [ Y / N (默认) ] : ");
                $answer = strtolower(trim(strtoupper(fgets(STDIN))));
                if (!in_array($answer, ['y', 'yes'])) {
                    return;
                }
            }
            $result = File::copyFile($source, $destination);
            Console::log($destination . '创建' . ($result ? Color::green('成功') : Color::red('失败')));
        }
    }

    public static function opCacheClear() {
        if (function_exists('apc_clear_cache')) {
            apc_clear_cache();
        }
        if (function_exists('opcache_reset')) {
            opcache_reset();
        }
    }

}
