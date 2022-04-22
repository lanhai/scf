<?php

namespace Scf\Core;

use JetBrains\PhpStorm\NoReturn;
use Scf\Command\Color;
use Scf\Helper\JsonHelper;
use Scf\Server\Http;
use Scf\Server\Table\Runtime;
use Scf\Util\Time;

/**
 * 带推送功能的控制台打印工具
 */
class Console {
    protected static bool $enablePush = false;
    private static string $subscribersTableKey = 'log_subscribers';

    /**
     * 设置服务器对象,此方法必须放在Config:init()后执行,否则读取不到配置文件
     * @return void
     */
    public static function enablePush($status = true) {
        self::$enablePush = $status;
        if (Runtime::instance()->get(self::$subscribersTableKey)) {
            Runtime::instance()->delete(self::$subscribersTableKey);
        }
    }

    /**
     * 订阅日志推送
     * @param $fd
     * @return bool
     */
    public static function subscribe($fd): bool {
        $subscribers = Runtime::instance()->get(self::$subscribersTableKey) ?: [];
        if (!in_array($fd, $subscribers)) {
            $subscribers[] = $fd;
            return Runtime::instance()->set(self::$subscribersTableKey, $subscribers);
        }
        return true;
    }

    /**
     * 取消订阅
     * @param $fd
     * @return bool
     */
    public static function unsubscribe($fd): bool {
        $subscribers = Runtime::instance()->get(self::$subscribersTableKey) ?: [];
        if ($subscribers) {
            foreach ($subscribers as $key => $subscriber) {
                if ($fd == $subscriber) {
                    unset($subscribers[$key]);
                }
            }
            return Runtime::instance()->set(self::$subscribersTableKey, $subscribers);
        }
        return true;
    }

    /**
     * 判断是否存在日志监听
     * @return bool
     */
    public static function hasSubscriber(): bool {
        $subscribers = Runtime::instance()->get(self::$subscribersTableKey) ?: [];
        return count($subscribers) > 0;
    }

    #[NoReturn]
    public static function exit() {
        self::write('bye!');
        exit;
    }

    /**
     * 接收控制台输入内容
     * @param $msg
     * @return string
     */
    public static function input($msg = null): string {
        !is_null($msg) and self::write($msg ?: '请输入内容');
        return self::receive();
    }

    /**
     * 接收控制台输入内容
     * @return string
     */
    protected static function receive(): string {
        $input = trim(fgets(STDIN));
        if ($input == 'exit' || $input == 'quit') {
            self::exit();
        }
        return $input;
    }

    /**
     * 输出一行横线到控制台
     * @return void
     */
    public static function line($len = 60) {
        self::write(str_repeat('-', $len + 1));
    }

    public static function write($str) {
        $str = $str . "\n";
        fwrite(STDOUT, $str);
    }

    /**
     * 推送日志到控制台
     * @param $message
     * @return void
     */
    public static function push($message) {
        self::log($message);
    }

    /**
     * 打印错误信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function error(string $str, bool $push = true) {
        self::log(Color::error($str), $push);
    }

    /**
     * 打印成功信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function success(string $str, bool $push = true) {
        self::log(Color::green($str), $push);
    }

    /**
     * 打印警告信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function warning(string $str, bool $push = true) {
        self::log(Color::warning($str), $push);
    }

    /**
     * 向控制台输出消息
     * @param string $str
     * @param bool $push
     */
    public static function log(string $str, bool $push = true) {
        if ($push) {
            $subscribers = Runtime::instance()->get(self::$subscribersTableKey) ?: [];
            if ($subscribers && self::$enablePush) {
                foreach ($subscribers as $subscriber) {
                    try {
                        Http::instance()->push($subscriber, $str);
                    } catch (Exception $exception) {
                        self::log('向socket客户端推送失败:' . $exception->getMessage(), false);
                    }
                }
            }
        }
        $str = date('m-d H:i:s') . "." . substr(Time::millisecond(), -3) . " " . $str . "\n";// . " 内存占用:" . Thread::memoryUseage()
        fwrite(STDOUT, $str);
        flush();
    }
}