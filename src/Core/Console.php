<?php

namespace Scf\Core;

use JetBrains\PhpStorm\NoReturn;
use Scf\Command\Color;
use Scf\Helper\ArrayHelper;
use Scf\Server\Http;
use Scf\Server\Table\Runtime;
use Scf\Util\Time;
use Swoole\Event;
use Swoole\Timer;
use function Laravel\Prompts\select;
use function Laravel\Prompts\text;

/**
 * 带推送功能的控制台打印
 */
class Console {
    protected static bool $enablePush = false;
    private static string $subscribersTableKey = 'log_subscribers';

    /**
     * 设置服务器对象,此方法必须放在Config:init()后执行,否则读取不到配置文件
     * @param bool $status
     * @return void
     */
    public static function enablePush(bool $status = true): void {
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
    public static function exit(): void {
        self::write('bye bye!');
        exit(0);
    }

    /**
     * 接收控制台输入内容
     * @param $label
     * @param string|null $default
     * @param bool $required
     * @param string|null $placeholder
     * @param string|null $hint
     * @return string
     */
    public static function input($label = null, ?string $default = null, bool $required = true, ?string $placeholder = null, ?string $hint = null): string {
        return text(
            label: $label ?: '请输入',
            placeholder: $placeholder ?: '',
            default: $default ?: '',
            required: $required,
            hint: $hint ?: ''
        );
    }
//    /**
//     * 接收控制台输入内容
//     * @param null $msg
//     * @param bool $break
//     * @return string
//     */
//    public static function input($msg = null, bool $break = true): string {
//        !is_null($msg) and self::write($msg ?: '请输入内容', $break);
//        return self::receive();
//    }
    public static function select($options = [], mixed $default = 0, int $start = 1, ?string $label = null): string {
        if (ArrayHelper::isAssociative($options)) {
            return select(
                label: $label ?: '请选择要执行的操作',
                options: $options,
                default: $default == 0 ? $options[0] : $default,
            );
        }
        $arr = [];
        foreach ($options as $k => $option) {
            if ($start > 0) {
                $arr[$k + $start] = $option;
            }
        }
        return select(
            label: $label ?: '请选择要执行的操作',
            options: $arr,
            default: $default
        );
    }

//    public static function select($options = []): string {
//        self::line();
//        foreach ($options as $k => $app) {
//            Console::write(($k + 1) . ':' . ($app['name'] ?? $app));
//        }
//        self::line();
//        return self::input('输入要进行的操作编号:', false);
//    }

    /**
     * 开始loading
     * @param $message
     * @param $callback
     * @return void
     */
    public static function startLoading($message, $callback): void {
        $i = 0;
        $tid = Timer::tick(100, function () use (&$i, $message) {
            $chars = ['-', '\\', '|', '/'];
            echo "\r$message " . $chars[$i++ % count($chars)];
            flush();
        });
        call_user_func($callback, ['tid' => $tid, 'len' => strlen($message) + 1]);
        //$callback($tid, strlen($message));
    }

    /**
     * 结束loading
     * @param array $timer
     * @return void
     */
    public static function endLoading(array $timer): void {
        Timer::clear($timer['tid']);
        echo "\r" . str_repeat(' ', $timer['len']) . "\r";
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
     * @param int $len
     * @return void
     */
    public static function line(int $len = 60): void {
        self::write(str_repeat('-', $len + 1));
    }

    public static function write($str, $break = true): void {
        $str = $str . ($break ? "\n" : "");
        fwrite(STDOUT, $str);
    }

    /**
     * 推送日志到控制台
     * @param $message
     * @return void
     */
    public static function push($message): void {
        self::log($message);
    }

    /**
     * 打印错误信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function error(string $str, bool $push = true): void {
        self::log(Color::red($str), $push);
    }

    /**
     * 打印成功信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function success(string $str, bool $push = true): void {
        self::log(Color::green($str), $push);
    }

    /**
     * 打印警告信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function warning(string $str, bool $push = true): void {
        self::log(Color::brown($str), $push);
    }

    /**
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function info(string $str, bool $push = true): void {
        self::log(Color::notice($str), $push);
    }

    /**
     * 向控制台输出消息
     * @param string $str
     * @param bool $push
     */
    public static function log(string $str, bool $push = true): void {
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