<?php

namespace Scf\Core;

use JetBrains\PhpStorm\NoReturn;
use Scf\Command\Color;
use Scf\Core\Table\Runtime;
use Scf\Core\Traits\Singleton;
use Scf\Helper\ArrayHelper;
use Scf\Server\Http;
use Scf\Util\Time;
use Swoole\Timer;
use Throwable;
use function Laravel\Prompts\confirm;
use function Laravel\Prompts\select;
use function Laravel\Prompts\text;

class Console {
    use Singleton;

    protected static string $enablePushKey = 'CONSOLE_LOG_PUSH_ENABLE';

    /**
     * 开启日志推送
     * @param int $status
     * @return void
     */
    public static function enablePush(int $status = 1): void {
        Runtime::instance()->set(self::$enablePushKey, $status);
    }


    #[NoReturn]
    public static function exit(): void {
        self::write('bye bye!');
        exit(0);
    }

    /**
     * @param string $label
     * @param bool $default
     * @param string $yes
     * @param string $no
     * @param mixed|null $required
     * @param string|null $hint
     * @return bool
     */
    public static function comfirm(string $label, bool $default = false, string $yes = "是", string $no = "否", mixed $required = null, ?string $hint = null): bool {
        return confirm(
            label: $label,
            default: $default,
            yes: $yes,
            no: $no,
            required: $required ?: false,
            hint: $hint ?: ''
        );
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

    /**
     * @param array $options
     * @param mixed $default 当start为0时默认值为第n个元素键值;为1时默认值为index:n
     * @param int $start 0:获取对应的键值;1:获取index
     * @param string|null $label
     * @param int $scroll
     * @return string|int
     */
    public static function select(array $options = [], mixed $default = 0, int $start = 1, ?string $label = null, int $scroll = 20): string|int {
        if (ArrayHelper::isAssociative($options)) {
            return select(
                label: $label ?: '请选择要执行的操作',
                options: $options,
                default: $default == 0 ? $options[0] : $default,
                scroll: $scroll
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
            options: $arr ?: $options,
            default: $start == 0 ? $options[$default] : $default,
            scroll: $scroll
        );
    }

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
        echo $str;
        //fwrite(STDOUT, $str);
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
        self::log(Color::cyan($str), $push);
    }

    /**
     * 向控制台输出消息
     * @param string $str
     * @param bool $push
     */
    public static function log(string $str, bool $push = true): void {
        if ($push && RUNNING_SERVER && defined('APP_ID') && Runtime::instance()->get(self::$enablePushKey) == STATUS_ON) {
            try {
                Http::instance()->pushConsoleLog(date('m-d H:i:s') . "." . substr((string)Time::millisecond(), -3), Log::filter($str));
            } catch (Throwable $e) {
                Console::warning("控制台消息推送失败:" . $e->getMessage(), false);
            }
        }
        if (defined('ENV_MODE') && ENV_MODE == MODE_NATIVE) {
            $str = date('m-d H:i:s') . "." . substr((string)Time::millisecond(), -3) . Color::notice("【Server】") . $str . "\n";
        } else {
            $str = date('m-d H:i:s') . "." . substr((string)Time::millisecond(), -3) . " " . $str . "\n";
        }
        echo $str;
        //fwrite(STDOUT, $str);
        flush();
    }
}