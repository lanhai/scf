<?php

namespace Scf\Core;

use DOMDocument;
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
    protected static $pushHandler = null;
    protected const STRUCTURED_FORMAT_MAX_BYTES = 1048576;

    protected static function currentTimestamp(): string {
        return date('m-d H:i:s') . "." . substr((string)Time::millisecond(), -3);
    }

    public static function timestamp(): string {
        return self::currentTimestamp();
    }

    protected static function shouldPushConsole(): bool {
        if (defined('PROXY_UPSTREAM_MODE') && PROXY_UPSTREAM_MODE === true) {
            return true;
        }
        if (defined('IS_GATEWAY_SUB_PROCESS') && IS_GATEWAY_SUB_PROCESS === true) {
            return true;
        }
        return defined('IS_HTTP_SERVER') && IS_HTTP_SERVER;
    }

    protected static function shouldGrayOldInstanceOutput(): bool {
        return defined('PROXY_UPSTREAM_MODE')
            && PROXY_UPSTREAM_MODE === true
            && Runtime::instance()->serverIsDraining();
    }

    protected static function oldProxyInstanceId(): string {
        $port = (int)(Runtime::instance()->httpPort() ?: 0);
        if ($port > 0) {
            return "U{$port}";
        }
        return defined('APP_NODE_ID') ? ('U' . APP_NODE_ID) : 'U';
    }

    protected static function applyTerminalGray(string $str): string {
        return "\033[90m{$str}\e[0m";
    }

    protected static function isGatewayMessage(string $str): bool {
        return str_starts_with(trim($str), '【Gateway】');
    }

    protected static function prettyStructuredMessage(string $str): string {
        if ($str === '' || strlen($str) > self::STRUCTURED_FORMAT_MAX_BYTES) {
            return $str;
        }

        $trimmed = trim($str);
        if ($trimmed === '') {
            return $str;
        }

        $formatted = self::formatStructuredPayload($trimmed);
        if ($formatted !== null) {
            return $formatted;
        }

        $lines = preg_split('/\r\n|\r|\n/', $str);
        if (!is_array($lines) || count($lines) === 0) {
            return $str;
        }

        $changed = false;
        foreach ($lines as $index => $line) {
            $formattedLine = self::prettyStructuredLine($line);
            if ($formattedLine !== $line) {
                $changed = true;
                $lines[$index] = $formattedLine;
            }
        }

        return $changed ? implode(PHP_EOL, $lines) : $str;
    }

    protected static function prettyStructuredLine(string $line): string {
        $trimmed = trim($line);
        if ($trimmed === '') {
            return $line;
        }

        $formatted = self::formatStructuredPayload($trimmed);
        if ($formatted !== null) {
            return $formatted;
        }

        $length = strlen($line);
        $positions = [];
        foreach (['{', '[', '<'] as $needle) {
            $offset = 0;
            while (($position = strpos($line, $needle, $offset)) !== false) {
                $positions[] = $position;
                $offset = $position + 1;
            }
        }

        $positions = array_values(array_unique($positions));
        sort($positions);
        foreach ($positions as $position) {
            if ($position <= 0 || $position >= $length) {
                continue;
            }

            $prefix = substr($line, 0, $position);
            if (!preg_match('/(?:[:：=]|=>)\s*$/u', $prefix)) {
                continue;
            }

            $payload = trim(substr($line, $position));
            $formatted = self::formatStructuredPayload($payload);
            if ($formatted !== null) {
                return rtrim($prefix) . PHP_EOL . $formatted;
            }
        }

        return $line;
    }

    protected static function formatStructuredPayload(string $payload): ?string {
        if ($payload === '' || strlen($payload) > self::STRUCTURED_FORMAT_MAX_BYTES) {
            return null;
        }

        $first = $payload[0] ?? '';
        if ($first === '{' || $first === '[') {
            return self::formatJsonPayload($payload);
        }
        if ($first === '<') {
            return self::formatXmlPayload($payload);
        }

        return null;
    }

    protected static function formatJsonPayload(string $payload): ?string {
        $decoded = json_decode($payload, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            return null;
        }

        $decoded = self::normalizeStructuredValue($decoded);
        return self::stringifyPrettyValue($decoded);
    }

    protected static function normalizeStructuredValue(mixed $value, int $depth = 0): mixed {
        if ($depth >= 6) {
            return $value;
        }

        if (is_array($value)) {
            foreach ($value as $key => $item) {
                $value[$key] = self::normalizeStructuredValue($item, $depth + 1);
            }
            return $value;
        }

        if (!is_string($value)) {
            return $value;
        }

        $trimmed = trim($value);
        if ($trimmed === '' || strlen($trimmed) > self::STRUCTURED_FORMAT_MAX_BYTES) {
            return $value;
        }

        $formatted = self::formatStructuredPayload($trimmed);
        if ($formatted !== null) {
            return $formatted;
        }

        $embedded = self::formatEmbeddedStructuredString($trimmed);
        if ($embedded !== null) {
            return $embedded;
        }

        $decoded = self::decodeBase64StructuredPayload($trimmed);
        if ($decoded !== null) {
            return $decoded;
        }

        return $value;
    }

    protected static function formatEmbeddedStructuredString(string $value): ?string {
        if (!str_contains($value, '<') && !str_contains($value, '{') && !str_contains($value, '[')) {
            return null;
        }

        $formatted = self::prettyStructuredMessage($value);
        return $formatted !== $value ? $formatted : null;
    }

    protected static function stringifyPrettyValue(mixed $value, int $depth = 0): string {
        $indent = str_repeat(' ', $depth * 4);
        $childIndent = str_repeat(' ', ($depth + 1) * 4);

        if (is_array($value)) {
            if ($value === []) {
                return '[]';
            }

            $isAssociative = ArrayHelper::isAssociative($value);
            $lines = [$isAssociative ? '{' : '['];
            $lastIndex = count($value) - 1;
            $index = 0;
            foreach ($value as $key => $item) {
                $line = $childIndent;
                if ($isAssociative) {
                    $line .= self::jsonEncodeScalar((string)$key) . ': ';
                }
                $line .= self::stringifyPrettyValue($item, $depth + 1);
                if ($index < $lastIndex) {
                    $line .= ',';
                }
                $lines[] = $line;
                $index++;
            }
            $lines[] = $indent . ($isAssociative ? '}' : ']');
            return implode(PHP_EOL, $lines);
        }

        if (is_string($value) && self::isReadableBlockString($value)) {
            return self::stringifyBlockString($value, $depth);
        }

        return self::jsonEncodeScalar($value);
    }

    protected static function jsonEncodeScalar(mixed $value): string {
        $encoded = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        return is_string($encoded) ? $encoded : 'null';
    }

    protected static function isReadableBlockString(string $value): bool {
        return str_contains($value, PHP_EOL)
            && (
                preg_match('/(^|\n)\s*<[A-Za-z!?]/', $value) === 1
                || preg_match('/(^|\n)\s*[\[{]/', $value) === 1
            );
    }

    protected static function stringifyBlockString(string $value, int $depth): string {
        $indent = str_repeat(' ', $depth * 4);
        $childIndent = str_repeat(' ', ($depth + 1) * 4);
        $lines = preg_split('/\r\n|\r|\n/', $value);
        if (!is_array($lines)) {
            $lines = [$value];
        }

        $output = ['"""'];
        foreach ($lines as $line) {
            $output[] = $childIndent . $line;
        }
        $output[] = $indent . '"""';
        return implode(PHP_EOL, $output);
    }

    protected static function decodeBase64StructuredPayload(string $value): ?string {
        if (strlen($value) < 16 || strlen($value) > self::STRUCTURED_FORMAT_MAX_BYTES) {
            return null;
        }
        if (!preg_match('/^[A-Za-z0-9+\/]+={0,2}$/', $value) || strlen($value) % 4 !== 0) {
            return null;
        }

        $decoded = base64_decode($value, true);
        if (!is_string($decoded)) {
            return null;
        }

        $decoded = trim($decoded);
        if ($decoded === '' || !preg_match('//u', $decoded)) {
            return null;
        }

        return self::formatStructuredPayload($decoded);
    }

    protected static function formatXmlPayload(string $payload): ?string {
        $previous = libxml_use_internal_errors(true);
        libxml_clear_errors();

        $dom = new DOMDocument('1.0', 'UTF-8');
        $dom->preserveWhiteSpace = false;
        $dom->formatOutput = true;
        $loaded = $dom->loadXML($payload, LIBXML_NONET);
        $errors = libxml_get_errors();
        libxml_clear_errors();
        libxml_use_internal_errors($previous);

        if (!$loaded || $errors) {
            return null;
        }

        $xml = $dom->saveXML($dom->documentElement);
        return is_string($xml) ? trim($xml) : null;
    }

    /**
     * 开启日志推送
     * @param int $status
     * @return void
     */
    public static function enablePush(int $status = 1): void {
        Runtime::instance()->set(self::$enablePushKey, $status);
    }

    public static function setPushHandler(?callable $handler): void {
        self::$pushHandler = $handler;
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
        self::log($str, $push, 'red');
    }

    /**
     * 打印成功信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function success(string $str, bool $push = true): void {
        self::log($str, $push, 'green');
    }

    /**
     * 打印警告信息
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function warning(string $str, bool $push = true): void {
        self::log($str, $push, 'brown');
    }

    /**
     * @param string $str
     * @param bool $push
     * @return void
     */
    public static function info(string $str, bool $push = true): void {
        self::log($str, $push, 'cyan');
    }

    /**
     * 向控制台输出消息
     * @param string $str
     * @param bool $push
     * @param null $color
     */
    public static function log(string $str, bool $push = true, $color = null): void {
        $timestamp = self::currentTimestamp();
        if ($push && self::shouldPushConsole() && defined('APP_ID') && Runtime::instance()->get(self::$enablePushKey) == STATUS_ON) {
            try {
                $message = Log::filter($str);
                if (is_callable(self::$pushHandler)) {
                    (self::$pushHandler)($timestamp, $message);
                } else {
                    Http::instance()->pushConsoleLog($timestamp, $message);
                }
            } catch (Throwable $e) {
                Console::warning("控制台消息推送失败:" . $e->getMessage(), false);
            }
        }
        if (defined('ENV_MODE') && ENV_MODE == MODE_NATIVE) {
            $body = self::prettyStructuredMessage($str);
            $body = self::isGatewayMessage($body) ? Color::gateway($body) : $body;
            $str = $timestamp . Color::notice("【Server】") . $body . "\n";
        } else {
            if (self::shouldGrayOldInstanceOutput()) {
                $prefix = '#' . self::oldProxyInstanceId() . ' ';
                $body = self::applyTerminalGray($prefix . self::prettyStructuredMessage($str));
            } else {
                $body = self::prettyStructuredMessage($str);
            }
            if (!self::shouldGrayOldInstanceOutput() && self::isGatewayMessage($body)) {
                $body = Color::gateway($body);
            } elseif (!self::shouldGrayOldInstanceOutput() && $color) {
                $body = Color::$color($body);
            }
            $str = $timestamp . " " . $body . "\n";
        }
        echo $str;
        //fwrite(STDOUT, $str);
        flush();
    }
}
