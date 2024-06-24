<?php


namespace Scf\Command\Handler;


use Scf\Core\Console;

class Base {
    protected array $config = [];

    protected function setConfig($k, $v) {
        $k = md5(static::class) . '_' . $k;
        $this->config[$k] = $v;
    }

    /**
     * 获取输入的值
     * @param $k
     * @return mixed|null
     */
    protected function getConfig($k): mixed {
        $k = md5(static::class) . '_' . $k;
        return $this->config[$k] ?? null;
    }

    protected function unsetConfig($k): bool {
        $k = md5(static::class) . '_' . $k;
        if (isset($this->config[$k])) {
            unset($this->config[$k]);
        }
        return true;
    }

    protected function resetConfig() {
        $this->config = [];
    }

    /**
     * 获取当前毫秒时间
     * @return float
     */
    protected function getMs(): float {
        list($t1, $t2) = explode(' ', microtime());
        return (float)sprintf('%.0f', (floatval($t1) + floatval($t2)) * 1000);
    }

    /**
     * 获取内存占用量
     * @return string
     */
    protected function getMemoryUseage(): string {
        return round(memory_get_usage() / 1024 / 1024, 2) . 'MB';
    }

    /**
     * 输出消息到控制台
     * @param $msg
     */
    protected function print($msg) {
       Console::write("------------------------------------------------【" . date('H:i:s', time()) . "】------------------------------------------------\n" . $msg . "\n------------------------------------------------------------------------------------------------------------");
    }

    protected function exit() {
        $this->print('bye!');
        exit;
    }

    /**
     * 接收控制台输入内容
     * @return string
     */
    protected function input(): string {
        return $this->receive();
    }

    /**
     * 接收控制台输入内容
     * @return string
     */
    protected function receive(): string {
        $input = trim(fgets(STDIN));
        if ($input == 'exit' || $input == 'quit') {
            $this->exit();
        }
        return $input;
    }
    /**
     * 写入文件
     * @param $file
     * @param $content
     * @return bool
     */
    protected function writeFile($file, $content): bool {
        try {
            $fp = fopen($file, "w");
        } catch (\Exception $err) {
            return false;
        }
        if ($fp) {
            fwrite($fp, $content);
            fclose($fp);
            return true;
        }
        return false;
    }
}