<?php

namespace Scf\Mode\Cli;

use JetBrains\PhpStorm\NoReturn;
use Scf\Command\Manager;
use Scf\Core\Console;

class Controller {
    protected array $_params = [];

    protected function _init(): void {
        $longopt = [
            'env:',
            'master:',
            'port:'
        ];
        $params = getopt('', $longopt);
        if ($params && str_contains($params, '#')) {
            $arr = explode('#', $params);
            foreach ($arr as $item) {
                if (str_contains($item, '=')) {
                    $p = explode('=', $item);
                    $this->_params[$p[0]] = $p[1];
                }
            }
        }
        ob_end_clean();
        ini_set("memory_limit", "-1");
        ignore_user_abort(true);
        set_time_limit(0);
    }

    /**
     * 输出消息到控制台
     * @param $msg
     */
    protected function print($msg): void {
        Console::write("------------------------------------------------【" . date('H:i:s', time()) . "】------------------------------------------------\n" . $msg . "\n------------------------------------------------------------------------------------------------------------");
    }

    protected function getCliArgs(int $offset = 2): array {
        return array_values(array_slice(Manager::instance()->getArgs(), $offset));
    }

    protected function getCliArg(int $index = 0, mixed $default = null, int $offset = 2): mixed {
        $args = $this->getCliArgs($offset);
        return $args[$index] ?? $default;
    }

    #[NoReturn] protected function exit(): void {
        $this->print('bye!');
        exit;
    }

} 
