<?php

namespace Scf\Core;

use JetBrains\PhpStorm\ArrayShape;
use Monolog\Logger;
use Scf\Command\Color;
use Scf\Component\SocketMessager;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Exception\AppError;
use Scf\Runtime\MasterDB;
use Scf\Helper\StringHelper;
use Scf\Server\Table\Counter;
use Scf\Server\Table\LogTable;
use Scf\Util\Time;
use Throwable;


class Log {
    use RequestInstance;

    protected string $_module = '';
    protected array $_config = [
        'enable_save_to_table' => true,
        'enable_sql' => true,
        'enable_error' => true,
        'enable_message' => true,
        'enable_access' => true,
        'check_dir' => true,
        'error_level' => Logger::NOTICE, // 错误日志记录等级
    ];
    protected string $backupCounterKey = 'log_backup_start';
    protected string $idCounterKey = 'log_id';


    /**
     * @var int 日志ID
     */
    protected int $id = 0;

    private static array $_instances = [];

    public function __construct($module = '') {
        $this->_module = $module;
    }

    public static function filter($msg): string {
        $symbols = [
            '[0;36m',
            '[36;4m',
            '[0m',
            '[0;31m',
            '[0;32m',
            '[0;33m',
            '[0;34m',
            '[0;35m',
            '[0;36m',
            '[0;37m',
            '[1;33m'
        ];
        foreach ($symbols as $symbol) {
            $msg = str_replace($symbol, "", $msg);
        }
        return $msg;
    }

    /**
     * 保存错误记录
     * @param Throwable|string|AppError $msg
     * @param string $code
     */
    public function error(AppError|Throwable|string $msg, string $code = "SERVICE_ERROR") {
        if (is_string($msg)) {
            $backTrace = debug_backtrace();
            $error['error'] = $msg;
            $error['code'] = $code;
            $error['file'] = $backTrace[0]['file'] ?? '--';
            $error['line'] = $backTrace[0]['line'] ?? '--';
        }elseif ($msg instanceof AppError) {
            $backTrace = $msg->getTrace();
            $error['error'] = $msg->getMessage();
            $error['code'] = $msg->getCode();
            $error['file'] = $backTrace[0]['file'] ?? '--';
            $error['line'] = $backTrace[0]['line'] ?? '--';
        } else {
            $backTrace = $msg->getTrace();
            $error['error'] = $msg->getMessage();
            $error['code'] = $msg->getCode();
            $error['file'] = $msg->getFile();
            $error['line'] = $msg->getLine();
        }
        $error['time'] = date('Y-m-d H:i:s');
        $error['ip'] = SERVER_ALIAS . '@' . SERVER_HOST;
        //推送到控制台
        $log = ['message' => $error['error'], 'file' => $error['file'] . ':' . $error['line'], 'date' => (date('Y-m-d H:i:s') . '.' . substr(Time::millisecond(), -3)), 'module' => $this->_module, 'backtrace' => $backTrace, 'host' => SERVER_HOST, 'node_id' => SERVER_NODE_ID];
        if ($this->_config['enable_save_to_table']) {
            //存到节点内存等待转存
            $table = LogTable::instance();
            $logId = Counter::instance()->incr($this->idCounterKey);
            $table->set($logId, ['type' => 'error', 'log' => $log]);
        } else {
            //保存日志到Redis&日志文件
            MasterDB::addLog('error', $log);
        }
        //推送到控制台
        Console::push(Color::red($log['message'] . ' @ ' . $log['file']));
        //通知机器人
        SERVER_LOG_REPORT == 'on' and SocketMessager::instance()->publish('error', $error);
    }

    /**
     * 保存信息日志
     * @param string $msg
     */
    public function info(string $msg = '') {
        $backTrace = debug_backtrace();
        $m = [
            'file' => !empty($backTrace[0]['file']) ? $backTrace[0]['file'] : '--',
            'line' => !empty($backTrace[0]['line']) ? $backTrace[0]['line'] : '--',
        ];
        $log = ['message' => $msg, 'file' => $m['file'] . ':' . $m['line'], 'date' => (date('Y-m-d H:i:s') . '.' . substr(Time::millisecond(), -3)), 'module' => $this->_module, 'host' => SERVER_HOST, 'node_id' => SERVER_NODE_ID];
        if ($this->_config['enable_save_to_table']) {
            //存到节点内存等待转存
            $table = LogTable::instance();
            $logId = Counter::instance()->incr($this->idCounterKey);
            $table->set($logId, ['type' => 'info', 'log' => $log]);
        } else {
            //储存到master节点
            MasterDB::addLog('info', $log);
        }
        //推送到控制台
        Console::push(Color::notice(JsonHelper::toJson($msg)));
    }

    /**
     * 转存内存里的日志
     * @return int
     */
    public function backup(): int {
        if (!$this->tableCount() || !$this->_config['enable_save_to_table']) {
            return 0;
        }
        $start = Counter::instance()->get($this->backupCounterKey);
        $logId = Counter::instance()->get($this->idCounterKey);
        $table = LogTable::instance();
        $count = $table->count();
        for ($id = $start + 1; $id <= $logId; $id++) {
            $row = $table->get($id);
            MasterDB::addLog($row['type'], $row['log']);
            $table->delete($id);
        }
        Counter::instance()->set($this->backupCounterKey, $logId);
        return $count;
    }

    /**
     * 开启共享内存储存
     * @param bool $status
     * @return $this
     */
    public function enableTable(bool $status = true): static {
        $this->_config['enable_save_to_table'] = $status;
        return $this;
    }


    /**
     * 设置写入模块文件夹
     * @param $module
     * @return $this
     */
    public function setModule($module): static {
        $this->_module = $module;
        return $this;
    }

    /**
     * 统计待转存日志数量
     * @return int
     */
    public function tableCount(): int {
        if (!$this->_config['enable_save_to_table']) {
            return 0;
        }
        return LogTable::instance()->count();
    }


    /**
     * 统计REDIS日志数量
     * @param null $day
     * @return array
     */
    #[ArrayShape(['error' => "int", 'info' => "int"])]
    public function count($day = null): array {
        $day = $day ?: date('Y-m-d');
        return [
            'error' => MasterDB::countLog('error', $day),
            'info' => MasterDB::countLog('info', $day)
        ];
    }

    /**
     * 读取REDIS里储存的日志
     * @param string $type
     * @param null $day
     * @param int $length
     * @return array|bool
     */
    public function get(string $type = 'ERROR', $day = null, int $length = 1000): bool|array {
        $day = $day ?: date('Y-m-d');
        return MasterDB::getLog(strtolower($type), $day, 0, $length);
    }

    /**
     * 格式换日志信息
     * @param $code
     * @param $msg
     * @param $file
     * @param $line
     * @return mixed
     */
    public function format($code, $msg, $file, $line): mixed {
        return StringHelper::varsTransform('"{code}" "{message}" "{file}:{line}"', [
            '{code}' => intval($code),
            '{message}' => $msg,// str_replace('"', '\\"', $msg),
            '{file}' => $file,// str_replace('"', '\\"', $file),
            '{line}' => intval($line),
        ]);
    }
}