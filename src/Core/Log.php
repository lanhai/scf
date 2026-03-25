<?php

namespace Scf\Core;

use Monolog\Logger;
use Scf\Cache\Redis;
use Scf\Core\Table\Counter;
use Scf\Core\Table\LogTable;
use Scf\Core\Table\Runtime;
use Scf\Database\Exception\NullPool;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Exception\AppError;
use Scf\Server\Manager;
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\File;
use Scf\Util\Time;
use Swoole\Coroutine\System;
use Throwable;
use Scf\Cloud\Ali\Dingtalk;


class Log extends Component {

    protected string $_module = '';
    protected array $_config = [
        'enable_sql' => true,
        'enable_error' => true,
        'enable_message' => true,
        'enable_access' => true,
        'check_dir' => true,
        'error_level' => Logger::NOTICE, // 错误日志记录等级
        'dingtalk_enable_notify' => false,
        'dingtalk_client_id' => null,
        'dingtalk_secret' => null,
        'dingtalk_group_template' => null,
        'dingtalk_robot_code' => null,
        'dingtalk_group_id' => null,
        'dingtalk_uid' => null
    ];
    protected string $backupCounterKey = 'log_backup_start';
    protected string $idCounterKey = 'log_id';
    protected array $logTypes = ['info', 'error', 'slow', 'crontab'];
    protected ?Dingtalk $dingtalkMessager = null;

    /**
     * @var int 日志ID
     */
    protected int $id = 0;

    public function _init(): void {
        if ($this->_config['dingtalk_enable_notify']) {
            $this->dingtalkMessager = Dingtalk::instance();
            $this->dingtalkMessager->setting('client_id', $this->_config['dingtalk_client_id']);
            $this->dingtalkMessager->setting('secret', $this->_config['dingtalk_secret']);
            $this->dingtalkMessager->setting('group_template', $this->_config['dingtalk_group_template']);
            $this->dingtalkMessager->setting('robot_code', $this->_config['dingtalk_robot_code']);
            $pool = Redis::pool(Manager::instance()->getConfig('service_center_server') ?: 'main');
            if ($pool instanceof NullPool) {
                Console::warning('Redis服务不可用,无法启用日志钉钉推送', false);
            } else {
                $result = $this->dingtalkAuthCheck();
                if ($result->hasError()) {
                    Console::warning($result->getMessage(), false);
                }
            }
        }
    }

    public function dingtalkAuthCallback($data): Result {
        $pool = Redis::pool(Manager::instance()->getConfig('service_center_server') ?: 'main');
        $params = [
            'code' => $data['code'],
            'grantType' => 'authorization_code'
        ];
        $apiResponse = Dingtalk::instance()->queryUserAccessToken($params);
        if ($apiResponse->hasError()) {
            return Result::error("获取授权TOKEN失败:" . $apiResponse->getMessage());
        }
        $responseData = $apiResponse->getData();
        $userToken = $responseData['accessToken'];
        //查询UID
        $infoQuery = Dingtalk::instance()->queryUserInfo(token: $userToken);
        if ($infoQuery->hasError()) {
            return Result::error("获取用户信息失败:" . $infoQuery->getMessage());
        }
        $unionId = $infoQuery->getData('unionId');
        $userId = Dingtalk::instance()->queryUserId($unionId);
        if ($userId->hasError()) {
            return Result::error("获取userid失败:" . $userId->getMessage());
        }
        $this->_config['dingtalk_uid'] = $userId->getData('userid');
        $pool->set('_LOG_DINGTALK_NOTIFY_UID_', $this->_config['dingtalk_uid'], -1);
        return $this->dingtalkAuthCheck();
    }

    protected function dingtalkAuthCheck(): Result {
        $pool = Redis::pool(Manager::instance()->getConfig('service_center_server') ?: 'main');
        $dingtalk = Dingtalk::instance();
        $uid = $this->_config['dingtalk_uid'] ?: $pool->get('_LOG_DINGTALK_NOTIFY_UID_');
        if (!$uid) {
            $authUri = $dingtalk->getAuthUri(APP_ID);
            return Result::error("日志钉钉推送尚未完成授权:" . $authUri);
        } else {
            $groupId = $this->_config['dingtalk_group_id'] ?: $pool->get('_LOG_DINGTALK_NOTIFY_GROUP_ID_');
            if (!$groupId) {
                $createResult = $dingtalk->createGroup($uid);
                if ($createResult->hasError()) {
                    return Result::error("钉钉日志推送群创建失败:" . $createResult->getMessage());
                } else {
                    $groupId = $createResult->getData('open_conversation_id');
                    $pool->set('_LOG_DINGTALK_NOTIFY_GROUP_ID_', $groupId, -1);
                    $sendResult = $dingtalk->sendGroupMessage($groupId, 'inner_app_template_action_card', [
                        'title' => '日志钉钉推送服务已启用',
                        'markdown' => "欢迎使用钉钉通知服务"
                    ]);
                    if ($sendResult->hasError()) {
                        return Result::error("日志钉钉推送服务启用发送消息失败:" . $sendResult->getMessage());
                    }
                }
            }
            $this->_config['dingtalk_uid'] = $uid;
            $this->_config['dingtalk_group_id'] = $groupId;
            return Result::success();
        }
    }

    public static function filter($msg): string {
        $symbols = [
            '[0m',
            '[0;31m',
            '[0;32m',
            '[0;33m',
            '[0;34m',
            '[0;35m',
            '[0;36m',
            '[0;37m',
            '[1;33m',
            '[36;4m',
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
     * @param string|null $file
     * @param string|null $line
     */
    public function error(AppError|Throwable|string $msg, string $code = "SERVICE_ERROR", string $file = null, string $line = null): void {
        if (is_string($msg)) {
            $backTrace = debug_backtrace();
            $error['error'] = $msg;
            $error['code'] = $code;
            $error['file'] = $file ?: $backTrace[0]['file'] ?? '--';
            $error['line'] = $line ?: $backTrace[0]['line'] ?? '--';
        } elseif ($msg instanceof AppError) {
            $backTrace = $msg->getTrace();
            $error['error'] = $msg->getMessage();
            $error['code'] = $msg->getCode();
            $error['file'] = $file ?: $backTrace[0]['file'] ?? '--';
            $error['line'] = $line ?: $backTrace[0]['line'] ?? '--';
        } else {
            $backTrace = $msg->getTrace();
            $error['error'] = $msg->getMessage();
            $error['code'] = $msg->getCode();
            $error['file'] = $file ?: $msg->getFile();
            $error['line'] = $line ?: $msg->getLine();
        }
        //推送到控制台
        $log = [
            'message' => $error['error'],
            'file' => $error['file'] . ':' . $error['line'],
            'time' => date('Y-m-d H:i:s') . '.' . substr(Time::millisecond(), -3),
            'module' => $this->_module ?: 'Log',
            'backtrace' => $this->formatBackTrace($backTrace),
            'host' => SERVER_HOST,
            'code' => $error['code']
        ];
        //存到节点内存等待转存
        $table = LogTable::instance();
        $logId = Counter::instance()->incr($this->idCounterKey);
        if (IS_HTTP_SERVER && $table->count() < 200) {
            $table->set($logId, ['type' => 'error', 'log' => $log]);
        } else {
            $this->push('error', $log);
        }
        //推送到控制台
        Console::error($log['message'] . ' @ ' . $log['file']);
        //通知机器人
        if ($this->_config['dingtalk_enable_notify'] && $this->_config['dingtalk_group_id'] && Redis::pool()->lock('LOG:ERROR:' . md5($log['file']), 60)) {
            $notifyMsg = ['⚠️错误通知⚠️', 'app:' . (Env::isDev() ? 'dev' : 'prod') . '@' . APP_ID];
            foreach ($log as $k => $v) {
                if ($k == 'backtrace') {
                    continue;
                }
                $notifyMsg[] = $k . ':' . $v;
            }
            Dingtalk::instance()->sendGroupMessage($this->_config['dingtalk_group_id'], 'inner_app_template_action_card', [
                'title' => '⚠️错误通知⚠️',
                'markdown' => implode("\n\n", $notifyMsg),
            ]);
        }
//        try {
//            SERVER_LOG_REPORT == SWITCH_ON and SocketMessager::instance()->publish('error', $error);
//        } catch (\Exception $exception) {
//            Console::warning("机器人推送错误日志失败:" . $exception->getMessage(), false);
//        }
    }


    /**
     * 保存信息日志
     * @param string $msg
     * @param bool $report
     */
    public function info(string $msg = '', bool $report = false): void {
        $backTrace = debug_backtrace();
        $m = [
            'file' => !empty($backTrace[0]['file']) ? $backTrace[0]['file'] : '--',
            'line' => !empty($backTrace[0]['line']) ? $backTrace[0]['line'] : '--',
        ];
        $log = [
            'message' => $msg,
            'file' => $m['file'] . ':' . $m['line'],
            'time' => date('Y-m-d H:i:s') . '.' . substr(Time::millisecond(), -3),
            'module' => $this->_module,
            'host' => SERVER_HOST,
        ];
        //存到节点内存等待转存
        $table = LogTable::instance();
        if (IS_HTTP_SERVER && $table->count() < 200) {
            $logId = Counter::instance()->incr($this->idCounterKey);
            $table->set($logId, ['type' => 'info', 'log' => $log]);
        } else {
            $this->push('info', $log);
        }
        //推送到控制台
        Console::info($msg);
        //通知机器人
        if ($report) {
            $m['time'] = date('Y-m-d H:i:s');
            $m['ip'] = SERVER_HOST;
            $m['message'] = $msg;
            if ($this->_config['dingtalk_enable_notify'] && $this->_config['dingtalk_group_id'] && Redis::pool()->lock('LOG:ACCESS:' . md5($log['file']), 60)) {
                $notifyMsg = ['📖日志通知📖', 'app:' . (Env::isDev() ? 'dev' : 'prod') . '@' . APP_ID];
                foreach ($m as $k => $v) {
                    $notifyMsg[] = $k . ':' . $this->stringifyNotifyValue($v);
                }
                Dingtalk::instance()->sendGroupMessage($this->_config['dingtalk_group_id'], 'inner_app_template_action_card', [
                    'title' => '📖日志通知📖',
                    'markdown' => implode("\n\n", $notifyMsg),
                ]);
            }
//            try {
//                SERVER_LOG_REPORT == SWITCH_ON and SocketMessager::instance()->publish('access', $m);
//            } catch (\Exception $exception) {
//                Console::warning("机器人推送错误日志失败:" . $exception->getMessage(), false);
//            }
        }
    }

    /**
     * 慢日志
     * @param $message
     * @return void
     */
    public function slow($message): void {
        $log['host'] = SERVER_HOST;
        $log = [
            'message' => $message,
            'file' => $message['path'],
            'time' => date('Y-m-d H:i:s') . '.' . substr(Time::millisecond(), -3),
            'module' => 'Request',
            'host' => SERVER_HOST
        ];
        //存到节点内存等待转存
        $table = LogTable::instance();
        if (IS_HTTP_SERVER && $table->count() < 200) {
            $logId = Counter::instance()->incr($this->idCounterKey);
            $table->set($logId, ['type' => 'slow', 'log' => $log]);
        } else {
            $this->push('slow', $log);
        }
        //通知机器人
        if ($this->_config['dingtalk_enable_notify'] && $this->_config['dingtalk_group_id'] && Redis::pool()->lock('LOG:ERROR:' . md5($log['file']), 60)) {
            $notifyMsg = ['⚠️慢日志⚠️', 'app:' . (Env::isDev() ? 'dev' : 'prod') . '@' . APP_ID];
            foreach ($log as $k => $v) {
                $notifyMsg[] = $k . ':' . $this->stringifyNotifyValue($v);
            }
            Dingtalk::instance()->sendGroupMessage($this->_config['dingtalk_group_id'], 'inner_app_template_action_card', [
                'title' => '⚠️慢日志⚠️',
                'markdown' => implode("\n\n", $notifyMsg),
            ]);
        }
    }

    protected function stringifyNotifyValue(mixed $value): string {
        if (is_string($value)) {
            return $value;
        }
        $json = JsonHelper::toJson($value);
        if ($json !== false) {
            return $json;
        }
        return var_export($value, true);
    }

    /**
     * 排程日志
     * @param array $log
     * @return void
     */
    public function crontab(array $log): void {
        $log['time'] = date('Y-m-d H:i:s') . '.' . substr(Time::millisecond(), -3);
        $table = LogTable::instance();
        if (IS_HTTP_SERVER && $table->count() < 200) {
            $logId = Counter::instance()->incr($this->idCounterKey);
            $table->set($logId, ['type' => 'crontab', 'log' => $log]);
        } else {
            $this->push('crontab', $log);
        }

    }

    /**
     * 转存内存里的日志
     * @return int
     */
    public function backup(): int {
        //主节点转存子节点日志到本地
        if (App::isMaster()) {
            $masterDB = Redis::pool($this->_config['server'] ?? 'main');
            if ($masterDB instanceof NullPool) {
                return 0;
            }
            foreach ($this->logTypes as $type) {
                $logLength = min(50, $masterDB->lLength('_LOGS_' . $type));
                if (!$logLength) {
                    continue;
                }
                for ($i = 0; $i < $logLength; $i++) {
                    $log = $masterDB->rPop('_LOGS_' . strtolower($type));
                    if ($log && !$this->saveToFile($type, $log)) {
                        Console::error('日志写入失败:' . $log);
                        $masterDB->rPush('_LOGS_' . strtolower($type), $log);
                    }
                }
            }
        }
        if (!$this->tableCount()) {
            return 0;
        }
        //将内存表的日志转存到redis/推送到master节点
        $start = Counter::instance()->get($this->backupCounterKey);
        $maxLogId = Counter::instance()->get($this->idCounterKey);
        $table = LogTable::instance();
        $count = $table->count();
        $logId = $maxLogId;
        for ($id = $start + 1; $id <= $maxLogId; $id++) {
            $row = $table->get($id);
            if ($row) {
                if ($this->push($row['type'], $row['log']) !== false) {
                    $table->delete($id);
                    $logId = $id;
                } else {
                    break;
                }
            }
        }
        Counter::instance()->set($this->backupCounterKey, $logId);
        return $count;
    }

    /**
     * 记录日志
     * @param string $type
     * @param mixed $log
     * @return bool|int
     */
    public function push(string $type, mixed $log): bool|int {
        if (App::isMaster()) {
            return $this->saveToFile($type, [
                'day' => Date::today(),
                'host' => SERVER_HOST,
                'log' => $log
            ]);
        }
        try {
            //TODO 日志推送到master节点
            $masterDB = Redis::pool($this->_config['service_center_server'] ?? 'main');
            if ($masterDB instanceof NullPool) {
                //非server 日志本地化
                if (!IS_HTTP_SERVER) {
                    if ($type == 'crontab') {
                        $dir = APP_LOG_PATH . '/' . $type . '/' . $log['task'] . '/';
                        $content = $log['message'];
                    } else {
                        $dir = APP_LOG_PATH . '/' . $type . '/';
                        $content = $log;
                    }
                    $fileName = $dir . date('Y-m-d', strtotime(Date::today())) . '.log';
                    if (!is_dir($dir)) {
                        mkdir($dir, 0775, true);
                    }
                    File::write($fileName, !is_string($content) ? JsonHelper::toJson($content) : $content, true);
                }
                return false;
            }
            $queueKey = "_LOGS_" . strtolower($type);
            return $masterDB->lPush($queueKey, [
                'day' => Date::today(),
                'host' => SERVER_HOST,
                'log' => $log
            ]);
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * @param string $type
     * @param array $log
     * @return bool
     */
    protected function saveToFile(string $type, array $log): bool {
        $message = $log['log'] ?? $log['message'];
        //本地化
        if ($type == 'crontab') {
            $dir = APP_LOG_PATH . '/' . $type . '/' . $message['task'] . '/';
        } else {
            $dir = APP_LOG_PATH . '/' . $type . '/';
        }
        if (!is_dir($dir)) {
            mkdir($dir, 0775, true);
        }
        $day = date('Y-m-d', strtotime($log['day']));
        $fileName = $dir . $day . '.log';
        $success = File::write($fileName, !is_string($message) ? JsonHelper::toJson($message) : $message, true);
        if ($success && $day == date('Y-m-d')) {
            if (Counter::instance()->exist(md5($fileName))) {
                Counter::instance()->incr(md5($fileName));
            } else {
                Counter::instance()->set(md5($fileName), $this->count($type, $day, $message['task'] ?? null));
            }
        }
        return $success;
    }

    /**
     * 统计日志
     * @param string $type
     * @param string $day
     * @param ?string $taskName
     * @return int
     */
    public function count(string $type, string $day, ?string $taskName = null): int {
        if ($taskName) {
            $dir = APP_LOG_PATH . '/' . $type . '/' . $taskName . '/';
        } else {
            $dir = APP_LOG_PATH . '/' . $type . '/';
        }
        $fileName = $dir . $day . '.log';
        if ($day == date('Y-m-d') && Counter::instance()->exist(md5($fileName))) {
            return Counter::instance()->get(md5($fileName));
        }
        $count = $this->countFileLines($fileName);
        $day == date('Y-m-d') and Counter::instance()->set(md5($fileName), $count);
        return $count;
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
        return LogTable::instance()->count();
    }

    /**
     * 清理过期日志
     * @param int $expireDays
     * @return int
     */
    public function clear(int $expireDays = 15): int {
        //清理过期日志
        $logPath = APP_LOG_PATH;
        $logs = Dir::scan($logPath);
        $clearCount = 0;
        if ($logs) {
            foreach ($logs as $log) {
                //只保留文件名称
                $file = basename($log);
                //判断日志是否 Y-m-d.log 格式 并获取日期
                if (!preg_match('/^[0-9]{4}-[0-9]{2}-[0-9]{2}\.log$/', $file)) {
                    continue;
                }
                $date = preg_replace('/^([0-9]{4}-[0-9]{2}-[0-9]{2})\.log$/', '$1', $file);
                // 把日志文件名中的日期转为时间戳
                $timestamp = strtotime($date);
                // 当前时间戳
                $now = time();
                // 计算相差的天数
                $diffDays = floor(($now - $timestamp) / 86400);
                // 判断是否超过过期天数
                if ($diffDays > $expireDays && file_exists($log) && unlink($log)) {
                    $clearCount++;
                }
            }
        }
        Runtime::instance()->set('_LOG_CLEAR_DAY_', Date::today());
        return $clearCount;
    }

    /**
     * 读取本地日志
     * @param $type
     * @param $day
     * @param $start
     * @param $size
     * @param ?string $subDir
     * @return ?array
     */
    public function get($type, $day, $start, $size, string $subDir = null): ?array {
        if ($subDir) {
            $dir = APP_LOG_PATH . '/' . $type . '/' . $subDir . '/';
        } else {
            $dir = APP_LOG_PATH . '/' . $type . '/';
        }
        $fileName = $dir . $day . '.log';
        if (!file_exists($fileName)) {
            return [];
        }
        if ($start < 0) {
            $size = abs($start);
            $start = 0;
        }
        clearstatcache();
        $logs = [];
        // 使用 tac 命令倒序读取文件，然后用 sed 命令读取指定行数
        $command = sprintf(
            'tac %s | sed -n %d,%dp',
            escapeshellarg($fileName),
            $start + 1,
            $start + $size
        );
        $result = System::exec($command);
        if ($result === false) {
            return [];
        }
        $lines = explode("\n", $result['output']);
        foreach ($lines as $line) {
            if (trim($line) && ($log = JsonHelper::is($line) ? JsonHelper::recover($line) : $line)) {
                $logs[] = $log;
            }
        }
        return $logs;
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

    /**
     * 统计日志文件行数
     * @param $file
     * @return int
     */
    protected function countFileLines($file): int {
        $line = 0; //初始化行数
        if (file_exists($file)) {
            $output = trim(System::exec("wc -l " . escapeshellarg($file))['output']);
            $arr = explode(' ', $output);
            $line = (int)$arr[0];
        }
        return $line;
    }

    protected function formatBackTrace($backTrace): array {
        $backTraceList = [];
        foreach ($backTrace as $item) {
            $backTraceList[] = [
                'file' => $item['file'] ?? $item['class'] ?? '--',
                'line' => $item['line'] ?? $item['function'],
                'class' => $item['class'] ?? '--',
                'function' => $item['function'] ?? '--',
                'type' => $item['type'] ?? '--',
                'object' => $item['object'] ?? []
            ];
        }
        return $backTraceList;
    }
}
