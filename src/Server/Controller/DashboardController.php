<?php

namespace Scf\Server\Controller;

use Scf\App\Updater;
use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Command\Handler\NodeManager;
use Scf\Component\Coroutine\Session;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Core\Table\MemoryMonitorTable;
use Scf\Core\Table\Runtime;
use Scf\Database\Dao;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Controller;
use Scf\Mode\Web\Document as DocumentComponent;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Server\Env;
use Scf\Server\Manager;
use Scf\Server\Task\CrontabManager;
use Scf\Server\Task\RQueue;
use Scf\Util\Auth;
use Scf\Util\Date;
use Swoole\Exception;
use Throwable;

class DashboardController extends Controller {

    public function init($path): void {
        if (!App::isReady() && $path != '/install' && $path != '/install_check') {
            Response::interrupt("应用尚未完成初始化安装", 'APP_NOT_INSTALL_YET', status: 200);
        }
        $publisPaths = ['/install', '/install_check', '/login', '/memory'];
        if (!in_array($path, $publisPaths) && !$this->isLogin()) {
            Response::interrupt("未授权的访问: " . $path, status: 200);
        }

    }

    /**
     * 内存占用
     * @return Result
     * @throws \Exception
     */
    public function actionMemory(): Result {
        Request::get([
            'filter'
        ])->assign($filter);
        // 一次性拉取数据 -> 组装行
        $buildRows = function () use ($filter) {
            $rows = [];
            $online = 0;
            $offline = 0;
            $realTotal = 0.0; // 累计实际内存占用（MB）
            $usageTotal = 0.0; // 累计分配（MB）
            $peakTotal = 0.0; // 累计峰值（MB）
            $rssTotal = 0.0; // 累计RSS（MB）
            $pssTotal = 0.0; // 累计PSS（MB'])
            //$globalSetKey = 'MEMORY_MONITOR_KEYS_' . SERVER_NODE_ID;
            //$keys = Redis::pool()->sMembers($globalSetKey) ?: [];
            $keys = Runtime::instance()->get('MEMORY_MONITOR_KEYS') ?: [];
            //根据id排序
            usort($keys, function ($a, $b) {
                // 取中间部分
                $aParts = explode(':', $a);
                $bParts = explode(':', $b);
                $aMid = $aParts[1] ?? $a;
                $bMid = $bParts[1] ?? $b;
                // 提取数字
                preg_match('/\d+$/', $aMid, $ma);
                preg_match('/\d+$/', $bMid, $mb);
                if ($ma && $mb) {
                    return intval($ma[0]) <=> intval($mb[0]);
                }
                // 没数字时走自然排序
                return strnatcmp($aMid, $bMid);
            });
            foreach ($keys as $key) {
                if ($filter && !str_contains($key, $filter)) {
                    continue;
                }
                // 解析进程标识：MEMORY_MONITOR:{process}:{pid}
                //$parts = explode(':', (string)$key);
                //$data = Redis::pool()->get($key);
                $data = MemoryMonitorTable::instance()->get($key);
                if (!$data) {
                    // 认为离线
                    $offline++;
                    $rows[] = [
                        $key,
                        '--',
                        '-',
                        '-',
                        '-',
                        '-', // rss
                        '-', // pss
                        '-',
                        Color::red('离线')
                    ];
                    continue;
                }
                $process = $data['process'] ?? '--';
                $pid = $data['pid'] ?? '--';
                $usage = (float)($data['usage_mb'] ?? 0);
                $real = (float)($data['real_mb'] ?? 0);
                $peak = (float)($data['peak_mb'] ?? 0);
                $usageTotal += $usage;
                $realTotal += $real; // 统计累计实际内存
                $peakTotal += $peak;

                $rssMb = null;
                $pssMb = null;
                if (!empty($data['rss_mb']) && is_numeric($data['rss_mb'])) {
                    $rssMb = (float)$data['rss_mb'];
                }
                if (!empty($data['pss_mb']) && is_numeric($data['pss_mb'])) {
                    $pssMb = (float)$data['pss_mb'];
                }

                if ($rssMb !== null) {
                    $rssTotal += $rssMb;
                }
                if ($pssMb !== null) {
                    $pssTotal += $pssMb;
                }

                $time = date('H:i:s', strtotime($data['time'])) ?? date('H:i:s');
                $status = Color::green('正常');
                $online++;
                $rows[] = [
                    'name' => $process,
                    'pid' => $pid,
                    'useage' => number_format($usage, 2) . ' MB',
                    'real' => number_format($real, 2) . ' MB',
                    'peak' => number_format($peak, 2) . ' MB',
                    'rss' => $rssMb === null ? '-' : (number_format($rssMb, 2) . ' MB'),
                    'pss' => $pssMb === null ? '-' : (number_format($pssMb, 2) . ' MB'),
                    'updated' => $time,
                    'status' => $status,
                    'rss_num' => $rssMb
                ];
            }
            ArrayHelper::multisort($rows, 'rss_num', SORT_DESC);
            // 排序完成后移除临时字段 rss_num，避免对外输出
            foreach ($rows as &$__row) {
                if (array_key_exists('rss_num', $__row)) {
                    unset($__row['rss_num']);
                }
            }
            unset($__row);
            return [
                'rows' => $rows,
                'online' => $online,
                'offline' => $offline,
                'total' => count($keys),
                'usage_total_mb' => round($usageTotal, 2),
                'real_total_mb' => round($realTotal, 2), // 累计实际内存占用（MB）
                'peak_total_mb' => round($peakTotal, 2),
                'rss_total_mb' => round($rssTotal, 2),
                'pss_total_mb' => round($pssTotal, 2),
            ];
        };
        $data = $buildRows();
        return Result::success($data);
    }

    /**
     * 清空数据表
     * @return Result
     */
    public function actionMaintainableClear(): Result {
        if (!Env::isDev()) {
            return Result::error('此操作只可在测试环境进行');
        }
        Request::post([
            'model' => Request\Validator::required('数据表模型不能为空')
        ])->assign($model);
        $tables = Config::get('database')['maintainable'] ?? [];
        $arr = ArrayHelper::index($tables, 'model');
        if (!isset($arr[$model])) {
            return Result::error('非法请求');
        }
        $condition = $arr[$model]['condition'] ?? [];
        /** @var Dao $model */
        $ar = $model::select();
        if ($condition) {
            $ar->where($condition);
        }
        $count = $ar->delete();
        if (!$count) {
            return Result::error('没有数据被删除');
        }
        return Result::success($count);
    }

    /**
     * 可维护数据表
     * @return Result
     */
    public function actionMaintainableTables(): Result {
        $tables = Config::get('database')['maintainable'] ?? [];
        if ($tables) {
            foreach ($tables as &$table) {
                /** @var Dao $model */
                $model = $table['model'];
                $ar = $model::select();
                if ($table['condition']) {
                    $ar->where($table['condition']);
                }
                $table['table'] = $ar->getDb() . '.' . $ar->getTable();
                $table['count'] = $ar->count();
            }
        }
        return Result::success($tables);
    }

    /**
     * 立即运行指定的任务
     * @return Result
     */
    public function actionCrontabRun(): Result {
        Request::post(['name'])->assign($name);
        return Result::success(CrontabManager::runRightNow($name));
    }

    /**
     * 指定任务当前状态
     * @return Result
     */
    public function actionCrontabStatus(): Result {
        Request::post(['name'])->assign($name);
        return Result::success(CrontabManager::status($name));
    }

    /**
     * 覆盖任务配置
     * @return Result
     */
    public function actionCrontabOverride(): Result {
        Request::post()->pack($data);
        try {
            $result = CrontabManager::saveOverrides($data);
            if ($result->hasError()) {
                return Result::error($result->getMessage());
            }
            if (!$result->getData()) {
                return Result::error('配置文件保存失败');
            }
            //重启排程任务
            return Result::success();
        } catch (Throwable $exception) {
            Console::error($exception->getMessage());
        }
        return Result::error('message');
    }

    /**
     * 日志
     * @return Result
     */
    public function actionLogs(): Result {
        Request::get([
            'type' => Request\Validator::required('日志类型不能为空'),
            'day' => Request\Validator::required('日期不能为空'),
            'page' => 1,
            'size' => 20
        ])->assign($logType, $day, $page, $size);
        $sysLogs = ['error', 'info', 'slow'];
        $subDir = null;
        if (!in_array($logType, $sysLogs)) {
            $subDir = str_replace("AppCrontab", "", str_replace("\\", "", $logType));
        }
        $total = Manager::instance()->countLog(!in_array($logType, $sysLogs) ? 'crontab' : $logType, $day, $subDir);
        $totalPage = $total ? ceil($total / $size) : 0;
        $page = min($page, $totalPage) ?: 1;
        if ($total) {
            $totalPage = ceil($total / $size);
            $pn = min($page, $totalPage) ?: 1;
            $start = ($pn - 1) * $size;
        } else {
            $start = 0;
        }
        $result = [
            'type' => $logType,
            'list' => Manager::instance()->getLog(!in_array($logType, $sysLogs) ? 'crontab' : $logType, $day, $start, $size, $subDir),
            'pages' => (int)$totalPage,
            'pn' => (int)$page,
            'total' => $total,
        ];
        return Result::success($result);
    }

    /**
     * 排程任务
     * @return Result
     */
    public function actionCrontabs(): Result {
        return Result::success(CrontabManager::allStatus());
    }

    /**
     * 更新
     * @return Result
     */
    public function actionUpdate(): Result {
        Request::post([
            'type' => Request\Validator::required('更新类型错误'),
            'version' => Request\Validator::required('版本号不能为空')
        ])->assign($type, $version);
        if ($type == 'framework' && file_exists(SCF_ROOT . '/build/update.pack')) {
            return Result::error('正在等待升级,请重启服务器');
        }
        $manager = new NodeManager();
        //向节点推送版本更新指令
        $result = $manager->appointUpdate($type, $version);
        if ($result->hasError()) {
            return Result::error($result->getMessage());
        }
        return Result::success($result->getData());
    }

    /**
     * 版本发布记录
     * @return Result
     */
    public function actionVersions(): Result {
        $list = [];
        Request::get([
            'type' => 'app',
        ])->assign($type);
        $result = Updater::instance()->getRemoteVersionsRecord();
        if ($result->hasError()) {
            return Result::error($result->getMessage());
        }
        if ($result->getData()) {
            foreach ($result->getData() as $item) {
                if (count($list) >= 20) {
                    break;
                }
                $item['build_type'] = (int)$item['build_type'];
                if ($type == 'app' && ($item['build_type'] != 1 && $item['build_type'] != 3)) {
                    continue;
                }
                if ($type == 'public' && ($item['build_type'] != 2 && $item['build_type'] != 3)) {
                    continue;
                }
                $list[] = $item;
            }
        }
        return Result::success($list);
    }

    /**
     * 应用初始化安装
     * @return Result
     */
    public function actionInstall(): Result {
        if (App::isReady()) {
            return Result::error("应用已完成安装", 'APP_INSTALLED');
        }
        Request::post([
            'key' => Request\Validator::required("安装秘钥不能为空"),
            'role' => 'master'
        ])->assign($key, $role);
        $secret = substr($key, 0, 32);
        $installKey = substr($key, 32);

        $app = App::installer();
        $app->public_path = 'public';
        $app->app_path = APP_DIR_NAME;
        $decode = Auth::decode($installKey, $secret);
        if (!$decode) {
            return Result::error("安装秘钥错误");
        }
        $config = JsonHelper::recover($decode);
        if (empty($config['key']) || empty($config['server'] || empty($config['dashboard_password']) || empty($config['expired']))) {
            return Result::error("安装秘钥错误");
        }
        if (time() > $config['expired']) {
            return Result::error("安装秘钥已过期");
        }
        $app->app_auth_key = $config['key'];
        $app->dashboard_password = $config['dashboard_password'];
        $server = $config['server'];
        $app->update_server = $server;
        $app->role = $role;
        $client = Http::create($server . '?time=' . time());
        $result = $client->get();
        if ($result->hasError()) {
            return Result::error('获取云端版本号失败:' . Color::red($result->getMessage()));
        }
        $remote = $result->getData();
        $appVersion = $remote['app'] ?? '';
        if ($appVersion) {
            $app->version = $appVersion[0]['version'];
            $app->appid = $appVersion[0]['appid'];
        } else {
            return Result::error('获取云端版本号失败');
        }
        $app->updated = date('Y-m-d H:i:s');
        if (!$app->add()) {
            return Result::error('安装失败');
        }
        return Result::success(['password' => $app->dashboard_password]);
    }

    /**
     * 安装检查
     * @return Result
     */
    public function actionInstallCheck(): Result {
        return Result::success(App::isReady());
    }

    /**
     * 检查子节点安装状态
     * @return Result
     */
    public function actionCheckSlaveNode(): Result {
        Request::post([
            'host' => Request\Validator::required("主机不能为空"),
            'port' => Request\Validator::required("端口不能为空"),
        ])->assign($host, $port);
        $client = Http::create($host . '/install_check', $port);
        $requestResult = $client->get();
        if ($requestResult->hasError()) {
            return Result::error($requestResult->getMessage());
        }
        $result = Result::factory($requestResult->getData());
        if ($result->hasError()) {
            return Result::error($result->getMessage());
        }
        return Result::success($result->getData());
    }

    /**
     * 安装节点
     * @return Result
     */
    public function actionInstallSlaveNode(): Result {
        Request::post([
            'host' => Request\Validator::required("主机名称不能为空"),
            'port' => Request\Validator::required("端口号不能为空"),
        ])->assign($host, $port);
        $client = Http::create($host . '/install', $port);
        $app = App::info();
        $installData = [
            str_shuffle(time()) => time(),
            'server' => App::info()->update_server,
            'key' => $app->app_auth_key,
            'dashboard_password' => $app->dashboard_password,
            'expired' => time() + 86400,
        ];
        $installSecret = Auth::encode(str_shuffle(time()), 'SCF_APP_INSTALL');
        $installCode = $installSecret . Auth::encode(JsonHelper::toJson($installData), $installSecret);
        $requestResult = $client->post([
            'role' => 'slave',
            'key' => $installCode
        ]);
        if ($requestResult->hasError()) {
            return Result::error($requestResult->getMessage());
        }
        $result = Result::factory($requestResult->getData());
        if ($result->hasError()) {
            return Result::error($result->getMessage());
        }
        return Result::success($result->getData());
    }

    /**
     * 服务器状态
     * @return Result
     */
    public function actionServer(): Result {
        $host = Request::header('host') ?: null;
        $referer = Request::header('referer');
        $protocol = (!empty($referer) && str_starts_with($referer, 'https')) ? 'wss://' : 'ws://';
        if (is_null($host)) {
            return Result::error('访问域名获取失败');
        }
        //$socketHost = $protocol . Request::header('host') . '/dashboard.socket';
        if (!str_contains($host, 'localhost') || Env::inDocker()) {
            $socketHost = $protocol . Request::header('host') . '/dashboard.socket';
        } else {
            $socketHost = $protocol . 'localhost:' . Runtime::instance()->httpPort();
        }
        $status = Manager::instance()->getStatus();
        $status['socket_host'] = $socketHost . '?token=' . Session::instance()->get('LOGIN_UID');
        $status['latest_version'] = App::latestVersion();

        $client = Http::create(FRAMEWORK_REMOTE_VERSION_SERVER);
        $remoteVersionResponse = $client->get();
        $remoteVersion = [
            'version' => FRAMEWORK_BUILD_VERSION,
            'build' => FRAMEWORK_BUILD_TIME,
        ];
        if (!$remoteVersionResponse->hasError()) {
            $remoteVersion = $remoteVersionResponse->getData();
        }
        $status['framework'] = [
            'is_phar' => FRAMEWORK_IS_PHAR,
            'version' => FRAMEWORK_BUILD_VERSION,
            'latest_version' => $remoteVersion['version'],
            'latest_build' => $remoteVersion['build'],
            'build' => FRAMEWORK_BUILD_TIME
        ];
        return Result::success($status);
    }

    /**
     * 队列管理
     * @return Result
     */
    public function actionQueue(): Result {
        Request::get([
            'status' => 0,
            'day' => Date::today('Y-m-d'),
            'page' => 1,
            'size' => 500
        ])->assign($status, $day, $page, $size);
        $rq = RQueue::instance();
        $count = $rq->count($status, $day);
        $totalPage = ceil($count / $size);
        $start = ($page - 1) * $size;
        $end = $start + $size;
        $list = $rq->lRange($start, $end, $status, $day);
        return Result::success([
            'day' => $day,
            'total' => $count,
            'pn' => $page,
            'pages' => $totalPage,
            'list' => $list
        ]);
    }

    /**
     * 文档
     * @return Result
     */
    public function actionWsDocument(): Result {
        Request::get([
            'action' => 'document',
            'mode' => MODE_CGI,
            'module'
        ])->assign($action, $mode, $module);
        $appStyle = Config::get('app')['module_style'];
        switch ($action) {
            case 'init':
                return Result::success([
                    'debug_appid' => Config::get('simple_webservice')['debug_appid'],
                    'debug_appkey' => Config::get('simple_webservice')['debug_appkey'],
                    'debug_mpappid' => Config::get('simple_webservice')['debug_mpappid'],
                    'gate_way' => Config::get('simple_webservice')['gateway'],
                    'modules' => $mode == MODE_CGI ? App::getModules() : \Scf\Mode\Rpc\App::loadModules($mode)
                ]);
            default:
                $apis = [];
                $module = StringHelper::lower2camel($module);
                if ($mode == MODE_CGI && $module != 'Ws') {
                    //公共接口
                    try {
                        $c = $appStyle == APP_MODULE_STYLE_MICRO ? DocumentComponent::create("\\App\\Controller\\Ws\\Common") : DocumentComponent::create("\\App\\Common\\Controller\\Ws");
                        $apis[] = array_merge(['name' => 'common'], $c);
                    } catch (\Exception $exception) {
                        //Response::interrupt('生成文档数据失败:' . $exception->getMessage());
                    }
                }
                //请求的模块
                $dirName = $mode == MODE_CGI ? 'Controller' : 'Service';
                $dir = $appStyle == APP_MODULE_STYLE_MICRO ? (APP_LIB_PATH . '/' . $dirName . '/' . $module) : (APP_LIB_PATH . '/' . $module . '/' . $dirName);
                if (!file_exists($dir)) {
                    Response::interrupt('服务模块不存在:' . $dir, status: 200);
                }
                $list = [];
                if ($files = scandir($dir)) {
                    foreach ($files as $file) {
                        if ($file === '.' || $file === '..') {
                            continue;
                        }
                        $file = str_replace(".php", "", $file);
                        $list[] = $file;
                    }
                }
                foreach ($list as $item) {
                    if ($item == '_config' || $item == 'config') {
                        continue;
                    }
                    try {
                        $document = ['name' => StringHelper::camel2lower($item)];
                        $c = $appStyle == APP_MODULE_STYLE_MICRO ? DocumentComponent::create("\\App\\{$dirName}\\" . $module . "\\" . $item) : DocumentComponent::create("\\App\\{$module}\\{$dirName}\\" . $item);
                        if (!$c['desc']) {
                            continue;
                        }
                        $apis[] = array_merge($document, $c);
                    } catch (\Exception $exception) {
                        Response::interrupt('生成文档数据失败(' . $item . '):' . $exception->getMessage(), status: 200);
                    }
                }
                return Result::success($apis);
        }
    }

    /**
     * 登陆检查
     * @return Result
     */
    public function actionCheck(): Result {
        return Result::success(Session::instance()->get('LOGIN_UID'));
    }

    /**
     * 登陆
     * @return Result
     */
    public function actionLogin(): Result {
        Request::post([
            'password' => Request\Validator::required("密码不能为空")
        ])->assign($password);
        $serverConfig = Config::server();
        $superPassword = $serverConfig['dashboard_password'] ?? null;
        if (App::info()->dashboard_password !== $password && App::info()->app_auth_key !== $password && $superPassword !== $password) {
            return Result::error('密码错误');
        } else {
            $token = Auth::encode(time());
            Session::instance()->set('LOGIN_UID', $token);
            return Result::success($token);
        }
    }

    /**
     * 退出登录
     * @return Result
     */
    public function actionLogout(): Result {
        return Result::success(Session::instance()->del('LOGIN_UID'));
    }

    /**
     * 获取登陆用户
     * @return bool
     */
    protected function isLogin(): bool {
        $loginUid = Session::instance()->get('LOGIN_UID');
        if (!$loginUid) {
            return false;
        }
        return true;
    }
}