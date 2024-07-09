<?php

namespace Scf\Server\Controller;

use Scf\App\Updater;
use Scf\Component\Coroutine\Session;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Exception;
use Scf\Core\Result;
use Scf\Cache\MasterDB;
use Scf\Database\Dao;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Controller;
use Scf\Mode\Web\Document as DocumentComponent;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Command\Color;
use Scf\Command\Handler\NodeManager;
use Scf\Server\Env;
use Scf\Server\Http;
use Scf\Server\Manager;
use Scf\Server\Task\Crontab;
use Scf\Server\Task\RQueue;
use Scf\Util\Auth;
use Scf\Util\Date;

class DashboardController extends Controller {

    public function init($path): void {
        if (!App::isReady() && $path != '/install' && $path != '/install_check') {
            Response::interrupt("应用尚未完成初始化安装", 'APP_NOT_INSTALL_YET', status: 200);
        }
        if ($path != '/install' && $path != '/install_check' && $path != '/login' && !$this->isLogin()) {//, '/logs', '/queue','/check'
            Response::interrupt("未授权的访问:" . $path);
        }
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
        return Result::success(Crontab::factory($name)->runRightNow());
    }

    /**
     * 指定任务当前状态
     * @return Result
     */
    public function actionCrontabStatus(): Result {
        Request::post(['name'])->assign($name);
        return Result::success(Crontab::factory($name)->status());
    }

    /**
     * 覆盖任务配置
     * @return Result
     */
    public function actionCrontabOverride(): Result {
        Request::post()->pack($data);
        try {
            $crontab = Crontab::factory($data['namespace']);
            $result = $crontab->saveOverrides($data);
            if ($result->hasError()) {
                return Result::error($result->getMessage());
            }
            if (!$result->getData()) {
                return Result::error('配置文件保存失败');
            }
            //重启排程任务
            return Result::success($crontab->reload());
        } catch (\Throwable $exception) {
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

        if ($logType !== 'error' && $logType !== 'info' && $logType !== 'slow') {
            $logType = strtolower('crontab' . str_replace("\\", "_", $logType));
        }
        $total = MasterDB::countLog($logType, $day);
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
            'list' => MasterDB::getLog($logType, $day, $start, $size),
            'pages' => (int)$totalPage,
            'pn' => (int)$page, 'total' => $total,
        ];
        return Result::success($result);
    }

    /**
     * 排程任务
     * @return Result
     */
    public function actionCrontabs(): Result {
        return Result::success(Crontab::instance()->getList());
    }

    /**
     * 更新
     * @return Result
     * @throws \Swoole\Exception
     */
    public function actionUpdate(): Result {
        Request::post([
            'type' => Request\Validator::required('更新类型错误'),
            'version' => Request\Validator::required('版本号不能为空')
        ])->assign($type, $version);
        $manager = new NodeManager();
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
        $client = \Scf\Client\Http::create($server . '?time=' . time());
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
        $client = \Scf\Client\Http::create($host . '/install_check', $port);
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
        $client = \Scf\Client\Http::create($host . '/install', $port);

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
     * @throws Exception
     */
    public function actionServer(): Result {
        $host = Request::header('host') ?: null;
        $referer = Request::header('referer') ?? null;
        $protocol = str_starts_with($referer, 'https') ? 'wss://' : 'ws://';
        if (is_null($host)) {
            return Result::error('访问域名获取失败');
        }
        //$socketHost = $protocol . Request::header('host') . '/dashboard.socket';
        if (!str_contains($host, 'localhost') || Env::inDocker()) {
            //$socketHost = $protocol . $this->getDomain($host) . '/server.socket';
            $socketHost = $protocol . Request::header('host') . '/dashboard.socket';
//            if (str_starts_with($host, Config::get('app')['master_host'])) {
//                $socketHost = $protocol . Config::get('app')['master_host'] . ':' . ($port - 2) . '/dashboard.socket';
//            } else {
//                $socketHost = $protocol . $host . '/dashboard.socket';
//            }
//            $socketHost = $protocol . explode(":", $host)[0] . ':' . (Http::instance()->getPort1() + 1);
        } else {
            $socketHost = $protocol . 'localhost:' . (Http::instance()->getPort() + 1);
        }
        $status = Manager::instance()->getStatus();
        $status['socket_host'] = $socketHost . '?token=' . Session::instance()->get('LOGIN_UID');
        $status['latest_version'] = App::latestVersion();

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
                    Response::interrupt('服务模块不存在:' . $dir);
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
                        Response::interrupt('生成文档数据失败(' . $item . '):' . $exception->getMessage());
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
        $configFile = App::src() . '/config/server.php';
        $serverConfig = file_exists($configFile) ? require $configFile : [];
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