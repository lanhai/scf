<?php

namespace Scf\Server\Controller;

use PhpZip\Exception\ZipException;
use PhpZip\ZipFile;
use Scf\App\Updater;
use Scf\Client\Http;
use Scf\Command\Color;
use Scf\Command\Handler\NodeManager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Core\Log;
use Scf\Core\Result;
use Scf\Core\Table\RouteTable;
use Scf\Core\Table\Runtime;
use Scf\Database\Dao;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Controller;
use Scf\Mode\Web\Document as DocumentComponent;
use Scf\Mode\Web\Request;
use Scf\Mode\Web\Response;
use Scf\Server\Manager;
use Scf\Server\Task\CrontabManager;
use Scf\Server\Task\RQueue;
use Scf\Util\Auth;
use Scf\Util\Date;
use Scf\Util\Des;
use Scf\Util\File;
use Scf\Util\MemoryMonitor;
use Scf\Util\Random;
use Throwable;

class DashboardController extends Controller {

    public static array $protectedActions = [
        '/nodes', '/memory'
    ];
    protected string $token;

    public function init($path): void {
        if (!App::isReady() && $path != '/install' && $path != '/install_check') {
            Response::interrupt("应用尚未完成初始化安装", 'APP_NOT_INSTALL_YET', status: 200);
        }
        $publisPaths = ['/install', '/install_check', '/login', '/memory', '/nodes', '/update_dashboard', '/ws_document', '/ws_sign_debug'];
        if (!in_array($path, $publisPaths) && !$this->isLogin()) {
            Response::interrupt("未授权的访问: " . $path, 'NOT_LOGIN', status: 200);
        }
    }

    /**
     * 向节点发送指令
     * @return Result
     */
    public function actionCommand(): Result {
        Request::post([
            'command' => Request\Validator::required('命令不能为空'),
            'host' => Request\Validator::required('节点不能为空'),
            'params'
        ])->assign($command, $host, $params);
        return Manager::instance()->sendCommandToNode($command, $host, $params ?: [], 'dashboard');
    }

    /**
     * 节点列表
     * @return Result
     */
    public function actionNodes(): Result {
        $nodes = Manager::instance()->getServers(false);
        return Result::success($nodes);
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

        return Result::success(MemoryMonitor::sum($filter));
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
        Request::post(['name', 'host'])->assign($name, $host);
        return Result::success(CrontabManager::runRightNow($name, $host));
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
            $subDir = CrontabManager::formatTaskName($logType);
        }
        if (is_numeric($day)) {
            $day = date('Y-m-d', $day);
        }
        $total = Log::instance()->count(!in_array($logType, $sysLogs) ? 'crontab' : $logType, $day, $subDir);
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
            'list' => Log::instance()->get(!in_array($logType, $sysLogs) ? 'crontab' : $logType, $day, $start, $size, $subDir),
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
        return Result::success((int)App::isReady() ? 1 : 0);
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
        $client = Http::create($host . '/~/install_check', $port);
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
            'port'
        ])->assign($host, $port);
        if ($port) {
            $host .= ':' . $port;
        }
        $client = Http::create($host . '/~/install');
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
            return Result::error($requestResult->getMessage(), data: $requestResult->getData());
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
        if (!str_contains($host, 'localhost') || Env::inDocker()) {
            $socketHost = $protocol . Request::header('host') . '/dashboard.socket';
        } else {
            $socketHost = $protocol . 'localhost:' . Runtime::instance()->httpPort();
        }
        $status = Manager::instance()->getStatus();
        $status['socket_host'] = $socketHost . '?token=' . $this->token;
        $remoteVersion = [
            'version' => FRAMEWORK_BUILD_VERSION,
            'build' => FRAMEWORK_BUILD_TIME,
        ];
        $status['latest_version'] = [];
        if (APP_SRC_TYPE == 'phar') {
            $status['latest_version'] = App::latestVersion();
        }
        $client = Http::create(FRAMEWORK_REMOTE_VERSION_SERVER);
        $remoteVersionResponse = $client->get();
        $client->close();
        if (!$remoteVersionResponse->hasError()) {
            $remoteVersion = $remoteVersionResponse->getData();
        }
        //控制面板
        $dashboardDir = SCF_ROOT . '/build/public/dashboard';
        $versionJson = $dashboardDir . '/version.json';
        if (!file_exists($versionJson)) {
            $currentDashboardVersion = [
                'version' => '0.0.0',
            ];
        } else {
            $currentDashboardVersion = JsonHelper::recover(File::read($versionJson));
        }
        $client = Http::create(str_replace('version.json', 'dashboard-version.json', FRAMEWORK_REMOTE_VERSION_SERVER));
        $dashboardVersionResponse = $client->get();
        if (!$dashboardVersionResponse->hasError()) {
            $dashboardVersion = $dashboardVersionResponse->getData();
        }
        $status['dashboard'] = [
            'version' => $currentDashboardVersion['version'],
            'latest_version' => $dashboardVersion['version'] ?? '--',
        ];
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
     * 面板更新
     * @return Result
     */
    public function actionUpdateDashboard(): Result {
        $dashboardDir = SCF_ROOT . '/build/public/dashboard';
        $versionJson = $dashboardDir . '/version.json';
        if (!file_exists($versionJson)) {
            $localVersion = '0.0.0';
        } else {
            $localVersion = JsonHelper::recover(File::read($versionJson))['version'] ?? '0.0.0';
        }
        $client = Http::create(str_replace('version.json', 'dashboard-version.json', FRAMEWORK_REMOTE_VERSION_SERVER));
        $dashboardVersionResponse = $client->get();
        if ($dashboardVersionResponse->hasError()) {
            return Result::error('版本信息获取失败:' . $dashboardVersionResponse->getMessage());
        }
        $dashboardVersion = $dashboardVersionResponse->getData();
        if ($localVersion == $dashboardVersion['version']) {
            return Result::success("当前已是最新版本");
        }
        $publicFilePath = SCF_ROOT . '/build/dashboard.zip';
        if (file_exists($publicFilePath)) {
            unlink($publicFilePath);
            clearstatcache();
        }
        if (!is_dir(SCF_ROOT . '/build/public/dashboard')) {
            mkdir(SCF_ROOT . '/build/public/dashboard', 0777, true);
        } else {
            $this->clearDashboard(SCF_ROOT . '/build/public/dashboard');
        }
        $downloadClient = Http::create($dashboardVersion['server'] . $dashboardVersion['file']);
        $downloadResult = $downloadClient->download($publicFilePath);
        if ($downloadResult->hasError()) {
            return Result::error("下载升级包失败:" . $downloadResult->getMessage());
        }
        //解压缩
        $zipFile = new ZipFile();
        try {
            $stream = File::read($publicFilePath);
            $zipFile->openFromString($stream)->setReadPassword('scfdashboard')->extractTo(SCF_ROOT . '/build/public/dashboard');
        } catch (ZipException $e) {
            unlink($publicFilePath);
            return Result::error('资源包解压失败:' . $e->getMessage());
        } finally {
            $zipFile->close();
        }
        unlink($publicFilePath);
        if (!File::write($versionJson, JsonHelper::toJson($dashboardVersion))) {
            return Result::error('版本文件更新时间');
        }
        return Result::success("面板已更新至:" . $dashboardVersion['version']);
    }

    /**
     * 清除资源文件夹
     * @param string $dir
     * @return bool
     */
    protected function clearDashboard(string $dir): bool {
        if (!is_dir($dir)) {
            return true;
        }
        //先删除目录下的文件：
        $dh = opendir($dir);
        while ($file = readdir($dh)) {
            if ($file != "." && $file != "..") {
                $fullpath = $dir . "/" . $file;
                if (!is_dir($fullpath)) {
                    try {
                        unlink($fullpath);
                    } catch (Throwable) {

                    }
                } else {
                    $this->clearDashboard($fullpath);
                }
            }
        }
        closedir($dh);
        return true;
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
     * 调试签名
     * @return Result
     */
    public function actionWsSignDebug(): Result {
        Request::post([
            'appid',
            'appkey',
            'path',
            'query',
            'body'
        ])->assign($appid, $appkey, $path, $query, $body);
        if (!$appid || !$appkey) {
            return Result::success([
                'sign' => '',
                'rand' => '',
                'timestamp' => '',
                'appid' => '',
            ]);
        }
        if (strlen($appkey) !== 32) {
            return Result::error('秘钥长度不合法');
        }
        $rand = Random::character(16);
        $timestamp = time();
        if ($query) {
            $queryString = ArrayHelper::toQueryMap($query);
            $path .= '?' . $queryString;
        }
        $signStr = $appid . ":" . $timestamp . ":" . $path;
        if ($body) {
            $signStr .= ":" . ArrayHelper::toQueryMap($body);
        }
        $sign = Des::encrypt($signStr, $appkey, 'AES-256-CBC', $rand);
        return Result::success([
            'sign' => $sign,
            'rand' => $rand,
            'timestamp' => $timestamp,
            'appid' => $appid
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
        if (!file_exists(APP_TMP_PATH . '/route_document.json')) {
            return Result::success([
                'modules' => [],
                'debug' => []
            ]);
        }
        $debug = Config::get('simple_webservice');
        unset($debug['apps']);
        return Result::success([
            'modules' => File::readJson(APP_TMP_PATH . '/route_document.json'),
            'debug' => $debug
        ]);
        return Result::success(RouteTable::instance()->rows());
        $appStyle = APP_MODULE_STYLE;
        switch ($action) {
            case 'init':
                return Result::success([
                    'debug_appid' => Config::get('simple_webservice')['debug_appid'],
                    'debug_appkey' => Config::get('simple_webservice')['debug_appkey'],
                    'debug_mpappid' => Config::get('simple_webservice')['debug_mpappid'],
                    'gate_way' => Config::get('simple_webservice')['gateway'],
                    'modules' => App::getModules($mode)
                ]);
            default:
                $apis = [];
                $module = StringHelper::lower2camel($module);
                if ($mode == MODE_CGI && $module != 'Ws') {
                    //公共接口
                    try {
                        $c = $appStyle == APP_MODULE_STYLE_SINGLE ? DocumentComponent::create("\\App\\Controller\\Ws\\Common") : DocumentComponent::create("\\App\\Common\\Controller\\Ws");
                        $apis[] = array_merge(['name' => 'common'], $c);
                    } catch (\Exception $exception) {
                        //Response::interrupt('生成文档数据失败:' . $exception->getMessage());
                    }
                }
                //请求的模块
                $dirName = $mode == MODE_CGI ? 'Controller' : 'Service';
                $dir = $appStyle == APP_MODULE_STYLE_SINGLE ? (APP_LIB_PATH . '/' . $dirName . '/' . $module) : (APP_LIB_PATH . '/' . $module . '/' . $dirName);
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
                        $c = $appStyle == APP_MODULE_STYLE_SINGLE ? DocumentComponent::create("\\App\\{$dirName}\\" . $module . "\\" . $item) : DocumentComponent::create("\\App\\{$module}\\{$dirName}\\" . $item);
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


    public function actionNotices(): Result {
        return Result::success([]);
    }

    public function actionRoutes(): Result {
        $baseRoutes = [
            [
                'path' => '/',
                'name' => 'Root',
                'component' => 'Layout',
                'meta' => [
                    'title' => '节点',
                    'icon' => 'computer-line',
                    'levelHidden' => true,
                    'breadcrumbHidden' => true
                ],
                'children' => [
                    [
                        'path' => '/index',
                        'name' => 'Index',
                        'component' => '/@/views/nodes/index.vue',
                        'meta' => [
                            'title' => '服务器节点',
                            'icon' => 'computer-line',
                            'noColumn' => true,
                            'noClosable' => true
                        ],
                    ]
                ]
            ],
            [
                'path' => '/document/ws',
                'name' => 'Document',
                'component' => 'Layout',
                'meta' => [
                    'title' => '文档',
                    'icon' => 'file-list-line',
                    'levelHidden' => true,
                    'breadcrumbHidden' => true,
                    'target' => '_blank'
                ]
            ]
        ];
        return Result::success(['list' => $baseRoutes]);
    }

    /**
     * 登陆检查
     * @return Result
     */
    public function actionCheck(): Result {
        if (!$this->verifyPassword($this->password)) {
            return Result::error('登陆已失效', 'LOGIN_EXPIRED');
        }
        $this->loginUser['token'] = $this->genterateToken();
        return Result::success($this->loginUser);
    }

    /**
     * 登陆
     * @return Result
     */
    public function actionLogin(): Result {
        Request::post([
            'password' => Request\Validator::required("密码不能为空")
        ])->assign($password);
        if (!$this->verifyPassword($password)) {
            return Result::error('密码错误');
        } else {
            $this->loginUser['token'] = $this->genterateToken();
            return Result::success($this->loginUser);
        }
    }


    /**
     * 退出登录
     * @return Result
     */
    public function actionLogout(): Result {
        return Result::success();
    }


    /**
     * 获取登陆用户
     * @return bool
     */
    protected function isLogin(): bool {
        $authorization = Request::header('authorization');
        if (empty($authorization) || !str_starts_with($authorization, "Bearer")) {
            Response::interrupt("需登陆后访问", 'NOT_LOGIN', status: 200);
        }
        $token = substr($authorization, 7);
        $this->token = $token;
        //判断是否超管
        $decodeToken = Auth::decode($token);
        if (!$decodeToken || !JsonHelper::is($decodeToken)) {
            Response::interrupt("登录已失效", 'TOKEN_NOT_VALID', status: 200);
        }
        $decodeData = JsonHelper::recover($decodeToken);
        $password = $decodeData['password'] ?? null;
        $this->password = $password;
        $expired = $decodeData['expired'] ?? 0;
        if (time() > $expired) {
            Response::interrupt("登录已过期", 'LOGIN_EXPIRED', status: 200);
        }
        return true;
    }

    protected array $loginUser = [
        'username' => '系统管理员',
        'avatar' => 'https://ascript.oss-cn-chengdu.aliyuncs.com/upload/20240513/04c3eeac-f118-4ea7-8665-c9bd4d20a05d.png',
        'token' => null
    ];
    protected string $password = '';

    /**
     * 生成token
     * @return string
     */
    protected function genterateToken(): string {
        $tokenData = [
            'password' => $this->password,
            'expired' => time() + 3600 * 24 * 7,
            'login_time' => time(),
            'user' => 'system'
        ];
        return Auth::encode(JsonHelper::toJson($tokenData));
    }

    /**
     * 密码验证
     * @param $password
     * @return bool
     */
    protected function verifyPassword($password): bool {
        $serverConfig = Config::server();
        $superPassword = $serverConfig['dashboard_password'] ?? null;
        if (App::info()->dashboard_password !== $password && App::info()->app_auth_key !== $password && $superPassword !== $password) {
            return false;
        }
        $this->password = $password;
        return true;
    }
}