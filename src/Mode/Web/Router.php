<?php

namespace Scf\Mode\Web;

use ReflectionClass;
use ReflectionMethod;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Table\RouteCache;
use Scf\Core\Table\RouteTable;
use Scf\Core\Table\SocketRouteTable;
use Scf\Core\Traits\CoroutineSingleton;
use Scf\Helper\ArrayHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Native\Controller as NativeController;
use Scf\Mode\Socket\Connection;
use Scf\Mode\Web\Exception\NotFoundException;
use Scf\Mode\Web\Route\AnnotationReader;
use Scf\Mode\Web\Route\AnnotationRouteRegister;
use Scf\Util\Dir;
use Throwable;

class Router {
    use CoroutineSingleton;

    protected array $_config = [
        // 默认模块
        'default_module' => 'Controller',
        // 默认控制器
        'default_controller' => 'Index',
        // 默认动作
        'default_action' => 'Index',
        // 允许的模块
        'modules' => [],
        // action方法前缀
        'method_prefix' => 'action',
        // url分区
        'url_partition' => '{module}/{controller}/{action}'
    ];

    /**
     * 模块
     * @var string
     */
    protected string $_module = '';

    /**
     * 控制器
     * @var string
     */
    protected string $_controller = '';

    /**
     * 动作
     * @var string
     */
    protected string $_action = '';


    /**
     * URL分区
     * @var array
     */
    protected array $_partitions = [];

    /**
     * 请求路径
     * @var string
     */
    protected string $_path = '';

    protected string $_defaultController = 'Index';
    protected string $_defaultAction = 'Index';

    /**
     * 查询参数
     * @var string
     */
    protected string $_query = '';
    /**
     * @var string 控制器访问路径
     */
    protected string $_ctrl_path;

    protected array $_params = [];
    protected bool $isAnnotationRoute = false;
    protected array $annotationRoute = [];

    /**
     * 构造函数
     * @param array|null $conf
     */
    public function __construct(array $conf = null) {
        is_array($conf) and $this->_config['modules'] = $conf;
    }

    public function setCtrlPath($path) {
        return $this->_ctrl_path = $path;
    }

    /**
     * 获取控制器访问路径
     * @return string
     */
    public function getCtrlPath(): string {
        return $this->_ctrl_path;
    }

    /**
     * 获取Module
     * @return string
     */
    public function getModule(): string {
        return $this->_module;
    }

    /**
     * 获取Controller
     * @return string
     */
    public function getController(): string {
        return $this->_controller;
    }

    /**
     * 获取Action
     * @return string
     */
    public function getAction(): string {
        return $this->_action;
    }

    /**
     * 获取URL分区
     * @return array
     */
    public function getPartitions(): array {
        return $this->_partitions;
    }

    /**
     * @return string
     */
    public function getPath(): string {
        return $this->_path;
    }

    /**
     * @return string
     */
    public function getQuery(): string {
        return $this->_query;
    }

    /**
     * 获得Path中某段数据
     * @param int|string $offset
     * @param null $default
     * @param null $limit
     * @return null|mixed
     */
    public function getFragment(int|string $offset, $default = null, $limit = null): mixed {
        if (is_numeric($offset)) {
            $args = ['/', trim($this->_path, '/')];
            !is_null($limit) && array_push($args, intval($limit));
            $fragments = call_user_func_array('explode', $args);
            return $fragments[$offset] ?? $default;
        }
        return $this->_partitions[$offset] ?? $default;
    }

    /**
     * 获取需要执行的方法名
     * @param $action
     * @return string
     */
    public function getMethod($action = null): string {
        $action = $action ?: $this->_action;
        return $this->_config['method_prefix'] . ucfirst($action);
    }

    public function isAnnotationRoute(): bool {
        return $this->isAnnotationRoute;
    }

    public function getAnnotationRoute(): array {
        return $this->annotationRoute;
    }

    public static function loadRoutes(): void {
        clearstatcache();
        $entryScripts = Dir::scan(APP_LIB_PATH, 4);
        $excludeFiles = [
            '_config.php',
            'config.php',
            '_config_.php',
            '_module_.php',
            'service.php',
            '_service.php',
            '_service_.php'
        ];
        $routeRows = RouteTable::instance()->rows();
        if ($routeRows) {
            $routeTable = RouteTable::instance();
            array_map([$routeTable, 'delete'], array_keys($routeRows));
        }
        $routeCache = RouteCache::instance()->rows();
        if ($routeCache) {
            $routeCacheTable = RouteCache::instance();
            array_map([$routeCacheTable, 'delete'], array_keys($routeCache));
        }
        $routes = [];
        foreach ($entryScripts as $entryScript) {
            $arr = explode(DIRECTORY_SEPARATOR, $entryScript);
            $fileName = array_pop($arr);
            if (in_array($fileName, $excludeFiles)) {
                continue;
            }
            $classFilePath = str_replace(APP_LIB_PATH . DIRECTORY_SEPARATOR, '', $entryScript);
            $maps = explode(DIRECTORY_SEPARATOR, $classFilePath);
            $maps[count($maps) - 1] = str_replace('.php', '', $fileName);
            $namespace = App::buildControllerPath(...$maps);
            $reader = AnnotationReader::instance();
            try {
                if (!is_subclass_of($namespace, Controller::class) && !is_subclass_of($namespace, NativeController::class) && !is_subclass_of($namespace, Connection::class)) {
                    continue;
                }
                $cls = new ReflectionClass($namespace);
                $methods = $cls->getMethods(ReflectionMethod::IS_PUBLIC);
                // 支持类级别的 Socket('/path/') 注解：一个类作为一个入口
                $classAnnotations = $reader->getClassAnnotations($cls);
                if (isset($classAnnotations['Socket']) && $classAnnotations['Socket']) {
                    SocketRouteTable::instance()->set($classAnnotations['Socket'], [
                        'entrance' => $classAnnotations['Socket'],
                        'class' => $namespace
                    ]);
                }
                foreach ($methods as $method) {
                    $annotations = $reader->getAnnotations($method);
                    if (!str_starts_with($method->getName(), 'action') && !isset($annotations['Route'])) {
                        continue;
                    }
                    $route = $annotations['Route'] ?? null;
                    if (!$route) {
                        $parts = explode("\\", substr($namespace . "\\" . substr($method->getName(), 6), strlen(APP_TOP_NAMESPACE) + 1));
                        $parts = array_filter($parts, fn($v) => $v !== 'Controller');
                        $route = '/' . join('/', array_map('\\Scf\\Helper\\StringHelper::camel2lower', $parts)) . '/';
                    }
                    // 规范化：前导/尾随/重复斜杠归一 + 去除多余空白
                    $route = preg_replace('#/{2,}#', '/', trim($route));
                    $route = '/' . trim($route, '/') . '/';

                    // 去重：以规范化后的路径为键
                    if (isset($routes[$route])) {
                        Console::warning("[{$method->getName()}@{$route}]已忽略重复的路由定义：{$route}");
                        continue;
                    }
                    $routes[$route] = $namespace . "\\" . $method->getName();
                    RouteTable::instance()->set(md5($route), [
                        'route' => $route,
                        'type' => isset($annotations['Route']) ? 2 : 1,
                        'method' => $annotations['Method'] ?? 'all',
                        'action' => $method->getName(),
                        'module' => $maps[0],
                        'controller' => $maps[count($maps) - 1],
                        'space' => $namespace,
                    ]);
                }
            } catch (Throwable $exception) {
                Console::error($exception->getMessage());
            }
        }
    }

    public function matchNormalRoute() {
        //匹配路由
        $routes = RouteTable::instance()->rows();
        $route = $this->_path;
        $matched = RouteCache::instance()->get(md5($route)) ?: null;
        if (!$matched) {
            /**
             * 查找匹配的控制器和方法
             */
            foreach ($routes as $item) {
                if ($item['route'] === $route) {
                    $matched = $item;
                    break;
                }
            }
            if (!$matched) {
                $route .= StringHelper::camel2lower($this->_defaultAction) . "/";
                foreach ($routes as $item) {
                    if ($item['route'] === $route) {
                        $matched = $item;
                        break;
                    }
                }
            }
            $matched and RouteCache::instance()->set(md5($route), $matched);
        }
        return $matched;
    }

    public function matchAnnotationRoute($server): bool {
        $this->_getPath($server);
        if (!$annotationRoute = AnnotationRouteRegister::instance()->match($server['request_method'], $server['path_info'])) {
            return false;
        }
        $this->isAnnotationRoute = true;
        $this->annotationRoute = $annotationRoute;
        $route = $annotationRoute['route'];
        $this->_module = StringHelper::lower2camel($route['module']);
        $this->_controller = StringHelper::lower2camel($route['controller']);
        $this->_action = StringHelper::lower2camel($route['action']);
        $this->_partitions['module'] = $this->_module;
        $this->_partitions['controller'] = $this->_controller;
        $this->_partitions['action'] = $this->_action;
        $this->_params = $annotationRoute['params'];
        $this->_path = $route['route'];
        return true;
    }

    /**
     * 路由调度
     * @param array $server
     * @param array $modules
     * @return void
     * @throws NotFoundException
     */
    public function dispatch(array $server, array $modules): void {
        $pathinfo = $this->_getPath($server);
        $pathinfo = $pathinfo ? explode('/', trim($pathinfo, '/')) : [];
        $map = [];
        $this->_partitions = [];
        $this->_module = APP_MODULE_STYLE == APP_MODULE_STYLE_MICRO ? 'Controller' : StringHelper::lower2camel($pathinfo[0] ?? 'Index');
        //检查模块是否存在
        $module = ArrayHelper::findColumn($modules, 'name', $this->_module);
        if (!$module) {
            throw new NotFoundException($this->_path);
        }
        $urlPartition = $module['url_partition'] ?? $this->_config['url_partition'];
        $this->_defaultController = $module['default_controller'] ?? $this->_config['default_controller'];
        $this->_defaultAction = $module['default_action'] ?? $this->_config['default_action'];
        $defaultPartition = [
            'controller' => $this->_defaultController,
            'action' => $this->_defaultAction
        ];
        foreach (explode('/', $urlPartition) as $i => $partition) {
            $partition = substr($partition, 1, -1);
            $map[$partition] = $i;
            $this->_partitions[$partition] = isset($pathinfo[$i]) ? StringHelper::lower2camel($pathinfo[$i]) : ($defaultPartition[$partition] ?? 'Index');
        }
        // 通过pathinfo判断模块
        $this->_controller = !empty($pathinfo[$map['controller']]) ? strip_tags(StringHelper::lower2camel($pathinfo[$map['controller']])) : StringHelper::lower2camel($this->_defaultController);
        $this->_action = !empty($pathinfo[$map['action']]) ? strip_tags(StringHelper::lower2camel($pathinfo[$map['action']])) : StringHelper::lower2camel($this->_defaultAction);
        if (count($pathinfo) <= 3) {
            $this->_path = "/" . join('/', array_map(
                    '\Scf\Helper\StringHelper::camel2lower',
                    $this->_partitions
                )) . "/";
            //$this->_path = "/" . StringHelper::camel2lower($this->_module) . "/" . StringHelper::camel2lower($this->_controller) . "/" . StringHelper::camel2lower($this->_action) . "/";
        } else {
            $this->_path = "/" . join('/', array_map(
                    '\Scf\Helper\StringHelper::camel2lower',
                    $pathinfo
                )) . "/";
        }
    }

    public function getDefaultController(): string {
        return $this->_defaultController;
    }

    public function getDefaultAction(): string {
        return $this->_defaultAction;
    }

    public function getRoute(): string {
        return $this->_path;
    }

    public function fixPartition($key, $value): void {
        $this->_partitions[$key] = $value;
        $property = '_' . $key;
        $this->$property = $value;
    }

    /**
     * 获取访问路径
     * @param $server
     * @return string
     */
    protected function _getPath($server): string {
        $fields = ['path_info', 'request_uri'];
        $this->_path = '';
        foreach ($fields as $f) {
            if (isset($server[$f])) {
                $this->_path = trim($server[$f]);
                break;
            }
        }
        $this->_path = rtrim($this->_path, '/');
        $this->_query = trim(!empty($server['query_string']) ? $server['query_string'] : '', '?');
        return $this->_path;
    }

    public function setPath($path): void {
        $this->_path = $path;
    }

    /**
     * 生成URL
     *
     * 例如:
     * url('Controller') --> /home/index/index
     * url('User/Profile') --> /home/user/profile
     * url('Admin/User/Profile', array('name'=>'hypo')) --> /admin/user/profile?name=hypo
     * url('@/User/Profile') --> /home/user/profile
     * url('/User/Profile') --> /user/profile
     *
     * @param string $path URL路径,可自动用当前的参数不全$pathinfo中不足的项
     * @param array $params GET参数
     * @return string
     */
    public function url(string $path, array $params = []): string {
        $partition = array_values($this->_partitions);
        $path = $path ? explode('/', $path) : [];

        if (empty($path)) {
            $path = $partition;
        } elseif ($path[0] == '') {
            array_shift($path);
        } elseif ($path[0] == '@') {
            $path[0] = $this->getModule();
        } else {
            $offset = count($partition) - count($path) - 1;
            if ($offset > 0) {
                $path = array_merge(array_slice($partition, 0, $offset), $path);
            }
        }
        $path = array_map('\Scf\Helper\StringHelper::camel2lower', $path);
        return '/' . join('/', $path) . ($params ? '?' . http_build_query($params) : '');
    }

    /**
     * 输出文本
     * @return string
     */
    public function toString(): string {
        return join('/', [$this->_module, $this->_controller, $this->_action]);
    }

}
