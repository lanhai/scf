<?php

namespace Scf\Mode\Web;

use Scf\Core\Traits\CoroutineSingleton;
use Scf\Helper\ArrayHelper;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Exception\NotFoundException;

class Router {
    use CoroutineSingleton;

    protected array $_config = [
        // 默认模块
        'default_module' => 'Index',
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

    /**
     * 查询参数
     * @var string
     */
    protected string $_query = '';
    /**
     * @var string 控制器访问路径
     */
    protected string $_ctrl_path;

//    /**
//     * 获得单利
//     * @param array|null $conf
//     * @return Router
//     */
//    public static function instance(array $conf = null): Router {
//        static $_obj;
//        if (is_null($_obj)) {
//            $_obj = new self($conf);
//        }
//        return $_obj;
//    }


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

    /**
     * 路由调度
     * @throws NotFoundException
     */
    public function dispatch($server, $modules): void {
        $pathinfo = $this->_getPath($server);
        $pathinfo = $pathinfo ? explode('/', trim($pathinfo, '/')) : [];
        $map = [];
        $this->_partitions = [];
        $this->_module = StringHelper::lower2camel($pathinfo[0] ?? $this->_config['default_module']);
        //检查模块是否存在
        $module = ArrayHelper::findColumn($modules, 'name', $this->_module);
        if (!$module) {
            throw new NotFoundException($this->_path);
        }
        $urlPartition = $module['url_partition'] ?? $this->_config['url_partition'];
        $defaultController = $module['default_controller'] ?? $this->_config['default_controller'];
        $defaultAction = $module['default_action'] ?? $this->_config['default_action'];
        foreach (explode('/', $urlPartition) as $i => $partition) {
            $partition = substr($partition, 1, -1);
            $map[$partition] = $i;
            $this->_partitions[$partition] = isset($pathinfo[$i]) ? StringHelper::lower2camel($pathinfo[$i]) : null;
        }
        // 通过pathinfo判断模块
        $this->_controller = !empty($pathinfo[$map['controller']]) ? strip_tags(StringHelper::lower2camel($pathinfo[$map['controller']])) : StringHelper::lower2camel($defaultController);
        $this->_action = !empty($pathinfo[$map['action']]) ? strip_tags(StringHelper::lower2camel($pathinfo[$map['action']])) : StringHelper::lower2camel($defaultAction);
        $this->_partitions['module'] = $this->_module;
        $this->_partitions['controller'] = $this->_controller;
        $this->_partitions['action'] = $this->_action;
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
     * url('Index') --> /home/index/index
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
