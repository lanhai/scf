<?php

namespace Scf\Mode\Web\Route;

use ReflectionClass;
use ReflectionMethod;
use Scf\Core\Component;
use Scf\Core\Console;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Controller;
use Scf\Server\Table\RouteTable;
use Scf\Util\Dir;
use Throwable;

class AnnotationRouteRegister extends Component {
    protected array $routes = [];

    // 获取类型对应的正则表达式
    private function getRegexForType($type): string {
        $patterns = [
            'int' => '\d+',
            'string' => '[^/]+',//\w+
            'email' => '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]+',//[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}
            // 添加更多类型及其对应的正则表达式
        ];
        return $patterns[$type] ?? $type; // 默认为直接使用传入的正则表达式
    }

    // 匹配路由
    public function match($method, $path): ?array {
        $routes = $this->routes();
        foreach ($routes as $route) {
            $allowMethods = explode(',', $route['method']);
            if ($route['method'] !== 'all' && !in_array(strtolower($method), $allowMethods)) {
                continue;
            }
            $routePattern = preg_replace_callback('/\{(\w+)(?::([^}]+))?\}/', function ($matches) {
                $param = $matches[1];
                $type = $matches[2] ?? 'string';
                $regex = $this->getRegexForType($type);
                return '(?P<' . $param . '>' . $regex . ')';
            }, $route['route']);

            $routePattern = '#^' . $routePattern . '$#';
            if (preg_match($routePattern, $path, $matches)) {
                return [
                    'route' => $route,
                    'params' => array_filter($matches, 'is_string', ARRAY_FILTER_USE_KEY)
                ];
            }
        }
        return null;
    }

    /**
     * 加载注解路由
     * @return void
     */
    public function load(): void {
        clearstatcache();
        $entryScripts = Dir::scan(APP_LIB_PATH, 4);
        $excludeFiles = [
            '_config.php',
            'config.php',
            'service.php',
            '_service.php'
        ];
        $routesCache = RouteTable::instance()->rows();
        if ($routesCache) {
            $routeTable = RouteTable::instance();
            array_map([$routeTable, 'delete'], array_keys($routesCache));
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
            $reader = new AnnotationReader();
            try {
                if (!is_subclass_of($namespace, Controller::class)) {
                    continue;
                }
                $cls = new ReflectionClass($namespace);
                $methods = $cls->getMethods(ReflectionMethod::IS_PUBLIC);
                foreach ($methods as $method) {
                    $annotations = $reader->getAnnotations($method);
                    if (!isset($annotations['Route'])) {
                        continue;
                    }
                    if (isset($routes[$annotations['Route']])) {
                        Console::warning("[{$method->getName()}@{$namespace}]已忽略重复的路由定义：" . $annotations['Route']);
                        continue;
                    }
                    $routes[$annotations['Route']] = $namespace . $method->getName();
                    RouteTable::instance()->set(md5($annotations['Route']), [
                        'route' => $annotations['Route'],
                        'method' => $annotations['Method'] ?? 'all',
                        'action' => $method->getName(),
                        'module' => $maps[0],
                        'controller' => $maps[count($maps) - 1],
                        'space' => $namespace,

                    ]);
                    //MasterDB::set('routes', $this->routes);
                }
            } catch (Throwable $exception) {
                Console::error($exception->getMessage());
            }
        }
    }

    public function routes(): array {
        return RouteTable::instance()->rows();
        //return MasterDB::get('routes');
    }
}