<?php

namespace Scf\Mode\Web\Route;

use Scf\Core\Component;
use Scf\Core\Table\RouteCache;
use Scf\Core\Table\RouteTable;

class AnnotationRouteRegister extends Component {

    // 获取类型对应的正则表达式
    private function getRegexForType($type): string {
        $patterns = [
            'int' => '\d+',
            'letter' => '[A-Za-z]+',
            'mixed' => '[A-Za-z0-9]+',
            'string' => '[^/]+',//\w+
            'email' => '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]+',//[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}
            // 添加更多类型及其对应的正则表达式
        ];
        return $patterns[$type] ?? $type; // 默认为直接使用传入的正则表达式
    }

    // 匹配路由
    public function match($method, $path): ?array {
        $matched = RouteCache::instance()->get(md5($path . $method)) ?: null;
        if (!$matched) {
            $routes = RouteTable::instance()->rows();
            foreach ($routes as $route) {
                $allowMethods = explode(',', $route['method']);
                if ($route['type'] !== 2 || ($route['method'] !== 'all' && !in_array(strtolower($method), $allowMethods))) {
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
                    $matched = [
                        'route' => $route,
                        'params' => array_filter($matches, 'is_string', ARRAY_FILTER_USE_KEY)
                    ];
                    $matched and RouteCache::instance()->set(md5($path . $method), $matched);
                }
            }
        }
        return $matched;
    }
}