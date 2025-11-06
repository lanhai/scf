<?php

namespace Scf\Mode\Web\Route;

use InvalidArgumentException;
use ReflectionClass;
use ReflectionMethod;
use Scf\Core\Component;
use Scf\Core\Console;

class AnnotationReader extends Component {

    /**
     * 解析类上的注解（仅接收 ReflectionClass）
     */
    public function getClassAnnotations(ReflectionClass $reflectionClass): array {
        $doc = $reflectionClass->getDocComment();
        if ($doc && !mb_detect_encoding($doc, 'UTF-8', true)) {
            $doc = mb_convert_encoding($doc, 'UTF-8', 'auto');
        } else {
            // 保险起见去掉可能的 BOM
            $doc = preg_replace('/^\xEF\xBB\xBF/', '', $doc);
        }
        return $this->parseAnnotationsFromDoc($doc);
    }

    public function getAnnotations($reflectionClassOrMethod): array {
        if ($reflectionClassOrMethod instanceof ReflectionClass) {
            $doc = $reflectionClassOrMethod->getDocComment();
            return $this->parseAnnotationsFromDoc($doc);
        }
        if ($reflectionClassOrMethod instanceof ReflectionMethod) {
            $doc = $reflectionClassOrMethod->getDocComment();
            return $this->parseAnnotationsFromDoc($doc, $reflectionClassOrMethod->getName() . "@" . $reflectionClassOrMethod->getFileName());
        }

        Console::warning('Argument must be a ReflectionClass or ReflectionMethod.');
        throw new InvalidArgumentException('Argument must be a ReflectionClass or ReflectionMethod.');
    }

    /**
     * 将 DocComment 文本解析为注解数组。
     * 兼容：@Name @Name() @Name("/path") @Name('x'), 会返回 ['Name' => '...']。
     */
    private function parseAnnotationsFromDoc(?string $doc, ?string $className = null): array {
        $annotations = $className ? [
            'label' => '未定义',
            'path_params' => [],
            'get' => [],
            'post' => [],
            'result' => []
        ] : [];
        if (!$doc) {
            return $annotations;
        }
        // 去掉 /** */ 和每行的 * 符号
        $doc = preg_replace('/^\/\*\*|\*\/$/', '', $doc);
        // 替换所有换行符为 \n
        $doc = str_replace(["\r\n", "\r"], "\n", $doc);
        // 按 \n 拆分
        $lines = explode("\n", $doc);
        //$lines = preg_split('/\R/', $doc);
        $cleaned = [];
        foreach ($lines as $line) {
            $line = trim(preg_replace('/^\s*\*\s?/', '', $line));
            if ($line !== '') {
                $cleaned[] = $line;
            }
        }
        if (!empty($cleaned[0]) && !str_starts_with($cleaned[0], '@')) {
            $annotations['label'] = $cleaned[0];
        }
        $doc = implode("\n", $cleaned);
        // 匹配 @Something 开头的注解
        if (preg_match_all('/@(\w+)([^\n]*)/', $doc, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $match) {
                $name = $match[1];
                $value = trim($match[2]);

                // @return 类型
                if ($name === 'return') {
                    continue;
                    //$value = trim($value);
                }
                // 处理 @Route("/path") 形式
                if (preg_match('/^\((.*)\)$/', $value, $vm)) {
                    $value = trim($vm[1]);
                }

                // 去掉外层引号
                if ((str_starts_with($value, '"') && str_ends_with($value, '"')) ||
                    (str_starts_with($value, "'") && str_ends_with($value, "'"))) {
                    $value = substr($value, 1, -1);
                }
                // 处理特殊注解如 @param int $param 描述
                if ($name === 'param' && preg_match('/^(\S+)\s+(\$\S+)\s*(.*)$/', $value, $pm)) {
                    $route = $annotations['Route'] ?? "";
                    $isPathParam = str_contains($route, substr($pm[2], 1));
                    $value = [
                        'type' => $pm[1],
                        'name' => '{' . substr($pm[2], 1) . '}',
                        'desc' => $pm[3] ?? '',
                        'required' => 1
                    ];
                    if ($route && $isPathParam) {
                        $annotations['path_params'][] = $value;
                        continue;
                    }
                } elseif (in_array($name, ['get', 'post', 'result']) && preg_match('/^(\S+)\s+(\S+)\s*(.*)$/', $value, $pm)) {
                    if (empty($pm[3])) {
                        Console::warning('参数' . $name . '描述文档错误:' . $className);
                        continue;
                    }
                    $desc = explode(";", $pm[3]);
                    $value = [
                        'name' => $pm[1],
                        'type' => $pm[2],
                        'desc' => $desc[0],
                    ];
                    if ($name !== 'result') {
                        $default = null;
                        $max = null;
                        $min = null;
                        foreach ($desc as $d) {
                            if (str_starts_with($d, 'default:')) {
                                $default = substr($d, 8);
                            } else if (str_starts_with($d, 'max:')) {
                                $max = substr($d, 4);
                            } else if (str_starts_with($d, 'min:')) {
                                $min = substr($d, 4);
                            }
                        }
                        $value['max'] = $max;
                        $value['min'] = $min;
                        $value['default'] = $default;
                        $value['required'] = in_array('R', $desc) ? 1 : 0;
                        $annotations[$name][] = $value;
                        continue;
                    }
                    $rootIndex = in_array('data', array_column($annotations['result'], 'name'));
                    if ($rootIndex === false && $value['name'] !== 'data') {
                        $annotations['result'][] = [
                            'name' => 'data',
                            'type' => 'object',
                            'desc' => "响应数据",
                            'level' => 1
                        ];
                    }
                    $level = count(explode('.', $value['name']));
                    $value['level'] = $level;
                    $annotations['result'][] = $value;
                    continue;
                } elseif ($name === 'errcode') {
                    $errcode = explode(':', $value);
                    $value = [
                        'code' => $errcode[0],
                        'desc' => $errcode[1]
                    ];
                }
                // 支持同名注解多次出现
                if (isset($annotations[$name])) {
                    if (!is_array($annotations[$name]) || !isset($annotations[$name][0])) {
                        $annotations[$name] = [$annotations[$name]];
                    }
                    $annotations[$name][] = $value;
                } else {
                    $annotations[$name] = $value;
                }
            }
        }
        return $annotations;
    }
//    private function parseAnnotationsFromDoc(?string $doc): array {
//        $annotations = [];
//        if (!$doc) {
//            return $annotations;
//        }
//
//        // 匹配形如 @Name 或 @Name(...) 的注解；捕获括号内原始内容
//        if (preg_match_all('/@(\w+)(?:\s*\(([^)]*)\))?/', $doc, $matches, PREG_SET_ORDER)) {
//            foreach ($matches as $match) {
//                $name = $match[1];
//                $raw = isset($match[2]) ? trim($match[2]) : null;
//
//                // 去掉最外层引号（若有）
//                if ($raw !== null && ((str_starts_with($raw, '"') && str_ends_with($raw, '"')) || (str_starts_with($raw, "'") && str_ends_with($raw, "'")))) {
//                    $raw = substr($raw, 1, -1);
//                }
//
//                $annotations[$name] = $raw;
//            }
//        }
//
//        return $annotations;
//    }
}
