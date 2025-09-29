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
        return $this->parseAnnotationsFromDoc($doc);
    }

    public function getAnnotations($reflectionClassOrMethod): array {
        if ($reflectionClassOrMethod instanceof ReflectionClass) {
            $doc = $reflectionClassOrMethod->getDocComment();
            return $this->parseAnnotationsFromDoc($doc);
        }
        if ($reflectionClassOrMethod instanceof ReflectionMethod) {
            $doc = $reflectionClassOrMethod->getDocComment();
            return $this->parseAnnotationsFromDoc($doc);
        }

        Console::warning('Argument must be a ReflectionClass or ReflectionMethod.');
        throw new InvalidArgumentException('Argument must be a ReflectionClass or ReflectionMethod.');
    }

    /**
     * 将 DocComment 文本解析为注解数组。
     * 兼容：@Name @Name() @Name("/path") @Name('x'), 会返回 ['Name' => '...']。
     */
    private function parseAnnotationsFromDoc(?string $doc): array {
        $annotations = [];
        if (!$doc) {
            return $annotations;
        }

        // 匹配形如 @Name 或 @Name(...) 的注解；捕获括号内原始内容
        if (preg_match_all('/@(\w+)(?:\s*\(([^)]*)\))?/', $doc, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $match) {
                $name = $match[1];
                $raw = isset($match[2]) ? trim($match[2]) : null;

                // 去掉最外层引号（若有）
                if ($raw !== null && ((str_starts_with($raw, '"') && str_ends_with($raw, '"')) || (str_starts_with($raw, "'") && str_ends_with($raw, "'")))) {
                    $raw = substr($raw, 1, -1);
                }

                $annotations[$name] = $raw;
            }
        }

        return $annotations;
    }
}
