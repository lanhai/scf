<?php

namespace Scf\Mode\Web\Route;

use InvalidArgumentException;
use ReflectionClass;
use ReflectionMethod;
use Scf\Core\Component;
use Scf\Core\Console;

class AnnotationReader extends Component {

    public function getAnnotations($reflectionClassOrMethod): array {
        $annotations = [];
        if ($reflectionClassOrMethod instanceof ReflectionClass) {
            $doc = $reflectionClassOrMethod->getDocComment();
        } else if ($reflectionClassOrMethod instanceof ReflectionMethod) {
            $doc = $reflectionClassOrMethod->getDocComment();
        } else {
            Console::warning('Argument must be a ReflectionClass or ReflectionMethod.');
            throw new InvalidArgumentException('Argument must be a ReflectionClass or ReflectionMethod.');
        }
        preg_match_all('/@(\w+)(?:\s*\(([^)]*)\))?/', $doc, $matches, PREG_SET_ORDER);
        foreach ($matches as $match) {
            $name = $match[1];
            $value = isset($match[2]) ? trim($match[2], '"\' ') : null;
            $annotations[$name] = $value;
        }
        return $annotations;
    }
}
