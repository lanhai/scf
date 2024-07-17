<?php

namespace Scf\Mode\Web\Route;

use InvalidArgumentException;

class AnnotationReader {

    public function getAnnotations($reflectionClassOrMethod): array {
        $annotations = [];
        if ($reflectionClassOrMethod instanceof \ReflectionClass) {
            $doc = $reflectionClassOrMethod->getDocComment();
        } else if ($reflectionClassOrMethod instanceof \ReflectionMethod) {
            $doc = $reflectionClassOrMethod->getDocComment();
        } else {
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
