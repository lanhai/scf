<?php

namespace Scf\Mode\Rpc;

use ReflectionClass;
use ReflectionException;
use ReflectionMethod;
use Scf\Core\Traits\Singleton;

class Document {
    use Singleton;

    /**
     * 本地服务信息
     * @param $nameSpace
     * @return string[]
     * @throws ReflectionException
     */
    public function local($nameSpace): array {
        $document = [
            'version' => 'unknow',
            'author' => '',
            'description' => '',
            'updated' => ''
        ];
        $cls = new ReflectionClass($nameSpace);
        $classDocument = $cls->getDocComment();
        if ($classDocument) {
            preg_match_all('/@(description|version|author|updated)(?:\[(\w+)\])?\s+?(.+)/', $classDocument, $matches);
            if ($matches) {
                for ($i = 0; $i < count($matches[0]); $i++) {
                    $rn = trim($matches[1][$i]); //key
                    $rc = trim($matches[3][$i]); //content
                    match ($rn) {
                        'description' => $document['desc'] = $rc,
                        'author' => $document['author'] = $rc,
                        'version' => $document['version'] = $rc,
                        'updated' => $document['updated'] = $rc,
                    };
                }
            }
        }
        return $document;
    }

    /**
     * @throws ReflectionException
     */
    public function create($nameSpace): array {
        $document = [];
        $cls = new ReflectionClass($nameSpace);
        $classDocument = $cls->getDocComment();
        if ($classDocument) {
            $commentArr = explode("\n", $classDocument);
            $document['desc'] = trim(str_replace("*", "", $commentArr[1] ?? "暂无说明"));
            preg_match_all('/@(description|version|author|updated|gateway_dev|gateway_release)(?:\[(\w+)\])?\s+?(.+)/', $classDocument, $matches);
            if ($matches) {
                for ($i = 0; $i < count($matches[0]); $i++) {
                    $rn = trim($matches[1][$i]); //key
                    $rc = trim($matches[3][$i]); //content
                    match ($rn) {
                        'description' => $document['desc'] = $rc,
                        'author' => $document['author'] = $rc,
                        'version' => $document['version'] = $rc,
                        'updated' => $document['updated'] = $rc,
                        'gateway_dev' => $document['gateway_dev'] = $rc,
                        'gateway_release' => $document['gateway_release'] = $rc,

                    };
                }
            }
        }
        $methods = $cls->getMethods(ReflectionMethod::IS_PUBLIC);//+ \ReflectionMethod::IS_PROTECTED + \ReflectionMethod::IS_PRIVATE
        foreach ($methods as $k => $method) {
            if ($method->getName()[0] === '_' || $method->getName() === 'moduleName') {
                unset($methods[$k]);
                continue;
            }
            $paramList = [];
            $desc = "";
            $return = [
                'type' => 'Result',
                'desc' => '含data,errCode,message,其中data字段数据类型可能是object/array/string/int/float其中的一种',
                'data' => [
                    'type' => 'object',
                    'desc' => '业务数据',
                    'fields' => []
                ]
            ];
            $methodDocument = [
                'desc' => '',
                'param' => [],
                'return' => []
            ];
            $comment = $method->getDocComment();
            if ($comment) {
                $commentArr = explode("\n", $comment);
                $desc = trim(str_replace("*", "", $commentArr[1] ?? "暂无说明"));
                preg_match_all('/@(desc|param|return|data)(?:\[(\w+)\])?\s+?(.+)/', $comment, $matches);
                if (!$matches) {
                    continue;
                }
                for ($i = 0; $i < count($matches[0]); $i++) {
                    $rn = trim($matches[1][$i]); //key
                    $rs = trim($matches[2][$i]); //指令场景
                    $rc = trim($matches[3][$i]); //内容
                    switch ($rn) {
                        case 'param':
                            if ($rc) {
                                $rcArr = explode(" ", $rc);
                                $methodDocument['param'][] = $rcArr[2];
                            } else {
                                $methodDocument['param'][] = "";
                            }
                            break;
                        case 'desc':
                            $desc = !empty($rc) ? $rc : "";
                            break;
                        case 'return':
//                                    if ($rc) {
//                                        $rcArr = explode(" ", $rc);
//                                        $return['type'] = $rcArr[0];
//                                        $return['desc'] = str_replace($rcArr[0] . " ", "", $rc);
//                                    }
                            break;
                        case 'data':
                            if ($rc) {
                                $rcArr = explode(" ", $rc);
                                if (in_array(strtolower($rcArr[0]), ['string', 'int', 'array'])) {
                                    $return['data']['type'] = $rcArr[0];
                                    $return['data']['desc'] = $rcArr[1];
                                    unset($return['data']['fields']);
                                } else {
                                    $return['data']['fields'][] = [
                                        'key' => $rcArr[0],
                                        'type' => $rcArr[1],
                                        'desc' => $rcArr[2]
                                    ];
                                }
                            }
                            break;
                    }
                }
            }
            $params = $method->getParameters();
            if ($params) {
                foreach ($params as $pk => $param) {
                    $paramList[] = [
                        'name' => $param->getName(),
                        'type' => $param->hasType() ? $param->getType()->getName() : '',
                        'desc' => $methodDocument['param'][$pk] ?? '',
                        'default_value' => $param->isDefaultValueAvailable() ? $param->getDefaultValue() : '',
                        'require' => !$param->isDefaultValueAvailable()
                    ];
                }
            }
            $document['methods'][] = [
                'name' => $method->getName(),
                'desc' => $desc,
                'params' => $paramList,
                'return' => $return
            ];
        }
        return $document;
    }
}