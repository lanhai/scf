<?php

namespace Scf\Mode\Web;

use ReflectionMethod;
use Scf\Core\Component;
use Scf\Core\Config;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\Route\AnnotationReader;

class Document extends Component {
    protected static array $_retulst_data = [];

    /**
     * 生成文档
     * @param $nameSpace
     * @return array
     */
    public static function create($nameSpace): array {
        $cls = new \ReflectionClass($nameSpace);
        $classDocument = $cls->getDocComment();
        $classArr = explode("\\", $nameSpace);
        $document = [];
        $document['desc'] = '';
        if ($classDocument) {
            $commentArr = explode("\n", $classDocument);
            $document['desc'] = trim(str_replace("*", "", $commentArr[1]));
            preg_match_all('/@(version|author|updated)(?:\[(\w+)\])?\s+?(.+)/', $classDocument, $matches);
            if ($matches) {
                for ($i = 0; $i < count($matches[0]); $i++) {
                    $rn = trim($matches[1][$i]); //指令名称
                    $rs = trim($matches[2][$i]); //指令场景
                    $rc = trim($matches[3][$i]); //内容
                    if ($rn === 'author') {
                        $document['author'] = $rc;
                    }
                    if ($rn === 'version') {
                        $document['version'] = $rc;
                    }
                    if ($rn === 'updated') {
                        $document['updated'] = $rc;
                    }
                }
            }
        }
        $methods = $cls->getMethods(ReflectionMethod::IS_PUBLIC);
        $reader = AnnotationReader::instance();
        foreach ($methods as $k => $method) {
            $annotations = $reader->getAnnotations($method);
            $comment = $method->getDocComment();
            $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
            if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
                $path = '/' . StringHelper::camel2lower($classArr[3]) . '/' . StringHelper::camel2lower($classArr[4]) . '/' . StringHelper::camel2lower(str_replace('action', '', $method->getName())) . '/';
            } else {
                $path = '/' . StringHelper::camel2lower($classArr[2]) . '/' . StringHelper::camel2lower($classArr[4]) . '/' . StringHelper::camel2lower(str_replace('action', '', $method->getName())) . '/';
            }
            if ($method->getName()[0] === '_' || !$comment) {
                unset($methods[$k]);
                continue;
            }
            $explain = [];
            $requestType = "GET";
            $return = [
                'type' => 'array',
                'desc' => '标准响应格式,包含data,errCode,message,其中data字段数据类型可能是object/array/string/int其中的一种(float当做string返回)',
                'data' => [
                    'type' => 'object/array',
                    'desc' => '业务数据',
                    'fields' => []
                ]
            ];
            $methodDocument = [
                'desc' => '',
                'param' => [],
                'return' => []
            ];
            $commentArr = explode("\n", $comment);
            $desc = trim(str_replace("*", "", $commentArr[1]));
            preg_match_all('/@(param|get|post|data|method|explain|path)(?:\[(\w+)\])?\s+?(.+)/', $comment, $matches);
            if (!$matches) {
                continue;
            }
            self::$_retulst_data = [];
            for ($i = 0; $i < count($matches[0]); $i++) {
                $rn = trim($matches[1][$i]); //指令名称
                $rs = trim($matches[2][$i]); //指令场景
                $rc = trim($matches[3][$i]); //内容
                if ($rn == 'post') {
                    $requestType = "POST";
                }
                if (in_array($rn, ['get', 'post'])) {
                    $rn = 'param';
                }
                switch ($rn) {
                    case 'param':
                        if ($rc) {
                            $rcArr = explode(" ", $rc);
                            $require = 1;
                            if (str_contains($rcArr[2], 'NR:')) {
                                $require = 0;
                                $rcArr[2] = str_replace("NR:", "", $rcArr[2]);
                            }
                            $params = [
                                'key' => $rcArr[0],
                                'type' => $rcArr[1],
                                'desc' => $rcArr[2],
                                'default' => isset($rcArr[3]) ? str_replace('default:', '', $rcArr[3]) : '',
                                'required' => $require
                            ];
                            if (str_contains($rcArr[0], '.')) {
                                $keyArr = explode('.', $rcArr[0]);
                                $params['key'] = $keyArr[1];
                                foreach ($methodDocument['param'] as &$p) {
                                    if ($p['key'] == $keyArr[0]) {
                                        if (isset($p['children'])) {
                                            $p['children'][] = $params;
                                        } else {
                                            $p['children'] = [$params];
                                        }
                                    }
                                }
                            } else {
                                $methodDocument['param'][] = $params;
                            }
                        } else {
                            $methodDocument['param'][] = "";
                        }
                        break;
                    case 'explain':
                        if (!empty($rc)) {
                            $explain[] = $rc;
                        }
                        break;
                    case 'path':
                        $path = !empty($rc) ? $rc : $path;
                        break;
                    case 'method':
                        $requestType = !empty($rc) ? $rc : "";
                        break;
                    case 'data':
                        if ($rc) {
                            $rcArr = explode(" ", $rc);
                            if (in_array(strtolower($rcArr[0]), ['string', 'int', 'array'])) {
                                $return['data']['type'] = $rcArr[0];
                                $return['data']['desc'] = $rcArr[1];
                                unset($return['data']['fields']);
                            } else {
                                $field = [
                                    'key' => $rcArr[0],
                                    'type' => $rcArr[1],
                                    'desc' => $rcArr[2] ?? '',
                                ];
                                if (str_contains($rcArr[0], '.')) {
                                    $keyArr = explode('.', $rcArr[0]);
                                    $parentArr = explode('.', $rcArr[0]);
                                    unset($parentArr[count($parentArr) - 1]);
                                    $field['key'] = $keyArr[count($keyArr) - 1];
                                    self::$_retulst_data[] = ['id' => $rcArr[0], 'parent' => implode('.', $parentArr), 'content' => $field];
                                } else {
                                    self::$_retulst_data[] = ['id' => $field['key'], 'parent' => false, 'content' => $field];
                                }
                            }
                        }
                        break;
                }
            }
            $return['data']['fields'] = self::createResultField();

            $document['methods'][] = [
                'name' => $method->getName(),
                'path' => $annotations['Route'] ?? $path,
                'is_annotation_route' => isset($annotations['Route']),
                'method' => $requestType,
                'desc' => $desc,
                'explain' => $explain ? implode("<br>", $explain) : $desc,
                'params' => $methodDocument['param'],
                'return' => $return
            ];
        }

        return $document;
    }

    /**
     * @return array
     */
    protected static function createResultField(): array {
        $list = [];
        foreach (self::$_retulst_data as $field) {
            if (!$field['parent']) {
                $c = $field['content'];
                if ($ch = self::getResultFieldChildrens($field['id'])) {
                    $c['children'] = $ch;
                }
                $list[] = $c;
            }
        }
        return $list;
    }

    /**
     * @param $parent
     * @return array
     */
    protected static function getResultFieldChildrens($parent): array {
        $childrens = [];
        foreach (self::$_retulst_data as $field) {
            if ($field['parent'] == $parent) {
                $c = $field['content'];
                if ($ch = self::getResultFieldChildrens($field['id'])) {
                    $c['children'] = $ch;
                }
                $childrens[] = $c;
            }
        }
        return $childrens;
    }

}