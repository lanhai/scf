<?php

namespace Scf\Core;

use Error;
use Filterus\Filter;
use JetBrains\PhpStorm\Pure;
use ReflectionClass;
use ReflectionProperty;
use Scf\Database\Tools\Calculator;
use Scf\Helper\JsonHelper;

/**
 * 数据结构类型基类
 * 提供基于注释的字段验证
 * @v 1.3 添加验证规则func和method的支持
 * @v 1.2 增加@skip标签,字段可以被赋值,但不会参加验证和toArray(true)返回,应用于ID字段等
 * @v 1.2.1 增加@ghost标签,字段可以被赋值,但不会参加验证和toArray()任何参数都不会返回,应用于表单重复密码字段等
 * @v 1.3 增加标签的标签,[scene],设置标签的应用场景
 * @version 1.2
 * @updated 2025-09-17 01:20:59
 */
class Struct {
    /**
     * @var array<string, ReflectionProperty[]> 缓存字段反射信息
     */
    protected static array $_fieldsCache = [];
    /**
     * @var array<string, array> 缓存解析后的规则模板
     */
    protected static array $_validateTemplateCache = [];
    /**
     * @var bool 只注册一次Filter
     */
    protected static bool $_filterRegistered = false;
    /**
     * @var array 验证信息
     */
    protected array $_validate = [];

    /**
     * @var array 错误
     */
    protected array $_errors = [];

    /**
     * @var string 当前场景
     */
    protected string $_scene = '';

    /**
     * 构造函数，未作数据验证
     * @param array|null $data
     * @param string $scene
     */
    public function __construct(array $data = null, string $scene = '') {
        // 分析验证规则
        $this->_parseValidateRules();
        // 设置场景
        $this->setScene($scene);
        // 设置默认值
        $this->_setDefault();
        // 创建验证
        if (!is_null($data)) {
            $this->install($data, false);
        }
    }

    /**
     * 验证数据
     * 验证规则写在公共变量的文档注释里面
     * 规则分指令和内容，用空格隔开
     * 目前提供三个指令
     * @default：默认值，数据类型和值用冒号隔开，数据类型有：
     *   int 整数
     *   float 浮点数
     *   string 字符串
     *   null 不需要指定值
     *   func 函数返回值，暂不支持传参
     *   method 在当前类中的方法
     *   array 数组，使用json格式
     *   bool 布尔值，true或者false
     * @rule：验证规则，错误提示使用｜隔开，@see Filterus\Filter::factory()
     *   除了Filter提供的验证方法之外,还内置2种验证规则
     *     func 调用函数,func($value)
     *     method 调用实例方法 func($value, $field, $all)
     * @required：必填字段，取值：true或者false，错误提示使用｜隔开
     * @skip 排除的字段,只是保存值,不验证,toArray(true)不返回值,
     * @ghost 鬼魂字段,只是保存值,不验证,toArray()任何参数不返回值
     *
     * 范例：
     * @default str:username
     * @default[insert] str:username
     * @required <true>|名称必须填写
     * @required[update] true|名称必须填写
     * @rule string,max:6|名称不能大于6个字符
     * @rule method:valid_some_field
     * @rule method:valid_some_field
     * @rule[update] method:valid_some_field
     * @param array|null $data
     * @return bool
     */
    public function validate(array $data = null): bool {
        // 初始化错误记录
        $this->_errors = [];
        if (!$this->_validate) {
            return true;
        }

        if (is_null($data)) {
            $data = $this->toArray();
        }

        $passed = true;

        foreach ($this->_validate as $f => $v) {
            if ($this->_isSkipField($f) || $this->_isGhostField($f)) {
                continue; // 排除跳过的字段或者魔鬼字段
            }
            // 必填值验证
            if (isset($v['required'])) {
                foreach ($v['required'] as $req) {
                    if ($this->_checkScene($req['scene'])) {
                        if (!isset($data[$f]) || $data[$f] === '') {
                            $this->addError($f, $req['error']);
                            $passed = false;
                            continue 2; // 继续,不执行规则验证,值都没得,还验证个串串
                        }
                    }
                }
            }

            // 规则验证,存在才验证,必要性验证已经处理过了
            if (isset($data[$f]) && $data[$f] !== '' && isset($v['rule'])) {
                foreach ($v['rule'] as $r) {
                    if ($this->_checkScene($r['scene'])) {
                        $validator = $r['content'];
                        if (
                            (is_string($validator) and call_user_func($validator, $data[$f]) === false) or // 调用函数
                            (is_array($validator) and call_user_func($validator, $data[$f], $f, $data) === false) or // 调用实例方法
                            ($validator instanceof Filter and $validator->validate($data[$f]) === false and !$data[$f] instanceof Calculator) // 调用验证库
                        ) {
                            $this->addError($f, $r['error']);
                            $passed = false;
                            // 此处没有continue之类的东东,因为要把该字段所有验证执行完
                        }
                    }
                }
            }
        }

        return $passed;
    }

    /**
     * 获得模型实例,此操作未作数据验证
     * @param array|null $data
     * @param string $scene 场景
     * @return static
     */
    public static function factory(array $data = null, string $scene = ''): static {
        $cls = static::class;
        return new $cls($data, $scene);
    }

    /**
     * 返回数组格式的数据
     * @param bool $filterNull 是否过滤NULL的数据
     * @return array
     */
    public function toArray(bool $filterNull = false): array {
        return $this->asArray($filterNull);
    }

    public function asArray(bool $filterNull = false): array {
        $fields = $this->_getFields();
        $_data = [];
        foreach ($fields as $prop) {
            $name = $prop->getName();
            if ($this->_isGhostField($name)) {
                continue; // 排除鬼魂字段
            }
            if (!$prop->isInitialized($this)) {
                if (!$filterNull) {
                    $_data[$name] = null;
                }
                continue;
                //$this->$f = null;
                //continue; // 过滤null字段
            }
            $value = $this->$name;
            if ($filterNull && !is_array($value)) {
//                if (is_null($this->$f)) {
//                    continue; // 过滤null字段
//                }
                if (is_string($value) && 'null' == strtolower($value)) {
                    continue; // 过滤null字段
                }
                if ($this->_isSkipField($name)) {
                    continue; // 排除skip字段
                }
            }
            $_data[$name] = $value;
        }
        return $_data;
    }

    /**
     * 设置场景
     * @param string $scene
     */
    public function setScene(string $scene): void {
        $this->_scene = $scene;
    }

    /**
     * 批量赋值字段
     * @param array $data
     * @param bool $validate 是否验证数据
     * @return bool
     */
    public function install(array $data, bool $validate = true): bool {
        $fields = $this->_getFields();
        $_data = [];

        foreach ($fields as $prop) {
            $name = $prop->getName();
            $format = $this->_validate[$name]['format'] ?? null;

            if (!is_null($format) && $format[0]['type'] == 'json') {
                if (array_key_exists($name, $data)) {
                    if (is_null($data[$name])) {
                        $value = null;
                    } elseif (!is_array($data[$name])) {
                        $value = JsonHelper::recover($data[$name]);
                    } else {
                        $value = $data[$name];
                    }
                } else {
                    $value = null;
                }
            } else {
                if (array_key_exists($name, $data)) {
                    $value = $data[$name];
                } elseif ($prop->isInitialized($this)) {
                    $value = $this->$name;
                } else {
                    $value = null;
                }
            }
            $_data[$name] = $value;
            $this->_assignField($prop, $name, $value);
        }
        // 后验证
        if ($validate && !$this->validate($_data)) {
            return false;
        }
        return true;
    }

    /**
     * 添加错误
     * @param string $field
     * @param string $error
     */
    public function addError(string $field, string $error): void {
        $this->_errors[$field] = $error;
    }

    /**
     * 是否存在错误
     * @param null $filed
     * @return bool
     */
    public function hasError($filed = null): bool {
        if (is_null($filed)) {
            return count($this->_errors) > 0;
        } else {
            return isset($this->_errors[$filed]);
        }
    }

    /**
     * 获取第一条错误
     * @return string|null
     */
    public function getError(): ?string {
        return $this->_errors ? array_values($this->_errors)[0] : null;
    }

    /**
     * 获取全部错误
     * @return array|null
     */
    public function getErrors(): ?array {
        return $this->_errors ? $this->_errors : null;
    }

    /**
     * 分析验证规则
     */
    protected function _parseValidateRules(): void {
        $clsName = static::class;
        if (!isset(self::$_validateTemplateCache[$clsName])) {
            self::$_validateTemplateCache[$clsName] = $this->_buildValidateTemplate();
        }
        $template = self::$_validateTemplateCache[$clsName];
        $this->_validate = [];
        foreach ($template as $name => $rules) {
            $this->_validate[$name] = [];
            if (isset($rules['skip'])) {
                $this->_validate[$name]['skip'] = $rules['skip'];
            }
            if (isset($rules['ghost'])) {
                $this->_validate[$name]['ghost'] = $rules['ghost'];
            }
            if (isset($rules['required'])) {
                $this->_validate[$name]['required'] = $rules['required'];
            }
            if (isset($rules['format'])) {
                $this->_validate[$name]['format'] = $rules['format'];
            }
            if (isset($rules['default'])) {
                foreach ($rules['default'] as $def) {
                    $value = $this->_resolveDefaultValue($def['type'], $def['raw']);
                    $this->_validate[$name]['default'][] = [
                        'content' => $value,
                        'scene' => $def['scene'],
                    ];
                }
            }
            if (isset($rules['rule'])) {
                foreach ($rules['rule'] as $rule) {
                    if (!self::$_filterRegistered) {
                        Filter::registerFilter('chs_string', '\Scf\Core\Filters\ChineseString');
                        self::$_filterRegistered = true;
                    }
                    $validator = match ($rule['kind']) {
                        'func' => $rule['raw'],
                        'method' => [$this, $rule['raw']],
                        'string' => Filter::factory(str_replace('string', 'chs_string', $rule['raw'])),
                        default => Filter::factory($rule['raw']),
                    };
                    $this->_validate[$name]['rule'][] = [
                        'content' => $validator,
                        'error' => $rule['error'],
                        'scene' => $rule['scene'],
                    ];
                }
            }
        }
    }

    /**
     * 获得子类中的字段
     * @return ReflectionProperty[]
     */
    protected function _getFields(): array {
        $clsName = static::class;
        if (!isset(self::$_fieldsCache[$clsName])) {
            $cls = new ReflectionClass($this);
            self::$_fieldsCache[$clsName] = $cls->getProperties(ReflectionProperty::IS_PUBLIC);
        }
        return self::$_fieldsCache[$clsName];
    }

    /**
     * 设置字段默认值
     */
    protected function _setDefault(): void {
        if ($this->_validate) {
            foreach ($this->_validate as $f => $v) {
                if (isset($v['default'])) {
                    foreach ($v['default'] as $def) {
                        if ($this->_checkScene($def['scene'])) {
                            $this->$f = $def['content'];
                        }
                    }
                }
            }
        }
    }

    /**
     * 检查是适用当前场景
     * 逻辑为:
     *  如果设置了当前场景,那么当前场景的设置或者未指定场景的设置会被应用
     *  否者,只有未指定场景的设置会被应用
     * @param $scene
     * @return bool
     */
    protected function _checkScene($scene): bool {
        return $scene == '' || $this->_scene == $scene;
    }

    /**
     * 是否为魔鬼字段
     * @param $field
     * @return bool
     */
    #[Pure]
    protected function _isGhostField($field): bool {
        if (isset($this->_validate[$field]['ghost'])) {
            foreach ($this->_validate[$field]['ghost'] as $v) {
                if ($this->_checkScene($v['scene'])) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 是否为跳过的字段
     * @param $field
     * @return bool
     */
    #[Pure] protected function
    _isSkipField($field): bool {
        if (isset($this->_validate[$field]['skip'])) {
            foreach ($this->_validate[$field]['skip'] as $v) {
                if ($this->_checkScene($v['scene'])) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param ReflectionProperty $prop
     * @param string $name
     * @param mixed $value
     */
    protected function _assignField(ReflectionProperty $prop, string $name, mixed $value): void {
        try {
            $this->$name = $value;
        } catch (Error $error) {
            $type = $prop->getType();
            $allowsNull = !$type || $type->allowsNull();
            if ($allowsNull) {
                $this->$name = null;
            } else {
                $this->addError($name, $error->getMessage());
            }
        }
    }

    /**
     * @return array<string, array>
     */
    protected function _buildValidateTemplate(): array {
        $template = [];
        $fields = $this->_getFields();
        foreach ($fields as $f) {
            $name = $f->getName(); // 字段名称
            $comment = $f->getDocComment(); // 字段规则注释
            $matches = null;
            if (!$comment) {
                continue;
            }
            preg_match_all('/@(default|rule|required|skip|ghost|format)(?:\[(\w+)\])?\s+?(.+)/', $comment, $matches);
            if (!$matches) {
                continue;
            }
            $template[$name] = [];
            for ($i = 0; $i < count($matches[0]); $i++) {
                $rn = trim($matches[1][$i]); // 指令名称
                $rs = trim($matches[2][$i]); // 指令场景
                $rc = trim($matches[3][$i]); // 规则内容

                switch ($rn) {
                    // 跳过
                    case 'skip':
                        $template[$name]['skip'][] = [
                            'scene' => $rs
                        ];
                        break;
                    // 鬼魂字段
                    case 'ghost':
                        $template[$name]['ghost'][] = [
                            'scene' => $rs
                        ];
                        break;
                    // 默认值
                    case 'default':
                        $rc = explode(':', $rc, 2);
                        $t = trim($rc[0]); // 类型:int,float,null,string
                        $v = isset($rc[1]) ? trim($rc[1]) : null; // 值

                        if (!is_null($v)) {
                            $template[$name]['default'][] = [
                                'type' => $t,
                                'raw' => $v,
                                'scene' => $rs,
                            ];
                        }
                        break;
                    // 规则,直接使用ircmaxell/filterus的字符串参数
                    case 'rule':
                        $rc = explode('|', $rc, 2);
                        $rc[0] = trim($rc[0]);
                        $kind = match (true) {
                            str_starts_with($rc[0], 'func') => 'func',
                            str_starts_with($rc[0], 'method') => 'method',
                            str_starts_with($rc[0], 'string') => 'string',
                            default => 'filter',
                        };
                        $raw = match ($kind) {
                            'func' => substr($rc[0], 5),
                            'method' => substr($rc[0], 7),
                            default => $rc[0],
                        };
                        $template[$name]['rule'][] = [
                            'kind' => $kind,
                            'raw' => $raw,
                            'error' => $rc[1] ?? "{$name}格式不正确",
                            'scene' => $rs,
                        ];
                        break;
                    //必填字段
                    case 'required':
                        $rc = explode('|', $rc);
                        $template[$name]['required'][] = [
                            'content' => true,
                            'scene' => $rs,
                            'error' => $rc[1] ?? "{$name}不能为空",
                        ];
                        break;
                    //数据转换
                    case 'format':
                        $template[$name]['format'][] = [
                            'type' => $rc
                        ];
                        break;
                }
            }
        }
        return $template;
    }

    /**
     * @param string $type
     * @param string|null $raw
     * @return mixed
     */
    protected function _resolveDefaultValue(string $type, ?string $raw): mixed {
        if (is_null($raw)) {
            return null;
        }
        return match ($type) {
            'int' => intval($raw),
            'float' => floatval($raw),
            'null' => null,
            'func' => call_user_func($raw),
            'method' => call_user_func_array([$this, $raw], []),
            'array' => json_decode($raw, true),
            'bool' => $raw === 'true',
            default => $raw,
        };
    }
}
