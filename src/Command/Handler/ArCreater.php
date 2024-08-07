<?php

namespace Scf\Command\Handler;

use JetBrains\PhpStorm\NoReturn;
use Scf\Command\Color;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Database\Pdo;
use Scf\Helper\ObjectHelper;
use Throwable;

class ArCreater {
    protected string $table;
    protected string $path;

    protected array $config = [];


    public function run(): void {
        $this->setDB();
    }

    /**
     * 设置数据库
     * @param $db
     * @return mixed
     */
    public function setDB($db = null): mixed {
        $allDB = Config::get('database')['mysql'] ?? [];
        $options = [];
        foreach ($allDB as $k => $c) {
            $options[$k] = $k;
        }
        $input = Console::select($options, ($db ?: 'default'), label: "请选择数据库");
        Console::line();
        try {
            //查找验证数据库
            $this->setConfig('db', $input);
            if (empty($this->db())) {
                Console::error($input . '=>数据库连接失败!');
                return $this->setDB();
            }
            Console::success('#' . $input . '[' . $allDB[$input]['name'] . ']连接成功');
            Console::line();
            return $this->setTABLE();
        } catch (\PDOException $e) {
            Console::error($input . '=>数据库连接失败:' . $e->getMessage());
        } catch (\Swoole\ExitException $exception) {
            Console::warning($exception->getMessage());
            return false;
        } catch (Throwable $exception) {
            Console::error($input . '=>数据库连接失败:' . $exception->getMessage());
        }
        Console::line();
        return $this->setDB();
    }

    /**
     * 设置数据表
     * @return mixed
     */
    protected function setTABLE(): mixed {
        $input = Console::input("请输入表名称", hint: '表名称无需添加修饰前缀,输入"<"返回上一步');
        if ($input == '<') {
            return $this->setDB();
        }
        //拼接完整表名
        $tablePrefix = $this->db()->getConfig('prefix');
        $completeTable = $tablePrefix . $input;
        //验证表名
        $dbName = $this->db()->getConfig('name');
        Console::line();
        try {
            $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
            $result = $this->db()->getDatabase()->exec($sql)->get();;
            $this->setConfig('columns', $result);
        } catch (Throwable $e) {
            Console::error('读取数据表失败:' . $e->getMessage());
            Console::line();
            return $this->setTABLE();
        }
        Console::success('数据表连接成功');
        Console::line();
        $this->setConfig('table', $input);
        $this->setConfig('prefix', $tablePrefix);
        return $this->setPATH();
    }

    /**
     * 设置文件路径
     * @param string $label
     * @return mixed
     */
    protected function setPATH(string $label = '请输入要创建的文件路径'): mixed {
        //拼接完整表名
        $tablePrefix = $this->db()->getConfig('prefix');
        $fileName = $this->getConfig('db') . '_' . $tablePrefix . $this->getConfig('table');
        $cache = Config::getDbTable($fileName, 'dao');
        $input = Console::input($label, default: $this->getConfig('path') ?: $cache, hint: '路径需严格遵循驼峰命名约定,示范:Demo/Dao/TestDao');
        //分析检查路径
        $nameSpaceArr = explode('/', $input);
        if (count($nameSpaceArr) < 2) {
            return $this->setPATH('路径不合法,请严格遵循规范创建');
        }
        Config::setDbTable($fileName, [
            'dao' => $input,
            'db' => $this->getConfig('db'),
            'table' => $tablePrefix . $this->getConfig('table'),
            'version' => date('YmdHis')
        ]);
        $this->setConfig('path', $input);
        $pathArr = explode('/', $input);
        $className = $pathArr[count($pathArr) - 1];
        $filePath = APP_PATH . "/src/lib/";
        $nameSpace = [];
        foreach ($nameSpaceArr as $k => $v) {
            if ($k == count($nameSpaceArr) - 1) {
                continue;
            }
            $nameSpace[] = $v;
            $filePath .= $v . '/';
        }
        $nameSpace = implode('\\', $nameSpace);
        $filePath = substr($filePath, 0, -1);
        if (!is_dir($filePath) && (!mkdir($filePath, 0777, true) || !chmod($filePath, 0777))) {
            Console::warning('创建文件夹出错,请确认拥有写入权限后重试');
            Console::line();
            return $this->setPATH();
        }
        if (file_exists($filePath . '/' . $className . '.php')) {
            $choose = Console::select(['使用别的路径/文件名称', '覆盖现有文件'], 2, label: "文件[" . $filePath . "/" . $className . ".php]已存在,请选择下一步操作");
            if (!$choose || $choose > 2) {
                Console::warning('请输入正确的选择');
                Console::line();
                return $this->setPATH();
            }
            if ($choose == 1) {
                return $this->setPATH();
            }
        }
        $this->setConfig('name_space', $nameSpace);
        $this->setConfig('class_name', $className);
        $this->setConfig('file_path', $filePath);
        return $this->createFILE();
    }

    protected function createFILE() {
        $db = $this->getConfig('db');
        $table = $this->getConfig('table');
        $completeTable = $this->getConfig('prefix') . $table;
        $filePath = $this->getConfig('file_path');
        $nameSpace = $this->getConfig('name_space');
        $className = $this->getConfig('class_name');
        if (!$this->db()) {
            Console::warning('数据库连接超时,请输入数据库名称');
            Console::line();
            return $this->setDB();
        }
        if (!$table) {
            Console::write('请输入数据表名称');
            Console::line();
            return $this->setTABLE();
        }
        $dbName = $this->db()->getConfig('name');
        // 获取表结构信息
        try {
            $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
            $columns = $this->db()->getDatabase()->exec($sql)->get();
        } catch (\PDOException $e) {
            Console::write('读取数据表失败:' . $e->getMessage() . ',请确认输入无误后重试');
            Console::line();
            return $this->setTABLE();
        } catch (Throwable $e) {
            return $this->setTABLE();
        }
        //拼接字段
        $bodyContent = "";
        $primaryKey = "";
        $columns = ObjectHelper::toArray($columns);
        $primaryKeys = [];
        $columnMaps = [];
        $createSql = "CREATE TABLE `{$completeTable}` (";
        $afterField = null;
        foreach ($columns as $v) {
            $attributes = [
                "`" . $v['Field'] . "`",
                $v['Type']
            ];
            if ($v['Null'] === 'NO') {
                $attributes[] = "NOT NULL";
            }
            $skipDefault = ['text', 'blob', 'json', 'geometry'];
            if ($v['Default'] !== '') {//!in_array(strtolower($v['Type']), $skipDefault)
                $attributes[] = "DEFAULT '{$v['Default']}'";
            }
            if (!empty($v['Extra'])) {
                $attributes[] = strtoupper($v['Extra']);
            }
            if (!empty($v['Comment'])) {
                $attributes[] = "COMMENT '{$v['Comment']}'";
            }
            $attributesStr = implode(' ', $attributes);
            $columnMaps[$v['Field']] = [
                'content' => $attributesStr . ($afterField ? " AFTER `{$afterField}`" : ''),
                'hash' => md5(serialize($attributes))
            ];
            // 拼接字段信息到 SQL 语句
            $createSql .= $attributesStr . ",";
            if ($v['Key'] == 'PRI') {
                !$primaryKey and $primaryKey = $v['Field'];
                $primaryKeys[] = "`{$v['Field']}`";
            }
            $comment = $v['Comment'] ?: $v['Field'];
            $commentArr = explode('|', $comment);
            if (is_array($commentArr)) {
                $fieldName = $commentArr[0];
            } else {
                $fieldName = $comment;
            }
            $isJson = false;
            if ($v['Type'] == 'json') {
                $isJson = true;
            }
            $rule = $this->transformType($v['Type']);
            $bodyContent .= '    /**';
            if ($rule['rule'] == 'int' || $rule['rule'] == 'float') {
                $ruleStr = $rule['rule'] . '|Calculator|null';
            } else {
                $ruleStr = '?' . $rule['rule'];
            }
            $bodyContent .= PHP_EOL . '     * @var ' . $ruleStr . ' ' . $comment;
            if ($rule['max']) {
                if ($rule['rule'] == 'int' || $rule['rule'] == 'float') {
//                    $max = 1;
//                    for ($i = 0; $i < $rule['max']; $i++) {
//                        $max = $max * 10;
//                    }
//                    $max = $max - 1;
//                    $bodyContent .= PHP_EOL . '     * @rule ' . $rule['rule'] . ',max:' . $max . '|' . $fieldName . '不能大于' . $max;
                    $bodyContent .= PHP_EOL . '     * @rule ' . $rule['rule'] . '|' . $fieldName . '只能为数字格式';
                } else {
                    $max = $rule['max'];
                    if ($isJson) {
                        $bodyContent .= PHP_EOL . '     * @rule array|' . $fieldName . '数据格式错误,需求为数组';
                    } else {
                        $bodyContent .= PHP_EOL . '     * @rule ' . $rule['rule'] . ',max:' . $max . '|' . $fieldName . '长度不能大于' . $max . '位';
                    }
                    //$bodyContent .= PHP_EOL . '     * @rule ' . $rule['rule'] . '|' . $fieldName . '只能为浮点数';
                }
            } else {
                if ($isJson) {
                    $bodyContent .= PHP_EOL . '     * @rule array|' . $fieldName . '数据格式错误,需求为数组';
                } else {
                    $bodyContent .= PHP_EOL . '     * @rule ' . $rule['rule'] . '|' . $fieldName . '数据格式错误';
                }
            }
            if ($v['Null'] == 'NO' && $v['Key'] != 'PRI') {
                $bodyContent .= PHP_EOL . '     * @required true|' . $fieldName . '不能为空';
            }
            if ($isJson) {
                $bodyContent .= PHP_EOL . '     * @format json';
            }
            $bodyContent .= PHP_EOL . '     */';
            $bodyContent .= PHP_EOL . '    public ' . $ruleStr . ' $' . $v['Field'] . ';' . PHP_EOL . PHP_EOL;
            $afterField = $v['Field'];
        }
        // 获取表主键信息
        if (count($primaryKeys)) {
            // 拼接主键信息到 SQL 语句
            $createSql .= "PRIMARY KEY (" . implode(',', $primaryKeys) . "), ";
        }
        //获取索引字段
        $indexMaps = [];
        try {
            $sql = "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = '{$completeTable}' AND TABLE_SCHEMA = '{$dbName}'";
            $indexes = $this->db()->getDatabase()->exec($sql)->get();
            if ($indexes) {
                foreach ($indexes as $i => $index) {
                    if ($index['INDEX_NAME'] == 'PRIMARY') {
                        continue;
                    }
                    if (isset($indexMaps[$index['INDEX_NAME']])) {
                        $indexMaps[$index['INDEX_NAME']]['content'] = str_replace('ASC)', "ASC,`{$index['COLUMN_NAME']}` ASC)", $indexMaps[$index['INDEX_NAME']]['content']);
                    } else {
                        $indexMaps[$index['INDEX_NAME']] = [
                            'content' => ($index['NON_UNIQUE'] != 0 ? "INDEX" : "UNIQUE INDEX") . " `{$index['INDEX_NAME']}`(`{$index['COLUMN_NAME']}` ASC) USING {$index['INDEX_TYPE']}" . ($index['INDEX_COMMENT'] ? " COMMENT '{$index['INDEX_COMMENT']}'" : ""),
                        ];
                    }
                    $indexMaps[$index['INDEX_NAME']]['hash'] = md5($indexMaps[$index['INDEX_NAME']]['content']);
                }
            }
        } catch (\PDOException $e) {
            Console::write('数据表索引获取失败:' . $e->getMessage() . ',请确认输入无误后重试');
            Console::line();
            return $this->setTABLE();
        } catch (Throwable $e) {
            return $this->setTABLE();
        }
        if ($indexMaps) {
            foreach ($indexMaps as $indexInfo) {
                $createSql .= $indexInfo['content'] . ", ";
            }
        }
        //去除末尾多余的逗号和空格
        $createSql = rtrim($createSql, ', ');
        $createSql .= ")";
        $space = 'App\\' . $nameSpace;
        //    protected string \$_createSql = "{$createSql}";
        $file = <<<EOF
<?php
namespace {$space};

use Scf\Database\Dao;
use Scf\Database\Tools\Calculator;

class {$className} extends Dao {

    protected string \$_dbName = "{$db}";
    protected string \$_table = "{$table}";
    protected string \$_primaryKey = "{$primaryKey}";
    
{$bodyContent}
}
EOF;
        $fileHandle = fopen($filePath . '/' . $className . '.php', "w");
        $res = fwrite($fileHandle, $file);
        fclose($fileHandle);
        if (!$res) {
            $choose = Console::select(['重试', '重设文件参数', '退出创建工具'], default: 1, label: '文件创建失败,请确认拥有相关路径写入权限后重试');
            if ($choose == 1) {
                return $this->createFILE();
            } elseif ($choose == 3) {
                $this->exit();
            } else {
                $this->resetConfig();
                return $this->setDB();
            }
        }
        Console::line();
        $table = $this->getConfig('db') . '_' . $completeTable;
        Config::setDbTable($table, [
            'create' => $createSql,
            'columns' => $columnMaps,
            'primary' => $primaryKeys,
            'index' => $indexMaps
        ]);
        $choose = Console::select(['创建其它数据结构文件', '创建管理面板Vue文件', '重新创建', '退出创建工具'], default: 3, label: Color::success('文件创建成功!请选择下一步操作'));
        if ($choose == 2) {
            $cpCreater = new CpCreater();
            return $cpCreater->selectDir(APP_PATH . '/cp/src/views', $nameSpace . '\\' . $className);
        } elseif ($choose == 4) {
            $this->exit();
        } elseif ($choose == 3) {
            return $this->setPATH();
        } else {
            $this->resetConfig();
            return $this->setDB($db);
        }
    }

    /**
     * 根据字段类型定义规则
     * @param $type
     * @return array
     */
    protected function transformType($type): array {
        $type = str_replace(" unsigned", "", $type);
        if ($type == 'json') {
            return ['type' => 'json', 'rule' => 'array', 'max' => 0];
        }
        preg_match_all("/^([\s\S]+?)\(([\s\S]+?)\)$/u", $type, $temp);
        if (!isset($temp[2][0])) {
            //return ['type' => 'string', 'rule' => 'string', 'max' => 0];
            $type = strtolower($type);
            $max = 0;
        } else {
            $type = strtolower($temp[1][0]);
            $max = $temp[2][0];
        }
        $rule = match ($type) {
            'bigint', 'tinyint', 'smallint', 'numeric', 'int' => 'int',
            'double', 'decimal', 'float' => 'float',
            'json' => 'array',
            default => 'string',
        };
        if (!is_numeric($max)) {
            $max = 0;
        }
        return ['type' => $type, 'rule' => $rule, 'max' => $max];
    }

    /**
     * @return Pdo
     */
    protected function db(): Pdo {
        $db = $this->getConfig('db');
        return Pdo::master($db);
    }

    protected function setConfig($k, $v): void {
        $k = md5(static::class) . '_' . $k;
        $this->config[$k] = $v;
    }

    /**
     * 获取输入的值
     * @param $k
     * @return mixed|null
     */
    protected function getConfig($k): mixed {
        $k = md5(static::class) . '_' . $k;
        return $this->config[$k] ?? null;
    }

    protected function unsetConfig($k): bool {
        $k = md5(static::class) . '_' . $k;
        if (isset($this->config[$k])) {
            unset($this->config[$k]);
        }
        return true;
    }

    protected function resetConfig(): void {
        $this->config = [];
    }


    #[NoReturn] protected function exit(): void {
        Console::line();
        Console::write('bye!');
        Console::line();
        exit;
    }
}