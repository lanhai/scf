<?php

namespace Scf\Command\Handler;

use JetBrains\PhpStorm\NoReturn;
use Scf\Core\Config;
use Scf\Core\Console;
use Scf\Database\Pdo;
use Scf\Helper\ObjectHelper;

class ArCreater {
    protected string $table;
    protected string $path;

    protected array $config = [];


    public function run(): void {
        Console::line();
        Console::write('请输入数据库配置名称(缺省:default):', false);
        $this->setDB();
    }

    /**
     * 设置数据库
     * @param $db
     * @return mixed
     */
    public function setDB($db = null): mixed {
        $inputDb = Console::input();
        $input = $inputDb ?: ($db ?: 'default');
        try {
            $allDB = Config::get('database')['mysql'] ?? [];
            //查找验证数据库
            $this->setConfig('db', $input);
            if (empty($this->db())) {
                Console::error($input . '=>数据库连接失败!');
                Console::write("请确认输入正确并重试:", false);
                return $this->setDB();
            }
            Console::line();
            Console::success('#' . $input . '[' . $allDB[$input]['name'] . ']连接成功');
            Console::write("请输入表名称(无需加前缀):", false);
            return $this->setTABLE();
        } catch (\PDOException $e) {
            Console::line();
            Console::error($input . '=>数据库连接失败:' . $e->getMessage());
        } catch (\Swoole\ExitException) {
            return false;
        } catch (\Throwable $exception) {
            Console::line();
            Console::error($input . '=>数据库连接失败:' . $exception->getMessage());
        }
        Console::write('请确认输入正确并重试:', false);
        return $this->setDB();
    }

    /**
     * 设置数据表
     * @return mixed
     */
    protected function setTABLE(): mixed {
        $input = Console::input();
        if (!$input) {
            Console::write('请输入正确的数据表名称:', false);
            return $this->setTABLE();
        }
        //拼接完整表名
        $tablePrefix = $this->db()->getConfig('prefix');
        $completeTable = $tablePrefix . $input;
        //验证表明
        $dbName = $this->db()->getConfig('name');
        try {
            $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
            $result = $this->db()->getDatabase()->exec($sql)->get();;
            $this->setConfig('columns', $result);
        } catch (\Throwable $e) {
            Console::line();
            Console::error('读取数据表失败:' . $e->getMessage());
            console::write('请确认输入无误后重试:', false);
            return $this->setTABLE();
        }
        Console::success('数据表连接成功,请输入要创建的文件路径,路径需严格遵循驼峰命名约定,示范:Demo/Dao/TestDao');
        Console::line();
        console::write('输入DAO文件路径:', false);
        $this->setConfig('table', $input);
        $this->setConfig('prefix', $tablePrefix);
        return $this->setPATH();
    }

    /**
     * 设置文件路径
     * @return mixed
     */
    protected function setPATH(): mixed {
        $input = Console::input();
        if (!$input) {
            if (!$input = $this->getConfig('path')) {
                Console::write('请输入正确的文件路径:', false);
                return $this->setPATH();
            }
        }
        //分析检查路径
        $nameSpaceArr = explode('/', $input);
        if (empty($nameSpaceArr)) {
            Console::write('路径不合法,请严格遵循规范创建,示范:Demo/Model/TestAR');
            Console::line();
            return $this->setPATH();
        }
        $this->setConfig('path', $input);
        $pathArr = explode('/', $input);
        $className = $pathArr[count($pathArr) - 1];
        $filePath = APP_PATH . "src/lib/";
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
            Console::write('创建文件夹出错,请确认拥有写入权限后重试(敲击回车重试)');
            Console::line();
            return $this->setPATH();
        }
        if (file_exists($filePath . '/' . $className . '.php')) {
            Console::write("文件[" . $filePath . "/" . $className . ".php]已存在,请选择下一步操作\n1:使用别的路径/文件名称\n2:覆盖现有文件");
            Console::line();
            $choose = intval(Console::input());
            if (!$choose || $choose > 2) {
                Console::write('请输入正确的选择');
                return $this->setPATH();
            }
            if ($choose == 1) {
                Console::write('请输入要创建的文件路径,路径需严格遵循驼峰命名约定,示范:Demo/Model/TestAR');
                Console::line();
                return $this->setPATH();
            }
        }
        $this->setConfig('name_space', $nameSpace);
        $this->setConfig('class_name', $className);
        $this->setConfig('file_path', $filePath);
        return $this->createFILE();
//        Console::write("校验通过,确定要执行创建操作吗\n1:确定创建[" . $filePath . "/" . $className . ".php]\n2:重置文件参数\n3:退出");
//        $choose = intval(Console::input());
//        if (!$choose || $choose > 3) {
//            Console::write('请输入正确的选择');
//            return $this->setPATH();
//        }
//        if ($choose == 1) {
//            return $this->createFILE();
//        } elseif ($choose == 2) {
//            $this->resetConfig();
//            Console::write("请输入数据库名称");
//            return $this->setDB();
//        } else {
//            $this->exit();
//        }
    }

    protected function createFILE() {
        $db = $this->getConfig('db');
        $table = $this->getConfig('table');
        $completeTable = $this->getConfig('prefix') . $table;
        $filePath = $this->getConfig('file_path');
        $nameSpace = $this->getConfig('name_space');
        $className = $this->getConfig('class_name');
        if (!$this->db()) {
            Console::write('数据库连接超时,请输入数据库名称');
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
//        $query = "DESCRIBE $completeTable";
//        $result = $this->db()->getDatabase()->exec($query)->get();

        $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
        $columns = $this->db()->getDatabase()->exec($sql)->get();
        if (!$columns) {
            try {
                $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
                $columns = $this->db()->getDatabase()->exec($sql)->get();
            } catch (\PDOException $e) {
                Console::write('读取数据表失败:' . $e->getMessage() . ',请确认输入无误后重试');
                Console::line();
                return $this->setTABLE();
            }
        }
        //拼接字段
        $bodyContent = "";
        $primaryKey = "";
        $columns = ObjectHelper::toArray($columns);
        $primaryKeys = [];
        $createSql = "CREATE TABLE `{$completeTable}` (";
        foreach ($columns as $v) {
            $fieldName = $v['Field'];
            $fieldType = $v['Type'];
            $skipDefault = ['text', 'blob', 'json', 'geometry'];

            $fieldNull = ($v['Null'] === 'NO') ? 'NOT NULL' : 'NULL';
            // 对于 float 类型，加入明确的默认值 '0.0'
            if (str_contains($fieldType, 'float')) {
                $fieldDefault = "DEFAULT '0.0'";
            } else {
                $fieldDefault = ($v['Default'] !== null) && !in_array(strtolower($v['Type']), $skipDefault) ? "DEFAULT '{$v['Default']}'" : '';
            }

            $fieldComment = ($v['Comment'] !== null) ? "COMMENT '{$v['Comment']}'" : '';
            // 拼接字段信息到 SQL 语句
            $createSql .= "`$fieldName` $fieldType $fieldNull $fieldDefault $fieldComment, ";

            if ($v['Key'] == 'PRI' && !$primaryKey) {
                $primaryKey = $v['Field'];
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
//            $isJson = strpos($comment, '{json') !== false;
//            if ($isJson) {
//                $comment = str_replace("{json}", "", $comment);
//            }
            $nullAble = true;
            if ($v['Null'] == 'NO' || $v['Key'] == 'PRI') {
                $nullAble = false;
            }
            $bodyContent .= '    /**';
            if ($rule['rule'] == 'int' || $rule['rule'] == 'float') {
                $ruleStr = $rule['rule'] . '|Calculator|null';
            } else {
                $ruleStr = '?' . $rule['rule'];
            }
            $bodyContent .= PHP_EOL . '     * @var ' . $ruleStr . ' ' . $comment;
//            if ($nullAble) {
//                $bodyContent .= PHP_EOL . '     * @var ?' . $rule['rule'] . ' ' . $comment;
//            } else {
//                $bodyContent .= PHP_EOL . '     * @var ' . $rule['rule'] . ' ' . $comment;
//            }
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
//            if ($nullAble) {
//                $bodyContent .= PHP_EOL . '    public ?' . $rule['rule'] . ' $' . $v['Field'] . ';' . PHP_EOL . PHP_EOL;
//            } else {
//                $bodyContent .= PHP_EOL . '    public ' . $rule['rule'] . ' $' . $v['Field'] . ';' . PHP_EOL . PHP_EOL;
//            }

        }

        // 获取表主键信息
        if (count($primaryKeys)) {
            // 拼接主键信息到 SQL 语句
            $createSql .= "PRIMARY KEY (" . implode(',', $primaryKeys) . "), ";
        }
        // 去除末尾多余的逗号和空格
        $createSql = rtrim($createSql, ', ');
        $createSql .= ")";
        $space = 'App\\' . $nameSpace;
        $file = <<<EOF
<?php
namespace {$space};

use Scf\Database\Dao;
use Scf\Database\Tools\Calculator;

class {$className} extends Dao {

    protected string \$_dbName = "{$db}";
    protected string \$_table = "{$table}";
    protected string \$_primaryKey = "{$primaryKey}";
    protected string \$_createSql = "{$createSql}";
    
{$bodyContent}
}
EOF;
        $fileHandle = fopen($filePath . '/' . $className . '.php', "w");
        $res = fwrite($fileHandle, $file);
        fclose($fileHandle);
        if (!$res) {
            Console::write("文件创建失败,请确认拥有相关路径写入权限后重试\n1:重试\n2:重设文件参数\n3:退出创建工具");
            Console::line();
            $choose = intval(Console::input());
            if ($choose == 1) {
                return $this->createFILE();
            } elseif ($choose == 3) {
                $this->exit();
            } else {
                $this->resetConfig();
                Console::write("请输入数据库名称");
                return $this->setDB();
            }
        }
        Console::line();
        Console::success("文件创建成功!请选择下一步操作\n");
        Console::write("1:创建其它数据结构文件(回车)\n2:创建管理面板Vue文件\n3:退出创建工具\n输入选项序号(回车选1):", false);
        $choose = intval(Console::input());
        if ($choose == 2) {
            $cpCreater = new CpCreater();
            return $cpCreater->selectDir(APP_PATH . 'cp/src/views', $nameSpace . '\\' . $className);
        } elseif ($choose == 3) {
            $this->exit();
        } else {
            $this->resetConfig();
            Console::line();
            Console::write("请输入数据库名称(回车继续在'{$db}'下创建):", false);
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

    protected function resetConfig() {
        $this->config = [];
    }

    /**
     * 输出消息到控制台
     * @param $msg
     */
    protected function print($msg) {
        Console::write("------------------------------------------------【" . date('H:i:s', time()) . "】------------------------------------------------\n" . $msg . "\n------------------------------------------------------------------------------------------------------------");
    }

    #[NoReturn] protected function exit(): void {
        $this->print('bye!');
        exit;
    }
}