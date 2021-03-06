<?php

namespace Scf\Command\Handler;

use JetBrains\PhpStorm\NoReturn;
use Scf\Core\Console;
use Scf\Database\Pdo;
use Scf\Helper\ObjectHelper;
use const APP_PATH;


class ArCreater {
    protected string $table;
    protected string $path;

    protected array $config = [];


    public function run() {
        Console::write('请输入数据库名称(缺省:default)');
        Console::line();
        $this->setDB();
    }

    /**
     * 设置数据库
     * @return mixed
     */
    public function setDB(): mixed {
        $input = Console::input() ?: 'default';
        //查找验证数据库
        $this->setConfig('db', $input);
        if (empty($this->db())) {
            Console::write($input . '=>数据库连接失败,请确认输入正确并重试');
            return $this->setDB();
        }
        Console::write('数据库连接成功,请输入数据表名称(无需加前缀)');
        Console::line();
        return $this->setTABLE();
    }

    /**
     * 设置数据表
     * @return mixed
     */
    protected function setTABLE(): mixed {
        $input = Console::input();
        if (!$input) {
            Console::write('请输入正确的数据表名称');
            Console::line();
            return $this->setTABLE();
        }
        //拼接完整表名
        $tablePrefix = $this->db()->getConfig('prefix');
        $completeTable = $tablePrefix . $input;
        //验证表明
        $dbName = $this->db()->getConfig('name');
        try {
            $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
            $result = $this->db()->connection()->exec($sql)->get();;
            $this->setConfig('columns', $result);
        } catch (\PDOException $e) {
            Console::write('读取数据表失败:' . $e->getMessage() . ',请确认输入无误后重试');
            Console::line();
            return $this->setTABLE();
        }
        Console::write('数据表连接成功,请输入要创建的文件路径,路径需严格遵循驼峰命名约定,示范:Demo/Model/TestAR');
        Console::line();
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
                Console::write('请输入正确的文件路径');
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
        if (!is_dir($filePath) && !mkdir($filePath)) {
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
                Console::write('请输入要创建的文件路径,路径需严格遵循驼峰命名约定,示范:Common/Struct/TestAR');
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
        $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
        $columns = $this->db()->connection()->exec($sql)->get();
        if (!$columns) {
            try {
                $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
                $columns = $this->db()->connection()->exec($sql)->get();
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
        foreach ($columns as $v) {
            if ($v['Key'] == 'PRI' && !$primaryKey) {
                $primaryKey = $v['Field'];
            }
            $comment = $v['Comment'] ? $v['Comment'] : $v['Field'];
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
            $bodyContent .= PHP_EOL . '     * @var ?' . $rule['rule'] . ' ' . $comment;
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
            $bodyContent .= PHP_EOL . '    public ?' . $rule['rule'] . ' $' . $v['Field'] . ';' . PHP_EOL . PHP_EOL;
//            if ($nullAble) {
//                $bodyContent .= PHP_EOL . '    public ?' . $rule['rule'] . ' $' . $v['Field'] . ';' . PHP_EOL . PHP_EOL;
//            } else {
//                $bodyContent .= PHP_EOL . '    public ' . $rule['rule'] . ' $' . $v['Field'] . ';' . PHP_EOL . PHP_EOL;
//            }

        }
        $space = 'App\\' . $nameSpace;
        $file = <<<EOF
<?php
namespace {$space};

use Scf\Database\Query;

class {$className} extends Query {

    protected string \$_dbName = "{$db}";
    protected string \$_table = "{$table}";
    protected string \$_primaryKey = "{$primaryKey}";
    
{$bodyContent}
}
EOF;
        $fileHandle = fopen($filePath . '/' . $className . '.php', "w");
        $res = fwrite($fileHandle, $file);
        fclose($fileHandle);
        if ($res == false) {
            Console::write("文件创建失败,请确认拥有相关路径写入权限后重试\n1:重试\n2:重设文件参数\n3:退出");
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
        Console::write("文件创建成功!请选择下一步操作\n1:创建管理面板Vue文件\n2:创建其它数据结构文件\n3:退出");
        Console::line();
        $choose = intval(Console::input());
        if ($choose == 1) {
            $cpCreater = new CpCreater();
            return $cpCreater->selectDir(APP_PATH . 'cp/src/views', $nameSpace . '\\' . $className);
        } elseif ($choose == 3) {
            $this->exit();
        } else {
            $this->resetConfig();
            Console::write("请输入数据库名称");
            Console::line();
            return $this->setDB();
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
            return ['type' => 'string', 'rule' => 'string', 'max' => 0];
        }
        $max = $temp[2][0];
        switch ($temp[1][0]) {
            case 'bigint':
            case 'tinyint':
            case 'smallint':
            case 'numeric':
            case 'int':
                $rule = 'int';
                break;
            case 'double':
            case 'decimal':
            case 'float':
                $rule = 'float';
                break;
            case 'json':
                $rule = 'array';
                break;
            default:
                if ($type == 'json') {
                    $rule = 'array';
                } else {
                    $rule = 'string';
                }
                break;
        }
        if (!is_numeric($max)) {
            $max = 0;
        }
        return ['type' => $temp[1][0], 'rule' => $rule, 'max' => $max];
    }

    /**
     * @return Pdo
     */
    protected function db(): Pdo {
        $db = $this->getConfig('db');
        return Pdo::master($db);
    }

    protected function setConfig($k, $v) {
        $k = md5(get_called_class()) . '_' . $k;
        $this->config[$k] = $v;
    }

    /**
     * 获取输入的值
     * @param $k
     * @return mixed|null
     */
    protected function getConfig($k): mixed {
        $k = md5(get_called_class()) . '_' . $k;
        return $this->config[$k] ?? null;
    }

    protected function unsetConfig($k): bool {
        $k = md5(get_called_class()) . '_' . $k;
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

    #[NoReturn] protected function exit() {
        $this->print('bye!');
        exit;
    }
}