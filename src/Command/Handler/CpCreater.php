<?php


namespace Scf\Command\Handler;

use Scf\Core\Config;
use Scf\Database\Pdo;
use Scf\Helper\StringHelper;
use Scf\Mode\Web\App;
use Scf\Root;

class CpCreater extends Base {
    protected string $tplList = "\n1:动态数据管理模板(根据配置展示/编辑相关字段)\n2:标准数据管理模板";

    /**
     * 选择模块
     * @param string $path
     * @param string $ar
     * @return mixed
     */
    public function selectDir(string $path = APP_PATH . 'cp/src/views', string $ar = ""): mixed {
        App::loadModules();
        if ($ar) {
            $this->setConfig('ar_namespace', $ar);
        }
        $dirList = [];
        $dirStrList = [];
        if (!is_dir($path)) {
            exit('管理面板目录不存在:' . $path . ',请确保管理面板目录位于和当前app应用路径的同级client目录下');
        }
        $i = 1;
        if ($list = scandir($path)) {
            foreach ($list as $dir) {
                if ($dir === '.' || $dir === '..' || str_contains($dir, '.')) {
                    continue;
                }
                $dirList[] = $dir;
                $dirStrList[] = $i . ':' . $dir;
                $i++;
                //$dirList[] = ['path' => $path . '/' . $dir, 'sub' => $this->dirList($path . '/' . $dir)];
            }
        }
        $dirStr = implode("\n", $dirStrList);
        $this->setConfig('module_dirs', $dirList);
        $this->setConfig('target_path', $path);
        $this->print("请选择要创建文件的模块,若创建新模块请直接输入模块名称\n" . $dirStr);
        return $this->setModule();
    }

    /**
     * 设置模块
     * @return mixed
     */
    public function setModule(): mixed {
        $input = $this->receive();
        if (!$input) {
            $this->print('请选择正确的模块编号');
            return $this->setModule();
        }
        if (!is_numeric($input)) {
            $input = StringHelper::camel2lower($input);
        } else {
            $modules = $this->getConfig('module_dirs');
            if (!isset($modules[$input - 1])) {
                $this->print('请选择正确的模块编号');
                return $this->setModule();
            }
            $input = $modules[$input - 1];
        }
        $path = $this->getConfig('target_path') . '/' . $input;
        if (!is_dir($path)) {
            if (!mkdir($path)) {
                $this->print('文件夹[' . $path . ']创建失败,请确保拥有写入权限后重试[回车]');
                $this->receive();
                return $this->setModule();
            }
            $this->print("文件夹创建成功!将在:" . $path . "目录下创建文件,请选择预设模板" . $this->tplList);
        } else {
            $this->print("将在:" . $path . "目录下创建文件,请选择预设模板" . $this->tplList);
        }
        $this->setConfig('module_name', $input);
        $this->setConfig('module_path', $path);
        return $this->createController();
    }

    /**
     * 创建文件
     * @return mixed
     */
    protected function createController(): mixed {
        $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;

        $input = $this->receive();
        if (!$input) {
            $this->print('请选择正确的预设模板');
            return $this->createController();
        }
        if (!is_numeric($input)) {
            $this->print("请输入正确的预设模板编号" . $this->tplList);
            return $this->createController();
        }
        $tplType = intval($input);
        $modulePath = $this->getConfig('module_path') . '/';
        $this->print('请输入文件名称(驼峰格式,无需带后缀)');
        $fileName = $this->receive();
        $this->setConfig('controller_name', $fileName);
        $controllerFile = $modulePath . StringHelper::lower2camel($fileName) . '.vue';
        if (file_exists($controllerFile)) {
            $this->print("文件[" . $controllerFile . "]已存在,请选择下一步操作\n1:创建其它文件\n2:覆盖当前文件");
            $input = intval($this->receive());
            if (!$input || $input > 2) {
                return $this->createController();
            }
            if ($input == 1) {
                $this->unsetConfig('controller_name');
                $this->print('请输入要创建的控制器文件名称');
                return $this->createController();
            }
        }
        //绑定数据操作
        $this->setConfig('render_map_setting', 'render/demo.json');
        $this->setConfig('render_db', 'default');
        $this->setConfig('render_table', 'api');
        //模板文件选择
        if ($tplType == 1) {
            $controllerFileContent = file_get_contents(Root::dir() . '/Command/Template/CpViewTpl.vue');
            if (!$arFile = $this->getConfig('ar_namespace')) {
                $this->print("请输入AR文件路径");
                $arFile = $this->receive();
            }
            if ($arFile) {
                if (!file_exists(APP_LIB_PATH . str_replace("\\", "/", $arFile) . '.php')) {
                    $this->print("AR文件不存在:" . APP_LIB_PATH . str_replace("\\", "/", $arFile) . '.php');
                    return $this->createController();
                }
                $nameSpaceArr = explode('/', $arFile);
                $nameSpace = [];
                foreach ($nameSpaceArr as $v) {
                    $nameSpace[] = $v;
                }
                $nameSpace = '\\App\\' . implode('\\', $nameSpace);
                $dbConfig = $nameSpace::factory()->config();
                $db = Pdo::master($dbConfig['db']);
                //拼接完整表名
                $tablePrefix = $db->getConfig('prefix');
                $completeTable = $tablePrefix . $dbConfig['table'];
                //验证表名
                $dbName = $db->getConfig('name');
                try {
                    $sql = "SHOW FULL COLUMNS FROM {$completeTable} FROM {$dbName}";
                    $columns = $db->getDatabase()->exec($sql)->get();
                    $this->setConfig('columns', $columns);
                } catch (\Throwable $e) {
                    $this->print('读取数据表失败:' . $e->getMessage() . ',请确认数据库链接可用');
                    return $this->setModule();
                }
                $mapSetting = [
                    'dialogWidth' => '50%',
                    'dialogTitle' => '数据',
                    'alias' => '在此填写权限节点别名',
                    'searchKeywordExplain' => '关键字',
                    'searchKeywordColumns' => [],
                    'autoLoad' => true,
                    'createable' => true,
                    'props' => [],
                ];
                foreach ($columns as $item) {
                    $mapSetting['props'][] = [
                        'label' => !empty($item['Comment']) ? $item['Comment'] : $item['Field'],
                        'type' => 'input',
                        'prop' => $item['Field'],
                        'span' => 8,
                        //'filter' => '',
                        'default' => !empty($item['Default']) ? $item['Default'] : '',
                        'display' => true,
                        'edit' => !($item['Field'] == $dbConfig['primary_key']),
//                        'options' => [
//                            ['label' => '', 'value' => '']
//                        ]
                    ];
                }
                $settingFileName = StringHelper::camel2lower($this->getConfig('module_name')) . '_' . StringHelper::camel2lower($fileName) . ".json";
                $jsonFile = APP_PATH . "cp/src/data/render/" . $settingFileName;
                if (!$this->writeFile($jsonFile, json_encode($mapSetting, 256))) {
                    $this->print("文件[" . $jsonFile . "]创建失败!,请确认拥有相关权限后重试!");
                    return $this->createAuthNode();
                }
                $this->setConfig('setting_file_path', $jsonFile);
                $this->setConfig('render_map_setting', 'render/' . $settingFileName);
                $this->setConfig('render_db', $dbConfig['db']);
                $this->setConfig('render_table', $dbConfig['table']);
                $this->setConfig('ar_namespace', $nameSpace);
            }
        } else {
            $this->print('请输入后端接口控制器文件名称(驼峰)');
            $apiControllerName = $this->receive();
            if (!$apiControllerName) {
                return $this->createController();
            }
            $apiModuleName = StringHelper::lower2camel($this->getConfig('module_name'));
            $apiControllerName = StringHelper::lower2camel($apiControllerName);
            if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
                $apiControllerPath = APP_LIB_PATH . 'Controller/Admin/' . StringHelper::lower2camel($apiModuleName);
            } else {
                $apiControllerPath = APP_LIB_PATH . 'Admin/Controller/' . StringHelper::lower2camel($apiModuleName);
            }
            $this->print("回车创建控制器文件[" . $apiControllerName . ".php]到:" . $apiControllerPath . "目录,若需要在其它目录创建,请输入文件夹名称");
            $dirName = $this->receive();
            if ($dirName) {
                $apiModuleName = $dirName;
                if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
                    $apiControllerPath = APP_LIB_PATH . 'Controller/Admin/' . StringHelper::lower2camel($dirName);
                } else {
                    $apiControllerPath = APP_LIB_PATH . 'Admin/Controller/' . StringHelper::lower2camel($dirName);
                }
            }
            if (!is_dir($apiControllerPath) && !mkdir($apiControllerPath, 0777, true)) {
                $this->print('创建文件夹[' . $apiControllerPath . ']失败!,请确认拥有相关权限后重试[回车]');
                $this->receive();
                return $this->createController();
            }
            $apiControllerFile = $apiControllerPath . '/' . $apiControllerName . '.php';
            if (file_exists($apiControllerFile)) {
                $this->print("文件[" . $apiControllerFile . "]已存在,请选择下一步操作\n1:创建其它文件\n2:覆盖当前文件\n3:退出");
                $input = intval($this->receive());
                if (!$input || $input > 2) {
                    $this->exit();
                }
                if ($input == 1) {
                    $this->print('请输入要创建的控制器文件名称(驼峰格式)');
                    $apiControllerName = $this->receive();
                    if (!$apiControllerName) {
                        $this->print('输入错误');
                        return $this->createController();
                    }
                    $apiControllerFile = $apiControllerPath . '/' . $apiControllerName . '.php';
                }
            }
            $this->setConfig('api_url', '/admin/' . StringHelper::camel2lower($apiModuleName) . '/' . StringHelper::camel2lower($apiControllerName) . '/');
            $this->setConfig('auth_action_path', StringHelper::lower2camel($apiModuleName) . "/" . StringHelper::lower2camel($apiControllerName));
            $this->setConfig('api_controller_name', StringHelper::camel2lower($apiControllerName));
            $apiControllerFileContent = file_get_contents(Root::dir() . '/Command/Template/cpController.tpl');
            $apiControllerFileContent = $this->codeFormat($apiControllerFileContent);
            if (!$this->writeFile($apiControllerFile, $apiControllerFileContent)) {
                $this->print("文件[" . $apiControllerFile . "]创建失败!,请确认拥有相关权限后重试!");
                return $this->createController();
            }
            $controllerFileContent = file_get_contents(Root::dir() . '/Command/Template/CpDataListTpl.vue');
        }
        $this->setConfig('file_name', $fileName);
        $controllerFileContent = $this->codeFormat($controllerFileContent);
        if (!$this->writeFile($controllerFile, $controllerFileContent)) {
            $this->print("文件[" . $controllerFile . "]创建失败!,请确认拥有相关权限后重试!选择是否创建模板文件" . $this->tplList);
            return $this->createController();
        }
        $this->print("文件创建成功,请选择下一步操作\n1:绑定权限节点\n2:创建其它文件\n3:退出");
        $input = intval($this->receive());
        if ($input == 1) {
            return $this->createAuthNode();
        } elseif ($input == 2) {
            $this->resetConfig();
            return $this->selectDir();
        }
        $this->exit();
    }


    /**
     * 创建权限节点
     * @return mixed
     */
    protected function createAuthNode(): mixed {
        $moduleStyle = Config::get('app')['module_style'] ?? APP_MODULE_STYLE_LARGE;
        if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
            $result = \App\Model\Admin\CpNodeModel::instance()->all(['parent' => 0]);
        } else {
            $result = \App\Admin\Model\CpNodeModel::instance()->all(['parent' => 0]);
        }
        if (!$result) {
            $this->print("请先添加至少一个可归属的权限节点");
            $this->resetConfig();
            return $this->selectDir();
        }
        $parentsStr = [];
        $i = 1;
        foreach ($result as $item) {
            $parentsStr[] = $i . ":" . $item['name'];
            $i++;
        }
        $nodeStr = implode("\n", $parentsStr);
        $this->print("请选择要绑定的节点分类\n" . $nodeStr);
        $num = intval($this->receive());
        if (!isset($result[$num - 1])) {
            $this->print("请输入正确的分类编号");
            return $this->createAuthNode();
        }
        $parent = $result[$num - 1]['id'];
        if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
            $node = \App\Model\Admin\CpNodeAR::factory();
        } else {
            $node = \App\Admin\Model\CpNodeAR::factory();
        }
        $node->parent = $parent;
        $this->print("请输入节点名称(windows环境暂不支持中文,创建之后至后台更改)");
        $name = $this->receive();
        if (!$name) {
            $this->print("节点名称不能为空");
            return $this->createAuthNode();
        }
        $node->name = $name;
        $node->active = 2;
        $this->print("请选择是否展示到导航菜单,1(回车):展示,2:不展示");
        $isNav = $this->receive();
        if (!$isNav || $isNav == 1) {
            $node->is_nav = 1;
        } else {
            $node->is_nav = 2;
        }
        $this->print("请输入路由访问路径,示范:/home");
        if (!$routePath = $this->receive()) {
            $this->print("访问路径不能为空");
            return $this->createAuthNode();
        }
        if ($actionPath = $this->getConfig('auth_action_path')) {
            $node->active = 3;
            $node->action_path = $actionPath;
        }
        $node->db_render = $this->getConfig('render_db') . ':' . $this->getConfig('render_table');
        $node->ar_namespace = $this->getConfig('ar_namespace');
        $node->route_path = $routePath;
        $node->route_component = 'views/' . $this->getConfig('module_name') . '/' . $this->getConfig('controller_name') . '.vue';
        $node->alias = StringHelper::lower2camel($this->getConfig('module_name') . '_' . $this->getConfig('controller_name') . 'View');
        $node->status = 1;
        $node->created = time();
        $node->created_by = 'dev';
        $node->remark = '待完善';
        $result = $node->save();
        if (!$node->save()) {
            $this->print("权限节点创建失败:" . $node->getError());
            return $this->createAuthNode();
        }
        if ($moduleStyle == APP_MODULE_STYLE_MICRO) {
            \App\Model\ConfigModel::instance()->update(\App\Model\ConfigModel::KEY_AUTH_NODES_VERSION, time());
        } else {
            \App\Common\Model\ConfigModel::instance()->update(\App\Common\Model\ConfigModel::KEY_AUTH_NODES_VERSION, time());
        }
        //更新配置文件
        $hasApi = $this->getConfig('auth_action_path') ?: false;
        if (!$hasApi) {
            $jsonFile = $this->getConfig('setting_file_path');
            if (!file_exists($jsonFile)) {
                $this->print("配置文件读取失败:" . $jsonFile);
                return $this->createAuthNode();
            }
            $setting = json_decode(file_get_contents($jsonFile), true);
            $setting['alias'] = $node->alias;
            if (!$this->writeFile($jsonFile, json_encode($setting, 256))) {
                $this->print("配置文件[" . $jsonFile . "]更新失败!,请确认拥有相关权限后重试!");
                return $this->createAuthNode();
            }
        }
        $this->print("权限节点绑定成功,请选择下一步操作\n1:继续创建\n2:退出");
        $input = intval($this->receive());
        if ($input == 1) {
            $this->resetConfig();
            return $this->selectDir();
        }
        $this->exit();
    }

    /**
     * 代码格式化
     * @param $content
     * @return array|string
     */
    protected function codeFormat($content): array|string {
        $formatData = [
            '{saveUrl}' => $this->getConfig('api_url') . 'save/',
            '{listUrl}' => $this->getConfig('api_url') . 'list/',
            '{removeUrl}' => $this->getConfig('api_url') . 'remove/',
            '{moduleName}' => $this->getConfig('module_name') ? StringHelper::lower2camel($this->getConfig('module_name')) : '',
            '{ApiControllerName}' => $this->getConfig('api_controller_name') ? StringHelper::lower2camel($this->getConfig('api_controller_name')) : '',
            '{arNamespace}' => $this->getConfig('ar_namespace'),
            'view-page-name' => StringHelper::lower2camel($this->getConfig('module_name') . '_' . $this->getConfig('file_name')),
//            'db="default"' => 'db="' . $this->getConfig('render_db') . '"',
//            'table="api"' => 'table="' . $this->getConfig('render_table') . '"',
            'render/demo.json' => $this->getConfig('render_map_setting')
        ];
        foreach ($formatData as $key => $val) {
            $content = str_replace($key, $val, $content);
        }
        return $content;
    }

    /**
     * 扫描目录
     * @param $path
     * @param string $rpath
     * @return array
     */
    public function fileList($path, string $rpath = ''): array {
        $flist = array();
        if (!is_dir($path)) {
            exit('文件夹不存在:' . $path);
        }
        if ($files = scandir($path)) {
            foreach ($files as $file) {
                if ($file === '.' || $file === '..') {
                    continue;
                }
                $rpath = trim(str_replace('\\', '', $rpath), '/') . '/';
                $fpath = $path . '/' . $file;
                if (is_dir($fpath)) {
                    $flist = array_merge($flist, $this->fileList($fpath, $rpath . $file));
                } else {
                    $flist[] = ltrim($rpath . $file, '/');
                }
            }
        }
        return array_reverse($flist);
    }
}