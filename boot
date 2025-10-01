<?php
declare(strict_types=1);

use Swoole\Coroutine\System;
use Swoole\Event;
use Swoole\Process;
use function Co\run;

version_compare(PHP_VERSION, '8.1.0', '<') and die('运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION);
//系统路径
const SCF_ROOT = __DIR__;
//读取系统环境参数
$envVariables = [];
run(function () use (&$envVariables) {
    $appEnv = trim(System::exec('echo ${APP_ENV}')['output']);
    $appDir = trim(System::exec('echo ${APP_DIR}')['output']);
    $appSrc = trim(System::exec('echo ${APP_SRC}')['output']);
    $serverRole = trim(System::exec('echo ${SERVER_ROLE}')['output']);
    $staticHandler = trim(System::exec('echo ${STATIC_HANDLER}')['output']);
    $hostIp = trim(System::exec('echo ${HOST_IP}')['output']);
    $osName = trim(System::exec('echo ${OS_NAME}')['output']);
    $netWorkMode = trim(System::exec('echo ${NETWORK_MODE}')['output']);
    $envVariables = compact('appEnv', 'appDir', 'appSrc', 'serverRole', 'staticHandler', 'hostIp', 'osName', 'netWorkMode');
    $envVariables = array_combine(
        array_map(function ($key) {
            return strtolower(preg_replace('/[A-Z]/', '_$0', $key));
        }, array_keys($envVariables)),
        $envVariables
    );
});
Event::wait();
define("ENV_VARIABLES", $envVariables);
define('IS_DEV', (ENV_VARIABLES['app_env'] === 'dev') || in_array('-dev', $argv));//开发模式
define('IS_PACK', in_array('-pack', $argv));//打包源码模式
define('NO_PACK', in_array('-nopack', $argv));//非打包源码模式

define('IS_HTTP_SERVER', in_array('server', $argv));
define('IS_HTTP_SERVER_START', in_array('start', $argv));
define('RUNNING_SERVER', in_array('server', $argv));//启动HTTP/SOCKET服务器
define('RUNNING_BUILD', in_array('build', $argv));//源码构建
define('RUNNING_INSTALL', in_array('install', $argv));//创建应用
define('RUNNING_TOOLBOX', in_array('toolbox', $argv));//cli工具
define('RUNNING_PACKAGE', in_array('package', $argv));//包发布到github
define('RUNNING_CREATE_AR', RUNNING_TOOLBOX && in_array('ar', $argv));//构建数据AR文件
define('RUNNING_BUILD_FRAMEWORK', RUNNING_BUILD && (in_array('framework', $argv) || in_array('dashboard', $argv)));//框架源码构建

const FRAMEWORK_IS_PHAR = IS_PACK || (!IS_DEV && !NO_PACK && !RUNNING_BUILD && !RUNNING_PACKAGE && !RUNNING_INSTALL);//框架源码是否打包

function _UpdateFramework_($boot = false): string {
    clearstatcache();
    $srcPack = __DIR__ . '/build/src.pack';
    $updatePack = __DIR__ . '/build/update.pack';
    $lockFile = __DIR__ . '/build/update.lock';

    // 辅助方法: 原子替换 (同目录临时文件 -> 重命名)
    $atomicReplace = static function (string $from, string $to): bool {
        // 确保目标目录存在
        $dir = dirname($to);
        if (!is_dir($dir)) {
            return false;
        }
        // 首先尝试简单重命名（如果在同一文件系统且未被占用这是最优方式）
        if (@rename($from, $to)) {
            return true;
        }
        // 备选方案: 复制到同目录下的临时文件然后重命名
        $tmp = $dir . '/.' . basename($to) . '.tmp.' . getmypid();
        if (!@copy($from, $tmp)) {
            return false;
        }
        // 尝试保留部分权限
        @chmod($tmp, 0644);
        if (!@rename($tmp, $to)) {
            // 如果重命名失败则清理临时文件
            @unlink($tmp);
            return false;
        }
        // 替换成功后删除原始 update pack
        @unlink($from);
        return true;
    };

    // 框架是否已打包?
    if (FRAMEWORK_IS_PHAR) {
        // 如果还没有 src.pack 且也没有 update.pack，这是严重错误。
        // (我们会在任何可能的更新后验证其存在性)

        $updated = false;

        // 获取简单锁以避免并发写入
        $lock = @fopen($lockFile, 'c');
        if ($lock) {
            @flock($lock, LOCK_EX);
        }

        // 如果存在 update.pack，尝试原子性地切换
        if (file_exists($updatePack)) {
            // 直接用 atomicReplace 覆盖目标路径（无需先 unlink $srcPack）
            if (!$atomicReplace($updatePack, $srcPack)) {
                echo "写入更新文件失败!\n";
                if ($lock) {
                    @flock($lock, LOCK_UN);
                    @fclose($lock);
                }
                if ($boot) {
                    // 启动时更新失败是致命的，因为无法保证一致性
                    exit(2);
                }
            } else {
                $updated = true;
                echo "框架源码包已更新\n";
                flush();
                clearstatcache();
            }
        }

        if ($lock) {
            @flock($lock, LOCK_UN);
            @fclose($lock);
        }

        // 任何更新后，确保 src.pack 存在
        if (!file_exists($srcPack)) {
            echo "内核文件不存在\n";
            exit(3);
        }
    }

    return $srcPack;
}

$srcPack = _UpdateFramework_(true);
// 注册scf命名空间
spl_autoload_register(function ($class) use ($srcPack) {
    // 将命名空间 Scf 映射到 PHAR 文件中的 src 目录
    if (str_starts_with($class, 'Scf\\')) {
        $classPath = str_replace('Scf\\', '', $class);
        $relative = str_replace('\\', '/', $classPath) . '.php';
        if (FRAMEWORK_IS_PHAR) {
            if ($class === 'Scf\\Command\\Caller' || $class === 'Scf\\Command\\Runner') {// || $class === 'Scf\\Root'
                // 这两个类强制使用非打包源码，避免在 PHAR 中被固定导致热更新受限
                if (IS_DEV || is_dir(__DIR__ . '/src/')) {
                    $filePath = __DIR__ . '/src/' . $relative;
                } else {
                    $filePath = __DIR__ . '/vendor/lhai/scf/src/' . $relative;
                }
            } else {
                $filePath = 'phar://' . $srcPack . '/' . $relative;
            }
        } else {
            if (is_dir(__DIR__ . '/src/')) {
                $filePath = __DIR__ . '/src/' . $relative;
            } else {
                $filePath = __DIR__ . '/vendor/lhai/scf/src/' . $relative;
            }
        }
        if (file_exists($filePath)) {
//            $fileContent = file_get_contents($filePath);
//            if (!str_contains($fileContent, 'declare(strict_types=1);')) {
//                $fileContent = str_replace('<?php', '<?php declare(strict_types=1);', $fileContent);
//                file_put_contents($filePath, $fileContent);
//            }
            require_once $filePath;
        }
    }
});
// TODO: 根据加载的APP配置设置时区
ini_set('date.timezone', 'Asia/Shanghai');
// 自动加载第三方库
require __DIR__ . '/vendor/autoload.php';

function start($argv): void {
    $caller = new Scf\Command\Caller();
    $caller->setScript(current($argv));
    $caller->setCommand(next($argv));
    $caller->setParams($argv);
    $ret = Scf\Command\Runner::instance()->run($caller);
    if (!empty($ret->getMsg())) {
        echo $ret->getMsg() . "\n";
    }
}

if (!RUNNING_SERVER) {
    $managerProcess = new Process(function () use ($argv) {
        start($argv);
    });
    $managerProcess->start();
    Process::wait();
} else {
    while (true) {
        $managerProcess = new Process(function () use ($argv) {
            $serverBuildVersion = require Scf\Root::dir() . '/version.php';
            define("FRAMEWORK_REMOTE_VERSION_SERVER", 'https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/version.json');
            define('FRAMEWORK_BUILD_TIME', $serverBuildVersion['build']);
            define('FRAMEWORK_BUILD_VERSION', $serverBuildVersion['version']);
            start($argv);
        });
        $managerProcess->start();
        Process::wait();
        if(!IS_HTTP_SERVER_START){
            break;
        }
        _UpdateFramework_();
        sleep(1);
    }
}
