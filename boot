<?php
declare(strict_types=1);
version_compare(PHP_VERSION, '8.1.0', '<') and die('运行此应用需PHP8.1(含)以上版本, 当前环境版本: ' . PHP_VERSION);
//系统路径
const SCF_ROOT = __DIR__;
//读取运行参数
define('IS_DEV', in_array('-dev', $argv));//开发模式
define('IS_PHAR', in_array('-phar', $argv));//打包源码模式
define('IS_SRC', in_array('-src', $argv));//非打包源码模式
define('IS_BUILD', in_array('build', $argv));//源码构建
define('IS_PUBLISH', in_array('publish', $argv));//发布
define('IS_TOOLBOX', in_array('toolbox', $argv));//cli工具
define('IS_HTTP_SERVER', in_array('server', $argv));//启动服务器
define('IS_PACKAGE', in_array('package', $argv));//包发布
const FRAMEWORK_IS_PHAR = IS_PHAR || (!IS_DEV && !IS_SRC && !IS_BUILD && !IS_PACKAGE);//框架源码是否打包
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
        if ($class === 'Scf\\Command\\Caller' || $class === 'Scf\\Command\\Runner' || $class === 'Scf\\Root') {
            // 这两个类强制使用非打包源码，避免在 PHAR 中被固定导致热更新受限
            $filePath = __DIR__ . '/src/' . $relative;
        } else if (FRAMEWORK_IS_PHAR) {
            $filePath = 'phar://' . $srcPack . '/' . $relative;
        } else {
            $filePath = __DIR__ . '/src/' . $relative;
        }
        if (file_exists($filePath)) {
//            $fileContent = file_get_contents($filePath);
//            if (!str_contains($fileContent, 'declare(strict_types=1);')) {
//                $fileContent = str_replace('<?php', '<?php declare(strict_types=1);', $fileContent);
//                file_put_contents($filePath, $fileContent);
//            }
            require $filePath;
        }
    }
});
// TODO: 根据加载的APP配置设置时区
ini_set('date.timezone', 'Asia/Shanghai');
// 自动加载第三方库
require __DIR__ . '/vendor/autoload.php';

// 优先引入root，因为含系统常量
use Scf\Root;

require Root::dir() . '/Const.php';

use Scf\Command\Caller;
use Scf\Command\Runner;

use Swoole\Process;
use function Co\run;

function start($argv): void {
    $caller = new Caller();
    $caller->setScript(current($argv));
    $caller->setCommand(next($argv));
    $caller->setParams($argv);
    $ret = Runner::instance()->run($caller);
    if (!empty($ret->getMsg())) {
        echo $ret->getMsg() . "\n";
    }
}

if (!IS_HTTP_SERVER) {
    start($argv);
} else {
    while (true) {
        $managerProcess = new Process(function () use ($argv) {
            $serverBuildVersion = require Root::dir() . '/version.php';
            define("FRAMEWORK_REMOTE_VERSION_SERVER", 'https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/version.json');
            define('FRAMEWORK_BUILD_TIME', $serverBuildVersion['build']);
            define('FRAMEWORK_BUILD_VERSION', $serverBuildVersion['version']);
            start($argv);
        });
        $managerProcess->start();
        Process::wait();
        _UpdateFramework_();
        sleep(1);
    }
}
