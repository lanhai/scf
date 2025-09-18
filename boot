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
    //替换升级包
    $srcPack = __DIR__ . '/build/src.pack';
    $updatePack = __DIR__ . '/build/update.pack';
    if (FRAMEWORK_IS_PHAR) {
        if (file_exists($updatePack)) {
            //file_exists($srcPack) and unlink($srcPack);
            if (!rename($updatePack, $srcPack)) {
                echo("写入更新文件失败!\n");
                if ($boot) {
                    die();
                }
            }
            echo "框架源码包已更新\n";
            flush();
            clearstatcache();
        }
        if (!file_exists($srcPack)) {
            die("内核文件不存在\n");
        }
    }
    return $srcPack;
}

$srcPack = _UpdateFramework_(true);
//注册scf命名空间
spl_autoload_register(function ($class) use ($srcPack) {
    // 将命名空间 Scf 映射到 PHAR 文件中的 src 目录
    if (str_starts_with($class, 'Scf\\')) {
        $classPath = str_replace('Scf\\', '', $class);
        if (FRAMEWORK_IS_PHAR) {
            $filePath = 'phar://' . $srcPack . '/' . str_replace('\\', '/', $classPath) . '.php';
        } else {
            $filePath = __DIR__ . '/src/' . str_replace('\\', '/', $classPath) . '.php';
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
//TODO 根据加载的APP配置设置时区
ini_set('date.timezone', 'Asia/Shanghai');
//自动加载三方库
require __DIR__ . '/vendor/autoload.php';

//优先引入root,因为含系统常量
use Scf\Root;

require Root::dir() . '/Const.php';

use Scf\Command\Caller;
use Scf\Command\Runner;

use Swoole\Process;
use function Co\run;

function execute($argv): void {
    _UpdateFramework_();
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
    execute($argv);
} else {
    while (true) {
        $managerProcess = new Process(function () use ($argv) {
            $serverBuildVersion = require Root::dir() . '/version.php';
            define("FRAMEWORK_REMOTE_VERSION_SERVER", 'https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/version.json');
            define('FRAMEWORK_BUILD_TIME', $serverBuildVersion['build']);
            define('FRAMEWORK_BUILD_VERSION', $serverBuildVersion['version']);
            execute($argv);
        });
        $managerProcess->start();
        Process::wait();
        sleep(3);
    }
//    $managerProcess = new Process(function (Process $process) use ($argv) {
//        $table = new Swoole\Table(2);
//        $table->column('time', Swoole\Table::TYPE_INT);
//        $table->create();
//        $version = time();
//        $table->set('updated', ['time' => $version]);//标记启动时间
//
//        $child = null; // holds current server process
//        function createServerProcess(array $argv): Process {
//            $serverProcess = new Process(function () use ($argv) {
//                // Become a new session (process group) leader. Then all workers/addProcess children inherit this PGID.
////                if (function_exists('posix_setsid')) {
////                    @posix_setsid();
////                }
//                execute($argv);
//            });
//            $serverProcess->start();
//            return $serverProcess;
//        }
//
//        $child = createServerProcess($argv);
//        // Lightweight control plane (HTTP) on localhost:SERVER_MANAGER_HTTP_PORT
//        $controlProcess = new Process(function () use ($table) {
//            $addr = '0.0.0.0:' . SERVER_MANAGER_HTTP_PORT;
//            $server = @stream_socket_server('tcp://' . $addr, $errno, $errstr);
//            if (!$server) {
//                fwrite(STDERR, "[manager] control server bind failed: {$errstr} ({$errno})\n");
//                return;
//            }
//            stream_set_blocking($server, true);
//            while (true) {
//                $conn = @stream_socket_accept($server, 0.2);
//                if (!$conn) {
//                    usleep(100000);
//                    continue;
//                }
//                stream_set_timeout($conn, 2);
//                $req = '';
//                while (!feof($conn)) {
//                    $buf = fread($conn, 8192);
//                    if ($buf === '' || $buf === false) break;
//                    $req .= $buf;
//                    if (str_contains($req, "\r\n\r\n")) break; // headers done
//                }
//                $line = strtok($req, "\r\n");
//                if (!$line) {
//                    fclose($conn);
//                    continue;
//                }
//                $parts = explode(' ', $line);
//                $method = $parts[0] ?? 'GET';
//                $path = $parts[1] ?? '/';
//
//                $write = function ($conn, int $code, string $status, array $headers, string $body) {
//                    $hdr = "HTTP/1.1 {$code} {$status}\r\n";
//                    foreach ($headers as $k => $v) {
//                        $hdr .= $k . ': ' . $v . "\r\n";
//                    }
//                    $hdr .= "Content-Length: " . strlen($body) . "\r\n\r\n";
//                    fwrite($conn, $hdr . $body);
//                };
//
//                if ($path === '/health') {
//                    $body = json_encode(['ok' => true, 'updated' => $table->get('updated', 'time') ?: 0], JSON_UNESCAPED_UNICODE);
//                    $write($conn, 200, 'OK', ['Content-Type' => 'application/json'], $body);
//                } elseif ($path === '/restart') {
//                    $table->set('updated', ['time' => time()]);
//                    $body = json_encode(['accepted' => true], JSON_UNESCAPED_UNICODE);
//                    $write($conn, 202, 'Accepted', ['Content-Type' => 'application/json'], $body);
////                    if (strtoupper($method) !== 'POST') {
////                        $write($conn, 405, 'Method Not Allowed', ['Allow' => 'POST'], 'Method Not Allowed');
////                    } else {
////                        $table->set('updated', ['time' => time()]);
////                        $body = json_encode(['accepted' => true], JSON_UNESCAPED_UNICODE);
////                        $write($conn, 202, 'Accepted', ['Content-Type' => 'application/json'], $body);
////                    }
//                } else {
//                    $write($conn, 404, 'Not Found', ['Content-Type' => 'text/plain'], 'Not Found');
//                }
//                fclose($conn);
//            }
//        });
//        $controlProcess->start();
//
//        while (true) {
//            $currentVersion = $table->get('updated', 'time') ?: 0;
//            if ($currentVersion !== $version) {
//                // version bumped -> spawn new child, then gracefully stop old child
//                $version = $currentVersion;
//                $table->set('updated', ['time' => $currentVersion]);
//                // start new child first (requires enable_reuse_port=true in server settings)
//
//                // give it a brief moment to bind
//                usleep(500 * 1000);
//                // gracefully stop the entire old server process group (master + workers + addProcess + any extra forks)
//                if ($child instanceof Process && $child->pid > 0) {
//                    $pid = $child->pid;
//                    //$pgid = -$pid; // negative pid -> signal the process group where PGID == pid
//                    // 1) ask gracefully
//                    //@posix_kill($pgid, SIGTERM);
//                    //@posix_kill($pid, SIGTERM);
//
//                    // 2) wait up to 15s for clean shutdown (respecting max_wait_time)
//                    $deadline = time() + 10;
//                    while (time() < $deadline) {
//                        // master gone?
//                        if (!@posix_kill($pid, 0)) {
//                            break;
//                        }
//                        // reap any zombies to avoid accumulation
//                        while ($ret = Process::wait(false)) { /* drain */
//                        }
//                        usleep(200 * 1000);
//                    }
//
//                    // 3) still alive? force kill the whole group
//                    if (@posix_kill($pid, 0)) {
//                        //@posix_kill($pgid, SIGKILL);
//                        @posix_kill($pid, SIGKILL);
//                        usleep(200 * 1000);
//                        while ($ret = Process::wait(false)) { /* drain */
//                        }
//                    }
//                }
//                sleep(5);
//                // swap
//                $newChild = createServerProcess($argv);
//                $child = $newChild;
//            }
//
//            // reap any exited children (non-blocking)
//            while ($ret = Process::wait(false)) { /* no-op; just prevent zombies */
//
//            }
//            usleep(1000);
//        }
//    });
//    $managerProcess->start();
//    Process::wait();
}
