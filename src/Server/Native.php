<?php

namespace Scf\Server;

use App\Window\Main;
use Scf\Core\Console;
use Scf\Command\Color;
use Scf\Core\Result;
use Scf\Core\Traits\Singleton;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Scf\Mode\Native\App;
use Scf\Mode\Web\Route\AnnotationRouteRegister;
use Scf\Root;
use Scf\Server\Runtime\Table;
use Scf\Server\Table\Runtime;
use Scf\Util\Dir;
use Swoole\Coroutine;
use Swoole\Timer;
use Swoole\Server;
use Throwable;


class Native {
    use Singleton;

    /**
     * @var Server
     */
    protected Server $server;

    /**
     * @var string 节点名称
     */
    protected string $name;

    /**
     * @var int 启动时间
     */
    protected int $started = 0;

    protected ?int $ipcMainClientId = null;

    /**
     * 启动服务
     * @param int $port
     * @return void
     */
    public function start(int $port = 9501): void {
        //一键协程化
        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);
        Table::register([
            'Scf\Server\Table\PdoPoolTable',
            'Scf\Server\Table\LogTable',
            'Scf\Server\Table\Counter',
            'Scf\Server\Table\Runtime',
            'Scf\Server\Table\RouteTable',
        ]);
        $this->server = new Server("localhost", $port, SWOOLE_PROCESS);
        $this->server->listen('localhost', $port, SWOOLE_SOCK_TCP);
        $this->server->set([
            'worker_num' => 4,
        ]);
        $this->server->on('connect', function ($server, $fd) {
            $this->ipcMainClientId = $fd;
            Runtime::instance()->set('_ICP_MAIN_CLIENT_ID_', $fd);
            $this->send($fd, App::command('/notification/send', ['title' => 'Hello from IPC Server', 'body' => 'IPC连接成功@' . date('Y-m-d H:i:s')]));
            $mainWindow = new Main();
            $mainWindow->init($this)->boot();
        });
        $this->server->on('receive', function (Server $server, $fd, $reactor_id, $data) {
            static $buffer = '';
            $buffer .= $data;
            while (strlen($buffer) >= 8) {
                // 读取前8字节的长度前缀
                $lengthPrefix = unpack('N', substr($buffer, 0, 8))[1];
                // 检查是否已经接收到了完整的数据包
                if (strlen($buffer) >= 8 + $lengthPrefix) {
                    // 提取完整的数据包
                    $message = substr($buffer, 8, $lengthPrefix);
                    // 处理数据包
                    Console::info('收到数据:' . $message);
                    if (!JsonHelper::is($message)) {
                        $this->send($fd, App::message("非法的数据格式"));
                        break;
                    }
                    $receive = JsonHelper::recover($message);
                    if ($receive['type'] == 'request' && !empty($receive['path'])) {
                        $path = $receive['path'];
                        if ($annotationRoute = AnnotationRouteRegister::instance()->match('TCP', $path)) {
                            $requestId = $receive['request_id'];
                            $class = new $annotationRoute['route']['space']();
                            $method = $annotationRoute['route']['action'];
                            if ($receive['data']) {
                                /** @var Result $result */
                                $result = call_user_func_array([$class, $method], ArrayHelper::merge($annotationRoute['params'], $receive['data']));
                            } else {
                                /** @var Result $result */
                                $result = call_user_func_array([$class, $method], $annotationRoute['params']);
                            }
                            $this->send($fd, App::response(JsonHelper::toJson([
                                'errCode' => $result->getErrCode(),
                                'message' => $result->getMessage(),
                                'data' => $result->getData()
                            ]), $requestId));
                        }
                    } else {
                        $this->send($fd, App::message("收到数据:" . $message));
                    }
                    break;
                }
                // 清理处理过的消息
                $buffer = substr($buffer, 8 + $lengthPrefix);

            }
        });
        $this->server->on('close', function ($server, $fd) {
            Console::warning("#{$fd} 客户端断开连接");
        });
        $this->server->on('WorkerStart', function ($server, $workerId) {
            \Scf\Core\App::mount(MODE_NATIVE);
            if ($workerId == 0) {
                AnnotationRouteRegister::instance()->load();
            }
        });
        $this->server->on('AfterReload', function ($server) {
            Console::info('服务器已重启');
            if ($fd = Runtime::instance()->get('_ICP_MAIN_CLIENT_ID_')) {
                $this->send($fd, App::command('/app/relaunch', []));
            }
        });
        $this->server->on('start', function ($server) use ($port) {
            \Scf\Core\App::mount(MODE_NATIVE);
            //创建主线程窗口
            $appClass = \Scf\Core\App::buildControllerPath('Window', 'Main');
            if (!class_exists($appClass)) {
                Console::error('应用入口文件不存在:' . $appClass);
                $server->shutdown();
            } else {
                Console::success('IPC服务器启动成功,listen:' . $port);
                if (Env::isDev()) {
                    $this->watchFileChange();
                }
            }
        });
        $this->server->start();
    }

    /**
     * 发送数据
     * @param $fd
     * @param $message
     * @return void
     */
    public function send($fd, $message): void {
        // 计算消息长度
        $messageLength = strlen($message);
        // 创建 8 字节长度前缀
        $lengthPrefix = pack('J', $messageLength);
        // 发送长度前缀
        $this->server->send($fd, $lengthPrefix);
        // 发送实际的消息内容
        $this->server->send($fd, $message);
    }

    /**
     * 重启服务器
     * @return void
     */
    protected function reload(): void {
        $countdown = 3;
        Console::log(Color::yellow($countdown) . '秒后重启服务器');
        Timer::tick(1000, function ($id) use (&$countdown) {
            $countdown--;
            if ($countdown == 0) {
                Timer::clear($id);
                $this->server->reload();
            } else {
                Console::log(Color::yellow($countdown) . '秒后重启服务器');
            }
        });
    }


    /**
     * 监听文件改动自动重启服务
     * @return void
     */
    protected function watchFileChange(): void {
        Coroutine::create(function () {
            $scanDirectories = function () {
                $appFiles = Dir::scan(APP_PATH . '/src');
                return [...$appFiles, ...Dir::scan(Root::dir())];
            };
            $files = $scanDirectories();
            $fileList = [];
            foreach ($files as $path) {
                $fileList[] = [
                    'path' => $path,
                    'md5' => md5_file($path)
                ];
            }
            while (true) {
                $currentFiles = $scanDirectories();
                $changedFiles = [];
                $changed = false;
                $currentFilePaths = array_map(fn($file) => $file, $currentFiles);
                foreach ($currentFilePaths as $path) {
                    if (!in_array($path, array_column($fileList, 'path'))) {
                        $fileList[] = [
                            'path' => $path,
                            'md5' => md5_file($path)
                        ];
                        $changed = true;
                        $changedFiles[] = $path;
                    }
                }
                foreach ($fileList as $key => &$file) {
                    if (!file_exists($file['path'])) {
                        $changed = true;
                        $changedFiles[] = $file['path'];
                        unset($fileList[$key]);
                        continue;
                    }
                    $getMd5 = md5_file($file['path']);
                    if (strcmp($file['md5'], $getMd5) !== 0) {
                        $file['md5'] = $getMd5;
                        $changed = true;
                        $changedFiles[] = $file['path'];
                    }
                }
                if ($changed) {
                    Console::warning('---------以下文件发生变动,即将重启---------');
                    foreach ($changedFiles as $f) {
                        Console::write($f);
                    }
                    Console::warning('-------------------------------------------');
                    $this->server->reload();
                }
                Coroutine::sleep(3);
            }
        });
    }

    /**
     * 向客户端推送内容
     * @param $fd
     * @param $str
     * @return bool
     */
    public function push($fd, $str): bool {
        if (!$this->server->exist($fd) || !$this->server->isEstablished($fd)) {
            Console::unsubscribe($fd);
            return false;
        }
        try {
            return $this->server->push($fd, $str);
        } catch (Throwable) {
            return false;
        }
    }


    public function stats(): array {
        return $this->server->stats();
    }

    protected function _master(): Server {
        return $this->server;
    }

    public static function server(): ?Server {
        try {
            return self::instance()->_master();
        } catch (Throwable) {
            return null;
        }
    }

    /**
     * @return ?Server
     */
    public static function master(): ?Server {
        return self::server();
    }
}