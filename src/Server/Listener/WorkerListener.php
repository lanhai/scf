<?php

namespace Scf\Server\Listener;

use Scf\Core\Console;
use Scf\Mode\Web\App;
use Scf\Command\Color;
use Scf\Server\Http;
use Swoole\Process;
use Swoole\Timer;
use Swoole\WebSocket\Server;

class WorkerListener extends Listener {


    protected function onWorkerStart(Server $server, $workerId): void {
        Timer::after(1000, function () use ($server, $workerId) {
            if (!Process::kill($server->master_pid, 0)) {
                Process::kill($server->worker_pid, SIGKILL);
            }
        });
        Console::enablePush();
        //Log::instance()->enableTable();
        //要使用app命名空间必须先加载模块
        App::isReady() and App::mount();
        //添加RPC服务
        \Scf\Mode\Rpc\App::addService(Http::instance()->getPort() + 5, $workerId);
        if ($workerId == 0) {
            $srcPath = App::src();
            $version = App::version();
            $publicVersion = App::publicVersion();
            $info = <<<INFO
---------Workers启动完成---------
内核版本：{$version}
资源版本：{$publicVersion}
源码目录：{$srcPath}
---------------------------------
INFO;
            Console::write(Color::green($info));
        }
    }

    protected function onWorkerError(Server $server, int $worker_id, int $worker_pid, int $exit_code, int $signal): void {
        if ($signal !== 2) {
            Console::log(Color::red('#' . $worker_pid . ' worker #' . $worker_id . ' 发生致命错误!signal:' . $signal . ',exit_code:' . $exit_code));
        }
        Timer::after(3000, function () use ($server, $worker_id) {
            if (!Process::kill($server->master_pid, 0)) {
                $server->stop($worker_id);
            }
        });
    }

    protected function onWorkerStop(Server $server, $workerId) {

    }
}