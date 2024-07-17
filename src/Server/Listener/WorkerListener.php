<?php

namespace Scf\Server\Listener;

use Scf\Command\Color;
use Scf\Core\Console;
use Scf\Mode\Web\App;
use Scf\Mode\Web\Route\AnnotationRouteRegister;
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
//        if ($workerId == 0) {
//            var_dump(get_included_files());
//        }
        //要使用app命名空间必须先加载模块
        App::mount();
        Console::enablePush();
        //Log::instance()->enableTable();
        //添加RPC服务
        try {
            \Scf\Mode\Rpc\App::addService(Http::instance()->getPort() + 5, $workerId);
        } catch (\Exception $e) {
            Console::error($e->getMessage());
        }
        if ($workerId == 0) {
            AnnotationRouteRegister::instance()->load();
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
            App::updateDatabase();
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