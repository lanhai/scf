<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Command\Util;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Server\Http;
use Swoole\Process;

class Server implements CommandInterface {

    public function commandName(): string {
        return 'server';
    }

    public function desc(): string {
        return 'http服务器管理';
    }

    public function help(Help $commandHelp): Help {
        $apps = App::all();
        $names = [];
        foreach ($apps as $app) {
            $names[] = $app['app_path'];
        }
        $commandHelp->addAction('start', '启动服务器');
        $commandHelp->addAction('bgs', '启动后台服务');
        $commandHelp->addAction('cmd', '执行指定命令');
        $commandHelp->addAction('stop', '停止服务器');
        $commandHelp->addAction('reload', '重启worker进程');
        $commandHelp->addAction('restart', '重启服务器');
        $commandHelp->addAction('status', '查看服务器状态');
        //$commandHelp->addAction('redis', '启动redis DB服务器');
        $commandHelp->addActionOpt('-apps_dir', '应用列表目录,默认为:' . dirname(SCF_ROOT) . '/apps/');
        $commandHelp->addActionOpt('-app', '启动的应用文件夹名【' . implode('|', $names) . '】,默认:app');
        $commandHelp->addActionOpt('-alias', '服务器别名,默认:应用APPID');
        $commandHelp->addActionOpt('-env', '运行环境,缺省:production');
        $commandHelp->addActionOpt('-role', '运行角色【master|slave】,缺省:slave');
        $commandHelp->addActionOpt('-mode', '运行方式【src|phar】,生产环境缺省:phar,开发环境缺省:src');
        $commandHelp->addActionOpt('-static', '是否开启静态资源访问【on(缺省)/off】');
        $commandHelp->addActionOpt('-crontab', '是否定时任务【on(缺省)/off】');
        $commandHelp->addActionOpt('-report', '是否推送日志信息给机器人【on(缺省)/off】');
        $commandHelp->addActionOpt('-port', 'http端口,缺省:9502');
        $commandHelp->addActionOpt('-d', '作为守护进程运行');
        $commandHelp->addActionOpt('-force', '强制停止');
        return $commandHelp;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            Env::initialize(MODE_CGI);
            return $this->$action();
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }

    protected function start(): void {
        if (!Manager::instance()->issetOpt('d')) {
            $msg = Color::note(Util::logo()) . "\n";
            echo $msg;
        }
        Http::create(SERVER_ROLE, '0.0.0.0', SERVER_PORT)->start();
    }

    protected function stop(): string {
        $pidFile = SERVER_MASTER_PID_FILE;
        if (file_exists($pidFile)) {
            $pid = intval(file_get_contents($pidFile));
            Console::log(Color::notice('正在结束结束进程,PID:' . $pid));
            if (!Process::kill($pid, 0)) {
                $msg = Color::danger("pid :{$pid} not exist ");
                Console::log(Color::notice('进程不存在,PID:' . $pid));
                unlink($pidFile);
            } else {
                $force = Manager::instance()->issetOpt('force');
                if ($force) {
                    Process::kill($pid, SIGKILL);
                } else {
                    Process::kill($pid);
                }
                //等待5秒
                $time = time();
                while (true) {
                    usleep(1000);
                    if (!Process::kill($pid, 0)) {
                        if (is_file($pidFile)) {
                            unlink($pidFile);
                        }
                        $msg = Color::success("结束服务器进程成功 {$pid} at " . date("Y-m-d H:i:s"));
                        break;
                    } else {
                        if (time() - $time > 15) {
                            $msg = Color::danger("结束服务器进程失败:{$pid} , 请尝试使用 [php boot server stop -force] 重试");
                            break;
                        }
                    }
                }
            }
        } else {
            $msg = Color::danger("服务器进程不存在");
        }
        return $msg;
    }

    protected function reload(): string {
        $pidFile = SERVER_MASTER_PID_FILE;
        if (file_exists($pidFile)) {
            Util::opCacheClear();
            $pid = file_get_contents($pidFile);
            if (!Process::kill($pid, 0)) {
                $msg = Color::danger("pid :{$pid} not exist ");
            } else {
                Process::kill($pid, SIGUSR1);
                $msg = "send server reload command to pid:{$pid} at " . date("Y-m-d H:i:s");
                $msg = Color::success($msg);
            }
        } else {
            $msg = Color::danger("服务器进程不存在!");
        }
        return $msg;
    }

    protected function restart(): string {
        $msg = $this->stop();
        Console::success($msg);
        $pidFile = SERVER_MASTER_PID_FILE;
        if (file_exists($pidFile)) {
            exit(0);
        }
        Console::log(Color::notice('正在准备重新启动...'));
        sleep(5);
        $this->start();
        return $msg;
    }

    protected function status(): void {
        //TODO 查询心跳
        Console::write(Color::green('TODO...'));
    }
}