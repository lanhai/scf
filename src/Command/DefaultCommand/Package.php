<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Console;
use Scf\Core\Env;
use Scf\Helper\JsonHelper;
use Scf\Helper\StringHelper;
use Swoole\Coroutine;
use Swoole\Coroutine\System;
use Swoole\Event;
use function Co\run;

class Package implements CommandInterface {

    public function commandName(): string {
        return 'package';
    }

    public function desc(): string {
        return '框架包管理';
    }

    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('publish', '发布最新版本');
        $commandHelp->addAction('version', '查询最新版本');
        return $commandHelp;
    }

    public function publish(): void {
        $latestVersion = $this->version();
        Console::info('最新版本:' . $latestVersion);
        Console::line();
        Console::info('正在推送代码到github...');
        System::exec("git add " . SCF_ROOT)['output'];
        System::exec('git commit -m "auto commit at ' . date('Y-m-d H:i:s') . '"')['output'];
        System::exec("git push")['output'];
        $defaultVersionNum = StringHelper::incrementVersion($latestVersion);
        $version = Console::input('请输入版本号', $defaultVersionNum);
        Console::info('正在推送版本标签:v' . $version);
        System::exec("git tag -a v$version -m 'release v$version'")['output'];
        System::exec("git push origin v$version")['output'];
        Console::success('框架composer包发布成功:v' . $version);
    }

    protected function version(): string {
        $version = '0.0.0';
        Console::startLoading('正在查询最新版本', function ($tid) use (&$version) {
            $tags = trim(System::exec('git ls-remote --tags origin')['output']);
            // 将输出行分割为数组
            $lines = explode("\n", trim($tags));
            // 定义版本号提取正则表达式
            $pattern = '/refs\/tags\/v?(\d+\.\d+\.\d+)/';
            // 用于存储版本号和对应的行
            $versions = [];
            // 提取版本号并存储
            foreach ($lines as $line) {
                if (preg_match($pattern, $line, $matches)) {
                    $version = $matches[1];
                    $versions[] = ['version' => $version, 'line' => $line];
                }
            }
            // 自定义比较函数按版本号排序
            usort($versions, function ($a, $b) {
                return version_compare($b['version'], $a['version']);
            });
            $line = $versions[0]['line'];
            // 正则表达式匹配版本号
            $pattern = '/v(\d+\.\d+\.\d+)/';
            if (preg_match($pattern, $line, $matches)) {
                // 提取的版本号
                $version = $matches[1];
            }
            Console::endLoading($tid);
        });
//        $cid[] = go(function () use (&$version) {
//
//        });
//        Coroutine::join($cid);
        return $version;
    }

    public function exec(): ?string {
        $action = Manager::instance()->getArg(0);
        if ($action && method_exists($this, $action) && $action != 'help') {
            $result = null;
            run(function () use ($action, &$result) {
                $result = $this->$action();
            });
            return $result;
        }
        return Manager::instance()->displayCommandHelp($this->commandName());
    }
}