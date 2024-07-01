<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\Console;
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
        $latestVersion = '0.0.0';
        Console::startLoading('正在查询最新版本', function ($tid) use (&$latestVersion) {
            $latestVersion = $this->version();
            Console::endLoading($tid);
        });
        Console::info('最新版本:' . $latestVersion);
        Console::startLoading('正在推送代码到github', function ($tid) use (&$latestVersion) {
            trim(System::exec("git add .")['output']);
            $commitResult = trim(System::exec('git commit -m "auto commit at ' . date('Y-m-d H:i:s') . '"')['output']);
            Console::info($commitResult);
            $pushTag = trim(System::exec("git push")['output']);
            Console::info($pushTag);
            Console::endLoading($tid);
        });
        $arr = explode('.', $latestVersion);
        $arr[count($arr) - 1] = (int)$arr[count($arr) - 1] + 1;
        $defaultVersionNum = implode('.', $arr);
        $version = Console::input('请输入版本号,(缺省 ' . $defaultVersionNum . '):', false) ?: $defaultVersionNum;
        Console::startLoading('正在推送版本标签:' . $version, function ($tid) use ($version) {
            $addTag = trim(System::exec("git tag -a v$version -m 'release v$version'")['output']);
            Console::info($addTag);
            $pushTag = trim(System::exec("git push origin v$version")['output']);
            Console::info($pushTag);
            Console::endLoading($tid);
        });
        Console::success('发布成功:v' . $version);
    }

    protected function version(): string {
        $version = '0.0.0';
        $cid[] = go(function () use (&$version) {
            $tags = trim(System::exec('git ls-remote --tags origin')['output']);
            $arr = explode("\n", $tags);
            // 正则表达式匹配版本号
            $pattern = '/v(\d+\.\d+\.\d+)/';
            if (preg_match($pattern, array_pop($arr), $matches)) {
                // 提取的版本号
                $version = $matches[1];
            }
        });
        Coroutine::join($cid);
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