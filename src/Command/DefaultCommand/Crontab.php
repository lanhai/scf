<?php

namespace Scf\Command\DefaultCommand;

use Scf\Command\Color;
use Scf\Command\CommandInterface;
use Scf\Command\Help;
use Scf\Command\Manager;
use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Env;
use Scf\Core\Server;
use Scf\Server\LinuxCrontab\LinuxCrontabManager;
use Scf\Server\Task\Crontab as TaskCrontab;
use Swoole\Coroutine;
use Throwable;
use function Swoole\Coroutine\run;

/**
 * 一次性 crontab 启动命令。
 *
 * 该命令面向 Linux crontab 这类“调起一次、执行一次、随后退出”的场景，
 * 负责从应用模块注册中解析出可执行的 Crontab 脚本，并按当前 app / role / env
 * 上下文执行指定脚本一次，而不是接管常驻的 CrontabManager 生命周期。
 */
class Crontab implements CommandInterface {
    /**
     * 已注册的任务缓存。
     *
     * @var array<int, array>
     */
    protected array $registeredTasks = [];

    /**
     * 最近一次任务解析失败的原因。
     *
     * @var string
     */
    protected string $resolveError = '';

    /**
     * 返回命令名称。
     *
     * @return string
     */
    public function commandName(): string {
        return 'crontab';
    }

    /**
     * 返回命令描述。
     *
     * @return string
     */
    public function desc(): string {
        return '一次性定时任务启动器';
    }

    /**
     * 定义帮助信息。
     *
     * `classname` 采用位置参数，保持 `boot crontab -app=... ClassName` 这种启动方式。
     *
     * @param Help $commandHelp 帮助信息容器
     * @return Help
     */
    public function help(Help $commandHelp): Help {
        $commandHelp->addAction('list', '列出当前 app / role 下可执行的已注册 crontab 脚本');
        $commandHelp->addAction('log', '查看 Linux crontab 统一日志（默认实时跟随）');
        $commandHelp->addActionOpt('{classname}', '要执行的脚本类名、显示名或完整命名空间');
        $commandHelp->addActionOpt('-app', '应用目录名, 例如 mtvideo');
        $commandHelp->addActionOpt('-env', '运行环境, 例如 dev');
        $commandHelp->addActionOpt('-role', '运行角色【master|slave】, 默认 master');
        $commandHelp->addActionOpt('-namespace', '显式指定完整命名空间, 优先级高于 classname');
        $commandHelp->addActionOpt('-entry_id', 'Linux 系统排程条目 id, 用于回写最近运行时间');
        $commandHelp->addActionOpt('-n', '查看最后 N 行日志（默认 100）');
        $commandHelp->addActionOpt('-lines', '同 -n');
        $commandHelp->addActionOpt('-f', '开启实时跟随（等价于 -follow=1）');
        $commandHelp->addActionOpt('-follow', '是否实时跟随，1 开启，0 关闭（默认 1）');
        $commandHelp->addActionOpt('-clear', '先清空日志文件；未指定 tail 参数时仅清空并退出');
        return $commandHelp;
    }

    /**
     * 执行命令入口。
     *
     * 该入口会先初始化 CGI 运行时并装载应用模块，然后将第一个位置参数解释为脚本名。
     * 当参数为 `list` 时输出可执行任务清单，否则将其当作目标脚本执行一次。
     *
     * @return string|null
     */
    public function exec(): ?string {
        $namespace = trim((string)Manager::instance()->getOpt('namespace', ''));
        $action = trim((string)Manager::instance()->getArg(0, ''));

        // `log` 只需要读取文件，不依赖应用运行时常量和模块装载。
        // 先走轻量路径，避免初始化阶段的模板缓存清理影响日志排查链路。
        if ($namespace === '' && strcasecmp($action, 'log') === 0) {
            return $this->showLog();
        }

        Env::initialize(MODE_CGI);
        App::mount(MODE_CGI);

        if ($namespace !== '') {
            return $this->runTask($namespace);
        }

        if ($action === '') {
            return Manager::instance()->displayCommandHelp($this->commandName());
        }

        if (strcasecmp($action, 'list') === 0) {
            return $this->listTasks();
        }

        if (strcasecmp($action, 'log') === 0) {
            return $this->showLog();
        }

        return $this->runTask($action);
    }

    /**
     * 查看 Linux crontab 统一日志。
     *
     * 该 action 封装了 `tail` 的常用参数，默认显示最近 100 行并实时跟随，
     * 便于直接在同一个 crontab CLI 入口里排查任务执行输出。
     *
     * @return string|null
     */
    protected function showLog(): ?string {
        $logFile = $this->crontabLogFile();
        $logDir = dirname($logFile);
        if (!is_dir($logDir) && !@mkdir($logDir, 0775, true) && !is_dir($logDir)) {
            return Color::danger('创建日志目录失败: ' . $logDir);
        }
        if (!is_file($logFile) && !@touch($logFile)) {
            return Color::danger('创建日志文件失败: ' . $logFile);
        }

        if ($this->shouldClearLog()) {
            if (@file_put_contents($logFile, '') === false) {
                return Color::danger('清空日志文件失败: ' . $logFile);
            }
            if (!$this->shouldTailAfterClear()) {
                return Color::success('日志已清空: ' . $logFile);
            }
        }

        $lines = $this->logTailLines();
        $follow = $this->shouldFollowLog();
        $tailArgs = $follow ? '-n ' . $lines . ' -F ' : '-n ' . $lines . ' ';
        $command = '/usr/bin/env tail ' . $tailArgs . escapeshellarg($logFile);
        $status = 0;
        passthru($command, $status);

        if ($status !== 0) {
            return Color::danger('日志查看命令执行失败: ' . $command);
        }

        return null;
    }

    /**
     * 返回 Linux crontab 统一日志文件路径。
     *
     * @return string
     */
    protected function crontabLogFile(): string {
        if (defined('APP_LOG_PATH')) {
            return APP_LOG_PATH . '/crontab/inst.log';
        }

        $appsRoot = defined('SCF_APPS_ROOT') ? rtrim((string)SCF_APPS_ROOT, '/') : (dirname(SCF_ROOT) . '/apps');
        $defaultApp = '';
        if (defined('ENV_VARIABLES') && is_array(ENV_VARIABLES)) {
            $defaultApp = trim((string)(ENV_VARIABLES['app_dir'] ?? ''));
        }
        if ($defaultApp === '') {
            $defaultApp = trim((string)(getenv('APP_DIR') ?: ''));
        }

        $app = trim((string)Manager::instance()->getOpt('app', $defaultApp !== '' ? $defaultApp : 'app'));
        $app = str_replace(['..', '\\'], '', trim($app, " \t\n\r\0\x0B/"));
        if ($app === '') {
            $app = 'app';
        }

        return $appsRoot . '/' . $app . '/log/crontab/inst.log';
    }

    /**
     * 解析日志输出行数参数。
     *
     * @return int
     */
    protected function logTailLines(): int {
        $raw = Manager::instance()->getOpt('n', Manager::instance()->getOpt('lines', 100));
        $lines = (int)$raw;
        if ($lines <= 0) {
            $lines = 100;
        }
        return min($lines, 1000);
    }

    /**
     * 判断是否需要实时跟随日志输出。
     *
     * @return bool
     */
    protected function shouldFollowLog(): bool {
        if (Manager::instance()->issetOpt('f')) {
            return true;
        }

        $follow = trim((string)Manager::instance()->getOpt('follow', '1'));
        return !in_array(strtolower($follow), ['0', 'false', 'off', 'no'], true);
    }

    /**
     * 判断是否要求先清空日志文件。
     *
     * @return bool
     */
    protected function shouldClearLog(): bool {
        return Manager::instance()->issetOpt('clear');
    }

    /**
     * 判断清空后是否还要继续 tail 输出。
     *
     * 为避免 `-clear` 默认进入阻塞跟随模式，这里约定：
     * - 仅传 `-clear` 时，清空后直接退出；
     * - 明确传了 `-f/-follow/-n/-lines` 时，清空后继续 tail。
     *
     * @return bool
     */
    protected function shouldTailAfterClear(): bool {
        return Manager::instance()->issetOpt('f')
            || Manager::instance()->issetOpt('follow')
            || Manager::instance()->issetOpt('n')
            || Manager::instance()->issetOpt('lines');
    }

    /**
     * 列出当前运行上下文可见的 crontab。
     *
     * @return string
     */
    protected function listTasks(): string {
        $tasks = $this->registeredTasks();
        if (!$tasks) {
            return Color::warning('当前 app / role 下未注册任何 crontab 脚本');
        }

        $lines = [
            Color::note('Registered Crontabs'),
            'app=' . APP_DIR_NAME . ', env=' . SERVER_RUN_ENV . ', role=' . SERVER_ROLE,
            '',
        ];

        foreach ($tasks as $task) {
            $lines[] = sprintf(
                '  %-24s  %-18s  %s',
                $this->shortClassName((string)$task['namespace']),
                $this->modeLabel((int)$task['mode']),
                (string)$task['name']
            );
        }

        $lines[] = '';
        $lines[] = '示例: php scf/boot crontab -app=' . APP_DIR_NAME . ' -env=' . SERVER_RUN_ENV . ' -role=' . SERVER_ROLE . ' ' . $this->shortClassName((string)$tasks[0]['namespace']);
        return implode(PHP_EOL, $lines);
    }

    /**
     * 执行指定 crontab 一次。
     *
     * 任务解析支持完整命名空间、脚本类名和任务显示名三种形式。
     * 执行时复用现有 `Scf\Server\Task\Crontab` 的 `execute()` 逻辑，
     * 让脚本中的 `$this->log()`、运行元数据和异常处理路径保持一致。
     *
     * @param string $identifier 用户传入的脚本标识
     * @return string
     */
    protected function runTask(string $identifier): string {
        // Linux crontab 场景下优先确认业务端口确实在线，避免因为残留 pid 文件
        // 或手工停服导致离线节点仍继续跑一次性任务。
        if (!$this->isApplicationOnline()) {
            return Color::warning(
                '应用未在线, 已跳过本次任务: app=' . APP_DIR_NAME
                . ', role=' . SERVER_ROLE
                . ', port=' . $this->resolveExpectedPort()
            );
        }

        $task = $this->resolveTask($identifier);
        if (is_null($task)) {
            $message = $this->resolveError ?: '未匹配到指定 crontab 脚本';
            return Color::danger($message);
        }

        $taskInstance = TaskCrontab::factory($task);
        if (!$taskInstance->validate()) {
            return Color::danger('任务配置校验失败: ' . $taskInstance->getError());
        }

        if (!method_exists(ltrim((string)$task['namespace'], '\\'), 'run')) {
            return Color::danger('任务脚本必须实现 run 方法: ' . $task['namespace']);
        }

        $entryId = trim((string)Manager::instance()->getOpt('entry_id', ''));
        $entryId !== '' && LinuxCrontabManager::markRunStarted($entryId);

        try {
            $this->executeTaskInCoroutine($taskInstance);
            $entryId !== '' && LinuxCrontabManager::markRunFinished($entryId, true, '任务执行完成');
            return Color::success('任务执行完成: ' . $task['namespace']);
        } catch (Throwable $throwable) {
            $entryId !== '' && LinuxCrontabManager::markRunFinished($entryId, false, $throwable->getMessage());
            return Color::danger('任务执行失败: ' . $throwable->getMessage());
        }
    }

    /**
     * 在协程运行时中执行一次性 crontab。
     *
     * Linux 系统 crontab 触发的是普通 CLI 进程，但大量任务实现会直接依赖
     * Redis/MySQL/HTTP 客户端的协程 Hook 能力。这里统一把“一次性执行”包进
     * `Coroutine\run()`，让脚本与常驻 CrontabManager 子进程保持同样的协程语义。
     *
     * @param TaskCrontab $taskInstance 已解析并完成校验的任务实例。
     * @return void
     * @throws Throwable 任务执行异常会原样抛回调用方，供 LinuxCrontabManager 回写状态。
     */
    protected function executeTaskInCoroutine(TaskCrontab $taskInstance): void {
        if (Coroutine::getCid() > 0) {
            $taskInstance->execute();
            return;
        }

        $error = null;
        run(function () use ($taskInstance, &$error): void {
            try {
                $taskInstance->execute();
            } catch (Throwable $throwable) {
                $error = $throwable;
            }
        });

        if ($error instanceof Throwable) {
            throw $error;
        }
    }

    /**
     * 判断当前应用是否在线。
     *
     * 这里不再依赖 pid 文件，因为 pid 文件在异常停机或外部回收场景下可能残留。
     * 启动器改为先检查当前 app 预期业务端口是否存在真实监听进程，再尽量确认监听者
     * 的命令行特征与当前 SCF 应用一致，以降低“其他进程恰好占用了同端口”的误判。
     *
     * @return bool
     */
    protected function isApplicationOnline(): bool {
        $port = $this->resolveExpectedPort();
        if ($port > 0 && Server::isListeningPortInUse($port)) {
            $pids = Server::findPidsByPort($port);
            if (!$pids) {
                // 某些宿主机无法稳定解析监听 PID，这时只要业务端口已被占用，
                // 就交给 gateway 控制面探针做进一步确认。
                return $this->isGatewayControlPlaneOnline();
            }

            foreach ($pids as $pid) {
                $commandLine = $this->readProcessCommandLine($pid);
                if ($commandLine === '') {
                    // 无法读取命令行时，端口监听本身已经比 pid 文件可靠得多，
                    // 这里不直接判离线，避免在权限受限环境把在线服务误判成离线。
                    return true;
                }
                if ($this->matchesCurrentApplicationProcess($commandLine)) {
                    return true;
                }
            }
        }

        // gateway + nginx/tcp relay 模式下，业务入口端口可能由 nginx 或 relay 占用，
        // 这时不能再用“业务端口进程命令行像不像当前 app”来判断在线状态。
        return $this->isGatewayControlPlaneOnline();
    }

    /**
     * 解析当前应用预期的业务端口。
     *
     * 该命令的“应用在线”语义以业务入口端口为准：
     * - 显式传入 `-port` 时优先尊重调用方参数；
     * - 否则沿用 server 配置中的默认业务端口。
     *
     * @return int
     */
    protected function resolveExpectedPort(): int {
        $optionPort = (int)Manager::instance()->getOpt('port', 0);
        if ($optionPort > 0) {
            return $optionPort;
        }

        $serverConfig = Config::server();
        return (int)($serverConfig['port'] ?? SERVER_PORT ?? 0);
    }

    /**
     * 判断当前应用的 gateway 控制面是否在线。
     *
     * 业务入口走 nginx / tcp relay 时，实际监听 9580 的往往不是 PHP 业务进程，
     * 而是 gateway 前置层。对于 Linux cron 这种“只在应用在线时执行”的语义，
     * 更稳的判据是 gateway 控制面的健康接口是否能返回 200。
     *
     * @return bool
     */
    protected function isGatewayControlPlaneOnline(): bool {
        $port = $this->resolveGatewayControlPort();
        if ($port <= 0) {
            return false;
        }

        $response = $this->requestGatewayHealth($port);
        if ($response['status'] !== 200) {
            return false;
        }

        $payload = json_decode((string)$response['body'], true);
        return is_array($payload) && (string)($payload['message'] ?? '') === 'ok';
    }

    /**
     * 解析 gateway 控制面端口。
     *
     * 规则与 gateway 自身保持一致：
     * - 显式传入 `-control_port` 时优先使用；
     * - 其次使用 server.php 里的 `gateway_control_port`；
     * - 配置为 0 时默认取业务端口 + 1000。
     *
     * @return int
     */
    protected function resolveGatewayControlPort(): int {
        $optionPort = (int)Manager::instance()->getOpt('control_port', Manager::instance()->getOpt('gateway_control_port', 0));
        if ($optionPort > 0) {
            return $optionPort;
        }

        $businessPort = $this->resolveExpectedPort();
        if ($businessPort <= 0) {
            return 0;
        }

        $serverConfig = Config::server();
        $configuredBusinessPort = (int)($serverConfig['port'] ?? $businessPort);
        $configuredControlPort = (int)($serverConfig['gateway_control_port'] ?? 0);
        if ($configuredControlPort > 0) {
            $offset = max(1, $configuredControlPort - $configuredBusinessPort);
            return $businessPort + $offset;
        }

        return $businessPort + 1000;
    }

    /**
     * 请求本地 gateway 健康接口。
     *
     * 这里复用 gateway 命令行同样的“本地 TCP 请求”思路，避免把 Linux cron
     * 的在线判断依赖到浏览器路由或外部 nginx 配置上。
     *
     * @param int $port gateway 控制面端口
     * @return array{status:int,body:string}
     */
    protected function requestGatewayHealth(int $port): array {
        $socket = @stream_socket_client(
            "tcp://127.0.0.1:{$port}",
            $errno,
            $errstr,
            1.5,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            return ['status' => 0, 'body' => $errstr ?: 'connect failed'];
        }

        stream_set_timeout($socket, 2);
        $request = "GET /_gateway/healthz HTTP/1.1\r\n"
            . "Host: 127.0.0.1:{$port}\r\n"
            . "Connection: close\r\n\r\n";
        fwrite($socket, $request);
        $response = stream_get_contents($socket);
        fclose($socket);

        if (!is_string($response) || $response === '') {
            return ['status' => 0, 'body' => 'empty response'];
        }

        [$headers, $body] = array_pad(explode("\r\n\r\n", $response, 2), 2, '');
        $status = 0;
        if (preg_match('#^HTTP/\S+\s+(\d{3})#', $headers, $matches)) {
            $status = (int)$matches[1];
        }

        return [
            'status' => $status,
            'body' => $body,
        ];
    }

    /**
     * 读取进程命令行。
     *
     * Linux 优先走 `/proc/<pid>/cmdline`，这样不依赖外部命令；
     * 其他宿主机再退回 `ps`，保证本地开发机也能尽量做特征识别。
     *
     * @param int $pid 监听该端口的进程 id
     * @return string
     */
    protected function readProcessCommandLine(int $pid): string {
        if ($pid <= 0) {
            return '';
        }

        $procCmdline = '/proc/' . $pid . '/cmdline';
        if (is_readable($procCmdline)) {
            $content = (string)file_get_contents($procCmdline);
            if ($content !== '') {
                return trim(str_replace("\0", ' ', $content));
            }
        }

        $ps = trim((string)@shell_exec('command -v ps'));
        if ($ps === '') {
            return '';
        }

        $output = [];
        @exec($ps . ' -p ' . (int)$pid . ' -o command= 2>/dev/null', $output);
        return trim(implode(' ', $output));
    }

    /**
     * 判断监听进程是否像是当前 SCF 应用。
     *
     * 这里不要求命令行完全一致，只要能看出它是从当前项目目录启动的
     * server / gateway / boot 相关进程即可。这样在开发目录、源码模式和
     * 打包模式之间都能保留较好的兼容性。
     *
     * @param string $commandLine 进程命令行
     * @return bool
     */
    protected function matchesCurrentApplicationProcess(string $commandLine): bool {
        $normalized = strtolower($commandLine);
        $indicators = [
            strtolower((string)APP_DIR_NAME),
            strtolower((string)APP_PATH),
            strtolower((string)SCF_ROOT),
            ' boot ',
            'boot gateway',
            'boot server',
            'gateway_upstream',
        ];

        foreach ($indicators as $indicator) {
            if ($indicator !== '' && str_contains($normalized, $indicator)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 解析目标任务。
     *
     * 这里优先按完整命名空间精确匹配，其次匹配短类名，再匹配任务显示名。
     * 如果短类名或显示名出现重名，则返回歧义提示，避免 Linux crontab 调错脚本。
     *
     * @param string $identifier 用户传入的脚本标识
     * @return array|null
     */
    protected function resolveTask(string $identifier): ?array {
        $tasks = $this->registeredTasks();
        $normalizedIdentifier = ltrim(trim($identifier), '\\');
        $lowerIdentifier = strtolower($normalizedIdentifier);
        $exactNamespaceMatch = [];
        $shortNameMatch = [];
        $displayNameMatch = [];

        foreach ($tasks as $task) {
            $namespace = ltrim((string)$task['namespace'], '\\');
            $shortName = $this->shortClassName($namespace);
            $displayName = (string)$task['name'];

            if (strtolower($namespace) === $lowerIdentifier) {
                $exactNamespaceMatch[] = $task;
                continue;
            }
            if (strtolower($shortName) === $lowerIdentifier) {
                $shortNameMatch[] = $task;
                continue;
            }
            if (strtolower($displayName) === $lowerIdentifier) {
                $displayNameMatch[] = $task;
            }
        }

        foreach ([$exactNamespaceMatch, $shortNameMatch, $displayNameMatch] as $matches) {
            if (count($matches) === 1) {
                $this->resolveError = '';
                return $matches[0];
            }
            if (count($matches) > 1) {
                $this->resolveError = '匹配到多个 crontab 脚本, 请改用完整命名空间: ' . implode(', ', array_map(
                    static fn(array $task): string => (string)$task['namespace'],
                    $matches
                ));
                return null;
            }
        }

        $this->resolveError = '未找到 crontab 脚本: ' . $identifier;
        return null;
    }

    /**
     * 汇总当前上下文可执行的任务清单。
     *
     * 这里保持与 `CrontabManager::load()` 相同的任务发现规则，
     * 让一次性命令与常驻任务在“哪些脚本属于当前节点角色”上保持一致。
     *
     * @return array<int, array>
     */
    protected function registeredTasks(): array {
        if ($this->registeredTasks) {
            return $this->registeredTasks;
        }

        $tasks = [];
        $serverConfig = Config::server();
        if (App::isMaster() && ($serverConfig['db_statistics_enable'] ?? false)) {
            $tasks[] = $this->normalizeTask([
                'name' => '统计数据入库',
                'namespace' => '\Scf\Database\Statistics\StatisticCrontab',
                'mode' => TaskCrontab::RUN_MODE_LOOP,
                'interval' => $serverConfig['db_statistics_interval'] ?? 3,
                'timeout' => 3600,
                'status' => STATUS_ON,
            ]);
        }

        foreach (App::getModules(MODE_CGI) as $module) {
            $commonTasks = $module['crontabs'] ?? $module['background_tasks'] ?? [];
            foreach ($commonTasks as $task) {
                $tasks[] = $this->normalizeTask($task);
            }

            $roleTasks = App::isMaster() ? ($module['master_crontabs'] ?? []) : ($module['slave_crontabs'] ?? []);
            foreach ($roleTasks as $task) {
                $tasks[] = $this->normalizeTask($task);
            }
        }

        usort($tasks, static function (array $left, array $right): int {
            return strcmp($left['namespace'], $right['namespace']);
        });

        $this->registeredTasks = $tasks;
        return $this->registeredTasks;
    }

    /**
     * 标准化单条任务配置。
     *
     * 一次性执行时仍然需要填充 `id/status/timeout` 等运行元数据，
     * 这样脚本内部如果调用 `$this->log()` 或依赖父类属性时不会缺字段。
     *
     * @param array $task 原始任务配置
     * @return array
     */
    protected function normalizeTask(array $task): array {
        $namespace = (string)($task['namespace'] ?? '');
        $normalizedNamespace = '\\' . ltrim($namespace, '\\');
        $task['namespace'] = $normalizedNamespace;
        $task['id'] = 'CRONTAB:' . md5((string)App::id() . $normalizedNamespace);
        $task['manager_id'] = 0;
        $task['created'] = time();
        $task['status'] = (int)($task['status'] ?? STATUS_ON);
        $task['timeout'] = (int)($task['timeout'] ?? 3600);
        return $task;
    }

    /**
     * 提取短类名。
     *
     * @param string $namespace 完整命名空间
     * @return string
     */
    protected function shortClassName(string $namespace): string {
        $namespace = ltrim($namespace, '\\');
        if (!str_contains($namespace, '\\')) {
            return $namespace;
        }
        return substr($namespace, strrpos($namespace, '\\') + 1);
    }

    /**
     * 格式化运行模式标签。
     *
     * @param int $mode 任务运行模式
     * @return string
     */
    protected function modeLabel(int $mode): string {
        return match ($mode) {
            TaskCrontab::RUN_MODE_ONECE => '一次执行',
            TaskCrontab::RUN_MODE_LOOP => '循环执行',
            TaskCrontab::RUN_MODE_TIMING => '定时执行',
            TaskCrontab::RUN_MODE_INTERVAL => '间隔执行',
            default => '未知模式',
        };
    }
}
