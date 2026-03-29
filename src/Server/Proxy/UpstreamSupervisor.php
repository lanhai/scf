<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Scf\App\Updater;
use Scf\Client\Http;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Scf\Helper\JsonHelper;
use Scf\Util\Auth;
use Swoole\Process;
use Throwable;

/**
 * gateway 进程内的 upstream 监督器。
 *
 * 负责在单独的子进程里接收启动/停止/sync 指令，
 * 并把 managed upstream 的生命周期与 gateway 的 registry 同步起来。
 */
class UpstreamSupervisor {

    protected Process $process;
    protected array $managedInstances = [];
    protected bool $running = true;
    protected bool $shutdownManagedInstancesOnExit = true;

    public function __construct(
        protected AppServerLauncher $launcher,
        protected array $plans = [],
        protected int $defaultStartTimeout = 25
    ) {
        $this->process = new Process([$this, 'run'], false, SOCK_DGRAM, false);
    }

    /**
     * 返回监督器子进程对象，供 gateway attach 到主 server。
     */
    public function getProcess(): Process {
        return $this->process;
    }

    /**
     * 向监督器子进程发送控制命令。
     *
     * 命令通过 JSON 编码写入子进程消息队列，保持轻量且无共享内存依赖。
     */
    public function sendCommand(array $command): bool {
        $payload = json_encode($command, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($payload === false) {
            return false;
        }
        return $this->process->write($payload);
    }

    /**
     * 监督器主循环。
     *
     * 启动时先按初始 plans 拉起 upstream，再持续处理控制命令；
     * 退出时统一回收所有托管实例。
     */
    public function run(Process $process): void {
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_PID, getmypid());
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_STARTED_AT, time());
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT, time());
        // gateway restart / shutdown 时，监督器既要继续收口托管 upstream，又不能
        // 被阻塞式 Process::read() 卡住。这里改成与 Crontab/RedisQueue manager
        // 一致的非阻塞 pipe 读取，并在 pipe 已关闭时把它当作监督器的终止信号。
        $commandPipe = fopen('php://fd/' . $process->pipe, 'r');
        is_resource($commandPipe) and stream_set_blocking($commandPipe, false);
        foreach ($this->plans as $plan) {
            try {
                $this->launchPlan($plan);
            } catch (Throwable $throwable) {
                Console::error("【Gateway】业务实例启动失败: " . $this->describePlan($plan) . ', error=' . $throwable->getMessage());
            }
        }

        while ($this->running) {
            Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT, time());
            $data = is_resource($commandPipe) ? fread($commandPipe, 65535) : false;
            if ($data === false || $data === '') {
                // 父进程退出或 pipe 已被内核回收后，继续 read 只会反复抛出
                // "Bad file descriptor"。这里把 EOF 视作监督器生命周期结束，交给
                // run() 尾部统一执行 shutdownAll()/detach 收口。
                if (!is_resource($commandPipe) || feof($commandPipe)) {
                    Console::warning('【Gateway】UpstreamSupervisor 控制 pipe 已关闭，结束运行');
                    break;
                }
                usleep(100000);
                continue;
            }
            $command = json_decode($data, true);
            if (!is_array($command)) {
                continue;
            }
            try {
                $this->handleCommand($command);
            } catch (Throwable $throwable) {
                Console::error("【Gateway】业务实例命令执行失败: " . json_encode($command, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . ', error=' . $throwable->getMessage());
            }
        }

        if ($this->shutdownManagedInstancesOnExit) {
            $this->shutdownAll();
        }
        is_resource($commandPipe) and fclose($commandPipe);
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT, time());
    }

    /**
     * 处理 gateway 下发的单条监督命令。
     */
    protected function handleCommand(array $command): void {
        $action = (string)($command['action'] ?? '');
        $requestId = (string)($command['request_id'] ?? '');
        $result = null;
        if ($action === 'spawn') {
            $plan = (array)($command['plan'] ?? []);
            $this->launchPlan($plan);
            $result = ['ok' => true, 'message' => 'spawn accepted', 'data' => []];
        } elseif ($action === 'stop_version') {
            $this->stopVersion((string)($command['version'] ?? ''));
            $result = ['ok' => true, 'message' => 'stop_version accepted', 'data' => []];
        } elseif ($action === 'stop_port') {
            $this->stopPort((int)($command['port'] ?? 0));
            $result = ['ok' => true, 'message' => 'stop_port accepted', 'data' => []];
        } elseif ($action === 'stop_instance') {
            $this->stopManagedInstance((array)($command['instance'] ?? []));
            $result = ['ok' => true, 'message' => 'stop_instance accepted', 'data' => []];
        } elseif ($action === 'sync_instances') {
            $this->syncInstances((array)($command['instances'] ?? []));
            $result = ['ok' => true, 'message' => 'sync_instances accepted', 'data' => []];
        } elseif ($action === 'install') {
            $result = $this->executeInstall(
                (string)($command['key'] ?? ''),
                (string)($command['role'] ?? 'master'),
                (array)($command['plans'] ?? [])
            );
        } elseif ($action === 'shutdown') {
            $this->running = false;
            $result = ['ok' => true, 'message' => 'shutdown accepted', 'data' => []];
        } elseif ($action === 'detach') {
            $this->running = false;
            $this->shutdownManagedInstancesOnExit = false;
            $result = ['ok' => true, 'message' => 'detach accepted', 'data' => []];
        }
        if ($requestId !== '' && is_array($result)) {
            Runtime::instance()->delete($this->commandResultKey($requestId));
            Runtime::instance()->set($this->commandResultKey($requestId), $result);
        }
    }

    /**
     * 判断监督器子进程是否仍然存活。
     *
     * Gateway restart 需要等待监督器退出自身，否则它会继续持有旧控制面的监听 FD。
     *
     * @return bool
     */
    public function isAlive(): bool {
        $pid = (int)($this->process->pid ?? 0);
        return $pid > 0 && @Process::kill($pid, 0);
    }

    /**
     * 按启动计划拉起一个托管 upstream，并等待它进入 ready 状态。
     */
    protected function launchPlan(array $plan): void {
        $version = trim((string)($plan['version'] ?? ''));
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            throw new RuntimeException('upstream 启动计划缺少 version 或 port');
        }
        if (!App::isReady()) {
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER, true);
            throw new RuntimeException('应用尚未完成初始化安装');
        }

        $key = $this->instanceKey($version, $host, $port);
        if (isset($this->managedInstances[$key])) {
            $existing = (array)$this->managedInstances[$key];
            $rpcPort = (int)($existing['metadata']['rpc_port'] ?? 0);
            $pid = (int)($existing['metadata']['pid'] ?? 0);
            $httpAlive = $this->launcher->isListening($host, $port, 0.2);
            $rpcAlive = $rpcPort <= 0 || $this->launcher->isListening($host, $rpcPort, 0.2);
            $pidAlive = $pid > 0 && @Process::kill($pid, 0);
            if ($httpAlive || $rpcAlive || $pidAlive) {
                return;
            }
            unset($this->managedInstances[$key]);
        }

        $instance = $this->launcher->launch([
            'app' => (string)($plan['app'] ?? APP_DIR_NAME),
            'env' => (string)($plan['env'] ?? SERVER_RUN_ENV),
            'role' => (string)($plan['role'] ?? SERVER_ROLE),
            'port' => $port,
            'rpc_port' => (int)($plan['rpc_port'] ?? 0),
            'src' => (string)($plan['src'] ?? APP_SRC_TYPE),
            'host' => $host,
            'extra' => (array)($plan['extra'] ?? []),
        ]);

        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        if (!$this->launcher->waitUntilServicesReady($host, $port, $rpcPort, (int)($plan['start_timeout'] ?? $this->defaultStartTimeout), 200, false)) {
            $this->stopInstance([
                'host' => $host,
                'port' => $port,
                'metadata' => [
                    'managed' => true,
                    'pid' => (int)($instance['pid'] ?? 0),
                ],
            ], 1);
            $message = $rpcPort > 0
                ? "业务 server 启动超时，HTTP/RPC 端口未在预期时间内就绪: {$host}:{$port}, rpc:{$rpcPort}"
                : "业务 server 启动超时，端口未就绪: {$host}:{$port}";
            throw new RuntimeException($message);
        }

        $this->managedInstances[$key] = [
            'version' => $version,
            'host' => $host,
            'port' => $port,
            'weight' => (int)($plan['weight'] ?? 100),
            'metadata' => [
                'managed' => true,
                'pid' => (int)($instance['pid'] ?? 0),
                'role' => (string)($plan['role'] ?? SERVER_ROLE),
                'rpc_port' => (int)($plan['rpc_port'] ?? 0),
                'command' => (string)($instance['command'] ?? ''),
                'started_at' => time(),
                'managed_mode' => 'gateway_supervisor',
            ],
        ];

    }

    /**
     * 在 UpstreamSupervisor 中执行安装流程。
     *
     * 该流程包含三步：
     * 1. 解析并落盘安装秘钥；
     * 2. 下载业务更新包并等待应用进入 ready；
     * 3. 直接在监督器中拉起业务实例，后续由 gateway 编排侧完成注册和 nginx 切流。
     *
     * @param string $key 安装秘钥
     * @param string $role 安装角色
     * @param array<int, array<string, mixed>> $plans 安装完成后需要拉起的业务实例计划
     * @return array<string, mixed>
     */
    protected function executeInstall(string $key, string $role = 'master', array $plans = []): array {
        if (App::isReady()) {
            return [
                'ok' => false,
                'message' => '应用已完成安装',
                'data' => [],
            ];
        }
        $key = trim($key);
        $role = trim($role) ?: 'master';
        if ($key === '') {
            return [
                'ok' => false,
                'message' => '安装秘钥不能为空',
                'data' => [],
            ];
        }

        Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER, true);
        Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_UPDATING, true);

        try {
            $installer = App::installer();
            $installer->public_path = 'public';
            $installer->app_path = APP_DIR_NAME;

            $secret = substr($key, 0, 32);
            $installKey = substr($key, 32);
            $decode = Auth::decode($installKey, $secret);
            if (!$decode) {
                return ['ok' => false, 'message' => '安装秘钥错误', 'data' => []];
            }
            $config = JsonHelper::recover($decode);
            if (empty($config['key']) || empty($config['server']) || empty($config['dashboard_password']) || empty($config['expired'])) {
                return ['ok' => false, 'message' => '安装秘钥错误', 'data' => []];
            }
            if (time() > (int)$config['expired']) {
                return ['ok' => false, 'message' => '安装秘钥已过期', 'data' => []];
            }

            $installer->app_auth_key = $config['key'];
            $installer->dashboard_password = $config['dashboard_password'];
            $installer->update_server = (string)$config['server'];
            $installer->role = $role;

            $client = Http::create($installer->update_server . '?time=' . time());
            $versionResult = $client->get();
            if ($versionResult->hasError()) {
                return ['ok' => false, 'message' => '获取云端版本号失败:' . $versionResult->getMessage(), 'data' => []];
            }
            $remote = $versionResult->getData();
            $appVersion = $remote['app'] ?? '';
            if (!$appVersion) {
                return ['ok' => false, 'message' => '获取云端版本号失败', 'data' => []];
            }

            $installer->version = $appVersion[0]['version'];
            $installer->appid = $appVersion[0]['appid'];
            $installer->updated = date('Y-m-d H:i:s');
            if (!$installer->add()) {
                return ['ok' => false, 'message' => '安装失败', 'data' => []];
            }

            $updater = Updater::instance();
            $targetVersion = (string)($updater->getVersion()['remote']['app']['version'] ?? '');
            if ($targetVersion !== '') {
                Console::info('【Gateway】开始执行安装更新:' . $targetVersion);
            }
            if (!$updater->updateApp(true)) {
                $message = $updater->getLastError() ?: ($targetVersion !== '' ? "更新失败:{$targetVersion}" : '更新失败');
                return ['ok' => false, 'message' => $message, 'data' => []];
            }

            while (!App::isReady()) {
                usleep(500000);
            }

            $launched = [];
            foreach ($plans as $plan) {
                if (!is_array($plan)) {
                    continue;
                }
                $this->launchPlan($plan);
                $launched[] = $this->describePlan($plan);
            }

            return [
                'ok' => true,
                'message' => '安装完成',
                'data' => [
                    'password' => (string)$installer->dashboard_password,
                    'launched' => $launched,
                ],
            ];
        } catch (Throwable $throwable) {
            return [
                'ok' => false,
                'message' => $throwable->getMessage(),
                'data' => [],
            ];
        } finally {
            Runtime::instance()->set(Key::RUNTIME_GATEWAY_INSTALL_UPDATING, false);
        }
    }

    /**
     * 按版本批量停止当前监督中的 upstream。
     */
    protected function stopVersion(string $version): void {
        if ($version === '') {
            return;
        }

        foreach ($this->managedInstances as $key => $instance) {
            if (($instance['version'] ?? '') !== $version) {
                continue;
            }
            $this->stopInstance($instance);
            unset($this->managedInstances[$key]);
        }
    }

    /**
     * 按端口停止当前监督中的 upstream。
     */
    protected function stopPort(int $port): void {
        if ($port <= 0) {
            return;
        }

        foreach ($this->managedInstances as $key => $instance) {
            if ((int)($instance['port'] ?? 0) !== $port) {
                continue;
            }
            $this->stopInstance($instance);
            unset($this->managedInstances[$key]);
        }
    }

    /**
     * 停止单个 upstream 实例，支持携带更完整的实例元数据。
     */
    protected function stopManagedInstance(array $instance): void {
        $host = (string)($instance['host'] ?? '127.0.0.1');
        $port = (int)($instance['port'] ?? 0);
        if ($port <= 0) {
            return;
        }

        $version = trim((string)($instance['version'] ?? ''));
        $key = $version !== '' ? $this->instanceKey($version, $host, $port) : null;
        if ($key !== null && isset($this->managedInstances[$key])) {
            $instance = array_replace_recursive($this->managedInstances[$key], $instance);
            unset($this->managedInstances[$key]);
        } else {
            foreach ($this->managedInstances as $managedKey => $managedInstance) {
                if ((string)($managedInstance['host'] ?? '127.0.0.1') !== $host || (int)($managedInstance['port'] ?? 0) !== $port) {
                    continue;
                }
                $instance = array_replace_recursive($managedInstance, $instance);
                unset($this->managedInstances[$managedKey]);
                break;
            }
        }

        $metadata = (array)($instance['metadata'] ?? []);
        $metadata['managed'] = true;
        $metadata['rpc_port'] = (int)($metadata['rpc_port'] ?? ($instance['rpc_port'] ?? 0));
        $instance['metadata'] = $metadata;
        $this->stopInstance($instance);
    }

    /**
     * 退出时回收所有仍在监督中的 upstream。
     */
    protected function shutdownAll(): void {
        foreach ($this->managedInstances as $key => $instance) {
            $this->stopInstance($instance);
            unset($this->managedInstances[$key]);
        }
    }

    /**
     * 用外部同步结果重建当前监督状态。
     *
     * 这个入口用于 gateway 恢复/重载后，将 registry 中已存在的 managed 实例
     * 与当前监督器的内存态重新对齐。
     */
    protected function syncInstances(array $instances): void {
        $synced = [];
        foreach ($instances as $instance) {
            if (!is_array($instance)) {
                continue;
            }
            $version = trim((string)($instance['version'] ?? ''));
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            if ($version === '' || $port <= 0) {
                continue;
            }
            $key = $this->instanceKey($version, $host, $port);
            $incomingMetadata = (array)($instance['metadata'] ?? []);
            $existingMetadata = (array)($this->managedInstances[$key]['metadata'] ?? []);
            if (!isset($incomingMetadata['pid']) || (int)$incomingMetadata['pid'] <= 0) {
                if (($existingMetadata['pid'] ?? 0) > 0) {
                    $incomingMetadata['pid'] = (int)$existingMetadata['pid'];
                }
            }
            if (!isset($incomingMetadata['command']) || trim((string)$incomingMetadata['command']) === '') {
                if (($existingMetadata['command'] ?? '') !== '') {
                    $incomingMetadata['command'] = (string)$existingMetadata['command'];
                }
            }
            if (!isset($incomingMetadata['started_at']) || (int)$incomingMetadata['started_at'] <= 0) {
                if (($existingMetadata['started_at'] ?? 0) > 0) {
                    $incomingMetadata['started_at'] = (int)$existingMetadata['started_at'];
                }
            }
            if (!isset($incomingMetadata['rpc_port']) || (int)$incomingMetadata['rpc_port'] <= 0) {
                if (($existingMetadata['rpc_port'] ?? 0) > 0) {
                    $incomingMetadata['rpc_port'] = (int)$existingMetadata['rpc_port'];
                }
            }
            $synced[$key] = [
                'version' => $version,
                'host' => $host,
                'port' => $port,
                'weight' => (int)($instance['weight'] ?? 100),
                'metadata' => $incomingMetadata,
            ];
        }
        $this->managedInstances = $synced;
    }

    /**
     * 调用 launcher 执行单个实例的优雅停机/强制回收。
     */
    protected function stopInstance(array $instance, int $graceSeconds = AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS): void {
        $this->launcher->stop($instance, $graceSeconds);
    }

    /**
     * 生成托管实例的内存索引键。
     */
    protected function instanceKey(string $version, string $host, int $port): string {
        return $version . '@' . $host . ':' . $port;
    }

    /**
     * 生成监督器命令结果在 Runtime 中的存储 key。
     *
     * @param string $requestId 命令请求 id
     * @return string
     */
    protected function commandResultKey(string $requestId): string {
        return 'upstream_supervisor:' . md5($requestId);
    }

    /**
     * 用于日志中展示启动计划的摘要信息。
     */
    protected function describePlan(array $plan): string {
        $version = trim((string)($plan['version'] ?? ''));
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        $rpcPort = (int)($plan['rpc_port'] ?? 0);
        return $rpcPort > 0
            ? "{$version} {$host}:{$port}, RPC:{$rpcPort}"
            : "{$version} {$host}:{$port}";
    }
}
