<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;
use Swoole\Process;
use Throwable;
class UpstreamSupervisor {

    protected Process $process;
    protected array $managedInstances = [];
    protected bool $running = true;

    public function __construct(
        protected AppServerLauncher $launcher,
        protected array $plans = [],
        protected int $defaultStartTimeout = 25
    ) {
        $this->process = new Process([$this, 'run'], false, SOCK_DGRAM, false);
    }

    public function getProcess(): Process {
        return $this->process;
    }

    public function sendCommand(array $command): bool {
        $payload = json_encode($command, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($payload === false) {
            return false;
        }
        return $this->process->write($payload);
    }

    public function run(Process $process): void {
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_PID, getmypid());
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_STARTED_AT, time());
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT, time());
        foreach ($this->plans as $plan) {
            try {
                $this->launchPlan($plan);
            } catch (Throwable $throwable) {
                Console::error("【Gateway】业务实例启动失败: " . $this->describePlan($plan) . ', error=' . $throwable->getMessage());
            }
        }

        while ($this->running) {
            Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT, time());
            $data = $process->read();
            if ($data === '' || $data === false) {
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

        $this->shutdownAll();
        Runtime::instance()->set(Key::RUNTIME_UPSTREAM_SUPERVISOR_HEARTBEAT_AT, time());
    }

    protected function handleCommand(array $command): void {
        $action = (string)($command['action'] ?? '');
        if ($action === 'spawn') {
            $plan = (array)($command['plan'] ?? []);
            $this->launchPlan($plan);
            return;
        }
        if ($action === 'stop_version') {
            $this->stopVersion((string)($command['version'] ?? ''));
            return;
        }
        if ($action === 'stop_port') {
            $this->stopPort((int)($command['port'] ?? 0));
            return;
        }
        if ($action === 'stop_instance') {
            $this->stopManagedInstance((array)($command['instance'] ?? []));
            return;
        }
        if ($action === 'sync_instances') {
            $this->syncInstances((array)($command['instances'] ?? []));
            return;
        }
        if ($action === 'shutdown') {
            $this->running = false;
            $this->shutdownAll();
            return;
        }
    }

    protected function launchPlan(array $plan): void {
        $version = trim((string)($plan['version'] ?? ''));
        $host = (string)($plan['host'] ?? '127.0.0.1');
        $port = (int)($plan['port'] ?? 0);
        if ($version === '' || $port <= 0) {
            throw new RuntimeException('upstream 启动计划缺少 version 或 port');
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

        $rpcInfo = (int)($plan['rpc_port'] ?? 0) > 0 ? ', RPC:' . (int)$plan['rpc_port'] : '';
        Console::success("【Gateway】业务实例已启动 {$version} {$host}:{$port}{$rpcInfo} PID:{$this->managedInstances[$key]['metadata']['pid']}");
    }

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

    protected function shutdownAll(): void {
        foreach ($this->managedInstances as $key => $instance) {
            $this->stopInstance($instance);
            unset($this->managedInstances[$key]);
        }
    }

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

    protected function stopInstance(array $instance, int $graceSeconds = AppServerLauncher::NORMAL_RECYCLE_GRACE_SECONDS): void {
        $this->launcher->stop($instance, $graceSeconds);
    }

    protected function instanceKey(string $version, string $host, int $port): string {
        return $version . '@' . $host . ':' . $port;
    }

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
