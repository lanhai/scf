<?php

namespace Scf\Server\Gateway;

use Scf\Core\Console;
use Scf\Core\Config;
use RuntimeException;

/**
 * gateway 侧 upstream 的内存状态机与派生统计协调器。
 *
 * 这一层负责把 registry 中持久化的 generation 状态加载到内存，维护 active / draining / offline
 * 的生命周期转换，并同步管理连接绑定、HTTP/RPC 处理量和 crontab / queue 运行态统计。
 */
class AppInstanceManager {

    protected array $state = [];
    protected array $rrIndex = [];
    protected array $connectionBindings = [];
    protected array $instanceConnectionCount = [];
    protected array $instanceRuntimeState = [];
    protected array $drainingHttpRpcStallState = [];
    protected bool $hasDrainingGeneration = false;

    /**
     * 注入 registry 并加载初始状态。
     *
     * @param UpstreamRegistry $registry upstream 状态的持久化存取入口。
     * @return void
     */
    public function __construct(
        protected UpstreamRegistry $registry
    ) {
        $this->reload();
    }

    /**
     * 重新从 registry 读取状态，重建派生标记。
     *
     * @return void
     */
    public function reload(): void {
        $this->state = $this->registry->load();
        $this->refreshStateFlags();
    }

    /**
     * 返回当前 registry 的原始内存态。
     *
     * 如需附加连接数等派生字段，请使用 snapshot()。
     *
     * @return array<string, mixed> 当前原始状态。
     */
    public function state(): array {
        return $this->state;
    }

    /**
     * 返回 registry 状态文件路径。
     *
     * @return string 状态文件路径。
     */
    public function stateFile(): string {
        return $this->registry->stateFile();
    }

    /**
     * 注册一个 upstream，并按需立即激活该 generation。
     *
     * @param string|null $version 目标 generation 版本。
     * @param string|null $host upstream 主机名或 IP。
     * @param int|null $port upstream HTTP 端口。
     * @param bool $activate 是否在注册后立即激活。
     * @param int $weight 实例权重。
     * @param array<string, mixed> $metadata 实例元数据。
     * @return void
     */
    public function bootstrap(?string $version, ?string $host, ?int $port, bool $activate = true, int $weight = 100, array $metadata = []): void {
        if (!$version || !$host || !$port) {
            return;
        }
        $this->registerUpstream($version, $host, $port, $weight, $metadata);
        if ($activate) {
            $this->activateVersion($version, 0);
        }
    }

    /**
     * 在指定 generation 中，仅保留目标实例，其余实例移除。
     *
     * @param string $version generation 版本。
     * @param string $host 目标实例主机名或 IP。
     * @param int $port 目标实例端口。
     * @return array<string, mixed> 更新后的状态快照。
     */
    public function removeOtherInstances(string $version, string $host, int $port): array {
        if (!isset($this->state['generations'][$version])) {
            return $this->snapshot();
        }

        $generation = &$this->state['generations'][$version];
        $generation['instances'] = array_values(array_filter($generation['instances'], static function ($instance) use ($host, $port) {
            return (($instance['host'] ?? '') === $host && (int)($instance['port'] ?? 0) === $port);
        }));

        if (!$generation['instances']) {
            $generation['status'] = 'offline';
            if (($this->state['active_version'] ?? null) === $version) {
                $this->state['active_version'] = null;
            }
        }

        $this->touchState();
        return $this->snapshot();
    }

    /**
     * 获取指定实例之外的同 generation 其它实例。
     *
     * @param string $version generation 版本。
     * @param string $host 目标实例主机名或 IP。
     * @param int $port 目标实例端口。
     * @return array<int, array<string, mixed>> 其它实例列表。
     */
    public function otherInstances(string $version, string $host, int $port): array {
        if (!isset($this->state['generations'][$version])) {
            return [];
        }

        return array_values(array_filter($this->state['generations'][$version]['instances'] ?? [], static function ($instance) use ($host, $port) {
            return !(($instance['host'] ?? '') === $host && (int)($instance['port'] ?? 0) === $port);
        }));
    }

    /**
     * 注册一个 upstream 实例到 registry。
     *
     * @param string $version generation 版本。
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param int $weight 实例权重。
     * @param array<string, mixed> $metadata 实例元数据。
     * @return array<string, mixed> 注册后的实例描述。
     * @throws RuntimeException 当 version 为空或 port 非法时抛出。
     */
    public function registerUpstream(string $version, string $host, int $port, int $weight = 100, array $metadata = []): array {
        $version = trim($version);
        if ($version === '') {
            throw new RuntimeException('version 不能为空');
        }
        if ($port <= 0) {
            throw new RuntimeException('port 非法');
        }

        $generation = $this->state['generations'][$version] ?? [
            'version' => $version,
            'status' => 'prepared',
            'created_at' => time(),
            'activated_at' => null,
            'drain_started_at' => null,
            'drain_deadline_at' => null,
            'metadata' => [],
            'instances' => [],
        ];

        foreach ($generation['instances'] as &$instance) {
            if ($instance['host'] === $host && (int)$instance['port'] === $port) {
                $instance['weight'] = max(1, $weight);
                $instance['metadata'] = $metadata;
                $instance['status'] = $generation['status'];
                $this->state['generations'][$version] = $generation;
                $this->touchState();
                return $instance;
            }
        }
        unset($instance);

        $instance = [
            'id' => md5($version . '@' . $host . ':' . $port),
            'version' => $version,
            'host' => $host,
            'port' => $port,
            'weight' => max(1, $weight),
            'status' => $generation['status'],
            'registered_at' => time(),
            'metadata' => $metadata,
        ];
        $generation['instances'][] = $instance;
        $this->state['generations'][$version] = $generation;
        $this->touchState();
        return $instance;
    }

    /**
     * 切换 active generation，并将旧 active 标记为 draining。
     *
     * @param string $version 需要激活的 generation 版本。
     * @param int $graceSeconds 旧 generation 的 draining 宽限时间，单位秒。
     * @return array<string, mixed> 更新后的状态快照。
     * @throws RuntimeException 当版本尚未注册时抛出。
     */
    public function activateVersion(string $version, int $graceSeconds = 30): array {
        if (!isset($this->state['generations'][$version])) {
            throw new RuntimeException('版本未注册:' . $version);
        }
        $now = time();
        foreach ($this->state['generations'] as $itemVersion => &$generation) {
            if ($itemVersion === $version) {
                $generation['status'] = 'active';
                $generation['activated_at'] = $now;
                $generation['drain_started_at'] = null;
                $generation['drain_deadline_at'] = null;
                foreach ($generation['instances'] as &$instance) {
                    $instance['status'] = 'active';
                }
                unset($instance);
                continue;
            }
            if ($generation['status'] === 'active') {
                $this->markGenerationDraining($generation, $now, $graceSeconds);
            }
        }
        unset($generation);
        $this->state['active_version'] = $version;
        $this->touchState();
        return $this->snapshot();
    }

    /**
     * 主动把某个 generation 置为 draining。
     *
     * @param string $version 需要进入 draining 的 generation 版本。
     * @param int $graceSeconds draining 宽限时间，单位秒。
     * @return array<string, mixed> 更新后的状态快照。
     * @throws RuntimeException 当版本尚未注册时抛出。
     */
    public function drainVersion(string $version, int $graceSeconds = 30): array {
        if (!isset($this->state['generations'][$version])) {
            throw new RuntimeException('版本未注册:' . $version);
        }
        $generation = &$this->state['generations'][$version];
        $this->markGenerationDraining($generation, time(), $graceSeconds);
        if (($this->state['active_version'] ?? null) === $version) {
            $this->state['active_version'] = null;
        }
        $this->touchState();
        return $this->snapshot();
    }

    /**
     * 删除整个 generation。
     *
     * @param string $version generation 版本。
     * @return array<string, mixed> 更新后的状态快照。
     */
    public function removeVersion(string $version): array {
        foreach (($this->state['generations'][$version]['instances'] ?? []) as $instance) {
            $this->clearInstanceRuntimeStatus((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0));
        }
        unset($this->state['generations'][$version]);
        if (($this->state['active_version'] ?? null) === $version) {
            $this->state['active_version'] = null;
        }
        $this->touchState();
        return $this->snapshot();
    }

    /**
     * 从所有 generation 中移除指定实例。
     *
     * @param string $host 目标实例主机名或 IP。
     * @param int $port 目标实例端口。
     * @return array<string, mixed> 更新后的状态快照。
     */
    public function removeInstance(string $host, int $port): array {
        $this->clearInstanceRuntimeStatus($host, $port);
        foreach ($this->state['generations'] as $version => &$generation) {
            $generation['instances'] = array_values(array_filter($generation['instances'], static function ($instance) use ($host, $port) {
                return !(($instance['host'] ?? '') === $host && (int)($instance['port'] ?? 0) === $port);
            }));
            if (!$generation['instances']) {
                $generation['status'] = 'offline';
                if (($this->state['active_version'] ?? null) === $version) {
                    $this->state['active_version'] = null;
                }
            }
        }
        unset($generation);
        $this->touchState();
        return $this->snapshot();
    }

    /**
     * 将指定实例直接标记为 offline，用于不可达或强制回收场景。
     *
     * @param string $host 目标实例主机名或 IP。
     * @param int $port 目标实例端口。
     * @param string|null $version 可选的 generation 过滤条件。
     * @return array<string, mixed> 更新后的状态快照。
     */
    public function markInstanceOffline(string $host, int $port, ?string $version = null): array {
        if ($host === '' || $port <= 0) {
            return $this->snapshot();
        }

        $changed = false;
        foreach ($this->state['generations'] as $itemVersion => &$generation) {
            if ($version !== null && $version !== '' && $itemVersion !== $version) {
                continue;
            }

            $matched = false;
            $hasLiveInstance = false;
            foreach ($generation['instances'] as &$instance) {
                $instanceHost = (string)($instance['host'] ?? '');
                $instancePort = (int)($instance['port'] ?? 0);
                if ($instanceHost === $host && $instancePort === $port) {
                    if (($instance['status'] ?? '') !== 'offline') {
                        $instance['status'] = 'offline';
                        $changed = true;
                    }
                    $matched = true;
                }
                if (($instance['status'] ?? '') !== 'offline') {
                    $hasLiveInstance = true;
                }
            }
            unset($instance);

            if (!$matched) {
                continue;
            }

            $this->clearInstanceRuntimeStatus($host, $port);
            if (!$hasLiveInstance) {
                if (($generation['status'] ?? '') !== 'offline') {
                    $generation['status'] = 'offline';
                    $changed = true;
                }
                $generation['drain_started_at'] = $generation['drain_started_at'] ?: time();
                $generation['drain_deadline_at'] = time();
                if (($this->state['active_version'] ?? null) === $itemVersion) {
                    $this->state['active_version'] = null;
                    $changed = true;
                }
            }
        }
        unset($generation);

        if ($changed) {
            $this->touchState();
        }

        return $this->snapshot();
    }

    /**
     * 更新实例运行态统计，供 draining 判定与诊断使用。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param array<string, mixed> $runtimeStatus 运行态统计数据。
     * @return void
     */
    public function updateInstanceRuntimeStatus(string $host, int $port, array $runtimeStatus): void {
        if ($host === '' || $port <= 0) {
            return;
        }
        $this->instanceRuntimeState[$this->runtimeStateKey($host, $port)] = [
            'http_request_processing' => max(0, (int)($runtimeStatus['http_request_processing'] ?? 0)),
            'rpc_request_processing' => max(0, (int)($runtimeStatus['rpc_request_processing'] ?? 0)),
            'redis_queue_processing' => max(0, (int)($runtimeStatus['redis_queue_processing'] ?? 0)),
            'crontab_busy' => max(0, (int)($runtimeStatus['crontab_busy'] ?? 0)),
            'mysql_inflight' => max(0, (int)($runtimeStatus['mysql_inflight'] ?? 0)),
            'redis_inflight' => max(0, (int)($runtimeStatus['redis_inflight'] ?? 0)),
            'outbound_http_inflight' => max(0, (int)($runtimeStatus['outbound_http_inflight'] ?? 0)),
            'updated_at' => time(),
        ];
    }

    /**
     * 读取实例最近一次运行态快照（可选新鲜度约束）。
     *
     * 当 upstream 已进入 draining/offline 且状态接口短暂不可达时，gateway 回收状态机
     * 可以回退到这份“最近快照”继续判断是否满足回收窗口条件，避免因为短时采样空洞
     * 让回收无谓拖到更晚窗口。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param int $maxAgeSeconds 允许快照距离当前的最大秒数；<=0 表示不限制。
     * @return array<string, mixed> 命中时返回快照，未命中或已过期返回空数组。
     */
    public function instanceRuntimeStatus(string $host, int $port, int $maxAgeSeconds = 15): array {
        if ($host === '' || $port <= 0) {
            return [];
        }
        $runtime = (array)($this->instanceRuntimeState[$this->runtimeStateKey($host, $port)] ?? []);
        if (!$runtime) {
            return [];
        }
        $updatedAt = (int)($runtime['updated_at'] ?? 0);
        if ($maxAgeSeconds > 0 && $updatedAt > 0 && (time() - $updatedAt) > $maxAgeSeconds) {
            return [];
        }
        return $runtime;
    }

    /**
     * 合并实例元数据补丁。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @param array<string, mixed> $metadataPatch 需要合并的元数据变更。
     * @return void
     */
    public function mergeInstanceMetadata(string $host, int $port, array $metadataPatch): void {
        if ($host === '' || $port <= 0 || !$metadataPatch) {
            return;
        }
        $changed = false;
        foreach ($this->state['generations'] as &$generation) {
            foreach ($generation['instances'] as &$instance) {
                if ((string)($instance['host'] ?? '') !== $host || (int)($instance['port'] ?? 0) !== $port) {
                    continue;
                }
                $metadata = (array)($instance['metadata'] ?? []);
                foreach ($metadataPatch as $key => $value) {
                    if ($value === null) {
                        continue;
                    }
                    if (($metadata[$key] ?? null) === $value) {
                        continue;
                    }
                    $metadata[$key] = $value;
                    $changed = true;
                }
                $instance['metadata'] = $metadata;
            }
            unset($instance);
        }
        unset($generation);
        if ($changed) {
            $this->touchState();
        }
    }

    /**
     * 清理单个实例的运行态统计缓存。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @return void
     */
    public function clearInstanceRuntimeStatus(string $host, int $port): void {
        if ($host === '' || $port <= 0) {
            return;
        }
        unset($this->instanceRuntimeState[$this->runtimeStateKey($host, $port)]);
    }

    /**
     * 使用外部 keeper 重建实例列表，清理不再存在的托管实例。
     *
     * @param callable $keeper 外部保活判定器，返回 true 表示保留实例。
     * @return array<string, mixed> 更新后的状态快照。
     */
    public function reconcileInstances(callable $keeper): array {
        $changed = false;
        foreach ($this->state['generations'] as $version => &$generation) {
            $beforeCount = count($generation['instances'] ?? []);
            $generation['instances'] = array_values(array_filter($generation['instances'] ?? [], static function ($instance) use ($keeper, $version) {
                return (bool)$keeper($instance, $version);
            }));

            if (count($generation['instances']) !== $beforeCount) {
                $changed = true;
            }

            if (!$generation['instances']) {
                if (($generation['status'] ?? '') !== 'offline') {
                    $generation['status'] = 'offline';
                    $changed = true;
                }
                if (($this->state['active_version'] ?? null) === $version) {
                    $this->state['active_version'] = null;
                    $changed = true;
                }
                continue;
            }

            if (($generation['status'] ?? '') === 'offline') {
                $generation['status'] = 'prepared';
                $changed = true;
            }
        }
        unset($generation);

        if ($changed) {
            $this->touchState();
        }
        return $this->snapshot();
    }

    /**
     * 返回当前状态快照，并补充连接数等派生字段。
     *
     * @return array<string, mixed> 包含派生统计的完整状态快照。
     */
    public function snapshot(): array {
        $this->tick();
        $state = $this->state;
        foreach ($state['generations'] as &$generation) {
            $connectionTotal = 0;
            foreach ($generation['instances'] as &$instance) {
                $instance['runtime_connections'] = $this->instanceConnectionCount[$instance['id']] ?? 0;
                $connectionTotal += $instance['runtime_connections'];
            }
            unset($instance);
            $generation['runtime_connections'] = $connectionTotal;
        }
        unset($generation);
        return $state;
    }

    /**
     * 返回所有 managed upstream 实例。
     *
     * @return array<int, array<string, mixed>> managed 实例列表。
     */
    public function managedInstances(): array {
        $instances = [];
        foreach ($this->state['generations'] as $generation) {
            foreach ($generation['instances'] ?? [] as $instance) {
                if (($instance['metadata']['managed'] ?? false) === true) {
                    $instances[] = $instance;
                }
            }
        }
        return $instances;
    }

    /**
     * 移除所有 managed upstream。
     *
     * @return array<string, mixed> 更新后的状态快照。
     */
    public function removeManagedInstances(): array {
        $changed = false;
        foreach ($this->state['generations'] as $version => &$generation) {
            $beforeCount = count($generation['instances'] ?? []);
            $generation['instances'] = array_values(array_filter($generation['instances'] ?? [], static function ($instance) {
                return (($instance['metadata']['managed'] ?? false) !== true);
            }));

            if (count($generation['instances']) !== $beforeCount) {
                $changed = true;
            }

            if (!$generation['instances']) {
                $generation['status'] = 'offline';
                if (($this->state['active_version'] ?? null) === $version) {
                    $this->state['active_version'] = null;
                }
                $changed = true;
            }
        }
        unset($generation);

        if ($changed) {
            $this->touchState();
        }

        return $this->snapshot();
    }

    /**
     * 选择当前 active generation 中的 HTTP upstream。
     *
     * @param string|null $affinityKey 可选亲和键，用于稳定落点。
     * @return array<string, mixed>|null 命中的实例，失败时返回 null。
     */
    public function pickHttpUpstream(?string $affinityKey = null): ?array {
        $this->tick();
        $activeVersion = $this->state['active_version'] ?? null;
        if (!$activeVersion || !isset($this->state['generations'][$activeVersion])) {
            return null;
        }
        return $this->pickGenerationInstance($this->state['generations'][$activeVersion], $affinityKey);
    }

    /**
     * 基于 HTTP upstream 选择对应 RPC upstream。
     *
     * @return array<string, mixed>|null 对应的 RPC 实例信息，失败时返回 null。
     */
    public function pickRpcUpstream(): ?array {
        $upstream = $this->pickHttpUpstream();
        if (!$upstream) {
            return null;
        }

        $rpcPort = (int)(($upstream['metadata']['rpc_port'] ?? 0));
        if ($rpcPort <= 0) {
            return null;
        }

        $upstream['port'] = $rpcPort;
        $upstream['rpc_port'] = $rpcPort;
        return $upstream;
    }

    /**
     * 为连接 fd 绑定一个 upstream，并增加连接计数。
     *
     * @param int $fd 连接文件描述符。
     * @param string|null $affinityKey 可选亲和键。
     * @return array<string, mixed>|null 绑定的 upstream，失败时返回 null。
     */
    public function bindConnectionUpstream(int $fd, ?string $affinityKey = null): ?array {
        $upstream = $this->pickHttpUpstream($affinityKey);
        if (!$upstream) {
            return null;
        }
        $this->connectionBindings[$fd] = [
            'instance_id' => $upstream['id'],
            'version' => $upstream['version'],
            'bound_at' => time(),
        ];
        $this->instanceConnectionCount[$upstream['id']] = ($this->instanceConnectionCount[$upstream['id']] ?? 0) + 1;
        return $upstream;
    }

    /**
     * 查询某个实例的连接绑定数量。
     *
     * @param string $version generation 版本。
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @return int 对应实例的连接数。
     */
    public function gatewayConnectionCountFor(string $version, string $host, int $port): int {
        if (!isset($this->state['generations'][$version])) {
            return 0;
        }
        foreach (($this->state['generations'][$version]['instances'] ?? []) as $instance) {
            if ((string)($instance['host'] ?? '') !== $host || (int)($instance['port'] ?? 0) !== $port) {
                continue;
            }
            return (int)($this->instanceConnectionCount[$instance['id']] ?? 0);
        }
        return 0;
    }

    /**
     * 返回当前仍绑定在指定实例上的 gateway 客户端 fd 列表。
     *
     * gateway 的长连接回收需要同时看“业务实例是否已进入 draining”和
     * “哪些客户端仍绑定在这台旧实例上”。这里直接复用 fd -> instance 的
     * 绑定表，供 gateway 在切流后主动断开旧 upstream 残留连接。
     *
     * @param string $version generation 版本。
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @return array<int, int> 仍绑定到该实例的客户端 fd 列表。
     */
    public function gatewayConnectionFdsFor(string $version, string $host, int $port): array {
        if (!isset($this->state['generations'][$version])) {
            return [];
        }

        $instanceId = '';
        foreach (($this->state['generations'][$version]['instances'] ?? []) as $instance) {
            if ((string)($instance['host'] ?? '') !== $host || (int)($instance['port'] ?? 0) !== $port) {
                continue;
            }
            $instanceId = (string)($instance['id'] ?? '');
            break;
        }
        if ($instanceId === '') {
            return [];
        }

        $fds = [];
        foreach ($this->connectionBindings as $fd => $binding) {
            if ((string)($binding['instance_id'] ?? '') !== $instanceId) {
                continue;
            }
            $fds[] = (int)$fd;
        }
        sort($fds);
        return $fds;
    }

    /**
     * 释放连接绑定，并同步减少实例连接数。
     *
     * @param int $fd 连接文件描述符。
     * @return void
     */
    public function releaseConnectionBinding(int $fd): void {
        if (!isset($this->connectionBindings[$fd])) {
            return;
        }
        $instanceId = $this->connectionBindings[$fd]['instance_id'];
        unset($this->connectionBindings[$fd]);
        if (isset($this->instanceConnectionCount[$instanceId])) {
            $this->instanceConnectionCount[$instanceId] = max(0, $this->instanceConnectionCount[$instanceId] - 1);
            if ($this->instanceConnectionCount[$instanceId] === 0) {
                unset($this->instanceConnectionCount[$instanceId]);
            }
        }
        $this->tick();
    }

    /**
     * 推进 draining 状态机，满足条件时把 generation 迁移到 offline。
     *
     * @return void
     */
    public function tick(): void {
        if (!$this->hasDrainingGeneration) {
            return;
        }
        $changed = false;
        $now = time();
        $activeDrainingVersions = [];
        foreach ($this->state['generations'] as $version => &$generation) {
            if (($generation['status'] ?? '') !== 'draining') {
                continue;
            }
            $activeDrainingVersions[$version] = true;
            $drainDeadlineAt = (int)($generation['drain_deadline_at'] ?? 0);
            $connections = $this->generationConnectionCount($generation);
            $httpProcessing = $this->generationHttpProcessingCount($generation);
            $rpcProcessing = $this->generationRpcProcessingCount($generation);
            if ($drainDeadlineAt > 0 && $now < $drainDeadlineAt) {
                $this->clearDrainingHttpRpcStallState($version);
                continue;
            }
            $stalledCounters = false;
            if ($connections === 0 && ($httpProcessing > 0 || $rpcProcessing > 0)) {
                $stalledCounters = $this->isDrainingHttpRpcCounterStalled($version, $httpProcessing, $rpcProcessing, $now);
            } else {
                $this->clearDrainingHttpRpcStallState($version);
            }

            if ($connections === 0 && ($httpProcessing === 0 && $rpcProcessing === 0 || $stalledCounters)) {
                if ($stalledCounters) {
                    Console::warning(
                        "【Gateway】旧业务实例HTTP/RPC计数长时间不变化且无连接，按兜底进入 recycle: "
                        . "generation={$version}, http={$httpProcessing}, rpc={$rpcProcessing}, "
                        . "stale_grace=" . $this->drainingHttpRpcStaleGraceSeconds() . "s, "
                        . "drain_started_at=" . (int)($generation['drain_started_at'] ?? 0)
                        . ", drain_deadline_at={$drainDeadlineAt}"
                    );
                }
                Console::info(
                    "【Gateway】旧业务实例draining完成，准备进入 recycle: generation={$version}, "
                    . "ws={$connections}, http={$httpProcessing}, rpc={$rpcProcessing}, "
                    . "drain_started_at=" . (int)($generation['drain_started_at'] ?? 0)
                    . ", drain_deadline_at={$drainDeadlineAt}"
                );
                $generation['status'] = 'offline';
                $generation['drain_started_at'] = $generation['drain_started_at'] ?: $now;
                foreach ($generation['instances'] as &$instance) {
                    $instance['status'] = 'offline';
                }
                unset($instance);
                $this->clearDrainingHttpRpcStallState($version);
                $changed = true;
            }
            if (($this->state['active_version'] ?? null) === $version && $generation['status'] !== 'active') {
                $this->state['active_version'] = null;
                $changed = true;
            }
        }
        unset($generation);
        foreach (array_keys($this->drainingHttpRpcStallState) as $version) {
            if (!isset($activeDrainingVersions[$version])) {
                unset($this->drainingHttpRpcStallState[$version]);
            }
        }
        if ($changed) {
            $this->touchState();
        }
    }

    /**
     * 判断 draining generation 的 HTTP/RPC 处理计数是否出现“无连接且长期不变化”。
     *
     * 这个判定专门用于兜住计数泄漏场景：旧代已经不再承接连接，但计数器卡在固定值，
     * 会导致 generation 永远停在 draining。只有签名持续不变化超过宽限才返回 true。
     *
     * @param string $version generation 版本
     * @param int $httpProcessing 当前 HTTP 处理中计数
     * @param int $rpcProcessing 当前 RPC 处理中计数
     * @param int $now 当前时间戳
     * @return bool
     */
    protected function isDrainingHttpRpcCounterStalled(string $version, int $httpProcessing, int $rpcProcessing, int $now): bool {
        if ($version === '') {
            return false;
        }
        $signature = $httpProcessing . ':' . $rpcProcessing;
        $state = (array)($this->drainingHttpRpcStallState[$version] ?? []);
        $lastSignature = (string)($state['signature'] ?? '');
        $since = (int)($state['since'] ?? 0);
        $lastWarnAt = (int)($state['last_warn_at'] ?? 0);
        if ($lastSignature !== $signature || $since <= 0) {
            $this->drainingHttpRpcStallState[$version] = [
                'signature' => $signature,
                'since' => $now,
                'last_warn_at' => 0,
            ];
            return false;
        }

        $staleGrace = $this->drainingHttpRpcStaleGraceSeconds();
        $elapsed = max(0, $now - $since);
        if ($elapsed >= $staleGrace) {
            return true;
        }

        if ($lastWarnAt <= 0 || ($now - $lastWarnAt) >= 15) {
            $this->drainingHttpRpcStallState[$version]['last_warn_at'] = $now;
            Console::warning(
                "【Gateway】旧业务实例draining计数未收敛，继续等待: generation={$version}, "
                . "http={$httpProcessing}, rpc={$rpcProcessing}, stalled={$elapsed}s, stale_grace={$staleGrace}s"
            );
        }
        return false;
    }

    /**
     * 清理指定 generation 的 draining 计数兜底状态。
     *
     * @param string $version generation 版本
     * @return void
     */
    protected function clearDrainingHttpRpcStallState(string $version): void {
        if ($version === '') {
            return;
        }
        unset($this->drainingHttpRpcStallState[$version]);
    }

    /**
     * 读取 draining 计数不变化兜底窗口（秒）。
     *
     * @return int
     */
    protected function drainingHttpRpcStaleGraceSeconds(): int {
        return max(3, (int)(Config::server()['gateway_drain_stale_counter_grace'] ?? 8));
    }

    /**
     * 返回当前 draining generation 的诊断摘要。
     *
     * @return array<int, array<string, mixed>> draining generation 诊断列表。
     */
    public function drainingDiagnostics(): array {
        $diagnostics = [];
        foreach ($this->state['generations'] as $version => $generation) {
            if (($generation['status'] ?? '') !== 'draining') {
                continue;
            }
            $diagnostics[] = [
                'version' => (string)$version,
                'connections' => $this->generationConnectionCount($generation),
                'http_processing' => $this->generationHttpProcessingCount($generation),
                'rpc_processing' => $this->generationRpcProcessingCount($generation),
                'redis_queue_processing' => $this->generationRedisQueueProcessingCount($generation),
                'crontab_busy' => $this->generationCrontabBusyCount($generation),
                'mysql_inflight' => $this->generationMysqlInflightCount($generation),
                'redis_inflight' => $this->generationRedisInflightCount($generation),
                'outbound_http_inflight' => $this->generationOutboundHttpInflightCount($generation),
                'drain_started_at' => (int)($generation['drain_started_at'] ?? 0),
                'drain_deadline_at' => (int)($generation['drain_deadline_at'] ?? 0),
            ];
        }
        return $diagnostics;
    }

    /**
     * 统计某个 generation 的连接占用。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int 连接占用数。
     */
    protected function generationConnectionCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $count += $this->instanceConnectionCount[$instance['id']] ?? 0;
        }
        return $count;
    }

    /**
     * 统计某个 generation 的 HTTP 处理中请求数。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int HTTP 处理中请求数。
     */
    protected function generationHttpProcessingCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['http_request_processing'] ?? 0);
        }
        return $count;
    }

    /**
     * 统计某个 generation 的 RPC 处理中请求数。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int RPC 处理中请求数。
     */
    protected function generationRpcProcessingCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['rpc_request_processing'] ?? 0);
        }
        return $count;
    }

    /**
     * 统计某个 generation 的 RedisQueue 处理中任务数。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int RedisQueue 处理中任务数。
     */
    protected function generationRedisQueueProcessingCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['redis_queue_processing'] ?? 0);
        }
        return $count;
    }

    /**
     * 统计某个 generation 的 crontab 忙碌数。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int crontab 忙碌数。
     */
    protected function generationCrontabBusyCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['crontab_busy'] ?? 0);
        }
        return $count;
    }

    /**
     * 统计某个 generation 的 MySQL 在途 I/O 数。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int MySQL 在途数。
     */
    protected function generationMysqlInflightCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['mysql_inflight'] ?? 0);
        }
        return $count;
    }

    /**
     * 统计某个 generation 的 Redis 在途 I/O 数。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int Redis 在途数。
     */
    protected function generationRedisInflightCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['redis_inflight'] ?? 0);
        }
        return $count;
    }

    /**
     * 统计某个 generation 的 Outbound HTTP 在途 I/O 数。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @return int Outbound HTTP 在途数。
     */
    protected function generationOutboundHttpInflightCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['outbound_http_inflight'] ?? 0);
        }
        return $count;
    }

    /**
     * 生成实例运行态缓存键。
     *
     * @param string $host upstream 主机名或 IP。
     * @param int $port upstream HTTP 端口。
     * @return string 运行态缓存键。
     */
    protected function runtimeStateKey(string $host, int $port): string {
        return $host . ':' . $port;
    }

    /**
     * 从 generation 中按权重和亲和键选择一个可用实例。
     *
     * @param array<string, mixed> $generation generation 状态。
     * @param string|null $affinityKey 可选亲和键。
     * @param array<int, string> $excludeInstanceIds 需要排除的实例 ID 列表。
     * @return array<string, mixed>|null 选中的实例，失败时返回 null。
     */
    protected function pickGenerationInstance(array $generation, ?string $affinityKey = null, array $excludeInstanceIds = []): ?array {
        $excluded = array_fill_keys(array_values(array_filter(array_map('strval', $excludeInstanceIds))), true);
        $instances = array_values(array_filter($generation['instances'] ?? [], static function ($instance) use ($excluded) {
            if (isset($excluded[(string)($instance['id'] ?? '')])) {
                return false;
            }
            return in_array($instance['status'] ?? 'offline', ['active', 'prepared'], true);
        }));
        if (!$instances) {
            return null;
        }
        if (count($instances) === 1) {
            return $instances[0];
        }

        $key = $generation['version'];
        $totalWeight = 0;
        foreach ($instances as $instance) {
            $totalWeight += max(1, (int)($instance['weight'] ?? 1));
        }
        if ($totalWeight <= 0) {
            return null;
        }

        if (!is_null($affinityKey) && $affinityKey !== '') {
            $index = abs(crc32($affinityKey)) % $totalWeight;
            return $this->pickInstanceByWeightedIndex($instances, $index);
        }

        $index = $this->rrIndex[$key] ?? 0;
        $picked = $this->pickInstanceByWeightedIndex($instances, $index % $totalWeight);
        $this->rrIndex[$key] = ($index + 1) % $totalWeight;
        return $picked;
    }

    /**
     * 按权重索引定位实例。
     *
     * @param array<int, array<string, mixed>> $instances 候选实例列表。
     * @param int $index 权重索引。
     * @return array<string, mixed>|null 命中的实例，失败时返回 null。
     */
    protected function pickInstanceByWeightedIndex(array $instances, int $index): ?array {
        $offset = 0;
        foreach ($instances as $instance) {
            $weight = max(1, (int)($instance['weight'] ?? 1));
            $offset += $weight;
            if ($index < $offset) {
                return $instance;
            }
        }
        return $instances[array_key_last($instances)] ?? null;
    }

    /**
     * 将 generation 标记为 draining，并写入截止时间。
     *
     * @param array<string, mixed> $generation generation 状态，按引用修改。
     * @param int $now 当前时间戳。
     * @param int $graceSeconds draining 宽限时间，单位秒。
     * @return void
     */
    protected function markGenerationDraining(array &$generation, int $now, int $graceSeconds): void {
        $generation['status'] = 'draining';
        $generation['drain_started_at'] = $now;
        $generation['drain_deadline_at'] = $graceSeconds > 0 ? ($now + $graceSeconds) : $now;
        foreach ($generation['instances'] as &$instance) {
            $instance['status'] = 'draining';
        }
        unset($instance);
    }

    /**
     * 标记状态变更并持久化。
     *
     * @return void
     */
    protected function touchState(): void {
        $this->state['updated_at'] = time();
        $this->refreshStateFlags();
        $this->registry->save($this->state);
    }

    /**
     * 刷新派生状态标记。
     *
     * @return void
     */
    protected function refreshStateFlags(): void {
        $this->hasDrainingGeneration = false;
        foreach (($this->state['generations'] ?? []) as $generation) {
            if (($generation['status'] ?? '') === 'draining') {
                $this->hasDrainingGeneration = true;
                break;
            }
        }
    }
}
