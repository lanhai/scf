<?php

namespace Scf\Server\Proxy;

use RuntimeException;

class AppInstanceManager {

    protected array $state = [];
    protected array $rrIndex = [];
    protected array $connectionBindings = [];
    protected array $instanceConnectionCount = [];
    protected array $instanceProxyHttpRequestCount = [];
    protected array $instanceRuntimeState = [];
    protected bool $hasDrainingGeneration = false;

    public function __construct(
        protected UpstreamRegistry $registry
    ) {
        $this->reload();
    }

    public function reload(): void {
        $this->state = $this->registry->load();
        $this->refreshStateFlags();
    }

    public function state(): array {
        return $this->state;
    }

    public function stateFile(): string {
        return $this->registry->stateFile();
    }

    public function bootstrap(?string $version, ?string $host, ?int $port, bool $activate = true, int $weight = 100, array $metadata = []): void {
        if (!$version || !$host || !$port) {
            return;
        }
        $this->registerUpstream($version, $host, $port, $weight, $metadata);
        if ($activate) {
            $this->activateVersion($version, 0);
        }
    }

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

    public function otherInstances(string $version, string $host, int $port): array {
        if (!isset($this->state['generations'][$version])) {
            return [];
        }

        return array_values(array_filter($this->state['generations'][$version]['instances'] ?? [], static function ($instance) use ($host, $port) {
            return !(($instance['host'] ?? '') === $host && (int)($instance['port'] ?? 0) === $port);
        }));
    }

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

    public function updateInstanceRuntimeStatus(string $host, int $port, array $runtimeStatus): void {
        if ($host === '' || $port <= 0) {
            return;
        }
        $this->instanceRuntimeState[$this->runtimeStateKey($host, $port)] = [
            'http_request_processing' => max(0, (int)($runtimeStatus['http_request_processing'] ?? 0)),
            'rpc_request_processing' => max(0, (int)($runtimeStatus['rpc_request_processing'] ?? 0)),
            'redis_queue_processing' => max(0, (int)($runtimeStatus['redis_queue_processing'] ?? 0)),
            'crontab_busy' => max(0, (int)($runtimeStatus['crontab_busy'] ?? 0)),
            'updated_at' => time(),
        ];
    }

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

    public function clearInstanceRuntimeStatus(string $host, int $port): void {
        if ($host === '' || $port <= 0) {
            return;
        }
        unset($this->instanceRuntimeState[$this->runtimeStateKey($host, $port)]);
    }

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

    public function pickHttpUpstream(?string $affinityKey = null): ?array {
        $this->tick();
        $activeVersion = $this->state['active_version'] ?? null;
        if (!$activeVersion || !isset($this->state['generations'][$activeVersion])) {
            return null;
        }
        return $this->pickGenerationInstance($this->state['generations'][$activeVersion], $affinityKey);
    }

    public function pickHttpUpstreamExcluding(array $excludeInstanceIds = [], ?string $affinityKey = null): ?array {
        $this->tick();
        $activeVersion = $this->state['active_version'] ?? null;
        if (!$activeVersion || !isset($this->state['generations'][$activeVersion])) {
            return null;
        }
        return $this->pickGenerationInstance($this->state['generations'][$activeVersion], $affinityKey, $excludeInstanceIds);
    }

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

    public function bindWebsocketUpstream(int $fd, ?string $affinityKey = null): ?array {
        return $this->bindConnectionUpstream($fd, $affinityKey);
    }

    public function retainProxyHttpRequest(array $instance): void {
        $instanceId = (string)($instance['id'] ?? '');
        if ($instanceId === '') {
            return;
        }
        $this->instanceProxyHttpRequestCount[$instanceId] = ($this->instanceProxyHttpRequestCount[$instanceId] ?? 0) + 1;
    }

    public function releaseProxyHttpRequest(array $instance): void {
        $instanceId = (string)($instance['id'] ?? '');
        if ($instanceId === '' || !isset($this->instanceProxyHttpRequestCount[$instanceId])) {
            return;
        }
        $this->instanceProxyHttpRequestCount[$instanceId] = max(0, $this->instanceProxyHttpRequestCount[$instanceId] - 1);
        if ($this->instanceProxyHttpRequestCount[$instanceId] === 0) {
            unset($this->instanceProxyHttpRequestCount[$instanceId]);
        }
    }

    public function websocketBinding(int $fd): ?array {
        return $this->connectionBindings[$fd] ?? null;
    }

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

    public function proxyHttpRequestCountFor(string $version, string $host, int $port): int {
        if (!isset($this->state['generations'][$version])) {
            return 0;
        }
        foreach (($this->state['generations'][$version]['instances'] ?? []) as $instance) {
            if ((string)($instance['host'] ?? '') !== $host || (int)($instance['port'] ?? 0) !== $port) {
                continue;
            }
            return (int)($this->instanceProxyHttpRequestCount[(string)($instance['id'] ?? '')] ?? 0);
        }
        return 0;
    }

    public function totalProxyHttpRequestCount(): int {
        return array_sum($this->instanceProxyHttpRequestCount);
    }

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

    public function releaseWebsocketBinding(int $fd): void {
        $this->releaseConnectionBinding($fd);
    }

    public function tick(): void {
        if (!$this->hasDrainingGeneration) {
            return;
        }
        $changed = false;
        $now = time();
        foreach ($this->state['generations'] as $version => &$generation) {
            if (($generation['status'] ?? '') !== 'draining') {
                continue;
            }
            $connections = $this->generationConnectionCount($generation);
            $httpProcessing = $this->generationHttpProcessingCount($generation);
            $rpcProcessing = $this->generationRpcProcessingCount($generation);
            if ($connections === 0 && $httpProcessing === 0 && $rpcProcessing === 0) {
                $generation['status'] = 'offline';
                $generation['drain_started_at'] = $generation['drain_started_at'] ?: $now;
                foreach ($generation['instances'] as &$instance) {
                    $instance['status'] = 'offline';
                }
                unset($instance);
                $changed = true;
            }
            if (($this->state['active_version'] ?? null) === $version && $generation['status'] !== 'active') {
                $this->state['active_version'] = null;
                $changed = true;
            }
        }
        unset($generation);
        if ($changed) {
            $this->touchState();
        }
    }

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
                'drain_started_at' => (int)($generation['drain_started_at'] ?? 0),
                'drain_deadline_at' => (int)($generation['drain_deadline_at'] ?? 0),
            ];
        }
        return $diagnostics;
    }

    protected function generationConnectionCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $count += $this->instanceConnectionCount[$instance['id']] ?? 0;
        }
        return $count;
    }

    protected function generationHttpProcessingCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['http_request_processing'] ?? 0);
            $count += (int)($this->instanceProxyHttpRequestCount[(string)($instance['id'] ?? '')] ?? 0);
        }
        return $count;
    }

    protected function generationRpcProcessingCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['rpc_request_processing'] ?? 0);
        }
        return $count;
    }

    protected function generationRedisQueueProcessingCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['redis_queue_processing'] ?? 0);
        }
        return $count;
    }

    protected function generationCrontabBusyCount(array $generation): int {
        $count = 0;
        foreach ($generation['instances'] as $instance) {
            $runtime = $this->instanceRuntimeState[$this->runtimeStateKey((string)($instance['host'] ?? '127.0.0.1'), (int)($instance['port'] ?? 0))] ?? [];
            $count += (int)($runtime['crontab_busy'] ?? 0);
        }
        return $count;
    }

    protected function runtimeStateKey(string $host, int $port): string {
        return $host . ':' . $port;
    }

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

    protected function markGenerationDraining(array &$generation, int $now, int $graceSeconds): void {
        $generation['status'] = 'draining';
        $generation['drain_started_at'] = $now;
        $generation['drain_deadline_at'] = $graceSeconds > 0 ? ($now + $graceSeconds) : $now;
        foreach ($generation['instances'] as &$instance) {
            $instance['status'] = 'draining';
        }
        unset($instance);
    }

    protected function touchState(): void {
        $this->state['updated_at'] = time();
        $this->refreshStateFlags();
        $this->registry->save($this->state);
    }

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
