<?php

namespace Scf\Server\Proxy;

use RuntimeException;

class UpstreamRegistry {

    public function __construct(
        protected string $stateFile
    ) {
    }

    public function stateFile(): string {
        return $this->stateFile;
    }

    public function load(): array {
        if (!is_file($this->stateFile)) {
            return $this->defaultState();
        }
        $content = @file_get_contents($this->stateFile);
        if ($content === false || $content === '') {
            return $this->defaultState();
        }
        $decoded = json_decode($content, true);
        if (!is_array($decoded)) {
            return $this->defaultState();
        }
        return $this->normalizeState($decoded);
    }

    public function save(array $state): void {
        $dir = dirname($this->stateFile);
        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new RuntimeException('创建代理状态目录失败:' . $dir);
        }
        $normalized = $this->normalizeState($state);
        $json = json_encode($normalized, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
        if ($json === false) {
            throw new RuntimeException('代理状态编码失败');
        }
        if (@file_put_contents($this->stateFile, $json, LOCK_EX) === false) {
            throw new RuntimeException('写入代理状态失败:' . $this->stateFile);
        }
    }

    protected function defaultState(): array {
        return [
            'active_version' => null,
            'generations' => [],
            'updated_at' => time(),
        ];
    }

    protected function normalizeState(array $state): array {
        $state['active_version'] = $state['active_version'] ?? null;
        $state['generations'] = is_array($state['generations'] ?? null) ? $state['generations'] : [];
        $state['updated_at'] = (int)($state['updated_at'] ?? time());

        foreach ($state['generations'] as $version => &$generation) {
            $generation['version'] = (string)($generation['version'] ?? $version);
            $generation['status'] = (string)($generation['status'] ?? 'prepared');
            $generation['created_at'] = (int)($generation['created_at'] ?? time());
            $generation['activated_at'] = isset($generation['activated_at']) ? (int)$generation['activated_at'] : null;
            $generation['drain_started_at'] = isset($generation['drain_started_at']) ? (int)$generation['drain_started_at'] : null;
            $generation['drain_deadline_at'] = isset($generation['drain_deadline_at']) ? (int)$generation['drain_deadline_at'] : null;
            $generation['metadata'] = is_array($generation['metadata'] ?? null) ? $generation['metadata'] : [];
            $generation['instances'] = is_array($generation['instances'] ?? null) ? $generation['instances'] : [];

            foreach ($generation['instances'] as &$instance) {
                $instance['id'] = (string)($instance['id'] ?? '');
                $instance['version'] = $generation['version'];
                $instance['host'] = (string)($instance['host'] ?? '127.0.0.1');
                $instance['port'] = (int)($instance['port'] ?? 0);
                $instance['weight'] = max(1, (int)($instance['weight'] ?? 100));
                $instance['status'] = (string)($instance['status'] ?? $generation['status']);
                $instance['registered_at'] = (int)($instance['registered_at'] ?? time());
                $instance['metadata'] = is_array($instance['metadata'] ?? null) ? $instance['metadata'] : [];
            }
            unset($instance);
        }
        unset($generation);

        return $state;
    }
}
