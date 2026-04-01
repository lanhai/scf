<?php

namespace Scf\Server\Gateway;

use RuntimeException;

/**
 * 上游实例注册表的持久化入口。
 *
 * 负责把 gateway 侧的 generation / instance 状态读写到状态文件，
 * 为 AppInstanceManager 提供唯一的持久化来源。
 */
class UpstreamRegistry {

    /**
     * @param string $stateFile 注册表状态文件路径。
     */
    public function __construct(
        protected string $stateFile
    ) {
    }

    /**
     * 返回当前使用的状态文件路径。
     */
    public function stateFile(): string {
        return $this->stateFile;
    }

    /**
     * 从磁盘读取并规范化 registry 状态。
     *
     * 文件缺失、为空或 JSON 非法时，返回默认空状态。
     */
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

    /**
     * 将规范化后的 registry 状态写回磁盘。
     */
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

    /**
     * 生成空 registry 的默认结构。
     */
    protected function defaultState(): array {
        return [
            'active_version' => null,
            'generations' => [],
            'updated_at' => time(),
        ];
    }

    /**
     * 归一化 registry 的 generation / instance 结构，保证读取侧字段齐全。
     */
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
