<?php

declare(strict_types=1);

namespace Scf\Server\SubProcess;

use RuntimeException;

/**
 * 子进程运行时基类。
 *
 * 责任边界：
 * 1) 统一管理从 SubProcessManager 注入的回调依赖；
 * 2) 让具体子进程类只关注自己的事件循环与生命周期。
 *
 * 架构位置：
 * - 位于 Server/SubProcess 目录，作为各独立子进程运行类的基础设施层。
 *
 * 设计意图：
 * - SubProcessManager 只保留“管理/编排”职责，具体运行逻辑拆分到独立类。
 */
abstract class AbstractRuntimeProcess {
    /**
     * @var array<string, callable>
     */
    protected array $callbacks;

    /**
     * @param array<string, callable> $callbacks 回调依赖映射
     */
    public function __construct(array $callbacks = []) {
        $this->callbacks = $callbacks;
    }

    /**
     * 调用一个已注入的回调依赖。
     *
     * @param string $name 回调名称
     * @param mixed ...$args 参数
     * @return mixed
     */
    protected function call(string $name, mixed ...$args): mixed {
        $callback = $this->callbacks[$name] ?? null;
        if (!is_callable($callback)) {
            throw new RuntimeException("SubProcess callback is not callable: {$name}");
        }

        return $callback(...$args);
    }
}
