<?php

declare(strict_types=1);

namespace Scf\Server\SubProcess;

use Scf\Core\Key;
use Scf\Core\Table\Runtime;
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

    /**
     * 捕获启动当前子进程的 SubProcessManager 代际令牌。
     */
    protected function captureManagerGeneration(): string {
        $generation = Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION);
        return is_string($generation) ? trim($generation) : '';
    }

    /**
     * 当前子进程是否仍属于共享表中生效的 manager 代际。
     */
    protected function managerGenerationIsCurrent(string $generation): bool {
        $generation = trim($generation);
        if ($generation === '') {
            return false;
        }
        $current = Runtime::instance()->get(Key::RUNTIME_SUBPROCESS_MANAGER_GENERATION);
        return is_string($current)
            && $current !== ''
            && hash_equals($generation, $current);
    }

    /**
     * 非阻塞 command pipe 在父 manager 消失后会进入 EOF。
     */
    protected function managerCommandPipeClosed(mixed $pipe): bool {
        if ($pipe === null || $pipe === false) {
            return true;
        }
        return is_resource($pipe) && @feof($pipe);
    }

    /**
     * 当前子进程是否仍持有本代对应的 Runtime PID 槽位。
     */
    protected function ownsRuntimeProcess(string $generation, string $pidKey, int $pid): bool {
        return $pid > 0
            && $this->managerGenerationIsCurrent($generation)
            && (int)(Runtime::instance()->get($pidKey) ?? 0) === $pid;
    }

    /**
     * 在确认 manager 代际仍生效后发布子进程 PID 与首个心跳。
     */
    protected function claimRuntimeOwnership(
        string $generation,
        string $pidKey,
        string $heartbeatKey,
        int $pid
    ): bool {
        if ($pid <= 0 || !$this->managerGenerationIsCurrent($generation)) {
            return false;
        }
        $runtime = Runtime::instance();
        $runtime->set($pidKey, $pid);
        if (!$this->ownsRuntimeProcess($generation, $pidKey, $pid)) {
            return false;
        }
        $runtime->set($heartbeatKey, time());
        return $this->ownsRuntimeProcess($generation, $pidKey, $pid);
    }

    /**
     * 仅由仍持有本代 PID 槽位的进程续写心跳。
     */
    protected function touchRuntimeOwnershipIfCurrent(
        string $generation,
        string $pidKey,
        string $heartbeatKey,
        int $pid,
        ?int $heartbeatAt = null
    ): bool {
        if (!$this->ownsRuntimeProcess($generation, $pidKey, $pid)) {
            return false;
        }
        Runtime::instance()->set($heartbeatKey, $heartbeatAt ?? time());
        return $this->ownsRuntimeProcess($generation, $pidKey, $pid);
    }

    /**
     * generation、PID 所有权、server 生命周期或 manager pipe 任一失效即自退。
     */
    protected function managedRuntimeShouldStop(
        string $generation,
        string $pidKey,
        int $pid,
        mixed $commandPipe = null,
        bool $checkCommandPipe = false
    ): bool {
        return !$this->ownsRuntimeProcess($generation, $pidKey, $pid)
            || !Runtime::instance()->serverIsAlive()
            || ($checkCommandPipe && $this->managerCommandPipeClosed($commandPipe));
    }

    /**
     * 仅清理仍由本代、本 PID 持有的运行态，避免旧代退出擦除新代状态。
     */
    protected function clearRuntimeOwnershipIfCurrent(
        string $generation,
        string $pidKey,
        string $heartbeatKey,
        int $pid
    ): bool {
        if (
            $pid <= 0
            || !$this->managerGenerationIsCurrent($generation)
            || (int)(Runtime::instance()->get($pidKey) ?? 0) !== $pid
        ) {
            return false;
        }

        Runtime::instance()->set($heartbeatKey, 0);
        if (
            !$this->managerGenerationIsCurrent($generation)
            || (int)(Runtime::instance()->get($pidKey) ?? 0) !== $pid
        ) {
            return false;
        }
        return Runtime::instance()->set($pidKey, 0);
    }
}
