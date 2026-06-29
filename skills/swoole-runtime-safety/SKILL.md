---
name: swoole-runtime-safety
description: 当你要修改 Swoole 运行时控制逻辑时使用，包括进程生命周期、协程调度、定时器、信号、退出行为、reload/restart 流程以及节点运行时协同。这个 skill 强制要求先验证当前运行时模型是否支持该改动，并优先采用 Swoole 原生机制。
---

# Swoole 运行时安全

## 概览

所有会影响运行时“怎么活着、怎么调度、怎么退出、怎么重载、怎么协同”的改动，都要先过这个 skill。核心原则是：不要靠直觉修改运行时控制流。

## 改前必须确认

至少要明确下面这些问题：

1. 这个机制在当前 Swoole 上下文里是否真的可行
2. 对父子进程协同会有什么影响
3. 对 timers、hooks、共享表和未完成协程会有什么影响
4. 这个行为是可恢复的，还是可能直接触发致命运行时异常

只要有任何一点还没想清楚，就先继续检查当前生命周期设计，不要急着补代码。

## 默认实现倾向

只要安全可行，优先选 Swoole 原生能力：

- 协程
- 定时器
- Channel
- Barrier
- 事件驱动协同

只有在明确验证以下情况后，才允许退回通用阻塞写法：

- 这条路径本身不在 Swoole 运行时里
- 原生方案和当前生命周期模型不兼容
- fallback 比原生方案更安全、更易维护

如果退回通用写法，要明确写出原因。

## 高风险改动清单

下面这些动作都属于高风险，必须额外验证：

- `exit`
- 强制杀进程
- 新增进程派生行为
- 手动关闭协程
- reload/restart 语义改动
- signal 处理变更

## 建议先看的位置

优先追：

- `/Users/lanhai/projects/swoole/scf/src/Core`
- `/Users/lanhai/projects/swoole/scf/src/App`
- `/Users/lanhai/projects/swoole/scf/src/Mode`
- 实际触发这段运行时行为的调用方

如果运行时问题只是更长业务链路的一部分，请和 `$swoole-chain-trace` 一起用。

真实生命周期检查锚点请看 `references/inspect-targets.md`。
