---
name: scf-release-publish
description: 当你要发布任意 SCF 应用时使用，包括 SCF build release 命令、打包类型判断以及项目规定的发布顺序。这个 skill 会强制遵守仓库里的发布入口、命令顺序和重试规则。
---

# SCF 发布流程

## 概览

凡是这套 Swoole 工作区里的应用发布或打包任务，都用这个 skill。核心原则是：按仓库既有发布路径执行，不要临时发明入口。

## SCF 应用发布规则

### 打包类型判断

发布前必须明确说明选用的打包类型：

- 这次如果包含 CP 编译或发布产物，使用 `build_type=3`
- 这次如果只有后端代码改动，使用 `build_type=1`

### 固定入口

发布命令必须从这里执行：

- `/Users/lanhai/projects/swoole/scf/bin`

固定使用：

- `./build release ...`

除非用户明确要求，否则不要用 `release/bin/build`，也不要直接跑 `php /Users/lanhai/projects/swoole/release/boot ...`。

## 发布框架规则

如果用户说的是“发布框架”，必须按这个顺序执行：

1. `framework build`
2. `framework push`
3. `/Users/lanhai/projects/swoole/etc/build_main.sh`

不能跳步，不能换顺序。

如果三步中的任意一步只是瞬时失败，就在当前步骤重试，直到成功后再继续下一步。

## 执行前要先说清楚

1. 这次是纯后端，还是包含 CP 发布产物
2. 选的是哪种打包类型
3. 实际会走哪条命令链

具体命令入口和 build type 对应关系请看 `references/command-map.md`。
