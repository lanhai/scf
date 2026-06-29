---
name: swoole-task-router
description: 当用户正在处理 Swoole 工作区任务，但还没有明确指定该用哪一个项目 skill 时使用。这个 skill 会根据任务是否属于链路追踪、CP 后台功能、运行时安全、Dao 生成、SpiderWs crawler 集成、SCF 发布或 dashboard 节点运维，把任务路由到最合适的本地 skill。
---

# Swoole 任务路由器

## 概览

当任务明确属于这套 Swoole 工作区，但还不确定该用哪个专门 skill 时，先用这个路由器。原则是：优先选择最窄、最贴切的 skill。

## 路由表

- 如果是缺陷排查、阻塞链路、升级流程、调度流程，或者任何改前必须先追全链路的问题，使用 `$swoole-chain-trace`
- 如果是任意 SCF app 的 CP/admin 后台页面、后端 Admin Controller、路由/菜单暴露，以及跨后端和 CP 前端的权限功能，使用 `$scf-cp-admin-feature`
- 如果任务专门是 CP/admin 权限节点、角色授权、页面权限或别名校验，使用 `$scf-access-node-workflow`
- 如果任务专门是后台 Controller 注解路由，以及让路由声明和方法放在一起，使用 `$scf-admin-route-annotation`
- 如果任务专门是 CP/admin 页面可见权限、`view_permissions`、`hasPermission` 或前端元素显示控制，使用 `$scf-page-visibility-permission`
- 如果涉及生命周期、协程、定时器、信号、exit、reload、restart 或进程协同，使用 `$swoole-runtime-safety`
- 如果涉及表结构更新、Dao 重新生成、DB mapping 变更和 dev-first 数据安全，使用 `$scf-dao-arcreator`
- 如果是某个 app 的 crawler 请求、SpiderWs 集成、Electron 网关分发、service/action 路由或采集链路排查，先查 `apps/<app>/skills` 中是否有 app-local SpiderWs/crawler skill；没有时使用 `$swoole-chain-trace`
- 如果核心问题是 SpiderWs 请求卡住或超时，并且要精确定位断点，使用 `$spiderws-timeout-trace`
- 如果是 SCF build release、打包类型判断或应用发布，使用 `$scf-release-publish`
- 如果用户明确要求“发布框架”，使用 `$scf-framework-publish`
- 如果是 dashboard 节点管理、升级、重载、重启或节点命令下发，使用 `$dashboard-node-ops`
- 如果问题专门出在 dashboard 触发的升级、重载或重启链路，使用 `$dashboard-upgrade-trace`

## 选择规则

1. 能用一个窄 skill 解决，就不要上多个宽 skill
2. 只有任务确实横跨两个关注点时，才组合使用多个 skill
3. 只要碰到运行时控制语义，就补上 `$swoole-runtime-safety`
4. 只要现有功能链路还不清楚，就先补上 `$swoole-chain-trace`
