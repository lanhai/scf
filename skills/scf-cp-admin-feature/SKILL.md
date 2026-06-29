---
name: scf-cp-admin-feature
description: 当你要为任意 SCF app 新增或修改一个 CP/admin 后台管理功能，并且功能同时涉及后端 Admin Controller、对应 CP 前端页面、路由/菜单暴露以及权限节点时使用。根据目标 app 调整 apps/<app> 后端、CP 前端工程、--app 参数、登录入口和权限节点配置。
---

# SCF CP 后台功能

## 概览

这个 skill 用于新增 SCF app 的 CP/admin 管理功能。目标不是只改其中一层，而是把后端接口、CP 页面、路由菜单和权限控制整条链路一次做完整。

先建立目标 app profile：

- app 名：`<app>`
- 后端根目录：`/Users/lanhai/projects/swoole/apps/<app>`
- 后端 Controller/API：`apps/<app>/src/lib/Admin/Controller`
- CP 前端根目录：`/Users/lanhai/projects/vue-admin/<frontend-app>`
- 权限节点命令：从 `/Users/lanhai/projects/swoole/scf/bin` 以 `--app=<app>` 执行

如果目标 app 有项目结构说明、路由索引或本地 app 级 skill，广泛扫描前先读这些 app-local 文档。

## 必走实现路径

### 1. 先做后端

后端 Controller/API 放在目标 app：

- `/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin/Controller`

优先使用注解路由，并让路由定义和控制器方法写在一起：

- 使用 docblock `@Route('/path/')`
- 修改现有接口时，要补齐或维护注解路由声明
- 路由路径优先使用 slash-separated segment

### 2. 再做 CP 前端

CP 页面和请求集成放在目标 app 对应的 CP 前端工程：

- API 目录通常是 `src/api`
- 页面目录通常是 `src/views`
- 必要时同步改 router/store

前端实现要沿用目标 CP 管理后台模式，不要另起一套结构。

### 3. 暴露路由和菜单

把页面入口补进目标 app 的后台路由/菜单输出里，例如相关的 `actionRoutes` 或等价聚合方法，保证页面在 CP 中真实可达。

### 4. 权限节点必须走系统流程

优先使用 `$scf-access-node-workflow`。如果必须走 HTTP 后台，只能通过目标 app 的系统接口处理权限节点：

- `POST /admin/system/access_node_save/`
- `POST /admin/system/save_role_access_nodes/`
- `POST /admin/system/access_node_delete/`

除非用户明确要求，否则不要直接改 `cp_access_node` 或 `cp_role`。

### 5. 环境安全

节点和角色操作必须先在 dev 环境执行，并保持和后续同步脚本兼容。需要真实 dev 后台会话时，先用 `$scf-dev-login`。

## 验证清单

结束前至少确认：

1. 后端路由能通过注解路由正常访问。
2. CP 菜单里能正常打开页面。
3. 节点已经挂到目标 app 的预期父节点和排序位置。
4. 目标角色已经具备预期别名和页面权限。
5. 所有命令、路径、前端工程和登录参数都按目标 app 调整。

## 注释标准

对于重要类和 public/protected 方法：

- 补类级责任说明 docblock
- 补方法 PHPDoc，包含 `@param`、`@return`、必要时的 `@throws`
- 在非平凡分支前补简短但有价值的行内注释

不要写没有信息量的注释。

真实后端/前端锚点请看 `references/project-map.md`。
