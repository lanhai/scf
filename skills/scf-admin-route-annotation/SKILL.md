---
name: scf-admin-route-annotation
description: 当任意 SCF app 需要新增、修复或补齐 Admin Controller 注解路由时使用。适用于 apps/<app>/src/lib/Admin/Controller 下的后台接口，要求根据目标 app 调整 Controller 路径、菜单入口和前端请求路径，让路由声明和控制器方法同处一处。
---

# SCF 后台注解路由

## 概览

这个 skill 适合“主要任务就是补后台路由”的场景，而不是整个功能链路都要新建。

先确认目标 app：

- 后端 Controller 根目录：`/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin/Controller`
- 菜单/路由输出入口：先在目标 app 内查 `actionRoutes`、`routes` 或既有菜单聚合方法
- 前端请求路径：从目标 app 对应 CP 前端工程确认

## 必守约定

### 1. 路由定义跟着控制器走

对 SCF app 的 Admin Controller，优先用注解路由：

- 使用 docblock `@Route('/path/')`
- 修改现有接口时，补齐或维护注解路由，保持路由定义和方法在同一处

### 2. 路径使用 slash-separated segment

新建或触碰的注解路由优先使用斜杠分段：

- 正确：`@Route('/admin/person/register/list/')`
- 避免：`@Route('/admin/person/register_list/')`

旧 snake_case endpoint 只有在兼容已有调用时才保留。

### 3. 保持 CP 页面入口一致

如果接口对应 CP 页面导航，还要确认目标 app 的路由/菜单输出仍然指向正确页面。先在本 app 内查 `actionRoutes`、`routes`、`menu` 或等价入口。

### 4. 避免路由分裂

如果注解路由能表达清楚，就不要把新路由只写到别的注册位置，造成 Controller 和路由声明分离。

## 常规检查项

确认：

1. Controller 方法有正确的注解。
2. 路由路径和前端请求路径一致。
3. 路由/菜单输出仍然指向正确的 CP 页面。
4. 根据目标 app 调整 namespace、Controller 路径和前端工程，不要默认使用其他 app 的路径。

如果这次改动是完整 CP 功能的一部分，就和 `$scf-cp-admin-feature` 一起用。

真实路由/菜单锚点请看 `references/examples.md`。
