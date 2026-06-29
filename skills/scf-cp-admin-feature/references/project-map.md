# SCF CP 功能项目地图

当你要把一个 SCF app 的 CP/admin 管理功能从后端做到前端时，先看这个文件。

## 先确认 app profile

| field | value |
| --- | --- |
| app name | `<app>` |
| backend root | `/Users/lanhai/projects/swoole/apps/<app>` |
| frontend root | `/Users/lanhai/projects/vue-admin/<frontend-app>` |
| Admin Controller root | `apps/<app>/src/lib/Admin/Controller` 或目标 app 的等价目录 |
| menu route anchor | 目标 app 内的 `actionRoutes`、`routes`、`menu` 或等价聚合方法 |

如果目标 app 有结构说明、路由索引或 app-local skill，先读这些文档，再做广泛扫描。

## 后端侧

常见 Controller 根目录：

```text
/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin/Controller
```

路由/菜单聚合入口需要在目标 app 内查找：

```bash
rg -n "actionRoutes|routes|menu|access" /Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin
```

## 前端侧

常见目录：

```text
/Users/lanhai/projects/vue-admin/<frontend-app>/src/api
/Users/lanhai/projects/vue-admin/<frontend-app>/src/views
/Users/lanhai/projects/vue-admin/<frontend-app>/src/router
/Users/lanhai/projects/vue-admin/<frontend-app>/src/store
```

## 权限相关入口

常见前端 API 文件：

```text
/Users/lanhai/projects/vue-admin/<frontend-app>/src/api/system.ts
```

常见后端配置：

```text
/Users/lanhai/projects/swoole/apps/<app>/src/config/access/nodes.yml
/Users/lanhai/projects/swoole/apps/<app>/src/config/app.php
```

## 实际推荐顺序

1. 先补目标 app 的后端 Controller 方法和 `@Route`。
2. 再补目标 CP 前端 API 封装，或者复用已有封装。
3. 在目标 CP 前端 `src/views` 对应域目录下补页面。
4. 把页面挂到目标 app 的路由/菜单输出。
5. 最后通过 `$scf-access-node-workflow` 补权限节点和角色授权。
