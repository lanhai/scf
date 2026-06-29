# SCF 后台注解路由锚点

当你需要给 SCF app 后台接口补路由归属时，先看这个文件。

## Controller 根目录

常见位置：

```text
/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin/Controller
```

如果目标 app 的 Admin Controller 目录不同，先读 app-local 项目结构说明或用 `rg` 小范围定位。

## 路由/菜单输出锚点

在目标 app 内优先查：

```bash
rg -n "actionRoutes|routes|menu|access" /Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin
```

## 前端页面路径锚点

从目标 CP 前端工程查 `src/views`、router/store 和 API 封装：

```text
/Users/lanhai/projects/vue-admin/<frontend-app>/src/views
/Users/lanhai/projects/vue-admin/<frontend-app>/src/router
/Users/lanhai/projects/vue-admin/<frontend-app>/src/store
/Users/lanhai/projects/vue-admin/<frontend-app>/src/api
```

新增后台路由时，拿目标 app 的现有映射去对齐后端暴露路径和前端页面位置。
