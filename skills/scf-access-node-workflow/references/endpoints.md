# SCF 权限节点接口索引

当你要处理任意 SCF app 的 CP/admin 权限节点和角色授权时，先看这个文件。

## App 参数

先确认：

- app 名：`<app>`
- 后端根目录：`/Users/lanhai/projects/swoole/apps/<app>`
- CP 前端根目录：目标 app 对应的 `/Users/lanhai/projects/vue-admin/<frontend-app>`
- System Controller 接口是否和默认 `/admin/system/*` 一致

## 前端接口文件

在目标 CP 前端工程里查系统接口封装，常见位置：

```text
/Users/lanhai/projects/vue-admin/<frontend-app>/src/api/system.ts
```

## 常见请求封装

- `getRoleList`
- `getRoleAccessNodes`
- `doSaveRoleAccessNodes`
- `getAccessNodeList`
- `doSaveAccessNode`
- `doDeleteAccessNode`

如果目标 app 命名不同，以该 app 现有封装为准。

## 对应后端接口

- `POST /admin/system/save_role_access_nodes/`
- `POST /admin/system/access_node_save/`
- `POST /admin/system/access_node_delete/`

## 相关后端数据访问层

常见路径：

```text
/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin/Dao/AccessNodeDao.php
/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin/Dao/RoleDao.php
```

优先走 app CLI 或系统控制器逻辑，把直接 SQL 当成例外情况。
