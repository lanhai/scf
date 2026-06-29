---
name: scf-access-node-workflow
description: 当任意 SCF app 需要新增、修改、分配或清理 CP/admin 权限节点、菜单节点、API 节点、角色授权或 view_permissions 时使用。根据目标 app 调整 --app、Dao、nodes.yml、CP 前端入口和后台 System Controller 路径，优先使用 dev-only AccessNode CLI。
---

# SCF 权限节点流程

## 概览

这个 skill 专门处理 SCF app 的 CP/admin 权限节点和角色授权。目标是让节点创建、角色绑定、菜单可见性始终沿用项目既有的系统控制器持久化语义。

先确认目标 app，再按 app 调整参数：

- app 名：`<app>`
- 后端根目录：`/Users/lanhai/projects/swoole/apps/<app>`
- CP 前端根目录：`/Users/lanhai/projects/vue-admin/<frontend-app>`
- AccessNode/Role Dao：通常在 `apps/<app>/src/lib/Admin/Dao`
- access config：通常是 `apps/<app>/src/config/access/nodes.yml`

## 首选 CLI

优先从 `/Users/lanhai/projects/swoole/scf/bin` 运行 app CLI：

```bash
./toolbox cli --app=<app> --env=dev --controller=AccessNode \
  --id=<节点ID> \
  --name=<节点名称> \
  --alias=<节点别名> \
  --parent=<父节点ID> \
  --api_path=<接口路径> \
  --active=2 \
  --is_nav=0 \
  --role-id=<角色ID> \
  --access=<节点别名> \
  --view-permission=<页面权限>
```

多节点时使用 `--file=/absolute/path/access-node.yml`。该 CLI 必须保持这些语义：

- 仅 dev 环境生效
- 使用目标 app 的 `AccessNodeDao::factory(...)->save(true)` 保存节点
- 使用目标 app 的 `RoleDao` 追加角色接口节点和页面权限
- 更新目标 app 的 `auth_nodes_version`
- 从 `cp_access_node` 重写目标 app 的 `src/config/access/nodes.yml`

如果目标 app 还没有 `App\Cli\Controller\AccessNode`，先参考 `$scf-access-node-cli` 增加同构 CLI，再执行节点变更。

## 备用接口

只有 CLI 不可用、或用户明确要求走 HTTP 后台时，才通过目标 app 的 System Controller 流程使用这些接口：

- `POST /admin/system/access_node_save/`
- `POST /admin/system/save_role_access_nodes/`
- `POST /admin/system/access_node_delete/`

受保护接口需要真实 dev 登录时，先使用 `$scf-dev-login` 获取目标 app 的 token 和 cookie jar。

## 硬性规则

1. 节点和角色操作先在 dev 环境做。
2. 除非用户明确要求，否则不要直接用 SQL 改 `cp_access_node` 或 `cp_role`。
3. 保持和后续同步脚本兼容。
4. 根据目标 app 调整 `--app`、Dao 命名空间、前端 API 文件和节点 YAML 路径，不要把其他 app 的参数硬套到当前 app。

## 验证要求

结束前确认：

1. 节点已挂到目标 app 的预期父节点和排序位置。
2. 目标角色已经拥有预期别名。
3. `src/config/access/nodes.yml` 已按目标 app 生成并包含该节点。
4. 页面能从目标 app 的 CP 菜单里打开。

如果权限节点是新管理页面的一部分，和 `$scf-cp-admin-feature` 一起用。

具体接口封装和相关 Dao 文件请看 `references/endpoints.md`。
