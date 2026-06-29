# SCF Dao 与表结构安全说明

当任务涉及 SCF app 的数据表、生成式 Dao 类或数据库映射刷新时，先看这个文件。

## 相关路径

- SCF 框架根目录：
  `/Users/lanhai/projects/swoole/scf`
- app 后端根目录：
  `/Users/lanhai/projects/swoole/apps/<app>`
- Dao 类通常位于：
  `apps/<app>/src/lib/*/Dao`
- 映射文件位于：
  `apps/<app>/src/config/db/*.yml`
- runtime update 文件通常位于：
  `apps/<app>/db/updates/*.yml`

## 强制流程

1. 先改 dev 环境真实表结构。
2. 再通过 SCF 既有 CLI `ArCreator`/`ArCreater` 流程重新生成。
3. 最后验证生成出来的 Dao、`src/config/db/*.yml` 和相关语法检查。

## App 特例

如果目标 app 的 `db/updates/*.yml` 由 worker 启动时通过框架数据库更新路径刷新，开发过程中不要手动追着同步 `db/updates`；如果 `src/config/db/{db}_{table}.yml` 和 Dao 已经从 dev schema 重新生成，`db/updates` 暂时仍旧是可接受状态。

如果目标 app 有项目结构说明、生成说明或 app-local skill，先读这些文档，再做广泛扫描。

## 重要提示

- 不要凭空编生成命令；真执行前先查本地 CLI help、既有脚本或团队文档确认入口。
- 不要手工改生成 Dao 和 db mapping。
- SQL 可用于只读验证；除非用户明确要求，不要直接写权限、角色或业务表数据。
