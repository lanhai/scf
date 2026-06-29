---
name: scf-dao-arcreator
description: 当你在 SCF 工作区应用中新增或修改数据库字段、表结构、生成式 Dao、src/config/db 映射或 ArCreator/ArCreater 生成物时使用。适用于任意 SCF app，必须先改 dev 真实表结构，再通过 SCF 既有生成流程刷新 Dao 和映射，禁止手工编辑生成物。
---

# SCF Dao 与 ArCreator

## 概览

只要任务涉及 SCF app 的表结构、Dao 生成、数据库映射更新，或者和数据库状态相关的后台节点操作，就用这个 skill。

这个 skill 是 SCF 层能力，覆盖任意 SCF app；不要再为单个 app 拆出重复的 Dao/ArCreator 技能。

## 强制规则

### 1. 默认使用 dev 环境

所有数据库和生成物相关操作默认使用 dev 配置；除非用户在当前轮次明确要求生产/线上环境。

### 2. 不允许手工改生成物

不要手工编辑：

- 根据表映射生成的 Dao 文件
- `src/config/db/*.yml` 映射配置
- 应由运行时或生成流程维护的 `db/updates/*.yml`

### 3. 真实表结构优先，再生成

正确流程只能是：

1. 先改 dev 环境真实表结构
2. 再走 SCF 既有 CLI `ArCreator`/`ArCreater` 生成流程
3. 最后核对生成出的 Dao 和映射文件

### 4. 不要凭空编生成命令

如果当前 app 的准确生成入口不确定，先查本地 CLI help、既有脚本或项目文档。确认入口前，不要手写生成物，也不要用猜测的命令覆盖文件。

### 5. 权限数据安全

涉及 CP 权限节点和角色时：

- 优先走系统控制器 API 或项目权限节点 CLI
- 除非用户明确要求，否则不要直接写 SQL

### 6. 错环境保护

如果误写到了错误环境：

1. 立刻回滚
2. 验证回滚结果
3. 明确报告误操作和验证状态

## 通用流程

1. 确认目标 app、DB alias、真实表名和 Dao 路径。
2. 用 `SHOW FULL COLUMNS` 或 `SHOW CREATE TABLE` 验证 dev 数据库已经是目标 schema。
3. 从 TTY 运行 SCF 生成器；常见入口如下，具体 app 以本地 CLI 为准：

```bash
php /Users/lanhai/projects/swoole/scf/boot toolbox ar -app=<app-name>
```

4. 按提示选择 DB alias、输入不带默认前缀的表名、填写或保留 Dao 路径，并确认覆盖既有生成文件。
5. 生成后检查：
   - Dao 类包含新增或调整字段
   - `src/config/db/{db}_{table}.yml` 版本和字段内容匹配真实表结构
   - 相关 PHP 文件通过 `php -l`
   - 相关差异只包含预期生成物

## 应用注意事项

- 后端根目录：`/Users/lanhai/projects/swoole/apps/<app>`
- Dao 通常位于：`src/lib/*/Dao`
- 映射文件位于：`src/config/db/*.yml`
- 如果目标 app 有结构说明、生成说明或 app-local skill，广泛扫描前先读这些文档。
- 生成命令进入 dev mode 时，仍要传 `-app=<app>`，确保 `APP_PATH` 指向目标 app。
- 不要手动追改 `db/updates/*.yml`。如果目标 app 的 runtime update 文件由 worker 或框架数据库更新路径刷新，`src/config/db` 和 Dao 已按 dev schema 生成后，`db/updates` 暂时旧是可接受状态。
- 默认前缀表请输入不带配置前缀的表名。例如真实表是 `t_<table>`，DB alias 为 `default` 时输入 `<table>`。
- 如果同时涉及 CP 权限节点或角色授权，权限变更必须走 `$scf-access-node-workflow`，不要借 Dao 任务直接写权限表。

## 输出要求

使用这个 skill 时，要明确说明：

1. 改的是哪张表或哪段 schema
2. 哪个 app、DB alias 和 Dao 路径被刷新
3. 哪些生成物是通过 `ArCreator`/`ArCreater` 刷新的
4. 生成后做了哪些验证

具体 Dao 位置和安全注意事项请看 `references/dao-safety.md`。
