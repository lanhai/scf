---
name: scf-access-node-cli
description: Manage SCF app CP/admin access nodes and role/page permissions through a dev-only CLI that mirrors System Controller persistence and rewrites nodes.yml. Use when an SCF app needs new, changed, deleted, or assigned access nodes, API permission nodes, menu/page nodes, or view_permissions without relying on a running HTTP backend.
---

# SCF Access Node CLI

## Rule

For SCF apps, prefer the app CLI access-node workflow over HTTP System Controller calls when creating or updating CP/admin permission nodes.

The workflow must remain equivalent to System Controller persistence:

- Dev environment only.
- Save nodes through the app `AccessNodeDao`.
- Save role grants through the app `RoleDao`.
- Update `auth_nodes_version`.
- Rewrite the app `src/config/access/nodes.yml` from `cp_access_node`.
- Verify DB state and generated YAML after running.

Do not raw-update `cp_access_node` or `cp_role` with SQL. SQL is allowed for read-only verification.

## Command Pattern

Run from `/Users/lanhai/projects/swoole/scf/bin`:

```bash
./toolbox cli --app=<app> --env=dev --controller=AccessNode \
  --id=<node-id> \
  --name=<node-name> \
  --alias=<node-alias> \
  --parent=<parent-node-id> \
  --api_path=<admin-api-path> \
  --active=2 \
  --is_nav=0 \
  --remark=<remark> \
  --orderby=0 \
  --status=1 \
  --role-id=<role-id> \
  --access=<node-alias> \
  --view-permission=<view-permission>
```

Use the target app name from the current task, for example `--app=<app>`.

## File Mode

Prefer file mode for more than one node or role:

```yaml
nodes:
  - id: <node-id>
    name: <node-name>
    alias: <node-alias>
    parent: <parent-node-id>
    api_path: /admin/<resource>/<action>/
    active: 2
    is_nav: 0
    remark: <remark>
    orderby: 0
    status: 1
roles:
  - id: <role-id>
    access:
      - <node-alias>
    view_permissions:
      - <permission-alias>
```

Run:

```bash
./toolbox cli --app=<app> --env=dev --controller=AccessNode --file=/absolute/path/access-node.yml
```

## For Other SCF Apps

If the target app does not yet have `App\Cli\Controller\AccessNode`, add the same controller pattern under:

```text
apps/<app>/src/lib/Cli/Controller/AccessNode.php
```

Adapt only the DAO/model imports if the app uses different class names for:

- access node DAO
- role DAO
- config model

Keep the same dev-only guard and `nodes.yml` rewrite behavior.

## Verification

After running, verify:

- `src/config/access/nodes.yml` version changed and contains the node.
- `cp_access_node` has the expected `id/alias/parent/api_path/status`.
- Target `cp_role.role_access` contains the node alias.
- Target `cp_role.view_permissions` contains the page permission, when applicable.
