---
name: scf-page-visibility-permission
description: 当任意 SCF app 的 CP/admin 前端需要新增或调整页面可见权限、view_permissions、hasPermission、按钮/列/筛选项/前端元素显示控制时使用。根据目标 app 调整 CP 前端工程、apps/<app>/src/config/app.php 和角色授权流程。
---

# SCF Page Visibility Permission

## What This Controls

Page visibility permissions are frontend display switches stored on the role as `view_permissions`.
They are separate from navigation/API access nodes and do not replace backend endpoint authorization.

先确认目标 app：

- 后端根目录：`/Users/lanhai/projects/swoole/apps/<app>`
- permission config：通常在 `apps/<app>/src/config/app.php` 的 `admin_role_permissions`
- CP 前端工程：`/Users/lanhai/projects/vue-admin/<frontend-app>`
- 前端权限工具：在目标 CP 工程中查 `hasPermission` 或等价工具

## Workflow

1. Locate the visible UI element in the target CP frontend `src/views`.
2. Choose a stable permission alias, usually `Domain.feature_action`, for example `<Domain>.<feature_action>`.
3. Add the permission option in `apps/<app>/src/config/app.php` under `admin_role_permissions`.
4. In the Vue page/component, import or use the target frontend's permission helper, commonly `hasPermission`.
5. Apply `v-if="hasPermission({permission: ['Alias.name']})"` to the smallest wrapper that owns the visible element, or create a computed when the alias is reused.
6. If the same business action appears in multiple page entry points, guard every entry point with the same alias unless separate authorization is intentionally needed.
7. Verify with `rg` that the alias exists once in the backend config list and on every relevant page element.

## Config Pattern

```php
[
    'label' => '业务页面名称',
    'permissions' => [
        ['label' => '执行具体动作', 'permission' => 'Domain.feature_action'],
    ]
]
```

## Frontend Pattern

```vue
<el-form-item v-if="canRunAction">
  <el-button @click="runAction">执行</el-button>
</el-form-item>

<script lang="ts" setup>
import {computed} from 'vue'
import {hasPermission} from '/@/utils/permission.ts'

const canRunAction = computed(() => {
  return hasPermission({permission: ['Domain.feature_action']})
})
</script>
```

If the target CP frontend uses a different helper path, follow that app's existing pattern instead of copying the example blindly.

## Role Assignment

After adding the config entry, assign it through the role page's `页面权限` tab or the System Controller workflow. Do not write `cp_role.view_permissions` directly with raw SQL unless the user explicitly asks for it.

For CLI/System Controller assignment, use `$scf-access-node-workflow`.

## Verification Notes

- Super admins may pass `hasPermission` automatically; test with a non-admin role when checking visibility.
- Keep labels user-facing and action-specific.
- If the feature also needs API/navigation authorization, add the corresponding access node in addition to this page visibility permission.
- Confirm all paths and aliases are adjusted for the target app, not copied from another app by habit.
