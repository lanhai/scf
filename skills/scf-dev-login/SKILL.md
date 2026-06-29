---
name: scf-dev-login
description: 当 Codex 需要调用任意 SCF app 的受保护 /admin/* dev API、System Controller、权限节点或角色接口时使用。根据目标 app 调整 base URL、登录账号、cookie jar、Bearer token 和入口路径；凭据必须来自目标 app 文档、配置或用户指令。
---

# SCF Dev Login

Use this skill whenever SCF app dev backend work needs a real CP/admin login session.

## App profile first

Before calling protected APIs, identify:

- app name: `<app>`
- dev base URL
- login endpoint, usually `POST /admin/user/login/`
- dev username and password
- whether the app requires both Bearer token and session cookie

Do not reuse credentials from another app. Read target app docs/config or ask the user if credentials are not available in the current task context.

## Important runtime rule

Normal SCF admin routes may depend on both the token and the session state.

- Always keep the session cookie jar produced at login.
- When the login response returns a token, send both:
  - `Authorization: Bearer <token>`
  - the matching cookie jar
- If either is missing, protected `/admin/*` routes may behave as if the login expired.

## Standard login workflow

1. Create a temporary cookie jar.
2. `POST <base-url>/admin/user/login/` with the target app dev credentials.
3. Parse the returned token field, usually `data.token`.
4. Reuse the same cookie jar and Bearer token for every protected request in that workflow.
5. Clean up the temporary cookie jar after the workflow finishes.

Example:

```bash
COOKIE_JAR=$(mktemp)
LOGIN_RESP=$(curl -sS -c "$COOKIE_JAR" \
  -X POST '<base-url>/admin/user/login/' \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  --data 'username=<username>&password=<password>')

TOKEN=$(printf '%s' "$LOGIN_RESP" | php -r '
$json = json_decode(stream_get_contents(STDIN), true);
echo $json["data"]["token"] ?? "";
')
```

Protected request pattern:

```bash
curl -sS -b "$COOKIE_JAR" \
  -X POST '<base-url>/admin/target/path/' \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  --data '...'
```

## When to prefer this skill

- Creating or granting CP/admin access nodes through `/admin/system/*`
- Calling protected business admin APIs in dev
- Doing dev-only API smoke tests that require a logged-in backend user
- Verifying page/menu visibility through a non-admin role

## Verification checklist

- Login response is JSON and returns success, usually `errCode: 0`.
- Token is non-empty when the app uses Bearer auth.
- Protected request returns JSON business data instead of an HTML error page or login-expired response.
- Cookie jar is cleaned up after the workflow finishes.
