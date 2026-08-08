# Codebase Knowledge Graph (codebase-memory-mcp)

This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.
ALWAYS prefer MCP graph tools over grep/glob/file-search for code discovery.

1. Use `search_graph` for functions, classes, routes, and variables.
2. Use `trace_path` for callers and callees.
3. Use `get_code_snippet` for specific function or class source.
4. Use `query_graph` for complex Cypher queries.
5. Use `get_architecture` for a high-level summary.

Fall back to `rg`/file search for literals, errors, configuration, non-code files, or insufficient graph results.

# SCF Framework Scope

## Main Project

- The primary framework directory and default working scope is `/Users/lanhai/projects/swoole/scf`.
- Start discovery, analysis, edits, and commands here unless the task explicitly targets a consuming app or connected dashboard.

## Connected Scope

- Dashboard frontend: `/Users/lanhai/projects/vue-admin/dashboard`.
- A specific `apps/<app>` backend or its CP frontend may be opened only when the requested SCF/app chain requires it.
- Ignore other directories by default and ask before expanding beyond the active framework/application chain.

## Preferred Skill Routing

- Prefer a matching project-local skill without waiting for the user to name it.
- For dashboard node management, state, version upgrade, command dispatch, reload/restart, logs, memory tables, or websocket-driven operations, use `dashboard-node-ops`.
- For dashboard-triggered upgrade/reload/restart chain failures, use `dashboard-upgrade-trace`.
- For bugs, runtime anomalies, request failures, unexpected data, or unclear root cause, prefer `diagnose`.
- For code review, prefer `review`; for test-first implementation, prefer `tdd`; for system mapping, prefer `zoom-out`.
- If multiple skills apply, choose the smallest useful set and use project-specific SCF/Swoole/dashboard skills first.

## Project Skill Directory (Mandatory)

- Canonical SCF/framework skills live in `/Users/lanhai/projects/swoole/scf/skills`.
- Read the matching project-local `SKILL.md` before using a global fallback.
- Maintained framework skills:
  - `scf-release-publish`
  - `scf-framework-publish`
  - `scf-access-node-cli`
  - `scf-access-node-workflow`
  - `scf-admin-route-annotation`
  - `scf-cp-admin-feature`
  - `scf-dao-arcreator`
  - `scf-dev-login`
  - `scf-page-visibility-permission`
  - `swoole-chain-trace`
  - `swoole-task-router`
  - `swoole-runtime-safety`
- Use `scf-dao-arcreator` for schema, Dao, DB mapping, and ArCreator/ArCreater work across SCF apps.
- Apply framework-level `scf-*` workflows to the target app by adjusting `--app`, backend/CP roots, login base URL/credentials, and System Controller endpoints.
- Resolve relative skill references against the selected skill's project-local directory.

## Completion Report Rule (Mandatory)

- After completing every user instruction in this project, use `/Users/lanhai/.codex/skills/wechat-direct-report/SKILL.md` to send the completion report to 兰海 over private WeChat.
- The WeChat report must be semantically identical to the final Codex reply and must not be shortened; use the skill's `--stdin` workflow.
- Do not start Hermes Manager, Gateway, dev servers, Electron, or watch processes only for notification.
- If local WeChat reporting is unavailable, mention the failure in the final reply.

## Code Commenting Standard (Mandatory)

- Important framework classes need class-level docblocks describing responsibility boundaries, architectural position, and design intent.
- Important public/protected methods need PHPDoc describing their lifecycle/workflow role and non-obvious inputs, outputs, side effects, `@param`, `@return`, and contract-relevant `@throws`.
- Add short, high-value comments before non-trivial lifecycle/state branches to explain the invariant or runtime design reason being protected.
- Do not add filler comments or restate syntax.

## Runtime Change Decision Rule (Mandatory)

- Before changing process lifecycle, coroutine scheduling, timers, signals, exit behavior, reload/restart flow, or runtime controls, verify the change against the current Swoole model.
- Before introducing `exit`, forced termination, direct coroutine shutdown, or process spawning, check effects on parent/child coordination, timers, hooks, shared tables, pending coroutines, and recoverability.
- Explicitly reason through whether the mechanism can work, its side effects, and its runtime basis before editing.
- If that reasoning is incomplete, inspect the existing lifecycle design first.

## End-to-End Chain Verification Rule (Mandatory)

- Before changing upgrades, scheduling, node communication, process coordination, dashboard actions, or another multi-step feature, trace the full chain.
- Cover the user-facing dashboard/CLI entry, API/controller or websocket entry, dispatch/IPC/socket/process handoffs, actual executor, and result/timeout/state-return path.
- Identify the exact failing step, why it fails, and its upstream/downstream dependencies before patching.
- Do not default to async, timeout, cache, fallback, or faster return behavior until evidence shows it fixes the root cause.
- If the chain is unclear, continue tracing rather than patching a visible symptom.

## Change Impact and Evidence Rules (Mandatory)

- Never blindly patch. First梳理完整上下游链路 and require evidence that the edited point is the root cause.
- Before substantial edits, communicate the current understanding, intended action, and expected impact.
- Evaluate runtime cost, complexity growth, global impact, and long-term maintenance weight.

## Swoole First Rule (Mandatory)

- Prefer Swoole-native coroutines, timers, barriers, channels, and event-driven mechanisms whenever they safely fit the runtime model.
- Do not choose blocking PHP patterns, ad-hoc polling, or non-Swoole concurrency primitives first when native mechanisms solve the problem cleanly.
- Fall back only after verifying the path is outside Swoole runtime, lifecycle-incompatible, or materially riskier with Swoole; state the reason.

## SCF App Administration Rules (Mandatory)

- For an SCF app CP/admin feature, load `$scf-cp-admin-feature` and follow backend controller, CP page/API, route/menu, permission node, and role verification through the full chain.
- Use `Route` annotations beside Admin Controller methods by default.
- New/touched routes use slash-separated resource/action paths rather than new snake_case action segments.
- Add permission nodes through the dev-only `AccessNode` CLI or `scf-access-node-workflow`; never directly modify `cp_access_node`, `cp_role`, or generated node output.
- For a protected dev Admin/System API session, load `scf-dev-login` and use the target app's documented credentials, matching Bearer token, and session cookie jar.

## Database and Query Safety (Mandatory)

- Use dev configuration by default and never write to production/online DB without explicit instruction in that turn.
- Roll back an accidental wrong-environment write immediately and report verification.
- For table mappings, update the real dev schema first and regenerate Dao classes and `src/config/db/*.yml` via `ArCreator`; never hand-edit generated artifacts.
- Prefer Dao `join()` for cross-table associations and statistics; use split/manual queries only when the framework cannot express the required semantics, and explain why.

## Framework Publish Workflow (Mandatory)

When the user asks to publish the framework:

1. Run `framework build`.
2. Run `framework push`.
3. After publish succeeds, run `/Users/lanhai/projects/swoole/etc/build_main.sh`.

- Run publish commands from `/Users/lanhai/projects/swoole/scf/bin` using the SCF bin entry scripts.
- Do not reorder or skip steps.
- Retry a transiently failing current step until it succeeds before continuing.

## Project Responsibility Index (Keep Updated)

### SCF Framework (`/Users/lanhai/projects/swoole/scf`)

- Boot and command dispatch: `bin/*`, `boot`.
- Core runtime/lifecycle: `src/Core`, `src/App`, `src/Mode`.
- HTTP/WS/RPC support: `src/Client`, `src/Rpc`, `src/Server`.
- Database/cache/utilities: `src/Database`, `src/Cache`, `src/Helper`, `src/Util`.
- Build/release tooling: `bin/build`, `tools`.

### SCF Node Dashboard (`/Users/lanhai/projects/vue-admin/dashboard`)

- Node state and version presentation.
- Upgrade, command dispatch, reload, and restart interfaces.
- Logs, tasks, memory tables, and runtime-state views.
- Frontend integration with SCF dashboard APIs and websocket endpoints.

## Dashboard-to-SCF Linkage

- Dashboard actions are only the user-facing entry to an SCF runtime/control chain.
- Trace each operation from the dashboard request through the SCF API/websocket handler, node dispatch, executor, and returned status before changing timeout or UI behavior.
- Keep command semantics and runtime truth authoritative in SCF; the dashboard should present and control that state without inventing a parallel lifecycle model.
