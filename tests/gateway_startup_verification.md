# Gateway 启动与 nginx 切流验证

2026-09-07，macOS / PHP 8.1 / Swoole 5.1.8 / nginx 1.29.7。

## 故障与执行链

`bin/server → boot → ServerRuntimeMain/ServerRuntimePorts → CliBootstrap → GatewayServer → UpstreamSupervisor → managed upstream → nginx sync → 真实入口探针 → 旧代排空`。

现场 19580 由 Docker 监听，但系统 nginx 的 `servers/*` 仍包含 mtvideo 旧测试实例的 19580 server/upstream 配置。启动预热在生成当前配置前直接启动 nginx，导致与本次 9680 无关的历史监听冲突阻止整个 nginx 启动。

重复 start 原来只向监听者发信号，外层 boot 可能重新拉起旧网关；CliBootstrap 随后无条件清理旧业务实例。启动计划一旦注册就标记完成，即使 nginx 同步失败也不再尝试切流，且日志可提前显示启动完成。worker 发起的保留 upstream 状态还可能被 master 的关闭回调覆盖。

## 修复后的边界

- 重复 start 优先通过本机内部命令交接，旧 boot 只消费自己 PID 对应的退出标记。旧版不支持交接时，兼容回收先沿准确的进程祖先链停止负责自动重拉的 boot。
- nginx 模式的 restart 默认保留业务实例。worker/master 使用共享交接状态；新控制面只保留可核实 PID、app、端口和 epoch 的同租约实例。
- 同版本启动也创建独立 generation。新实例先通过业务健康检查，再同步 nginx，并核对真实业务入口返回的 upstream 端口；之后才让旧代停止接收新业务并排空。
- nginx 配置的写入、测试、reload/start、实际生效确认在锁内执行。文件用原子 rename 替换；失败恢复原文件。仅收到 reload 信号或 `nginx -T` 成功不能算完成。
- 其它端口的退役配置须同时满足：同 app 的 SCF 生成标记、无有效租约、无所属进程、全部本机后端离线。配置备份移到 include 目录外，兼容 `include servers/*`；不终止外部端口所有者，也不移除仍有活跃后端的配置。
- nginx 配置暂时失败时保留可重试候选和旧代，不重新启动健康候选，不输出 Gateway 启动完成。

nginx HUP 保留旧 worker 完成已有请求；配置应用失败时旧 worker 继续服务。依据：[nginx 控制机制](https://nginx.org/en/docs/control.html)。SCF 业务实例仍须另行保留和排空，不能只依赖 nginx reload。

## 实际验证

对真实 `mtvideo -dev -port=9680` 的 `/_scf_internal/upstream/cutover_probe` 连续建立 HTTP 连接，检查响应成功及返回的 upstream 端口：

| 操作 | 时长 | 请求数 | 失败数 | 实际切流 |
| --- | ---: | ---: | ---: | --- |
| 重复 start | 55 秒 | 552 | 0 | 9681 → 9682 |
| 显式 restart | 50 秒 | 520 | 0 | 9682 → 9681 |
| 最终代码重复 start | 45 秒 | 718 | 0 | 9681 → 9682 |

三轮保持 epoch 55，合计 1790 次探测、0 次失败。最终 9680 业务入口可用，流量命中 9682。

隔离 nginx 回归使用独立主配置、PID 和临时端口，不读取系统 include。测试覆盖真实端口冲突复现、退役配置备份、保留活跃后端、非法配置回滚、HUP 信号发送成功但实际 bind 失败、慢响应跨 reload 完整返回。

```sh
php tests/gateway_nginx_startup_behavior_test.php
php tests/gateway_startup_cutover_behavior_test.php
php tests/gateway_boot_handoff_behavior_test.php
php tests/gateway_boot_handoff_recovery_test.php
php tests/process_respawn_backoff_regression.php
php tests/system_spawn_guard_regression.php
```

启动状态测试覆盖健康门槛、nginx 失败、入口目标错误、原候选重试、旧代排空及 worker/master 共享交接；boot 测试启动真实父子进程，验证回收旧监听者后外层 boot 不再重拉，并检查 app/端口精确匹配及 PID 退出标记隔离。

`system_spawn_guard_regression.php` 首两次在 100ms 子进程超时检查失败；单独执行该未修改 runner 的诊断以及原测试复跑通过。其它新增回归与语法检查通过。

## 验证范围

本次为源码目录模式的本机 dev 验证，未执行框架打包或发布。初次替换尚不支持 handoff 的旧进程仍使用兼容回收；后续新版本之间使用平滑交接。控制面连接在控制面重启时仍会重新建立。旧业务已无连接和在途工作却迟迟不退出时，既有后台定向回收机制仍保留；它不再阻塞新入口启动。

## 16:08 后补充：交接无回执导致启动 Fatal error

用户再次报告 `Gateway 交接请求未获确认，保留当前服务: 无有效响应`。现场租约 epoch 55 已为 `stopped`，控制监听者 42953 随后退出；孤立业务进程 42997 仍持有 9681/9686，但 nginx 业务探针超时。此前“只要无回执就抛异常”的分支没有区分已停止的服务与仍在进行的交接，且 port preparation 在异常处理之外，直接产生 PHP Fatal error / 255。

用独立 PHP 控制监听进程及真实租约文件，在修改前稳定复现同一异常；修复后覆盖以下 8 种行为：

- `stopped` 但控制端口残留：核实所属 PID 后自动回收并继续。
- `running` 租约超过 upstream 的宽限期：允许恢复失效的控制面。
- 已进入 `restarting` 但回执丢失：等待原控制面退出，不发送强制终止信号。
- 明确响应“Gateway 已在关闭中”：按已有退出流程等待。
- HTTP 请求期间监听者消失：继续启动，不把无回执视为失败。
- 控制子进程异常退出导致回执丢失，但外层 boot 仍准备自动重拉：监听完全释放后结束请求前记录并再次核实身份的旧 boot，避免旧命令重新抢占端口。
- 明确拒绝交接：保留原进程并报告拒绝原因。
- 有效 `running` 租约却无回执：有界等待后保留原进程，启动 CLI 输出端口、PID、租约和传输诊断，退出码为 1，无未捕获异常。

恢复只发生在 fork 前的 CLI，沿相同 app、dev/prod、role、业务端口的 PHP boot 命令核对监听者与祖先进程；不因端口相交回收其它命令，也不追杀等待期间出现的新代 PID。

本轮启动自动清理了无响应的旧业务残留，建立 epoch 56 并恢复 9680。随后两轮真实重复 start 共 65 秒、846 次请求、0 次失败，分别验证 9682 → 9681、9681 → 9682；显式 restart 再验证 30 秒、486 次请求、0 次失败，9682 → 9681。三轮共 1332 次成功请求，epoch 56 保持不变，当前入口转发到 9681。冷恢复之前服务已不可用，不计入无中断切流验证。

新增 8 种恢复回归、旧 boot 父子回收回归、真实 nginx 回归、启动切流状态回归及 respawn backoff 回归通过。既有 `system_spawn_guard_regression.php` 在并行运行时再次出现 100ms 子进程断言失败，单独复跑通过；它调用的 `scf_process_output` 与 HEAD 完全相同，未在本次修改。PHP 语法和 `git diff --check` 通过。
