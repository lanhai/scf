# Docker nginx PID 归属检查回归

2026-09-15。用户现场为 Windows 服务器上的 Docker 生产环境，日志持续报告“无法确认 nginx PID 归属，停止启动或重载”。尚未连接生产服务器，以下为本地源码和隔离容器验证。

## 根因与执行链

`GatewayBusinessCoordinator → bootstrapManagedUpstreams → completeManagedStartupCutover → syncNginxProxyTargets → GatewayNginxProxyHandler::sync → isNginxRunning → ProcessInspector::snapshot(true) → ps`。

本地已有的 `registry.cn-chengdu.aliyuncs.com/huiyun/swoole:latest`（镜像 ID `76078d9f7659`，Linux/amd64，PHP 8.1.31 / Swoole 5.1.8）没有 `/bin/ps` 或 `/usr/bin/ps`。旧实现执行 `ps -axo pid=,ppid=,state=,command=` 得到空快照；只要 PID 文件中的数字仍有进程占用，nginx 检查就会抛出同一异常。候选业务实例虽已启动，后续每轮切流仍失败。容器重启还可能保留 PID 文件，而其数字已分配给 PHP 等新进程。

修改前，在该镜像中将私有 nginx PID 文件指向测试 PHP 的存活 PID，真实调用 `isNginxRunning()`，稳定复现原始异常。生产具体镜像标签尚未取得，因此没有把本地镜像缺少 ps 当作已在生产执行检查的结果。

## 修改

- Linux `ProcessInspector` 从当前 namespace 的 `/proc` 读取进程表，不调用 ps，也无需在生产镜像补装 procps。
- 单 PID 强制核验直接读该 PID，绕过全表采样和缓存；读取 cmdline 前后核对 stat 的启动时刻及进程名，拒绝混合身份。完整快照超时或遇到仍存在但无法读取的进程时保持失败关闭，破坏性操作不能使用旧数据。
- nginx 只有在 PID 对应存活的 `nginx: master process` 时才能走 reload；死亡、僵尸或被其它进程复用的 PID 走启动判断。进程存在但身份不可读时仍停止操作，并报告 PID、PID 文件与平台。
- macOS 保留原有带时限的 ps；配置事务回滚、入口配置指纹核验及旧业务排空顺序保持现有设计。

`/proc/PID/stat`、`cmdline` 和 PID 复用语义参考 [Linux 内核 procfs 文档](https://www.kernel.org/doc/html/latest/filesystems/proc.html)。nginx 主进程与 reload 行为参考 [nginx 控制文档](https://nginx.org/en/docs/control.html)。

## 验证结果

| 环境 / 场景 | 结果 |
| --- | --- |
| SCF Linux/amd64 镜像，无 ps，PHP 8.1.31 | 进程读取、父子树、CLI 参数匹配、退出后强制刷新通过 |
| 同一 SCF 镜像，Swoole 5.1.8 协程 | 快照和单 PID 读取通过 |
| 原生 Linux/arm64，PHP 8.2 / nginx 1.22，无 ps | 真实 nginx 冷启动、归属识别、重载、失败回滚、慢响应跨 reload 通过 |
| 同一原生 Linux 容器，首次启动 + 3 次实际 docker restart | 4 次均 READY，入口分别返回 boot-1 / boot-2 / boot-3 / boot-4 |
| 每次容器启动前将旧 PID 文件指向 PHP/PID 1 | 正确识别为其它进程，PHP 未被错误发送信号，随后启动 nginx 成功 |
| macOS / 原有 PHP + Swoole / 真实 nginx | 原有 nginx 启动回归通过 |
| 原有启动切流、respawn backoff、system spawn guard | 全部通过 |

本机 ARM Mac 使用 Rosetta 运行 amd64 镜像时，内核 cmdline 暴露原始 nginx argv，未反映 nginx 修改后的 master 标题。因此完整 nginx 行为和重启测试使用原生 Linux/arm64 容器；amd64 SCF 镜像验证了故障复现以及实际 PHP/Swoole 的 procfs 读取。未声称已在 Windows 生产服务器完成实测。

## 可重复执行

测试镜像由已有 `node:24-bookworm-slim` 原生 arm64 镜像安装 `php-cli nginx` 构建，不安装 procps。测试容器只读挂载 SCF 源码，`--network none`，不挂载应用数据或宿主 nginx 配置，不发布端口。

```sh
docker build --platform linux/arm64 -t scf-nginx-procfs-test:local - <<'DOCKER'
FROM node:24-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends php-cli nginx && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["php"]
DOCKER

php tests/process_inspector_linux_behavior_test.php
php tests/gateway_nginx_startup_behavior_test.php
php tests/gateway_startup_cutover_behavior_test.php
php tests/process_respawn_backoff_regression.php
php tests/system_spawn_guard_regression.php

docker run --rm --network none --platform linux/amd64 --entrypoint php \
  -v "$PWD:/scf:ro" registry.cn-chengdu.aliyuncs.com/huiyun/swoole:latest \
  /scf/tests/process_inspector_linux_behavior_test.php

docker run --rm --network none --platform linux/arm64 \
  -e SCF_TEST_NGINX=/usr/sbin/nginx -v "$PWD:/scf:ro" \
  scf-nginx-procfs-test:local /scf/tests/gateway_nginx_startup_behavior_test.php
```

`tests/fixtures/nginx_container_restart.php` 仅作为隔离 Linux 测试容器的 PID 1 运行。它在私有 `/tmp` 保留状态，输出 READY 后保持运行，供 Docker 真正停止和重启。只对本次创建的测试容器执行重启；验证完成后已移除该容器。

本轮仅修改源码与测试，未构建或发布框架包，未更新生产容器。生产需要加载修复后的框架代码，单纯重启仍使用旧包的容器不会带入此修复。
