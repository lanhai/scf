# Swoole 5.1.8 Darwin Worker SIGBUS 排查与隔离回补

## 生产证据（2026-10-10）

通过已保存凭据只读核对 `longfu`：应用 `1.10.87`、框架 `1.3.82`、PHP `8.1.32`、Swoole `5.1.8`，平台 macOS 26.1 arm64/kqueue。

10 月 10 日错误日志记录 6 次 `Send of 5 bytes failed with errno=32 Broken pipe`。对应的 09:01:32、09:11:56、13:23:22、14:21:48 四份 macOS `.ips` 显示相同的 `EXC_BAD_ACCESS / SIGBUS / EXC_ARM_DA_ALIGN`，PC 为 `0x1`；故障线程的 x0 为 `13`（SIGPIPE）。实际崩溃的是普通 event worker。

调用链为登录/夸克统计接口或框架统计协程 → Dao/DB → ConnectionPool::borrow → 淘汰过期连接 → 原生 I/O 断管 → SIGPIPE → kqueue 信号分发 → Worker SIGBUS → manager 拉起替代进程和回补 inflight。inflight 回补只能修复计数，不能恢复该进程中被中断的请求/协程。

“5 字节”与 MySQL 4 字节包头加 COM_QUIT 命令相符；PHP mysqlnd 在连接析构时发送 COM_QUIT，生产报错也落在连接池替换旧连接的位置。这支持“旧连接析构触发断管”的判断，但没有抓包确认具体断连接来源，因此不把 MySQL 空闲超时、网络或服务重启视为已确认根因。

补充只读核对：生产池配置为 `max_lifetime=3600`、`connection_auto_ping_interval=0`、`connection_idle_timeout=30`。部署包 `DB::createPool` 中启动 `idleCheck()` 的代码已被注释，因此 30 秒空闲回收没有运行；1 小时最大寿命仅在再次借用连接时检查，并非定时销毁。已经断开的连接可以继续留在池中，直到被借用或淘汰才暴露问题。该机制解释了发现断管的时机，尚不足以证明最初断连的原因。

MySQL 全局及本次只读连接的 `wait_timeout` 均为 28,800 秒，连续运行约 3.1 天。错误日志有 `2026-10-10T00:13:49.967338+08:00`、连接 ID `112461` 的 `IO-layer timeout before wait_timeout was reached`，但应用断管日志没有 MySQL 连接 ID，无法把两端记录对应起来。不能据此认定这一次 I/O 超时解释了全部断管。后续应关联连接 ID、建立时间、最后使用时间及淘汰原因，再对应服务端断连日志。

## 本地连接池修复（尚未发布）

`Driver::ping()` 使用实际 PDO 执行 `SELECT 1` 并释放结果，探测计入既有 MySQL inflight。`ConnectionPool::borrow()` 在独占连接的边界检查寿命和空闲时间，按 `connection_auto_ping_interval` 探测旧会话；失败仅新建并验证一次，第二次失败向上传递并释放池容量。等待池容量超时不再创建池外连接。归还时更新最后使用时间，避免把长事务持有时间误算为空闲。淘汰过程中保留 active 槽位直到关闭完成，不关闭其他协程正在使用的连接。

mtvideo 本地 `src/config/app.php` 的探测间隔由 0 调整为 10 秒。此配置配合新版连接池生效；不是后台 ping 定时器。健康复用连接至多每 10 秒增加一次探测 SQL，0 仍代表关闭探测。该借用探测和以下空闲回收独立配置，空闲回收本身不增加探测 SQL。

实际 PDO/TCP 回归使用本机临时 MySQL 协议测试端（无真实数据库、无业务数据）：先成功查询，再由测试端关闭连接；原代码将断开的 PDO 交给业务，修改后替换并探测新连接，随后执行业务查询。另验证持续故障只替换一次、建连失败清理、容量等待、空闲/寿命淘汰及在用连接隔离。

```sh
cd /Users/lanhai/projects/swoole/scf
php -n -d extension=tools/swoole-sigpipe/build/swoole-src-5.1.8/modules/swoole.so tests/database_pool_checkout_behavior_test.php
python3 tests/database_pool_disconnect_socket_test.py
```

### worker 空闲连接回收（2026-10-10，本地尚未发布）

`IdleConnectionReaper` 在 `WorkerStart` 接入，所有池共用一个 `Timer::after` 到期定时器，用 `WeakMap` 登记池，池内只维护与 Channel FIFO 同序的空闲归还时间。不创建常驻协程，不按固定周期扫描全部连接；没有空闲连接或 `connection_idle_timeout=0` 时不继续调度。定时器已排定后，热连接反复借还也不反复创建/清除定时器，下一次到期唤醒会重新计算期限。

每轮最多关闭 32 个已到期空闲 PDO，积压分批间隔 100ms。不会发 `SELECT 1`，不会调用会立即补建连接的 `discard()`；只有下一次业务借用才建连。被借出的 PDO、长事务持有时间及尚未到期的 FIFO 顺序保持不变。归还时空闲队列已满则关闭多余连接，取消原来的五秒入列等待。若关闭挂起且有人因 `maxOpen` 等待，保持容量记账并在关闭后把关闭标记交给消费者，由借用端在原容量约束内新建，避免无限等待。

`WorkerExit` 首先停用调度并清除未触发定时器；`WorkerStop` 兜底。退出后的业务归还不重新挂定时器，共享 `serverIsDraining()` 排空标记也会阻止下一轮/下一次关闭。退出钩子不遍历池、不主动关闭 PDO、不等待回收任务。默认不在 CLI、独立 crontab 进程或不支持协程的 worker 开启后台回收，避免延长短任务生命；这些路径保留借用时的过期检查。已经开始的 PDO 关闭不能撤销，其原生关闭 I/O 仍遵循现有驱动语义；不把这些本地测试当成所有网络故障下零等待的保证。

验证：真实 PDO/本地 MySQL 协议端只收到业务 `SELECT 99` 和 COM_QUIT，没有维护 SQL 或第二次握手。80 个过期连接分三轮关闭；在用连接、关闭挂起时的等待消费者、满队列归还、排空、退出后归还、CLI 均通过。10,000 次热借还只建连一次、保留同一个定时器（时间结果仅本机微基准，不代表生产负载）。真实 Swoole 5.1.8 进程模式分别重载 HTTP worker 和协程 task worker，在途操作正常完成，60 秒维护定时器被清除，旧 PID 约 0.2–0.3 秒完成退出，没有 WorkerError 或强制终止。

```sh
php -n -d extension=tools/swoole-sigpipe/build/swoole-src-5.1.8/modules/swoole.so tests/database_pool_idle_reaper_behavior_test.php
python3 tests/database_pool_disconnect_socket_test.py --idle
python3 tests/database_pool_idle_worker_exit_test.py
```

成本是每个空闲连接一个标量时间条目、每 worker 最多一个到期定时器及到期时的关闭操作，不能宣称零 CPU/内存开销。恢复已有 30 秒空闲期限会减少长期闲置会话；超过该期限之后重新出现的业务仍须按需建连，借用端原本已做相同的过期替换。Darwin/Swoole 5.1.8 的 SIGPIPE 原生修复仍应先于启用新回收逻辑部署：关闭已经断开的 PDO 也可能触发原问题。

健康检查与下一条 SQL 之间仍有断连窗口；PDO 关闭旧连接时也可能发送 COM_QUIT，因此不能保证所有断管写入都消失。原生 Swoole 补丁仍须先应用，否则探测或淘汰也可能触发原来的 Worker 崩溃。本次新逻辑仅重试探测，不改变 `Connection::call()` 既有业务 SQL 重试行为，不提供写操作或事务自动重放的保证。生产需要更新 Swoole 原生扩展，并部署框架及应用配置；本地修改没有替换生产扩展、发布包或重启生产进程。

## 原生根因与修复点

- [Swoole v5.1.8 worker.cc](https://github.com/swoole/swoole-src/blob/v5.1.8/src/server/worker.cc) 的 `worker_signal_init()` 为 SIGPIPE 注册 SIG_IGN。
- [Swoole v5.1.8 signal.cc](https://github.com/swoole/swoole-src/blob/v5.1.8/src/os/signal.cc) 把该值保存在 kqueue 信号 handler 中。
- [Swoole v5.1.8 kqueue.cc](https://github.com/swoole/swoole-src/blob/v5.1.8/src/reactor/kqueue.cc) 只检查 handler 非空就调用，因 SIG_IGN 等于地址 1，最终跳到 PC=1。
- [上游 master](https://github.com/swoole/swoole-src/blob/master/src/reactor/kqueue.cc) 已包含 `handler != SIG_IGN` 判断；本目录的补丁只回补这个判断，保留原有信号、调度与错误返回语义。
- [PHP 8.1.32 mysqlnd](https://github.com/php/php-src/blob/php-8.1.32/ext/mysqlnd/mysqlnd_connection.c) 的 `send_close` 说明 COM_QUIT 析构路径。

`Swoole\Process::signal()` 在普通 Worker 中会拒绝覆盖系统注册的 SIGPIPE；pcntl 屏蔽也未阻止 kqueue 递送。扩大 PHP task worker guard 的实验已撤回。修复应落在原生扩展，不能通过吞 Notice、调大超时或 inflight 回补解决。

影响：只改变 kqueue 对 SIG_IGN 的处理，不增加定时器、协程、重试或业务状态；实际断管写入仍失败，普通 callable 信号仍执行。补丁不解决 MySQL 连接为何断开，也不解决事务行锁问题。当前系统扩展与生产进程均未替换。

## 隔离验证

`build-isolated.sh` 从官方 v5.1.8 源码构建回补扩展，生成到本目录被忽略的 `build/` 中。脚本不执行 `make install`，也不启动、重启或停止任何既有服务。

```sh
cd /Users/lanhai/projects/swoole/scf
bash tools/swoole-sigpipe/build-isolated.sh
python3 tools/swoole-sigpipe/verify-isolated.py
```

event worker 回归通过关闭 socket 对端后写入 5 字节，验证：写操作返回 false/Broken pipe；原 Worker 的 PID 保持不变；SIGUSR1 的 callable 仍执行。原系统 Swoole 5.1.8 已复现 signal=10；回补扩展的 event worker 和既有 task worker 回归均通过，结果与 SHA-256 记录在 `validation.txt`。验证器为每个测试使用独立进程组，以免原生 Server 关闭过程的信号波及运行它的 shell。

本地模块只用于验证根因。生产更新须在生产主机匹配现有 PHP ABI、编译选项及库依赖重新构建，并保留旧扩展作为回滚文件；本次没有把本地模块当作生产可直接安装的制品。

## 行锁超时：独立故障，尚未锁定当时的阻塞事务

10 月 9 日 15:46:48–15:47:44 共 12 次 SQL 1205，均来自 `actionTaskQueryClaim → claim → transactional → lockAdmin → lockTask → SELECT t_kuake_task ... FOR UPDATE`，涉及任务 129/130。两任务各有 20,000 条 Query。任务行锁用于串行化同任务的领取、提交和回收操作，因此任何较长的持锁事务会阻塞同任务领取。

只读检查时 `innodb_lock_wait_timeout=50`，无当前行锁等待，未见持锁不释放的历史事务。慢日志关闭，`events_statements_history_long` 没有记录，无法反查昨日持锁者。对任务 130 的当前候选查询设 1.5 秒执行上限后读取：二审驳回分支约 74.67ms，普通队列约 20.53ms。二审分支有 index_merge/filesort，但当前测量不足以证明它导致了昨日超过 50 秒的阻塞。

未修改领取的事务/锁语义或生产数据库。下次发生时需要只读捕获 `innodb_trx + data_lock_waits + data_locks + processlist`，对应阻塞连接与实际 SQL 后再确定修复点。生产生效需要另外授权更新 Swoole 扩展并重启使用该扩展的应用进程；SCF 应用/框架包发布不会替换 PHP 原生扩展。
