# SCF

Swoole Cloud Framework（SCF）命令与启动说明文档。  
本文基于当前 `bin` 与 `src/Command` 实现整理，重点覆盖：

- 各模块启动方式
- 参数格式与默认行为
- 应用初始化 / 安装 / 发布流程

## 1. 运行入口与参数约定

### 1.1 建议运行位置

多数脚本使用相对路径 `php ../boot ...`，建议在 `scf/bin` 目录执行：

```bash
cd <scf_root>/bin
```

### 1.2 参数格式

- 基本格式：`./<script> <action> -key=value -flag`
- `action` 为位置参数（如 `start` / `stop` / `release`）
- 选项建议统一写成 `-key=value`
- 布尔开关直接写 `-d`、`-force` 这类无值参数
- 查看帮助：`./<script> -h` 或 `php ../boot <command> -h`

## 2. bin 目录入口总览

| 脚本 | 实际转发 | 说明 |
| --- | --- | --- |
| `./server` | `boot gateway ...` | 固定走 Gateway 启动链路 |
| `./gateway` | `boot gateway ...` | 无参数默认 `start`；支持 `start/stop/reload/restart/status` 快捷 |
| `./crontab` | `boot crontab ...` | 一次性任务入口；`log` 默认注入 `-nopack`（未显式指定 pack 时） |
| `./build` | `boot build ...` | 应用打包/发布/回滚 |
| `./install` | `boot install ...` | 应用创建/拉取/列表 |
| `./framework` | `boot framework ...` | 框架打包/推送/版本查询 |
| `./native` | `boot native ...` | 桌面应用相关 |
| `./toolbox` | `boot toolbox ...` | 工具箱 |
| `./cli` | `boot toolbox cli ...` | `toolbox cli` 快捷入口 |
| `./ar` | `boot toolbox ar ...` | `toolbox ar` 快捷入口 |

## 3. Command 模块说明（src/Command）

当前 Runner 注册的主命令：

- `server`
- `gateway`
- `gateway_upstream`
- `crontab`
- `install`
- `build`
- `framework`
- `native`
- `toolbox`
- `run`
- `bot`

### 3.1 server（HTTP 服务管理）

常用 action：

- `start`
- `stop`
- `reload`
- `restart`
- `status`

常用参数：

- `-app=<应用目录>`
- `-env=<dev|production>`
- `-role=<master|slave>`
- `-port=<端口>`
- `-d`（守护进程）
- `-force`（强制停止）

示例：

```bash
./server start -app=example_app -env=dev -role=master
./server stop -app=example_app -role=master
./server reload -app=example_app -role=master
```

说明：`./server` 现在固定走 `gateway` 命令链路。

### 3.2 gateway（代理网关）

action：

- `start`
- `stop`
- `reload`
- `restart`
- `restart_crontab`
- `restart_redisqueue`
- `status`

常用参数：

- `-app`
- `-env`
- `-role`
- `-host`
- `-port`
- `-rpc_port`
- `-control_port`
- `-upstream_host`
- `-upstream_port`
- `-upstream_rpc_port`
- `-spawn_upstream`
- `-reuse_upstream`

示例：

```bash
./gateway start -app=example_app -env=dev -role=master -port=9502
./gateway stop -app=example_app -role=master
./server restart_crontab -app=example_app -role=master
```

说明：`bin/gateway` 快捷脚本只对 `start/stop/reload/restart/status` 做了首参识别；`restart_crontab`、`restart_redisqueue` 建议使用 `./server ...` 或 `php ../boot gateway ...` 直接调用。

### 3.3 gateway_upstream（代理托管业务实例，内部命令）

action：

- `start`

参数：

- `-app`
- `-env`
- `-role`
- `-port`
- `-rport`

示例：

```bash
php ../boot gateway_upstream start -app=example_app -env=dev -port=19502 -rport=19602
```

### 3.4 crontab（一次性定时任务）

action：

- `list`（列出可执行任务）
- `log`（查看统一日志）
- `<classname>`（执行任务）

参数：

- `-app`
- `-env`
- `-role`
- `-namespace=<完整类名>`（优先级高于 classname）
- `-entry_id=<Linux crontab 条目ID>`
- `-n=<行数>` / `-lines=<行数>`
- `-f` / `-follow=1|0`
- `-clear`

示例：

```bash
./crontab list -app=example_app -env=dev -role=master
./crontab log -app=example_app -n=200 -f
./crontab TaskClass -app=example_app -env=dev -role=master
php ../boot crontab -app=example_app -entry_id=123
```

### 3.5 install（应用初始化/安装）

action：

- `create`（创建新应用）
- `pull`（从云端拉取已发布应用）
- `list`（查看本地应用）

参数：

- `-appid`
- `-path`
- `-key`
- `-server`

示例：

```bash
./install create -path=example_app -appid=SCF_EXAMPLE_APPID -key=your_app_auth_key
./install pull -path=example_app
./install pull -path=example_app -key=your_install_key -server=https://xx/upload/scfapp/example_app-version.json
./install list
```

### 3.6 build（应用发布）

action：

- `release`（默认 action，不写也会进入）
- `rollback`
- `history`

参数：

- `-app=<应用目录>`
- `-file=<build目录配置名，不含.json>`
- `-version=<回滚目标版本>`

示例：

```bash
./build release -app=example_app
./build -app=example_app
./build release -app=example_app -file=buildfile
./build rollback -app=example_app -version=1.2.3
./build history -app=example_app
```

说明：

- `release` 交互模式会选择打包类型：
  - `1` 源码打包
  - `2` 静态资源打包
  - `3` 全部打包
- 发布成功后会更新 `build/<APP_ID>-version.json` 并输出安装秘钥。

### 3.7 framework（框架发布）

action：

- `build`（打包并上传框架包）
- `push`（推送代码与 tag）
- `dashboard`（打包上传 dashboard 静态包）
- `version`（查询最新 tag 版本）

示例：

```bash
./framework build
./framework push
./framework version
./framework dashboard
```

### 3.8 toolbox（工具箱）

action：

- `cli`
- `rpc`
- `ar`
- `info`
- `trans`
- `logs`
- `nodes`
- `config`

示例：

```bash
./toolbox info -app=example_app
./cli -app=example_app
./ar -app=example_app
```

### 3.9 run（脚本执行）

参数：

- `{path}`：应用目录下脚本路径（不含 `.php` 后缀，建议以 `/` 开头）

示例：

```bash
php ../boot run /scripts/demo -app=example_app
```

### 3.10 bot（机器人）

action：

- `run`

参数：

- `-app`

## 4. 应用初始化 / 安装 / 启动（推荐流程）

### 4.1 创建新应用（本地初始化）

```bash
cd <scf_root>/bin
./install create -path=example_app -appid=SCF_EXAMPLE_APPID -key=your_app_auth_key
```

不传参数时会进入交互输入。

### 4.2 拉取已有应用（云端安装）

```bash
./install pull -path=example_app
```

会要求输入安装秘钥；也可显式传：

```bash
./install pull -path=example_app -key=your_install_key -server=https://your-version-json-url
```

### 4.3 启动应用（Gateway 模式）

```bash
./server start -app=example_app -env=dev -role=master
```

## 5. 应用发布流程

### 5.1 交互发布

```bash
./build release -app=example_app
```

流程包括：

- 选择打包类型（源码/静态资源/全部）
- 填写版本号与版本说明
- 上传包到 OSS
- 更新版本引导文件
- 生成安装秘钥

### 5.2 配置文件发布

在 `build` 目录准备 `buildfile.json`（示例）：

```json
{
  "type": 3,
  "version": "1.2.3",
  "remark": "release note"
}
```

执行：

```bash
./build release -app=example_app -file=buildfile
```

### 5.3 回滚与历史

```bash
./build rollback -app=example_app -version=1.2.2
./build history -app=example_app
```

## 6. 常用快速命令

```bash
# 查看某命令帮助
php ../boot server -h
php ../boot gateway -h
php ../boot crontab -h

# 查看本地应用
./install list

# 启动/停止
./server start -app=example_app -env=dev -role=master
./server stop -app=example_app -role=master

# 查看一次性任务日志
./crontab log -app=example_app -n=200 -f
```

## 7. 备注

- 当前命令体系由 `src/Command/Runner.php` 注册，`boot` 负责初始化和分发。
- `server/gateway` 属于进程循环管理链路，建议始终显式传 `-app`、`-env`、`-role`，减少默认值差异带来的歧义。

## 8. 部署环境变量（系统级）

本节为“应用部署时可配置的系统环境变量”说明，基于当前 `boot` 与 `Env::initialize` 真实实现整理。

### 8.1 变量总表

| 变量名 | 用途 | 默认值 | 常见取值 |
| --- | --- | --- | --- |
| `APP_DIR` | 默认应用目录（未传 `-app` 时生效） | `app` | `example_app` |
| `APP_ENV` | 默认运行环境（未传 `-env` 且未传 `-dev` 时生效） | `production` | `dev` / `production` |
| `SERVER_ROLE` | 默认节点角色（未传 `-role` 时生效） | `master` | `master` / `slave` |
| `APP_SRC` | 应用源码模式默认值 | 自动推导 | `dir` / `phar` |
| `HOST_IP` | 容器/组网场景内网 IP（优先用于 `SERVER_HOST`） | 自动探测 | `172.17.x.x` 等 |
| `NETWORK_MODE` | 网络模式开关（影响 IP 获取逻辑） | 空 | `single` / `group` |
| `SCF_UPDATE_SERVER` | 框架更新/版本引导地址 | `https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/scf/version.json` | 自建版本服务 URL |
| `TZ` | 时区（boot 与 gateway 入口读取） | `Asia/Shanghai` | `Asia/Shanghai` |
| `SCF_FROM_CRON` | 标记 Linux crontab 运行态（一般由系统任务自动注入） | 空 | `1` |
| `SCF_PHP_BIN` | `bin/crontab` 使用的 PHP 可执行路径 | 自动探测 | `/usr/bin/php` 等 |
| `STATIC_HANDLER` | 启动期保留字段（已采集但当前无直接逻辑分支） | 空 | 自定义 |
| `OS_NAME` | 启动期保留字段（已采集但当前无直接逻辑分支） | 空 | 自定义 |

### 8.2 参数优先级（很关键）

大多数启动参数优先级为：

`命令行参数 > 环境变量 > 框架默认值`

例如：

- `-app` 优先于 `APP_DIR`
- `-env` / `-dev` 优先于 `APP_ENV`
- `-role` 优先于 `SERVER_ROLE`

但 `APP_SRC` 存在特殊优先级（见 `Env::initialize`）：

`-phar / -dir > APP_SRC > -src > 自动推导`

也就是说，设置了 `APP_SRC` 时，会覆盖 `-src` 的默认选择逻辑。

### 8.3 各变量的实际影响范围

1. `APP_DIR`

- 决定默认应用目录（`APP_PATH`、日志目录、PID 文件命名）
- `server/gateway` 的 stop/restart 控制标记文件也依赖它

2. `APP_ENV`

- 影响 `SERVER_RUN_ENV`（`dev`/`production`）
- 进而影响默认源码模式推导（生产偏向 `phar`，开发偏向 `dir`）

3. `SERVER_ROLE`

- 影响 master/slave 角色行为
- 影响 PID 文件、节点标识、gateway 进程识别

4. `APP_SRC`

- 指定应用运行源码类型（`dir` 或 `phar`）
- 常用于强制部署模式，避免自动推导误判

5. `HOST_IP` + `NETWORK_MODE`

- 在 Docker 或 `NETWORK_MODE=group` 时优先用于内网地址
- `SCF_FROM_CRON=1` 时若未配置 `HOST_IP`，会退化到本机地址/`127.0.0.1`，避免 cron 阻塞

6. `SCF_UPDATE_SERVER`

- 用于框架自愈下载与版本检查
- 也会被 dashboard/升级链路读取

7. `TZ`

- 影响启动期与 gateway/direct-upstream 运行时的时区

8. `SCF_FROM_CRON` / `SCF_PHP_BIN`

- `SCF_FROM_CRON`：标记一次性任务来自系统 cron
- `SCF_PHP_BIN`：`bin/crontab` 找不到 php 时可显式指定；LinuxCrontabManager 也会自动注入

### 8.4 部署推荐（systemd / Docker / Crontab）

1. systemd（推荐显式设置）

```ini
[Service]
Type=simple
WorkingDirectory=<scf_root>/bin
Environment=APP_DIR=example_app
Environment=APP_ENV=production
Environment=SERVER_ROLE=master
Environment=APP_SRC=phar
Environment=TZ=Asia/Shanghai
Environment=NETWORK_MODE=single
ExecStart=/bin/bash <scf_root>/bin/server start -app=example_app -env=production -role=master
ExecStop=/bin/bash <scf_root>/bin/server stop -app=example_app -role=master
Restart=always
```

2. Docker（建议最少变量）

```bash
export APP_DIR=example_app
export APP_ENV=production
export SERVER_ROLE=master
export APP_SRC=phar
export TZ=Asia/Shanghai
export NETWORK_MODE=group
export HOST_IP=172.17.0.10
/bin/bash <scf_root>/bin/server start -app=example_app -env=production -role=master
```

3. Linux Crontab（一次性任务）

统一使用 `scf/bin/crontab`，例如：

```bash
SCF_FROM_CRON=1 SCF_PHP_BIN=/usr/bin/php /bin/bash <scf_root>/bin/crontab \
  TaskClass -app=example_app -env=production -role=master
```

### 8.5 线上实践建议

- 线上务必显式设置：`APP_DIR`、`APP_ENV`、`SERVER_ROLE`、`APP_SRC`、`TZ`
- 生产部署建议显式传参：`-app`、`-env`、`-role`，不要只依赖环境变量
- 如果使用私有更新源，务必设置 `SCF_UPDATE_SERVER`
- `STATIC_HANDLER`、`OS_NAME` 当前版本属于保留输入，建议仅在有定制代码时使用
