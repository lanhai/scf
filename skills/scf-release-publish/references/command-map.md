# SCF 发布命令地图

当你要执行应用发布时，先看这个文件。

## 真实命令入口

- `/Users/lanhai/projects/swoole/scf/bin/build`

这个脚本实际会转发到：

- `php ../boot build ...`

## Build 命令实现

- `/Users/lanhai/projects/swoole/scf/src/Command/DefaultCommand/Build.php`

代码里已确认的关键点：

- 命令名是 `build`
- 默认 action 是 `release`
- 交互式 build type 选项是：
  - `1` 源码打包
  - `2` 静态资源文件打包
  - `3` 全部打包

## 项目规则叠加

发布任意 SCF app 时：

- 如果 CP 构建产物有变化，使用 `build_type=3`
- 如果只有后端代码变化，使用 `build_type=1`

并且必须：

- 在 `/Users/lanhai/projects/swoole/scf/bin` 下执行
- 使用 `./build release ...`
