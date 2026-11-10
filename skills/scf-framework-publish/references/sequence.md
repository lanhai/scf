# 框架发布顺序说明

当用户要求“发布框架”时，先看这个文件。

## 真实命令归属

- 框架命令实现：
  `/Users/lanhai/projects/swoole/scf/src/Command/DefaultCommand/Framework.php`
- 最终同步/构建脚本：
  `/Users/lanhai/projects/swoole/etc/build_main_fast.sh`（仅更新框架）或 `/Users/lanhai/projects/swoole/etc/build_main.sh`（更新运行环境）

## 固定顺序

1. `framework build`
2. `framework push`
3. 按变更范围选择上面的镜像脚本。将刚推送的 Composer 标签（去掉 `v` 前缀）通过 `SCF_FRAMEWORK_VERSION` 传给脚本。

## 代码里能确认的点

- `Framework::help()` 已明确暴露 `build` 和 `push`
- 两种模式都更新 release 工作区、刷新框架文件并构建推送 Docker 运行镜像。fast 模式要求基础镜像已经存在，直接复用该镜像。

不要更改这三步的顺序。
