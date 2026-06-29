# 框架发布顺序说明

当用户要求“发布框架”时，先看这个文件。

## 真实命令归属

- 框架命令实现：
  `/Users/lanhai/projects/swoole/scf/src/Command/DefaultCommand/Framework.php`
- 最终同步/构建脚本：
  `/Users/lanhai/projects/swoole/etc/build_main.sh`

## 固定顺序

1. `framework build`
2. `framework push`
3. `/Users/lanhai/projects/swoole/etc/build_main.sh`

## 代码里能确认的点

- `Framework::help()` 已明确暴露 `build` 和 `push`
- `build_main.sh` 会更新 release 工作区、刷新框架文件并构建推送 docker 镜像

不要更改这三步的顺序。
