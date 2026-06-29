# 运行时安全检查锚点

当你要改生命周期或控制流时，先看这些入口。

## 优先起点

- `/Users/lanhai/projects/swoole/scf/src/Core`
- `/Users/lanhai/projects/swoole/scf/src/App`
- `/Users/lanhai/projects/swoole/scf/src/Mode`

## 现有控制面入口

- `/Users/lanhai/projects/swoole/scf/src/Command/DefaultCommand/Gateway.php`

这个文件已经负责这些真实控制动作：

- `reload`
- `restart`
- `restart_crontab`
- `restart_redisqueue`

在发明新生命周期路径之前，先把它当作现有控制 API 来理解。
