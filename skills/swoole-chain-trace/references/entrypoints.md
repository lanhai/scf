# Swoole 工作区入口索引

当任务需要从真实代码入口开始追一条完整链路时，优先看这个文件。

## CP 功能链路

1. 前端页面与请求入口
   - `/Users/lanhai/projects/vue-admin/<frontend-app>/src/views`
   - `/Users/lanhai/projects/vue-admin/<frontend-app>/src/api`
2. 后端路由与菜单输出入口
   - `/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin`
   - 在目标 app 内查 `actionRoutes`、`routes`、`menu` 或等价聚合入口
3. 后端 Controller 执行入口
   - `/Users/lanhai/projects/swoole/apps/<app>/src/lib/Admin/Controller`

## App 业务链路

1. 业务调用方
   - `/Users/lanhai/projects/swoole/apps/<app>/src/lib`
2. 组件、网关或服务调用层
   - 先在目标 app 内查组件名、service/action、队列名、socket 事件或请求封装
3. 外部服务接收层
   - 按实际链路进入对应工程，例如 crawler、dashboard 或其他服务工程

## Dashboard 节点运维链路

1. Dashboard 页面入口
   - `/Users/lanhai/projects/vue-admin/dashboard/src/views`
2. 后端运行时/控制面入口
   - `/Users/lanhai/projects/swoole/scf/src/Command/DefaultCommand/Gateway.php`
   - `/Users/lanhai/projects/swoole/scf/src/Server/Gateway`

## 发布链路

1. 应用发布命令入口
   - `/Users/lanhai/projects/swoole/scf/bin/build`
   - `/Users/lanhai/projects/swoole/scf/src/Command/DefaultCommand/Build.php`
2. 框架发布命令入口
   - `/Users/lanhai/projects/swoole/scf/src/Command/DefaultCommand/Framework.php`
3. 最终镜像同步/构建脚本
   - `/Users/lanhai/projects/swoole/etc/build_main.sh`
