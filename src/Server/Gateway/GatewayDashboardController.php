<?php

namespace Scf\Server\Gateway;

use Scf\Core\Result;
use Scf\Mode\Web\Request;
use Scf\Server\Controller\DashboardController;
use Scf\Server\LinuxCrontab\LinuxCrontabManager;
use Throwable;

/**
 * Gateway 侧的 dashboard 适配控制器。
 *
 * 该类只负责把 dashboard 请求转换为 GatewayServer 的编排调用，不承载
 * 业务状态管理，也不直接操作 upstream 生命周期，职责边界保持在控制面转发层。
 */
class GatewayDashboardController extends DashboardController {

    /**
     * 注入 gateway 实例，便于复用其控制面编排逻辑。
     *
     * @param GatewayServer $gateway gateway 控制器和编排能力提供者
     * @return void
     */
    public function __construct(
        protected GatewayServer $gateway
    ) {
    }

    /**
     * 补齐 dashboard 会话信息，供基类在后续 action 中复用。
     *
     * @param string|null $token 当前 dashboard 会话 token
     * @param string $currentUser 当前登录用户名，默认 system
     * @return static 便于链式继续调用控制器动作
     */
    public function hydrateDashboardSession(?string $token, string $currentUser = 'system'): static {
        $this->token = (string)$token;
        $this->currentUser = $currentUser;
        $this->loginUser['token'] = $token;
        return $this;
    }

    /**
     * dashboard 节点命令入口。
     *
     * @return Result 节点命令执行结果
     */
    public function actionCommand(): Result {
        Request::post([
            'command' => Request\Validator::required('命令不能为空'),
            'host' => Request\Validator::required('节点不能为空'),
            'params'
        ])->assign($command, $host, $params);

        return $this->gateway->dashboardCommand((string)$command, (string)$host, (array)($params ?: []));
    }

    /**
     * dashboard 节点列表接口。
     *
     * @return Result 节点列表返回结果
     */
    public function actionNodes(): Result {
        return Result::success($this->gateway->dashboardNodes());
    }

    /**
     * slave 指令历史分页查询入口。
     *
     * @return Result
     */
    public function actionCommandHistory(): Result {
        Request::get([
            'page',
            'size',
            'host',
            'state',
        ])->assign($page, $size, $host, $state);
        return Result::success(
            $this->gateway->dashboardCommandHistory(
                (int)($page ?: 1),
                (int)($size ?: 20),
                (string)($host ?: ''),
                (string)($state ?: '')
            )
        );
    }

    /**
     * dashboard 服务器状态接口，包含 socket 地址和运行态摘要。
     *
     * @return Result 服务器状态返回结果
     */
    public function actionServer(): Result {
        Request::get([
            'refresh_versions',
        ])->assign($refreshVersions);
        $forceRefreshVersions = in_array(strtolower(trim((string)$refreshVersions)), ['1', 'true', 'yes'], true);
        return Result::success(
            $this->gateway->dashboardServerStatus(
                $this->token,
                (string)(Request::header('host') ?: ''),
                (string)(Request::header('referer') ?: ''),
                '',
                $forceRefreshVersions
            )
        );
    }

    /**
     * dashboard 版本升级入口。
     *
     * @return Result 版本升级执行结果
     */
    public function actionUpdate(): Result {
        Request::post([
            'type' => Request\Validator::required('更新类型错误'),
            'version' => Request\Validator::required('版本号不能为空')
        ])->assign($type, $version);

        return $this->gateway->dashboardUpdate((string)$type, (string)$version);
    }

    /**
     * dashboard 安装入口。
     *
     * worker 只负责收取安装秘钥并把请求转给 UpstreamSupervisor；
     * 真正的安装、更新包下载和业务实例拉起都在 UpstreamSupervisor 子进程里完成。
     *
     * @return Result
     */
    public function actionInstall(): Result {
        Request::post([
            'key' => Request\Validator::required('安装秘钥不能为空'),
            'role' => 'master',
        ])->assign($key, $role);

        return $this->gateway->dashboardInstall((string)$key, (string)$role);
    }

    /**
     * dashboard 排程视图入口。
     *
     * @return Result 排程视图数据
     */
    public function actionCrontabs(): Result {
        return Result::success($this->gateway->dashboardCrontabs());
    }

    /**
     * 保存 Linux 排程，并在成功后广播到 slave 节点。
     *
     * @return Result
     */
    public function actionLinuxCrontabSave(): Result {
        Request::post()->pack($data);
        try {
            $manager = new LinuxCrontabManager();
            $entry = $manager->save($data, (int)($data['sync_local'] ?? 0) === 1);
            return Result::success([
                'entry' => $entry,
                'replication' => $this->gateway->replicateLinuxCrontabConfigToSlaveNodes(),
            ]);
        } catch (Throwable $throwable) {
            return Result::error($throwable->getMessage());
        }
    }

    /**
     * gateway 模式下的 Linux 排程页还需要感知在线 slave 节点。
     *
     * 这样前端在配置 `role=slave` 的排程时，可以直接弹出节点管理视图，
     * 查看当前有哪些 slave 已经收到并安装了该任务。
     *
     * @return Result
     */
    public function actionLinuxCrontabs(): Result {
        try {
            $manager = new LinuxCrontabManager();
            $overview = $manager->overview();
            $overview['nodes'] = $this->gateway->dashboardNodes();
            return Result::success($overview);
        } catch (Throwable $throwable) {
            return Result::error($throwable->getMessage());
        }
    }

    /**
     * 删除 Linux 排程，并在成功后广播到 slave 节点。
     *
     * @return Result
     */
    public function actionLinuxCrontabDelete(): Result {
        Request::post([
            'id' => Request\Validator::required('排程 ID 不能为空'),
        ])->assign($id);
        try {
            $manager = new LinuxCrontabManager();
            $result = $manager->delete((string)$id);
            return Result::success([
                'result' => $result,
                'replication' => $this->gateway->replicateLinuxCrontabConfigToSlaveNodes(),
            ]);
        } catch (Throwable $throwable) {
            return Result::error($throwable->getMessage());
        }
    }

    /**
     * 切换 Linux 排程启用状态，并在成功后广播到 slave 节点。
     *
     * @return Result
     */
    public function actionLinuxCrontabSetEnabled(): Result {
        Request::post([
            'id' => Request\Validator::required('排程 ID 不能为空'),
            'enabled' => Request\Validator::required('启用状态不能为空'),
        ])->assign($id, $enabled);
        try {
            $manager = new LinuxCrontabManager();
            $result = $manager->setEnabled((string)$id, (int)$enabled === 1);
            return Result::success([
                'result' => $result,
                'replication' => $this->gateway->replicateLinuxCrontabConfigToSlaveNodes(),
            ]);
        } catch (Throwable $throwable) {
            return Result::error($throwable->getMessage());
        }
    }

    /**
     * 重新同步 Linux 排程，并广播最新配置到 slave 节点。
     *
     * @return Result
     */
    public function actionLinuxCrontabSync(): Result {
        try {
            $manager = new LinuxCrontabManager();
            $result = $manager->sync();
            return Result::success([
                'result' => $result,
                'replication' => $this->gateway->replicateLinuxCrontabConfigToSlaveNodes(),
            ]);
        } catch (Throwable $throwable) {
            return Result::error($throwable->getMessage());
        }
    }
}
