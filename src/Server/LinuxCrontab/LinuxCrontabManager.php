<?php

namespace Scf\Server\LinuxCrontab;

use Scf\Core\App;
use Scf\Core\Config;
use Scf\Core\Exception;
use Scf\Util\Date;
use Scf\Util\Dir;
use Scf\Util\File;

/**
 * Linux crontab 独立管理器。
 *
 * 该组件负责维护“应用配置里的计划任务定义”与“当前系统用户的 crontab 条目”之间的映射，
 * 服务于 dashboard 上独立的 Linux 排程管理页面。它与现有常驻式 CrontabManager
 * 完全隔离，只复用应用里已有的任务脚本发现能力，最终通过 `scf/bin/crontab`
 * 调度一次性启动器来执行目标脚本。
 */
class LinuxCrontabManager {
    /**
     * 当前应用在系统 crontab 中的标记前缀。
     */
    private const SYSTEM_MARKER_PREFIX = 'SCF_LINUX_CRONTAB';

    /**
     * 配置文件名。
     */
    private const CONFIG_FILE_NAME = 'crontabs.json';

    /**
     * 旧版配置文件所在目录。
     *
     * 该路径只用于兼容此前误放到 `src/config` 的排程数据，
     * 新逻辑会统一迁移到应用 `db` 目录，避免被发布流程打进源码包。
     */
    private const LEGACY_CONFIG_DIRECTORY = '/config/';

    /**
     * 当前正式使用的排程存储目录。
     *
     * Linux 排程属于节点运行态配置，不应该跟随源码配置一起发布，
     * 因此统一落到应用根目录下的 `db` 目录。
     */
    private const STORAGE_DIRECTORY = '/db/';

    /**
     * Linux crontab 执行日志目录（相对 APP_LOG_PATH）。
     */
    private const CRONTAB_LOG_SUB_DIRECTORY = '/crontab';

    /**
     * Linux crontab 执行日志文件名。
     */
    private const CRONTAB_LOG_FILE_NAME = 'inst.log';

    /**
     * 一次性 crontab 命令最大执行时长（秒）。
     */
    private const COMMAND_TIMEOUT_SECONDS = 180;

    /**
     * 超时后额外等待再强制终止的宽限时间（秒）。
     */
    private const COMMAND_TIMEOUT_KILL_AFTER_SECONDS = 5;

    /**
     * 支持的调度类型。
     *
     * - interval: 每 N 分钟执行一次
     * - daily: 每天指定的多个时间点执行
     * - weekly: 每周指定星期几 + 多个时间点执行
     */
    private const SCHEDULE_TYPES = ['interval', 'daily', 'weekly'];

    /**
     * `flock` 命令解析结果缓存。
     *
     * Linux 排程页会频繁生成预览命令和摘要说明；这里缓存一次系统探测结果，
     * 避免同一轮请求里反复执行 `command -v flock`。
     *
     * @var string|null
     */
    protected ?string $flockPath = null;

    /**
     * `timeout` 命令解析结果缓存。
     *
     * @var string|null
     */
    protected ?string $timeoutPath = null;

    /**
     * 返回 dashboard 页面初始化所需的全部数据。
     *
     * @return array
     * @throws Exception
     */
    public function overview(): array {
        $config = $this->readConfig();
        $systemCrontab = $this->readSystemCrontab();
        $flockAvailable = $this->isFlockAvailable();
        if (empty($config['items']) && $systemCrontab !== '') {
            $recoveredItems = $this->recoverEntriesFromSystemCrontab($systemCrontab);
            if ($recoveredItems) {
                $config = ['items' => $recoveredItems];
                self::persistConfigPayload([
                    'items' => $recoveredItems,
                    'updated_at' => time(),
                ]);
            }
        }
        $installedLineCount = $this->extractInstalledLineCount($systemCrontab);
        $items = [];

        foreach (($config['items'] ?? []) as $item) {
            $normalized = $this->normalizeEntry($item, false);
            $localApplicable = $this->shouldInstallOnCurrentNode($normalized);
            $lockRequested = $this->isLockRequested($normalized);
            $expectedLineCount = $localApplicable && (int)($normalized['enabled'] ?? 0) === 1
                ? count($this->buildCronLines($normalized))
                : 0;
            $actualLineCount = $localApplicable ? (int)($installedLineCount[$normalized['id']] ?? 0) : 0;
            $normalized['cron_expression'] = $this->buildCronExpressionPreview($normalized);
            $normalized['command_preview'] = $this->buildCommand($normalized);
            $normalized['local_applicable'] = $localApplicable;
            $normalized['lock_requested'] = $lockRequested;
            $normalized['lock_runtime_available'] = $localApplicable ? $flockAvailable : null;
            $normalized['lock_effective'] = $lockRequested && (!$localApplicable || $flockAvailable);
            $normalized['lock_warning'] = $this->lockWarningMessage($normalized, $localApplicable, $flockAvailable);
            $normalized['expected_line_count'] = $expectedLineCount;
            $normalized['actual_line_count'] = $actualLineCount;
            $normalized['system_installed'] = $localApplicable && $expectedLineCount > 0 && $actualLineCount === $expectedLineCount;
            $normalized['system_partial'] = $localApplicable && $actualLineCount > 0 && $actualLineCount < $expectedLineCount;
            $normalized['schedule_summary'] = $this->buildScheduleSummary($normalized);
            $items[] = $normalized;
        }

        usort($items, static function (array $left, array $right): int {
            return (int)($right['updated_at'] ?? 0) <=> (int)($left['updated_at'] ?? 0);
        });

        return [
            'app' => APP_DIR_NAME,
            'env' => SERVER_RUN_ENV,
            'default_env' => $this->defaultEnv(),
            'role' => SERVER_ROLE,
            'src_type' => APP_SRC_TYPE,
            'config_file' => $this->configFile(),
            'system_available' => $this->isSystemCrontabAvailable(),
            'flock_available' => $flockAvailable,
            'flock_path' => $this->resolveFlockPath(),
            'items' => $items,
            'available_tasks' => $this->availableTasks(),
            'env_options' => $this->envOptions(),
            'weekday_options' => $this->weekdayOptions(),
            'schedule_type_options' => [
                ['label' => '每隔 N 分钟', 'value' => 'interval'],
                ['label' => '每日定时', 'value' => 'daily'],
                ['label' => '每周定时', 'value' => 'weekly'],
            ],
        ];
    }

    /**
     * 从当前节点系统 crontab 中恢复托管排程条目。
     *
     * 当配置文件被清空、覆盖或迁移异常时，页面不能因为 `items` 为空就完全失去
     * 已安装排程的可见性。这里按当前 env/role 作用域扫描系统 crontab 里的托管行，
     * 把能明确反解出来的条目重新组装成配置结构，并回写到 db 存储。
     *
     * @param string $content
     * @return array<int, array<string, mixed>>
     */
    protected function recoverEntriesFromSystemCrontab(string $content): array {
        $grouped = [];
        $scopePattern = '/#\s+' . preg_quote($this->scopeMarker() . ':', '/') . '([a-z0-9_]+)/i';
        $legacyPattern = '/#\s+' . preg_quote(self::legacyMarkerPrefix() . ':', '/') . '([a-z0-9_]+)/i';

        foreach (preg_split('/\r?\n/', $content) as $line) {
            $line = trim((string)$line);
            if ($line === '' || str_starts_with($line, '#')) {
                continue;
            }

            if (preg_match($scopePattern, $line, $matches)) {
                $id = trim((string)($matches[1] ?? ''));
            } elseif (preg_match($legacyPattern, $line, $matches) && $this->lineTargetsCurrentNode($line)) {
                $id = trim((string)($matches[1] ?? ''));
            } else {
                continue;
            }

            if ($id === '') {
                continue;
            }
            $grouped[$id][] = $line;
        }

        $entries = [];
        foreach ($grouped as $id => $lines) {
            $entry = $this->recoverEntryFromCronLines($id, $lines);
            if ($entry !== null) {
                $entries[] = $entry;
            }
        }

        usort($entries, static function (array $left, array $right): int {
            return strcmp((string)($left['name'] ?? ''), (string)($right['name'] ?? ''));
        });

        return $entries;
    }

    /**
     * 从同一条排程的多条 cron 行里恢复配置定义。
     *
     * @param string $id
     * @param array<int, string> $lines
     * @return array<string, mixed>|null
     */
    protected function recoverEntryFromCronLines(string $id, array $lines): ?array {
        if (!$lines) {
            return null;
        }

        $namespace = '';
        $env = '';
        $role = '';
        $times = [];
        $weekdays = [];
        $isInterval = false;
        $intervalMinutes = 5;
        $lockEnabled = false;

        foreach ($lines as $line) {
            if ($namespace === '' && preg_match("/-namespace='([^']+)'/", $line, $matches)) {
                $namespace = trim((string)$matches[1]);
            }
            if ($env === '' && preg_match("/-env='([^']+)'/", $line, $matches)) {
                $env = trim((string)$matches[1]);
            }
            if ($role === '' && preg_match("/-role='([^']+)'/", $line, $matches)) {
                $role = trim((string)$matches[1]);
            }
            if (!$lockEnabled && str_contains($line, 'flock')) {
                $lockEnabled = true;
            }

            $expression = preg_replace('/\s+cd\s+.+$/', '', $line);
            $expression = trim((string)$expression);
            if ($expression === '') {
                continue;
            }
            $parts = preg_split('/\s+/', $expression);
            if (count($parts) < 5) {
                continue;
            }

            [$minute, $hour, , , $weekdayField] = array_slice($parts, 0, 5);
            $minute = trim((string)$minute);
            $hour = trim((string)$hour);
            $weekdayField = trim((string)$weekdayField);

            if (preg_match('/^\*\/(\d+)$/', $minute, $matches)) {
                $isInterval = true;
                $intervalMinutes = max(1, (int)$matches[1]);
            } elseif (is_numeric($minute) && is_numeric($hour)) {
                $times[] = str_pad((string)(int)$hour, 2, '0', STR_PAD_LEFT) . ':' . str_pad((string)(int)$minute, 2, '0', STR_PAD_LEFT);
            }

            if ($weekdayField !== '*' && $weekdayField !== '') {
                foreach (explode(',', $weekdayField) as $day) {
                    $day = trim($day);
                    if ($day === '' || !is_numeric($day)) {
                        continue;
                    }
                    $weekdays[] = (int)$day;
                }
            }
        }

        if ($namespace === '') {
            return null;
        }

        $times = array_values(array_unique($times));
        sort($times);
        $weekdays = array_values(array_unique($weekdays));
        sort($weekdays);

        $scheduleType = $isInterval ? 'interval' : (!empty($weekdays) ? 'weekly' : 'daily');

        return $this->normalizeEntry([
            'id' => $id,
            'name' => $this->shortClassName($namespace),
            'namespace' => $namespace,
            'schedule_type' => $scheduleType,
            'interval_minutes' => $intervalMinutes,
            'times' => $scheduleType === 'interval' ? [] : $times,
            'weekdays' => $weekdays,
            'lock_enabled' => $lockEnabled ? 1 : 0,
            'enabled' => 1,
            'env' => $env !== '' ? $env : $this->scopeEnv(),
            'role' => $role !== '' ? $role : SERVER_ROLE,
            'remark' => 'recovered_from_system_crontab',
        ], false);
    }

    /**
     * 返回当前节点本机系统 crontab 的只读快照。
     *
     * 该接口用于 dashboard 排错与人工核对，直接展示当前用户维度下
     * `crontab -l` 的完整内容，并额外附带当前应用托管条目数量等摘要信息。
     *
     * @return array<string, mixed>
     */
    public function installedSnapshot(): array {
        $content = $this->readSystemCrontab();
        $installedLineCount = $this->extractInstalledLineCount($content);
        $managedLineCount = array_sum($installedLineCount);

        return [
            'app' => APP_DIR_NAME,
            'env' => SERVER_RUN_ENV,
            'role' => SERVER_ROLE,
            'system_available' => $this->isSystemCrontabAvailable(),
            'has_content' => $content !== '',
            'managed_entry_count' => count($installedLineCount),
            'managed_line_count' => $managedLineCount,
            'content' => $content === '' ? "# 当前节点本机尚未安装任何 crontab 条目" : $content,
        ];
    }

    /**
     * 保存一条排程配置。
     *
     * 配置总是先落到应用 db 目录，但是否立刻改写“当前节点本机排程”由调用方控制。
     * 这样 master 节点可以仅维护 slave 任务定义，而不误改自己的系统 crontab。
     *
     * @param array $payload 前端提交的配置
     * @param bool $syncCurrentNode 是否允许本次保存同时同步当前节点本机排程
     * @return array
     * @throws Exception
     */
    public function save(array $payload, bool $syncCurrentNode = false): array {
        $config = $this->readConfig();
        $items = $config['items'] ?? [];
        $entry = $this->normalizeEntry($payload, true);
        $matched = false;
        $previousEntry = null;

        foreach ($items as $index => $item) {
            if (($item['id'] ?? '') !== $entry['id']) {
                continue;
            }
            $previousEntry = $this->normalizeEntry((array)$item, false);
            $entry['created_at'] = (int)($item['created_at'] ?? $entry['created_at']);
            $entry['last_run_at'] = (int)($item['last_run_at'] ?? 0);
            $entry['last_finish_at'] = (int)($item['last_finish_at'] ?? 0);
            $entry['last_run_status'] = trim((string)($item['last_run_status'] ?? ''));
            $entry['last_run_message'] = trim((string)($item['last_run_message'] ?? ''));
            $items[$index] = $entry;
            $matched = true;
            break;
        }

        if (!$matched) {
            $items[] = $entry;
        }

        if ($syncCurrentNode) {
            $this->assertEntryCanSyncWithLock($entry);
        }

        $this->writeConfig(['items' => array_values($items)]);
        if ($syncCurrentNode && $this->entryAffectsCurrentNode($entry, $previousEntry)) {
            $this->sync();
        }
        return $entry;
    }

    /**
     * 导出可复制到其他节点的排程配置。
     *
     * 该数据用于 master 通过 socket 下发到 slave，本身只描述排程定义，
     * 不依赖当前节点的系统 crontab 安装状态。
     *
     * @return array<string, mixed>
     */
    public function replicationPayload(?string $role = null): array {
        $config = $this->readConfig();
        if ($role === null || $role === '') {
            return $config;
        }

        $items = [];
        foreach (($config['items'] ?? []) as $item) {
            $entry = $this->normalizeEntry((array)$item, false);
            if ((string)($entry['role'] ?? '') !== $role) {
                continue;
            }
            $items[] = $entry;
        }

        return [
            'items' => $items,
        ];
    }

    /**
     * 将 master 下发的 Linux 排程配置应用到当前节点。
     *
     * slave 侧需要保留自己的运行态字段，因此这里会在覆盖配置定义时
     * 尽量保留本地 `last_run_*` 结果，再按当前节点 env/role 重新同步系统排程。
     *
     * @param array<string, mixed> $payload master 下发的配置包
     * @return array<string, mixed>
     * @throws Exception
     */
    public static function applyReplicationPayload(array $payload): array {
        $manager = new self();
        return $manager->applyReplicatedConfig($payload);
    }

    /**
     * 返回可直接挂到节点心跳 `tasks` 字段里的 Linux 排程状态。
     *
     * 节点页已经有现成的排程抽屉与日志入口，这里把 Linux 排程映射成旧任务表
     * 能识别的结构，尽量复用当前展示逻辑，而不再为节点详情页单独造一套列表协议。
     *
     * @return array<int, array<string, mixed>>
     */
    public static function nodeTasks(): array {
        $manager = new self();
        return $manager->buildNodeTasks();
    }

    /**
     * 记录某条 Linux 排程开始执行。
     *
     * 该方法由 `boot crontab` 一次性启动器在真正执行脚本前调用，
     * 让 dashboard 可以看到最近一次调度触发时间，而不是只看配置更新时间。
     *
     * @param string $entryId 排程条目 id
     * @return void
     */
    public static function markRunStarted(string $entryId): void {
        self::updateRuntimeState($entryId, [
            'last_run_at' => time(),
            'last_run_status' => 'running',
            'last_run_message' => '',
        ]);
    }

    /**
     * 记录某条 Linux 排程执行结束。
     *
     * @param string $entryId 排程条目 id
     * @param bool $success 本次执行是否成功
     * @param string $message 最近一次结果摘要
     * @return void
     */
    public static function markRunFinished(string $entryId, bool $success, string $message = ''): void {
        self::updateRuntimeState($entryId, [
            'last_finish_at' => time(),
            'last_run_status' => $success ? 'success' : 'failed',
            'last_run_message' => trim($message),
        ]);
    }

    /**
     * 根据 Linux 排程条目 id 解析对应的任务命名空间。
     *
     * 该方法用于系统 crontab 的“短命令”执行链路：排程行只保存 entry_id，
     * 真正执行时再从配置中回查 namespace，避免 crontab 行超长导致安装失败。
     *
     * @param string $entryId 排程条目 id
     * @return string 命名空间；未找到时返回空字符串
     */
    public static function resolveNamespaceByEntryId(string $entryId): string {
        $entryId = trim($entryId);
        if ($entryId === '') {
            return '';
        }

        $payload = self::readConfigPayloadFromFile(self::resolveReadableConfigFile());
        if (!is_array($payload)) {
            return '';
        }

        foreach (array_values((array)($payload['items'] ?? [])) as $item) {
            if ((string)($item['id'] ?? '') !== $entryId) {
                continue;
            }
            return '\\' . ltrim(trim((string)($item['namespace'] ?? '')), '\\');
        }

        return '';
    }

    /**
     * 更新某条排程的运行态字段。
     *
     * 运行记录是 Linux 系统 cron 回写回应用配置的唯一来源，因此这里采用
     * “读整个配置 -> 定位单条记录 -> 回写整个文件”的方式，避免和 dashboard
     * 页面读取逻辑分叉成两套存储。
     *
     * @param string $entryId 排程条目 id
     * @param array<string, mixed> $state 需要回写的运行态字段
     * @return void
     */
    protected static function updateRuntimeState(string $entryId, array $state): void {
        $entryId = trim($entryId);
        if ($entryId === '') {
            return;
        }

        $file = self::resolveReadableConfigFile();
        if (!is_file($file)) {
            return;
        }

        $json = File::readJson($file);
        if (!is_array($json)) {
            return;
        }

        $items = array_values(is_array($json['items'] ?? null) ? $json['items'] : $json);
        $updated = false;
        foreach ($items as &$item) {
            if ((string)($item['id'] ?? '') !== $entryId) {
                continue;
            }
            foreach ($state as $key => $value) {
                $item[$key] = $value;
            }
            $item['updated_at'] = (int)($item['updated_at'] ?? time());
            $updated = true;
            break;
        }
        unset($item);

        if (!$updated) {
            return;
        }

        $payload = [
            'items' => $items,
            'updated_at' => (int)($json['updated_at'] ?? time()),
        ];
        if (!self::persistConfigPayload($payload)) {
            return;
        }
    }

    /**
     * 删除一条排程配置，并从系统 crontab 中移除。
     *
     * @param string $id 排程 id
     * @return array
     * @throws Exception
     */
    public function delete(string $id): array {
        $id = trim($id);
        if ($id === '') {
            throw new Exception('排程 ID 不能为空');
        }

        $config = $this->readConfig();
        $deletedEntry = null;
        $items = array_values(array_filter(
            $config['items'] ?? [],
            function (array $item) use ($id, &$deletedEntry): bool {
                if ((string)($item['id'] ?? '') === $id) {
                    $deletedEntry = $this->normalizeEntry($item, false);
                    return false;
                }
                return true;
            }
        ));

        $this->writeConfig(['items' => $items]);
        $sync = [];
        if ($deletedEntry && $this->entryAffectsCurrentNode(null, $deletedEntry)) {
            $sync = $this->sync();
        }
        return ['id' => $id, 'sync' => $sync];
    }

    /**
     * 切换某条配置是否写入系统 crontab。
     *
     * @param string $id 排程 id
     * @param bool $enabled 是否启用
     * @return array
     * @throws Exception
     */
    public function setEnabled(string $id, bool $enabled): array {
        $config = $this->readConfig();
        $items = $config['items'] ?? [];
        $updated = null;
        $previousEntry = null;

        foreach ($items as &$item) {
            if ((string)($item['id'] ?? '') !== $id) {
                continue;
            }
            $previousEntry = $this->normalizeEntry((array)$item, false);
            $item['enabled'] = $enabled ? 1 : 0;
            $item['updated_at'] = time();
            $updated = $item;
            break;
        }

        if (!$updated) {
            throw new Exception('未找到目标排程配置');
        }

        $normalizedUpdated = $this->normalizeEntry((array)$updated, false);
        if ($enabled) {
            $this->assertEntryCanSyncWithLock($normalizedUpdated);
        }

        $this->writeConfig(['items' => array_values($items)]);
        if ($updated && $this->entryAffectsCurrentNode($normalizedUpdated, $previousEntry)) {
            $this->sync();
        }
        return $updated;
    }

    /**
     * 将当前应用的托管条目重新写入系统 crontab。
     *
     * 该方法只会替换当前应用自己托管的标记行，不会动用户 crontab 中的其他系统条目。
     *
     * @return array
     * @throws Exception
     */
    public function sync(): array {
        $config = $this->readConfig();
        $this->assertEntriesCanSyncWithLock((array)($config['items'] ?? []));
        $enabledCount = count(array_filter(
            $config['items'] ?? [],
            static fn(array $item): bool => (int)($item['enabled'] ?? 0) === 1
        ));

        if (!$this->isSystemCrontabAvailable()) {
            return [
                'managed_line_count' => 0,
                'enabled_count' => $enabledCount,
                'skipped' => true,
                'reason' => 'system_crontab_unavailable',
            ];
        }
        $managedLines = [];

        foreach (($config['items'] ?? []) as $item) {
            $normalized = $this->normalizeEntry($item, false);
            if ((int)($normalized['enabled'] ?? 0) !== 1 || !$this->shouldInstallOnCurrentNode($normalized)) {
                continue;
            }
            foreach ($this->buildCronLines($normalized) as $line) {
                $managedLines[] = $line;
            }
        }

        $current = $this->readSystemCrontab();
        $unmanagedLines = $this->stripManagedLines($current);
        $nextLines = $unmanagedLines;
        $scopeMarker = $this->scopeMarker();

        if ($managedLines) {
            if ($nextLines && trim((string)end($nextLines)) !== '') {
                $nextLines[] = '';
            }
            $nextLines[] = '# ' . $scopeMarker . ':BEGIN';
            foreach ($managedLines as $line) {
                $nextLines[] = $line;
            }
            $nextLines[] = '# ' . $scopeMarker . ':END';
        }

        $content = rtrim(implode(PHP_EOL, $nextLines));
        if ($content !== '') {
            $content .= PHP_EOL;
        }

        $this->writeSystemCrontab($content);
        return [
            'managed_line_count' => count($managedLines),
            'enabled_count' => $enabledCount,
        ];
    }

    /**
     * 返回当前应用可供 Linux 排程选择的任务脚本。
     *
     * 这里复用应用模块的任务发现规则，但不依赖常驻排程表；
     * dashboard 只需要“有哪些脚本可调度”，不需要读取现有运行态。
     *
     * @return array
     */
    public function availableTasks(): array {
        $items = [];
        $serverConfig = Config::server();

        if ($serverConfig['db_statistics_enable'] ?? false) {
            $items[] = [
                'name' => '统计数据入库',
                'namespace' => '\Scf\Database\Statistics\StatisticCrontab',
                'roles' => [NODE_ROLE_MASTER],
            ];
        }

        foreach ($this->loadCgiModules() as $module) {
            foreach (($module['crontabs'] ?? $module['background_tasks'] ?? []) as $task) {
                $items[] = [
                    'name' => (string)($task['name'] ?? $this->shortClassName((string)($task['namespace'] ?? ''))),
                    'namespace' => '\\' . ltrim((string)($task['namespace'] ?? ''), '\\'),
                    'roles' => [NODE_ROLE_MASTER, NODE_ROLE_SLAVE],
                ];
            }

            foreach (($module['master_crontabs'] ?? []) as $task) {
                $items[] = [
                    'name' => (string)($task['name'] ?? $this->shortClassName((string)($task['namespace'] ?? ''))),
                    'namespace' => '\\' . ltrim((string)($task['namespace'] ?? ''), '\\'),
                    'roles' => [NODE_ROLE_MASTER],
                ];
            }

            foreach (($module['slave_crontabs'] ?? []) as $task) {
                $items[] = [
                    'name' => (string)($task['name'] ?? $this->shortClassName((string)($task['namespace'] ?? ''))),
                    'namespace' => '\\' . ltrim((string)($task['namespace'] ?? ''), '\\'),
                    'roles' => [NODE_ROLE_SLAVE],
                ];
            }
        }

        $indexed = [];
        foreach ($items as $item) {
            if (($item['namespace'] ?? '') === '\\') {
                continue;
            }
            $namespace = (string)$item['namespace'];
            $roles = array_values(array_unique(array_filter(
                (array)($item['roles'] ?? []),
                static fn(string $role): bool => $role === NODE_ROLE_MASTER || $role === NODE_ROLE_SLAVE
            )));
            sort($roles);

            if (!isset($indexed[$namespace])) {
                $indexed[$namespace] = [
                    'name' => $item['name'],
                    'namespace' => $namespace,
                    'short_name' => $this->shortClassName($namespace),
                    'roles' => $roles ?: [NODE_ROLE_MASTER, NODE_ROLE_SLAVE],
                ];
                continue;
            }

            $indexed[$namespace]['roles'] = array_values(array_unique(array_merge(
                (array)($indexed[$namespace]['roles'] ?? []),
                $roles
            )));
            sort($indexed[$namespace]['roles']);
        }

        ksort($indexed);
        return array_values($indexed);
    }

    /**
     * 读取 CGI 模式下的模块配置，并在未预加载时主动兜底加载。
     *
     * dashboard 请求不一定命中过依赖模块列表的业务链路，
     * 因此这里不能假设 `App::getModules()` 一定已经有值。
     *
     * @return array
     */
    protected function loadCgiModules(): array {
        $srcLibPath = App::src() . '/lib';
        if (!is_dir($srcLibPath)) {
            return [];
        }

        $depth = (is_dir($srcLibPath . '/Controller') || is_dir($srcLibPath . '/Cli') || is_dir($srcLibPath . '/Crontab') || is_dir($srcLibPath . '/Rpc')) ? 3 : 2;
        $files = array_unique(Dir::scan($srcLibPath, $depth));
        $allowFiles = [
            'config.php',
            '_config.php',
            '_module_.php',
        ];
        $modules = [];

        foreach ($files as $file) {
            if (!in_array(basename($file), $allowFiles, true)) {
                continue;
            }

            $config = require $file;
            if (!is_array($config)) {
                continue;
            }

            $allowMode = $config['mode'] ?? MODE_CGI;
            $allowModes = is_array($allowMode) ? $allowMode : [$allowMode];
            if (!in_array(MODE_CGI, $allowModes, true)) {
                continue;
            }

            $modules[] = $config;
        }

        return $modules;
    }

    /**
     * 返回星期选项。
     *
     * @return array
     */
    public function weekdayOptions(): array {
        return [
            ['label' => '周日', 'value' => 0],
            ['label' => '周一', 'value' => 1],
            ['label' => '周二', 'value' => 2],
            ['label' => '周三', 'value' => 3],
            ['label' => '周四', 'value' => 4],
            ['label' => '周五', 'value' => 5],
            ['label' => '周六', 'value' => 6],
        ];
    }

    /**
     * 返回当前应用可选的运行环境列表。
     *
     * `app.php` 代表默认生产环境，对外展示为 `production`；
     * `app_xxx.php/yml` 代表名为 `xxx` 的环境配置。
     *
     * @return array<int, string>
     */
    protected function envOptions(): array {
        $configPath = App::src() . '/config';
        $options = ['production' => true];
        if (is_dir($configPath)) {
            foreach ((array)glob($configPath . '/app*.php') as $file) {
                $options[$this->envNameFromConfigBasename((string)basename($file))] = true;
            }
            foreach ((array)glob($configPath . '/app*.yml') as $file) {
                $options[$this->envNameFromConfigBasename((string)basename($file))] = true;
            }
        }
        if (SERVER_RUN_ENV !== '') {
            $options[(string)SERVER_RUN_ENV] = true;
        }

        $envs = array_keys($options);
        usort($envs, static function (string $left, string $right): int {
            if ($left === 'production') {
                return -1;
            }
            if ($right === 'production') {
                return 1;
            }
            return strcmp($left, $right);
        });
        return $envs;
    }

    /**
     * 新建排程时默认使用的运行环境。
     *
     * @return string
     */
    protected function defaultEnv(): string {
        return SERVER_RUN_ENV ?: 'production';
    }

    /**
     * 从配置文件名中提取环境名。
     *
     * @param string $basename
     * @return string
     */
    protected function envNameFromConfigBasename(string $basename): string {
        if ($basename === 'app.php' || $basename === 'app.yml') {
            return 'production';
        }
        if (preg_match('/^app_([^.]+)\.(php|yml)$/i', $basename, $matches)) {
            return strtolower((string)$matches[1]);
        }
        return 'production';
    }

    /**
     * 读取配置文件。
     *
     * @return array
     */
    protected function readConfig(): array {
        $storageFile = self::storageConfigFile();
        $storageConfig = self::readConfigPayloadFromFile($storageFile);
        if ($storageConfig !== null && !empty($storageConfig['items'])) {
            return $storageConfig;
        }

        $legacyFile = self::legacyConfigFile();
        $legacyConfig = self::readConfigPayloadFromFile($legacyFile);
        if ($legacyConfig !== null) {
            // 旧文件里仍有配置时，优先恢复到新的 db 存储，避免页面因为空的新文件而看不到任务。
            self::persistConfigPayload($legacyConfig);
            return $legacyConfig;
        }

        if ($storageConfig !== null) {
            return $storageConfig;
        }

        return ['items' => []];
    }

    /**
     * 写入配置文件。
     *
     * @param array $data 配置数据
     * @return void
     * @throws Exception
     */
    protected function writeConfig(array $data): void {
        $payload = [
            'items' => array_values($data['items'] ?? []),
            'updated_at' => time(),
        ];

        if (!self::persistConfigPayload($payload)) {
            throw new Exception('写入排程配置文件失败');
        }
    }

    /**
     * 构建节点心跳里复用的排程任务列表。
     *
     * 节点页现在需要直接显示 Linux 排程是否已经落进当前节点的系统 crontab，
     * 因此这里除了本地配置与最近运行态，还会附带“预期安装行数 / 实际安装行数”
     * 等字段。系统 crontab 内容只在本方法内读取一次，避免每条任务都重复执行系统命令。
     *
     * @return array<int, array<string, mixed>>
     */
    protected function buildNodeTasks(): array {
        $systemAvailable = $this->isSystemCrontabAvailable();
        $flockAvailable = $this->isFlockAvailable();
        $installedLineCount = $systemAvailable
            ? $this->extractInstalledLineCount($this->readSystemCrontab())
            : [];
        $items = [];
        foreach (($this->readConfig()['items'] ?? []) as $item) {
            $entry = $this->normalizeEntry((array)$item, false);
            if (!$this->shouldInstallOnCurrentNode($entry)) {
                continue;
            }
            $lockRequested = $this->isLockRequested($entry);
            $expectedLineCount = (int)($entry['enabled'] ?? 0) === 1
                ? count($this->buildCronLines($entry))
                : 0;
            $actualLineCount = (int)($installedLineCount[$entry['id']] ?? 0);
            $systemInstalled = $systemAvailable && $expectedLineCount > 0 && $actualLineCount === $expectedLineCount;
            $systemPartial = $systemAvailable && $expectedLineCount > 0 && $actualLineCount > 0 && $actualLineCount < $expectedLineCount;
            $systemStale = $systemAvailable && $expectedLineCount === 0 && $actualLineCount > 0;
            $items[] = [
                'id' => $entry['id'],
                'name' => $entry['name'],
                'namespace' => $entry['namespace'],
                'manager_id' => 'linux',
                'scheduler_type' => 'linux',
                'mode' => $this->nodeTaskMode($entry),
                'times' => $this->nodeTaskTimes($entry),
                'override' => [
                    'times' => $this->nodeTaskTimes($entry),
                    'interval' => (int)$entry['interval_minutes'] * 60,
                ],
                'interval' => (int)$entry['interval_minutes'] * 60,
                'interval_humanize' => Date::secondsHumanize((int)$entry['interval_minutes'] * 60),
                'status' => (int)($entry['enabled'] ?? 0) === 1 ? 1 : 0,
                'real_status' => (int)($entry['enabled'] ?? 0) === 1 ? 1 : 0,
                'is_busy' => (string)($entry['last_run_status'] ?? '') === 'running' ? 1 : 0,
                'run_count' => (int)($entry['last_run_at'] ?? 0) > 0 ? 1 : 0,
                'last_run' => (int)($entry['last_run_at'] ?? 0),
                'next_run' => 0,
                'logs' => [],
                'env' => $entry['env'],
                'role' => $entry['role'],
                'lock_enabled' => (int)($entry['lock_enabled'] ?? 1),
                'lock_requested' => $lockRequested,
                'lock_runtime_available' => $flockAvailable,
                'lock_effective' => $lockRequested && $flockAvailable,
                'lock_warning' => $this->lockWarningMessage($entry, true, $flockAvailable),
                'schedule_type' => $entry['schedule_type'],
                'schedule_summary' => $this->buildScheduleSummary($entry),
                'remark' => $entry['remark'],
                'system_available' => $systemAvailable,
                'expected_line_count' => $expectedLineCount,
                'actual_line_count' => $actualLineCount,
                'system_installed' => $systemInstalled,
                'system_partial' => $systemPartial,
                'system_stale' => $systemStale,
            ];
        }

        usort($items, static function (array $left, array $right): int {
            return (int)($right['last_run'] ?? 0) <=> (int)($left['last_run'] ?? 0);
        });

        return $items;
    }

    /**
     * 应用来自 master 的复制配置。
     *
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     * @throws Exception
     */
    protected function applyReplicatedConfig(array $payload): array {
        $incomingItems = array_values(is_array($payload['items'] ?? null) ? $payload['items'] : []);
        $currentItems = [];
        foreach (($this->readConfig()['items'] ?? []) as $item) {
            $id = (string)($item['id'] ?? '');
            if ($id === '') {
                continue;
            }
            $currentItems[$id] = (array)$item;
        }

        $nextItems = [];
        foreach ($incomingItems as $incomingItem) {
            $entry = $this->normalizeEntry((array)$incomingItem, false);
            $current = (array)($currentItems[$entry['id']] ?? []);
            if ($current) {
                $entry['last_run_at'] = (int)($current['last_run_at'] ?? $entry['last_run_at']);
                $entry['last_finish_at'] = (int)($current['last_finish_at'] ?? $entry['last_finish_at']);
                $entry['last_run_status'] = trim((string)($current['last_run_status'] ?? $entry['last_run_status']));
                $entry['last_run_message'] = trim((string)($current['last_run_message'] ?? $entry['last_run_message']));
            }
            $nextItems[] = $entry;
        }

        $this->writeConfig([
            'items' => $nextItems,
        ]);
        $sync = $this->sync();

        return [
            'item_count' => count($nextItems),
            'sync' => $sync,
        ];
    }

    /**
     * 标准化并校验排程配置。
     *
     * @param array $payload 原始输入
     * @param bool $validate 是否执行严格校验
     * @return array
     * @throws Exception
     */
    protected function normalizeEntry(array $payload, bool $validate): array {
        $namespace = '\\' . ltrim((string)($payload['namespace'] ?? ''), '\\');
        $name = trim((string)($payload['name'] ?? ''));
        $scheduleType = trim((string)($payload['schedule_type'] ?? 'daily'));
        $times = array_values(array_unique(array_filter(array_map(
            static fn(mixed $time): string => trim((string)$time),
            (array)($payload['times'] ?? [])
        ))));
        sort($times);

        $weekdays = array_values(array_unique(array_map(
            static fn(mixed $day): int => (int)$day,
            array_filter((array)($payload['weekdays'] ?? []), static fn(mixed $day): bool => $day !== '' && $day !== null)
        )));
        sort($weekdays);

        $entry = [
            'id' => trim((string)($payload['id'] ?? '')) ?: $this->generateEntryId(),
            'name' => $name ?: $this->shortClassName($namespace),
            'namespace' => $namespace,
            'schedule_type' => $scheduleType,
            'interval_minutes' => max(1, (int)($payload['interval_minutes'] ?? 5)),
            'times' => $times,
            'weekdays' => $weekdays,
            'lock_enabled' => array_key_exists('lock_enabled', $payload)
                ? ((int)($payload['lock_enabled'] ?? 0) === 1 ? 1 : 0)
                : 1,
            'last_run_at' => max(0, (int)($payload['last_run_at'] ?? 0)),
            'last_finish_at' => max(0, (int)($payload['last_finish_at'] ?? 0)),
            'last_run_status' => trim((string)($payload['last_run_status'] ?? '')),
            'last_run_message' => trim((string)($payload['last_run_message'] ?? '')),
            'enabled' => (int)($payload['enabled'] ?? 0) === 1 ? 1 : 0,
            'env' => trim((string)($payload['env'] ?? $this->defaultEnv())) ?: $this->defaultEnv(),
            'role' => trim((string)($payload['role'] ?? SERVER_ROLE)) ?: SERVER_ROLE,
            'remark' => trim((string)($payload['remark'] ?? '')),
            'created_at' => (int)($payload['created_at'] ?? time()),
            'updated_at' => time(),
        ];

        if (!$validate) {
            return $entry;
        }

        if ($entry['namespace'] === '\\') {
            throw new Exception('任务脚本不能为空');
        }
        if (!in_array($entry['schedule_type'], self::SCHEDULE_TYPES, true)) {
            throw new Exception('排程类型非法');
        }
        if ($entry['role'] !== NODE_ROLE_MASTER && $entry['role'] !== NODE_ROLE_SLAVE) {
            throw new Exception('节点角色只能是 master 或 slave');
        }

        foreach ($times as $time) {
            if (!preg_match('/^\d{2}:\d{2}$/', $time)) {
                throw new Exception('执行时间格式错误，需为 HH:MM');
            }
        }
        foreach ($weekdays as $day) {
            if ($day < 0 || $day > 6) {
                throw new Exception('星期几配置非法');
            }
        }

        switch ($entry['schedule_type']) {
            case 'interval':
                if ($entry['interval_minutes'] < 1 || $entry['interval_minutes'] > 59) {
                    throw new Exception('间隔分钟必须在 1 到 59 之间');
                }
                $entry['times'] = [];
                break;
            case 'daily':
                if (!$entry['times']) {
                    throw new Exception('每日定时至少需要一个执行时间');
                }
                break;
            case 'weekly':
                if (!$entry['times']) {
                    throw new Exception('每周定时至少需要一个执行时间');
                }
                if (!$entry['weekdays']) {
                    throw new Exception('每周定时至少需要选择一个星期');
                }
                break;
        }

        return $entry;
    }

    /**
     * 构建 cron 表达式预览。
     *
     * @param array $entry 排程配置
     * @return string
     */
    protected function buildCronExpressionPreview(array $entry): string {
        return implode("\n", $this->buildExpressions($entry));
    }

    /**
     * 构建人类可读的排程说明。
     *
     * @param array $entry 排程配置
     * @return string
     */
    protected function buildScheduleSummary(array $entry): string {
        $summary = match ($entry['schedule_type']) {
            'interval' => '每隔 ' . $entry['interval_minutes'] . ' 分钟执行一次',
            'daily' => '每日 ' . implode('、', $entry['times']) . ' 执行',
            'weekly' => '每周 ' . implode('、', $this->weekdayLabels($entry['weekdays'])) . ' 的 ' . implode('、', $entry['times']) . ' 执行',
            default => '未知',
        };

        if ($entry['schedule_type'] !== 'weekly' && !empty($entry['weekdays'])) {
            $summary .= '（仅限 ' . implode('、', $this->weekdayLabels($entry['weekdays'])) . '）';
        }

        if ($this->isLockRequested($entry)) {
            if (!$this->shouldInstallOnCurrentNode($entry)) {
                $summary .= '，已开启互斥锁';
            } elseif ($this->isFlockAvailable()) {
                $summary .= '，重入时跳过';
            } else {
                $summary .= '，已开启互斥锁（当前节点未安装 flock，同步时会失败）';
            }
        }

        return $summary;
    }

    /**
     * 判断一条排程是否应安装到当前节点。
     *
     * Linux 排程允许在同一份配置里同时维护 master/slave、不同 env 的任务，
     * 但系统 crontab 只能安装“当前节点自己应该执行”的那一部分。
     *
     * @param array<string, mixed> $entry
     * @return bool
     */
    protected function shouldInstallOnCurrentNode(array $entry): bool {
        $entryRole = trim((string)($entry['role'] ?? SERVER_ROLE)) ?: SERVER_ROLE;
        if ($entryRole !== SERVER_ROLE) {
            return false;
        }

        $currentEnv = strtolower((string)(SERVER_RUN_ENV ?: 'production'));
        $entryEnv = strtolower(trim((string)($entry['env'] ?? $currentEnv)) ?: $currentEnv);
        return $entryEnv === $currentEnv;
    }

    /**
     * 判断一次配置变更是否会影响当前节点本机排程。
     *
     * 这里要同时看“变更前”和“变更后”两侧：
     * - 新配置命中当前节点时，需要允许把它写入本机；
     * - 旧配置原本命中当前节点、但新配置改成别的 env/role 或被关闭时，
     *   仍然要在当前节点做一次同步，把本机旧条目清掉。
     *
     * @param array<string, mixed>|null $currentEntry 变更后的配置
     * @param array<string, mixed>|null $previousEntry 变更前的配置
     * @return bool
     */
    protected function entryAffectsCurrentNode(?array $currentEntry, ?array $previousEntry = null): bool {
        $currentApplicable = $currentEntry && (int)($currentEntry['enabled'] ?? 0) === 1
            ? $this->shouldInstallOnCurrentNode($currentEntry)
            : false;
        $previousApplicable = $previousEntry && (int)($previousEntry['enabled'] ?? 0) === 1
            ? $this->shouldInstallOnCurrentNode($previousEntry)
            : false;

        return $currentApplicable || $previousApplicable;
    }

    /**
     * 把 Linux 排程类型映射成节点页沿用的运行模式枚举。
     *
     * @param array<string, mixed> $entry
     * @return int
     */
    protected function nodeTaskMode(array $entry): int {
        return $entry['schedule_type'] === 'interval' ? 3 : 2;
    }

    /**
     * 生成节点页“运行时间”列可直接展示的时间数组。
     *
     * weekly 在旧排程表里没有独立星期维度，因此这里把星期几直接拼进时间文本，
     * 让节点页不改列结构也能看出限制条件。
     *
     * @param array<string, mixed> $entry
     * @return array<int, string>
     */
    protected function nodeTaskTimes(array $entry): array {
        if ($entry['schedule_type'] === 'interval') {
            return [];
        }

        $times = array_values((array)($entry['times'] ?? []));
        if (!empty($entry['weekdays']) && is_array($entry['weekdays'])) {
            $weekdayLabel = implode('/', $this->weekdayLabels((array)$entry['weekdays']));
            return array_values(array_map(
                static fn(string $time): string => $weekdayLabel . ' ' . $time,
                $times
            ));
        }

        return $times;
    }

    /**
     * 构建当前配置对应的所有 cron 表达式。
     *
     * 一个 weekly/daily 配置可能拆成多条系统 crontab 行，因此返回数组。
     *
     * @param array $entry 排程配置
     * @return array
     */
    protected function buildExpressions(array $entry): array {
        $weekdaySuffix = $this->buildWeekdayCronSuffix($entry);

        return match ($entry['schedule_type']) {
            'interval' => ['*/' . $entry['interval_minutes'] . ' * * *' . $weekdaySuffix],
            'daily' => array_values(array_map(
                fn(string $time): string => $this->timeToCron($time) . ' * *' . $weekdaySuffix,
                $entry['times']
            )),
            'weekly' => array_values(array_map(
                fn(string $time): string => $this->timeToCron($time) . ' * *' . $weekdaySuffix,
                $entry['times']
            )),
            default => [],
        };
    }

    /**
     * 构建 cron 表达式里的星期字段后缀。
     *
     * 该约束独立于调度类型存在：
     * - interval: 可限制只在指定星期几按固定分钟间隔执行
     * - daily: 可限制只在指定星期几的固定时间执行
     * - weekly: 继续保持“必须选择星期几”的显式语义
     *
     * @param array $entry 排程配置
     * @return string
     */
    protected function buildWeekdayCronSuffix(array $entry): string {
        if (empty($entry['weekdays']) || !is_array($entry['weekdays'])) {
            return ' *';
        }

        return ' ' . implode(',', array_map('intval', $entry['weekdays']));
    }

    /**
     * 将 weekday 数字列表转为中文标签。
     *
     * @param array $weekdays
     * @return array
     */
    protected function weekdayLabels(array $weekdays): array {
        $options = [];
        foreach ($this->weekdayOptions() as $option) {
            $options[(int)$option['value']] = (string)$option['label'];
        }

        return array_values(array_map(
            static fn(int $day): string => $options[$day] ?? ('周' . $day),
            $weekdays
        ));
    }

    /**
     * 构建一条配置对应的系统 crontab 行。
     *
     * @param array $entry 排程配置
     * @return array
     */
    protected function buildCronLines(array $entry): array {
        $command = $this->buildCommand($entry);
        $marker = '# ' . $this->entryMarker((string)$entry['id']);
        $lines = [];

        foreach ($this->buildExpressions($entry) as $expression) {
            $lines[] = $expression . ' ' . $command . ' ' . $marker;
        }

        return $lines;
    }

    /**
     * 构建调度命令。
     *
     * 所有托管条目都通过 `scf/bin/crontab` 转发脚本执行，并显式传入完整命名空间，
     * 这样即使短类名重名也不会在系统层调错脚本。
     *
     * @param array $entry 排程配置
     * @return string
     */
    protected function buildCommand(array $entry): string {
        $command = escapeshellarg(SCF_ROOT . '/bin/crontab')
            . ' -app=' . escapeshellarg(APP_DIR_NAME)
            . ' -env=' . escapeshellarg((string)$entry['env'])
            . ' -role=' . escapeshellarg((string)$entry['role']);

        if (APP_SRC_TYPE === 'dir') {
            $command .= ' -dir';
        } elseif (APP_SRC_TYPE === 'phar') {
            $command .= ' -phar';
        }

        $command .= ' -entry_id=' . escapeshellarg((string)$entry['id']);
        $command .= ' ' . escapeshellarg($this->shortClassName((string)$entry['namespace']));

        $timeoutPath = $this->resolveTimeoutPath();
        if ($timeoutPath !== '') {
            // 给一次性命令增加硬超时，避免异常阻塞导致锁长时间不释放。
            $command = escapeshellarg($timeoutPath)
                . ' -k ' . self::COMMAND_TIMEOUT_KILL_AFTER_SECONDS . 's '
                . self::COMMAND_TIMEOUT_SECONDS . 's '
                . $command;
        }

        $flockPath = $this->resolveFlockPath();
        $lockFile = '/tmp/' . APP_DIR_NAME . '_' . preg_replace('/[^a-z0-9_]+/i', '_', (string)$entry['id']) . '.lock';
        if ((int)($entry['lock_enabled'] ?? 1) === 1 && $flockPath !== '') {
            $command = escapeshellarg($flockPath)
                . ' -n '
                . escapeshellarg($lockFile)
                . ' '
                . $command;
        }

        return $command;
    }

    /**
     * 返回 Linux crontab 执行日志目录。
     *
     * @return string
     */
    protected function crontabLogDirectory(): string {
        return APP_LOG_PATH . self::CRONTAB_LOG_SUB_DIRECTORY;
    }

    /**
     * 返回 Linux crontab 执行日志文件。
     *
     * @return string
     */
    protected function crontabLogFile(): string {
        return $this->crontabLogDirectory() . '/' . self::CRONTAB_LOG_FILE_NAME;
    }

    /**
     * 读取当前系统 crontab。
     *
     * 没有配置任何系统 crontab 时，`crontab -l` 在多数环境会返回非 0。
     * 对 dashboard 来说这不应该算业务错误，因此此处统一返回空字符串。
     *
     * @return string
     */
    protected function readSystemCrontab(): string {
        $command = $this->resolveSystemCrontabCommand();
        if ($command === '') {
            return '';
        }

        $output = [];
        $code = 0;
        @exec($command . ' -l 2>/dev/null', $output, $code);
        if ($code !== 0 && !$output) {
            return '';
        }

        return implode(PHP_EOL, $output);
    }

    /**
     * 写入系统 crontab。
     *
     * @param string $content 最终要写入的 crontab 内容
     * @return void
     * @throws Exception
     */
    protected function writeSystemCrontab(string $content): void {
        $command = $this->resolveSystemCrontabCommand();
        if ($command === '') {
            throw new Exception('系统未安装 crontab 命令');
        }

        $tempFile = tempnam(sys_get_temp_dir(), 'scf_crontab_');
        if (!$tempFile) {
            throw new Exception('创建临时 crontab 文件失败');
        }

        try {
            if (!File::write($tempFile, $content)) {
                throw new Exception('写入临时 crontab 文件失败');
            }

            $output = [];
            $code = 0;
            @exec($command . ' ' . escapeshellarg($tempFile) . ' 2>&1', $output, $code);
            if ($code !== 0) {
                throw new Exception('写入系统 crontab 失败: ' . implode("\n", $output));
            }
        } finally {
            @unlink($tempFile);
        }
    }

    /**
     * 统计系统 crontab 中当前应用各条目实际存在的行数。
     *
     * @param string $content 当前系统 crontab 内容
     * @return array
     */
    protected function extractInstalledLineCount(string $content): array {
        $result = [];
        foreach (preg_split('/\r?\n/', $content) as $line) {
            $scopePattern = '/' . preg_quote($this->scopeMarker() . ':', '/') . '([a-z0-9_]+)/i';
            if (preg_match($scopePattern, $line, $matches)) {
                $id = $matches[1];
                $result[$id] = (int)($result[$id] ?? 0) + 1;
                continue;
            }

            // 兼容旧版只按 app 打标的 crontab 行。升级后的首轮同步会把它们迁走，
            // 在此之前仍然需要把“属于当前 env/role 的旧条目”计入安装状态。
            $legacyPattern = '/' . preg_quote(self::legacyMarkerPrefix() . ':', '/') . '([a-z0-9_]+)/i';
            if (preg_match($legacyPattern, $line, $matches) && $this->lineTargetsCurrentNode($line)) {
                $id = $matches[1];
                $result[$id] = (int)($result[$id] ?? 0) + 1;
            }
        }
        return $result;
    }

    /**
     * 去除系统 crontab 中由当前应用托管的条目。
     *
     * @param string $content 当前系统 crontab 内容
     * @return array
     */
    protected function stripManagedLines(string $content): array {
        $lines = preg_split('/\r?\n/', $content);
        $kept = [];
        $scopeMarker = $this->scopeMarker();
        $legacyMarker = self::legacyMarkerPrefix();

        foreach ($lines as $line) {
            // 只清理“当前 env/role 作用域”对应的块，避免同机 master/slave 共用一份系统
            // crontab 时互相覆盖。旧版 app 级 block marker 没有作用域，因此统一去掉，
            // 具体条目是否保留再交给下面的逐行判断处理。
            if (str_contains($line, $scopeMarker . ':BEGIN')
                || str_contains($line, $scopeMarker . ':END')
                || str_contains($line, $legacyMarker . ':BEGIN')
                || str_contains($line, $legacyMarker . ':END')) {
                continue;
            }
            if (preg_match('/' . preg_quote($scopeMarker . ':', '/') . '[a-z0-9_]+/i', $line)) {
                continue;
            }
            if (preg_match('/' . preg_quote($legacyMarker . ':', '/') . '[a-z0-9_]+/i', $line) && $this->lineTargetsCurrentNode($line)) {
                continue;
            }
            if ($line === '' && (!$kept || end($kept) === '')) {
                continue;
            }
            $kept[] = $line;
        }

        while ($kept && trim((string)end($kept)) === '') {
            array_pop($kept);
        }

        return $kept;
    }

    /**
     * 判断系统是否可用 crontab 命令。
     *
     * @return bool
     */
    protected function isSystemCrontabAvailable(): bool {
        return $this->resolveSystemCrontabCommand() !== '';
    }

    /**
     * 返回配置文件路径。
     *
     * @return string
     */
    protected function configFile(): string {
        $storageFile = self::storageConfigFile();
        $storagePayload = self::readConfigPayloadFromFile($storageFile);
        if ($storagePayload !== null && !empty($storagePayload['items'])) {
            return $storageFile;
        }

        $legacyFile = self::legacyConfigFile();
        $legacyPayload = self::readConfigPayloadFromFile($legacyFile);
        if ($legacyPayload === null) {
            return $storageFile;
        }

        if (self::persistConfigPayload($legacyPayload)) {
            return $storageFile;
        }

        if ($storagePayload !== null) {
            return $storageFile;
        }

        return $legacyFile;
    }

    /**
     * 从指定文件中读取排程配置。
     *
     * 新老文件在历史上存在两种结构：
     * - {'items': [...]}
     * - 直接以数组保存 [...]
     *
     * 这里统一归一成 {'items': [...]}，并把“空文件/坏 JSON”视为无效配置，
     * 避免它们遮住仍然有数据的旧文件。
     *
     * @param string $file
     * @return array<string, mixed>|null
     */
    protected static function readConfigPayloadFromFile(string $file): ?array {
        if (!is_file($file)) {
            return null;
        }

        $json = File::readJson($file);
        if (!is_array($json)) {
            $raw = File::read($file);
            if (is_string($raw) && $raw !== '') {
                // 兼容 BOM 和异常中断后残留的空字节，避免“文件看起来有内容，但 items 直接读空”。
                $raw = preg_replace('/^\xEF\xBB\xBF/', '', $raw) ?? $raw;
                $raw = str_replace("\0", '', $raw);
                $decoded = json_decode(trim($raw), true);
                if (is_array($decoded)) {
                    $json = $decoded;
                }
            }
        }
        if (!is_array($json)) {
            return null;
        }

        $items = array_values(is_array($json['items'] ?? null) ? $json['items'] : $json);
        if (!$items && !array_key_exists('items', $json)) {
            return null;
        }

        return [
            'items' => $items,
        ];
    }

    /**
     * 生成条目标记。
     *
     * @param string $id 排程 id
     * @return string
     */
    protected function entryMarker(string $id): string {
        return $this->scopeMarker() . ':' . $id;
    }

    /**
     * 返回当前节点在系统 crontab 中使用的作用域标记。
     *
     * 同一台机器上如果同时跑 master/slave，它们共用的是同一个系统用户 crontab。
     * 因此标记必须细化到 app + env + role，才能避免两边同步时互相抹掉对方的条目。
     *
     * @return string
     */
    protected function scopeMarker(): string {
        return self::SYSTEM_MARKER_PREFIX . ':' . APP_DIR_NAME . ':' . $this->scopeEnv() . ':' . SERVER_ROLE;
    }

    /**
     * 旧版只按 app 打标的 marker 前缀。
     *
     * 该前缀只用于兼容迁移，避免升级后首轮同步前页面状态全部失真。
     *
     * @return string
     */
    protected static function legacyMarkerPrefix(): string {
        return self::SYSTEM_MARKER_PREFIX . ':' . APP_DIR_NAME;
    }

    /**
     * 返回当前节点参与系统 crontab 分片的环境名。
     *
     * @return string
     */
    protected function scopeEnv(): string {
        return strtolower((string)(SERVER_RUN_ENV ?: 'production'));
    }

    /**
     * 判断一条 legacy crontab 行是否属于当前 env/role。
     *
     * 旧版 marker 只有 app，没有 env/role 维度。迁移阶段需要结合命令本身的参数，
     * 只移除当前节点真正管理的那部分旧条目，避免把同机另一个角色的任务误删掉。
     *
     * @param string $line 单行 crontab 内容
     * @return bool
     */
    protected function lineTargetsCurrentNode(string $line): bool {
        return str_contains($line, "-app='" . APP_DIR_NAME . "'")
            && str_contains($line, "-env='" . $this->scopeEnv() . "'")
            && str_contains($line, "-role='" . SERVER_ROLE . "'");
    }

    /**
     * 判断条目是否请求了 Linux 层互斥锁。
     *
     * @param array<string, mixed> $entry 排程配置
     * @return bool
     */
    protected function isLockRequested(array $entry): bool {
        return (int)($entry['lock_enabled'] ?? 1) === 1;
    }

    /**
     * 返回当前节点是否可用 `flock`。
     *
     * @return bool
     */
    protected function isFlockAvailable(): bool {
        return $this->resolveFlockPath() !== '';
    }

    /**
     * 解析当前节点上的 `flock` 命令路径。
     *
     * @return string
     */
    protected function resolveFlockPath(): string {
        if (!is_null($this->flockPath)) {
            return $this->flockPath;
        }

        $resolved = trim((string)@shell_exec('command -v flock 2>/dev/null'));
        if ($resolved !== '' && is_executable($resolved)) {
            $this->flockPath = $resolved;
            return $this->flockPath;
        }

        $candidates = [];
        $pathEnv = (string)(getenv('PATH') ?: '');
        if ($pathEnv !== '') {
            foreach (explode(PATH_SEPARATOR, $pathEnv) as $directory) {
                $directory = trim($directory);
                if ($directory === '') {
                    continue;
                }
                $candidates[] = rtrim($directory, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'flock';
            }
        }

        // 兼容 GUI/守护进程环境没有加载 shell PATH 时的常见安装位置。
        $candidates = array_merge($candidates, [
            '/opt/homebrew/opt/util-linux/bin/flock',
            '/opt/homebrew/bin/flock',
            '/usr/local/opt/util-linux/bin/flock',
            '/usr/local/bin/flock',
            '/usr/bin/flock',
            '/bin/flock',
        ]);

        foreach (array_unique($candidates) as $candidate) {
            if (is_executable($candidate)) {
                $this->flockPath = $candidate;
                return $this->flockPath;
            }
        }

        $this->flockPath = '';
        return $this->flockPath;
    }

    /**
     * 解析当前节点上的 `timeout` 命令路径。
     *
     * @return string
     */
    protected function resolveTimeoutPath(): string {
        if (!is_null($this->timeoutPath)) {
            return $this->timeoutPath;
        }

        $resolved = trim((string)@shell_exec('command -v timeout 2>/dev/null'));
        if ($resolved !== '' && is_executable($resolved)) {
            $this->timeoutPath = $resolved;
            return $this->timeoutPath;
        }

        $candidates = [];
        $pathEnv = (string)(getenv('PATH') ?: '');
        if ($pathEnv !== '') {
            foreach (explode(PATH_SEPARATOR, $pathEnv) as $directory) {
                $directory = trim($directory);
                if ($directory === '') {
                    continue;
                }
                $candidates[] = rtrim($directory, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'timeout';
            }
        }

        // 兼容守护进程 PATH 精简场景下的常见 timeout 安装位置。
        $candidates = array_merge($candidates, [
            '/usr/local/bin/timeout',
            '/usr/bin/timeout',
            '/bin/timeout',
        ]);

        foreach (array_unique($candidates) as $candidate) {
            if (is_executable($candidate)) {
                $this->timeoutPath = $candidate;
                return $this->timeoutPath;
            }
        }

        $this->timeoutPath = '';
        return $this->timeoutPath;
    }

    /**
     * 返回条目在当前节点上的互斥锁提示。
     *
     * @param array<string, mixed> $entry 排程配置
     * @param bool $localApplicable 是否作用于当前节点
     * @param bool $flockAvailable 当前节点是否可用 flock
     * @return string
     */
    protected function lockWarningMessage(array $entry, bool $localApplicable, bool $flockAvailable): string {
        if (!$this->isLockRequested($entry) || !$localApplicable || $flockAvailable) {
            return '';
        }

        return '当前节点未安装 flock，互斥锁不会生效；写入或同步到本机系统排程时会失败。';
    }

    /**
     * 校验单条排程在当前节点同步时所需的互斥锁能力。
     *
     * @param array<string, mixed> $entry 排程配置
     * @return void
     * @throws Exception 当前节点缺少 flock 且条目要求互斥锁时抛错
     */
    protected function assertEntryCanSyncWithLock(array $entry): void {
        if (!$this->shouldInstallOnCurrentNode($entry)) {
            return;
        }
        if ((int)($entry['enabled'] ?? 0) !== 1) {
            return;
        }
        if (!$this->isLockRequested($entry)) {
            return;
        }
        if ($this->isFlockAvailable()) {
            return;
        }

        throw new Exception('当前节点未安装 flock，无法把启用了互斥锁的排程写入系统 crontab: '
            . ($entry['name'] ?? $this->shortClassName((string)($entry['namespace'] ?? '')))
            . '（' . ($entry['namespace'] ?? '--') . '）');
    }

    /**
     * 校验当前节点即将同步的排程集合是否都满足互斥锁前置条件。
     *
     * @param array<int, array<string, mixed>> $items 排程配置列表
     * @return void
     * @throws Exception 只要有一条启用互斥锁的本机排程缺少 flock 就抛错
     */
    protected function assertEntriesCanSyncWithLock(array $items): void {
        $blocked = [];
        foreach ($items as $item) {
            $entry = $this->normalizeEntry((array)$item, false);
            try {
                $this->assertEntryCanSyncWithLock($entry);
            } catch (Exception) {
                $blocked[] = ($entry['name'] ?? $this->shortClassName((string)($entry['namespace'] ?? '')))
                    . '（' . ($entry['namespace'] ?? '--') . '）';
            }
        }

        if ($blocked) {
            throw new Exception('当前节点未安装 flock，以下启用了互斥锁的排程无法写入系统 crontab: '
                . implode('、', $blocked));
        }
    }

    /**
     * 生成排程 ID。
     *
     * @return string
     */
    protected function generateEntryId(): string {
        return 'lc_' . substr(md5(APP_DIR_NAME . '|' . microtime(true) . '|' . mt_rand()), 0, 12);
    }

    /**
     * 提取短类名。
     *
     * @param string $namespace 完整命名空间
     * @return string
     */
    protected function shortClassName(string $namespace): string {
        $namespace = ltrim($namespace, '\\');
        if (!str_contains($namespace, '\\')) {
            return $namespace;
        }
        return substr($namespace, strrpos($namespace, '\\') + 1);
    }

    /**
     * 把 `HH:MM` 转换成 cron 前两段。
     *
     * @param string $time 时间字符串
     * @return string
     */
    protected function timeToCron(string $time): string {
        [$hour, $minute] = explode(':', $time, 2);
        return (int)$minute . ' ' . (int)$hour;
    }

    /**
     * 返回当前正式使用的排程存储文件路径。
     *
     * @return string
     */
    protected static function storageConfigFile(): string {
        return APP_PATH . self::STORAGE_DIRECTORY . self::storageConfigFileName();
    }

    /**
     * 返回旧版误放在源码配置目录里的排程文件路径。
     *
     * @return string
     */
    protected static function legacyConfigFile(): string {
        return App::src() . self::LEGACY_CONFIG_DIRECTORY . self::CONFIG_FILE_NAME;
    }

    /**
     * 返回当前节点角色对应的排程文件名。
     *
     * 本地联调时 master/slave 可能共用同一个应用目录，Linux 排程又属于节点本地运行态，
     * 因此 slave 侧需要独立落盘，避免和 master 的 `crontabs.json` 相互覆盖。
     *
     * @return string
     */
    protected static function storageConfigFileName(): string {
        if (defined('SERVER_ROLE') && SERVER_ROLE === NODE_ROLE_SLAVE) {
            return 'crontabs_slave.json';
        }

        return self::CONFIG_FILE_NAME;
    }

    /**
     * 解析当前可读取的排程文件。
     *
     * 新路径存在时始终优先读取；否则回退到旧路径，
     * 让运行态回写和页面读取都能兼容历史数据。
     *
     * @return string
     */
    protected static function resolveReadableConfigFile(): string {
        $storageFile = self::storageConfigFile();
        if (is_file($storageFile)) {
            return $storageFile;
        }

        $legacyFile = self::legacyConfigFile();
        if (is_file($legacyFile)) {
            return $legacyFile;
        }

        return $storageFile;
    }

    /**
     * 把排程数据写入正式存储文件，并在成功后清理旧版遗留文件。
     *
     * 这里集中处理目录创建、JSON 编码和旧文件迁移，
     * 避免页面保存与运行态回写分叉成两套不同的存储逻辑。
     *
     * @param array<string, mixed> $payload 待持久化的完整数据
     * @return bool
     */
    protected static function persistConfigPayload(array $payload): bool {
        $file = self::storageConfigFile();
        $dir = dirname($file);
        if (!is_dir($dir) && !mkdir($dir, 0775, true) && !is_dir($dir)) {
            return false;
        }

        $encoded = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
        if ($encoded === false) {
            return false;
        }

        $tempFile = $file . '.tmp.' . getmypid() . '.' . mt_rand(1000, 9999);
        // 先写临时文件再 rename，避免 dashboard 正在读配置时拿到半写 JSON。
        if (!File::write($tempFile, $encoded . PHP_EOL)) {
            return false;
        }
        if (!@rename($tempFile, $file)) {
            @unlink($tempFile);
            return false;
        }

        // 一旦新存储写入成功，就把旧的 config 版本移除，避免后续被源码发布打包。
        $legacyFile = self::legacyConfigFile();
        if ($legacyFile !== $file && is_file($legacyFile)) {
            @unlink($legacyFile);
        }

        return true;
    }

    /**
     * 解析当前节点可用的 crontab 命令绝对路径。
     *
     * 常驻 dashboard/gateway 进程的 PATH 经常比交互 shell 更短，
     * 只依赖 `command -v crontab` 很容易把“系统里明明有 crontab”误判成不可用，
     * 进而导致从系统排程恢复 items 失败。这里先试 PATH，再兜底常见安装路径。
     *
     * @return string
     */
    protected function resolveSystemCrontabCommand(): string {
        $command = trim((string)@shell_exec('command -v crontab 2>/dev/null'));
        if ($command !== '' && is_executable($command)) {
            return $command;
        }

        foreach ([
            '/usr/bin/crontab',
            '/bin/crontab',
            '/usr/sbin/crontab',
            '/opt/homebrew/bin/crontab',
            '/usr/local/bin/crontab',
        ] as $candidate) {
            if (is_executable($candidate)) {
                return $candidate;
            }
        }

        return '';
    }
}
