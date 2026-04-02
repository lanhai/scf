<?php

namespace Scf\Server\Gateway;

use RuntimeException;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Key;
use Scf\Core\Table\Runtime;

/**
 * Gateway 的 nginx 配置生成与同步器。
 *
 * 负责把 gateway 当前可见的 active upstream 结构翻译成 nginx upstream/server
 * 配置，并在需要时执行 test / reload / start。
 */
class GatewayNginxProxyHandler {

    protected ?array $nginxRuntimeMeta = null;
    protected ?string $lastObservedRuntimeMetaSignature = null;
    protected bool $runtimeMetaLoadedFromCache = false;

    /**
     * 绑定 gateway 与实例管理器，负责把当前运行态同步成 nginx 配置。
     *
     * @param GatewayServer $gateway 当前 gateway 运行实例。
     * @param AppInstanceManager $instanceManager 业务实例管理器。
     * @return void
     */
    public function __construct(
        protected GatewayServer $gateway,
        protected AppInstanceManager $instanceManager
    ) {
    }

    /**
     * 是否启用 nginx 作为对外流量入口。
     *
     * @return bool gateway 配置或同步开关开启时返回 true。
     */
    public function enabled(): bool {
        return $this->gateway->nginxProxyModeEnabled()
            || (bool)($this->gateway->serverConfig()['gateway_nginx_sync_enabled'] ?? false);
    }

    /**
     * 在 gateway 启动早期预热 nginx 环境探测结果并写入缓存。
     *
     * 这一步只负责解析 nginx 可执行文件、主配置路径和 conf 目录等运行时信息，
     * 让后续真正需要 sync 时能够直接复用缓存结果；它不会生成业务配置，也不会
     * 触发 nginx test/reload/start。
     *
     * @return array<string, mixed> 包含 runtime_meta 及其是否首次观测到变化。
     */
    public function warmupRuntimeMeta(bool $ensureRunning = false): array {
        $meta = $this->nginxRuntimeMeta();
        $started = false;
        if ($ensureRunning && !$this->isNginxRunning()) {
            // 启动早期如果发现 nginx 进程不存在，就直接按当前主配置拉起，
            // 避免 gateway 自己已上线但入口层仍然是断的。
            $this->startNginx();
            $started = true;
        }
        return [
            'runtime_meta' => $meta,
            'runtime_meta_changed' => $this->markRuntimeMetaObserved($meta),
            'started' => $started,
        ];
    }

    /**
     * 重新生成 nginx 配置并按需重载。
     *
     * 这里是 gateway 把自身运行态投影到 nginx 的唯一入口。
     *
     * @param string|null $reason 本次同步触发原因，便于日志和返回值追踪。
     * @return array 同步结果摘要，包含文件路径、变更标记和运行时元数据。
     * @throws RuntimeException 当配置写入、校验或 reload/start 失败时抛出。
     */
    public function sync(?string $reason = null): array {
        $runtimeMeta = $this->nginxRuntimeMeta();
        $globalFile = $this->generatedGlobalFile();
        $upstreamFile = $this->generatedUpstreamFile();
        $serverFile = $this->generatedServerFile();
        $exampleFile = $this->generatedExampleServerFile();
        $conflictCleaned = $this->cleanupConflictingManagedConfigFiles();
        $globalContent = $this->renderGlobalConfig();
        $upstreamContent = $this->renderUpstreamConfig();
        $serverContent = $this->renderServerConfig();

        $globalChanged = $this->writeIfChanged($globalFile, $globalContent);
        $upstreamChanged = $this->writeIfChanged($upstreamFile, $upstreamContent);
        $serverChanged = $this->writeIfChanged($serverFile, $serverContent);

        $exampleChanged = false;
        if (!$this->nginxServerFileExists()) {
            $exampleChanged = $this->writeIfChanged($exampleFile, $serverContent);
        }

        $tested = false;
        $reloaded = false;
        $configChanged = $globalChanged || $upstreamChanged || $serverChanged || $exampleChanged || $conflictCleaned;
        $shouldEnsureRunning = $this->shouldReloadNginx() && !$this->isNginxRunning();
        $shouldForceReload = $this->shouldReloadNginx()
            && $this->isNginxRunning()
            && $this->shouldForceReloadForSyncReason($reason);

        // “配置文件已经写好了，但 nginx 进程没起来”时，不能因为内容没变化就跳过。
        // 首次部署、手工停止 nginx、机器重启后 gateway 先起来等场景下，都需要在
        // 当前配置未变化的前提下重新执行 `nginx -t` 和 `nginx start`。
        //
        // 另外在 gateway 启动接管阶段，即使生成出来的 include 文件内容与上一轮一致，
        // 也要强制执行一次 reload。否则“文件已经在磁盘上，但 nginx 仍跑着旧配置”
        // 的场景下，启动日志会显示“已同步”，实际入口层却没有吃到新的 upstream。
        if ($configChanged || $shouldEnsureRunning || $shouldForceReload) {
            $this->testNginxConfig();
            $tested = true;
            if ($this->shouldReloadNginx()) {
                $this->reloadOrStartNginx();
                $reloaded = true;
            }
        }

        // 只有当前这次同步真的触发了 reload/start，才要求 nginx 立刻能在 `-T`
        // 里看到这些文件。手工 reload 模式下配置可以先落盘、稍后再由运维触发加载。
        if ($reloaded) {
            $this->validateManagedFilesLoaded([$globalFile, $upstreamFile, $serverFile]);
        }

        return [
            'reason' => $reason ?: 'manual',
            'global_file' => $globalFile,
            'upstream_file' => $upstreamFile,
            'server_file' => $serverFile,
            'example_file' => $exampleFile,
            'global_changed' => $globalChanged,
            'upstream_changed' => $upstreamChanged,
            'server_changed' => $serverChanged,
            'example_changed' => $exampleChanged,
            'tested' => $tested,
            'reloaded' => $reloaded,
            'business_upstream_name' => $this->businessUpstreamName(),
            'control_upstream_name' => $this->controlUpstreamName(),
            'active_instances' => $this->activeInstances(),
            'runtime_meta' => $runtimeMeta,
            'runtime_meta_changed' => $this->markRuntimeMetaObserved($runtimeMeta),
        ];
    }

    /**
     * 返回适合日志摘要展示的 nginx 关键配置。
     *
     * 这里改成结构化数据而不是直接拼好的长句，方便 GatewayServer 按“路径/参数”
     * 等分组输出，避免终端宽度稍窄时把整块日志折成难读的多段。
     *
     * @return array<string, string>
     */
    public function startupSummaryData(): array {
        $runtimeMeta = $this->nginxRuntimeMeta();
        $disableLogs = (bool)($this->gateway->serverConfig()['gateway_nginx_disable_global_logs'] ?? true);
        $proxyTimeouts = $this->resolveNginxProxyTimeouts();
        $proxyConnectTimeout = $proxyTimeouts['connect_timeout'];
        $proxyIoTimeout = $proxyTimeouts['business_io_timeout'];
        $clientIoTimeout = $proxyTimeouts['client_io_timeout'];
        $buffering = (bool)($this->gateway->serverConfig()['gateway_nginx_proxy_buffering'] ?? true);
        $realIpHeader = trim((string)($this->gateway->serverConfig()['gateway_nginx_real_ip_header'] ?? 'X-Forwarded-For'));
        $realIpRecursive = (bool)($this->gateway->serverConfig()['gateway_nginx_real_ip_recursive'] ?? true);
        $trustedProxies = $this->trustedRealIpProxies();
        $bodyBytes = (int)($this->gateway->serverConfig()['package_max_length'] ?? 10 * 1024 * 1024);
        $businessKeepalive = $this->resolveBusinessUpstreamKeepalive();
        $controlKeepalive = $this->resolveControlUpstreamKeepalive();
        $workerProcesses = $this->resolveNginxWorkerProcessCount();
        $listenPort = $this->gateway->businessPort();
        $serverName = trim($this->gateway->resolvedGatewayNginxServerName());
        $defaultUpstream = $this->shouldRouteBusinessTrafficToControlPlane() ? 'control' : 'business';

        return [
            'bin' => (string)($runtimeMeta['bin'] ?? 'nginx'),
            'conf' => (string)($runtimeMeta['conf_path'] ?? '--'),
            'dir' => (string)($runtimeMeta['conf_dir'] ?? '--'),
            'server' => 'listen:' . $listenPort
                . ', server_name:' . ($serverName !== '' ? $serverName : '_')
                . ', default_upstream:' . $defaultUpstream
                . ', control_paths:/dashboard.socket,/_gateway,/~',
            'body' => $this->formatBytes(max(0, $bodyBytes)) . ' (' . $this->clientMaxBodySizeDirectiveValue() . ')',
            'proxy' => 'buffer:' . ($buffering ? 'on' : 'off')
                . ', conn:' . $proxyConnectTimeout . 's'
                . ', io:' . $proxyIoTimeout . 's'
                . ', client:' . $clientIoTimeout . 's',
            'upstream' => 'workers:' . $workerProcesses
                . ', business_keepalive:' . $businessKeepalive
                . ', control_keepalive:' . $controlKeepalive,
            'log' => 'http:' . ($disableLogs ? 'off' : 'keep')
                . ', error:' . ($disableLogs ? '/dev/null crit' : 'keep')
                . ', static:off',
            'realip' => ($realIpHeader !== '' ? $realIpHeader : '--')
                . ', recursive:' . ($realIpRecursive ? 'on' : 'off')
                . ', trusted:' . ($trustedProxies ? implode(',', $trustedProxies) : '--'),
        ];
    }

    /**
     * 校验当前 nginx 生效配置里已经包含 gateway 生成的 include 文件。
     *
     * 这里使用 `nginx -T` 读取当前完整配置展开结果。如果这些文件没有出现在
     * 生效配置里，就说明：
     * 1. 主配置没有 include 到目标目录；
     * 2. reload/start 后入口层仍未加载这批配置；
     * 3. 当前使用的 nginx 与生成文件目录不一致。
     *
     * 无论哪种情况，都必须直接报错而不是继续把“已同步”打印成成功。
     *
     * @param array<int, string> $paths 需要确认已被加载的文件路径。
     * @return void
     * @throws RuntimeException 当当前生效配置中未找到目标文件时抛出。
     */
    protected function validateManagedFilesLoaded(array $paths): void {
        $bin = $this->nginxBin();
        $command = escapeshellarg($bin) . ' -T';
        $confPath = (string)($this->nginxRuntimeMeta()['conf_path'] ?? '');
        if ($confPath !== '') {
            $command .= ' -c ' . escapeshellarg($confPath);
        }
        $command .= ' 2>&1';
        exec($command, $output, $code);
        if ($code !== 0) {
            throw new RuntimeException("nginx 生效配置导出失败:\n" . implode("\n", $output));
        }

        $effectiveConfig = implode("\n", $output);
        $missing = [];
        foreach (array_filter(array_unique($paths)) as $path) {
            $normalizedPath = str_replace('\\', '/', (string)$path);
            if ($normalizedPath === '') {
                continue;
            }
            if (!str_contains($effectiveConfig, $normalizedPath)) {
                $missing[] = $normalizedPath;
            }
        }

        if ($missing) {
            throw new RuntimeException(
                "nginx 尚未加载 gateway 生成的配置文件: " . implode(' | ', $missing)
                . "\nconf-path=" . ($confPath !== '' ? $confPath : '(empty)')
                . "\nconf-dir=" . (string)($this->nginxRuntimeMeta()['conf_dir'] ?? '')
            );
        }
    }

    /**
     * 判断本次同步原因是否需要在 nginx 已运行时强制 reload 一次。
     *
     * 这里主要覆盖 gateway 启动接管场景。启动时 include 文件可能早已存在，
     * `writeIfChanged()` 会判断为“无内容变化”，但 nginx 进程仍可能尚未载入
     * 这些文件，或者载入的是上一版 upstream。对这类原因强制 reload 一次，
     * 可以保证“已同步”确实意味着入口层已经吃到当前配置。
     *
     * @param string|null $reason 本次同步触发原因。
     * @return bool 需要强制 reload 时返回 true。
     */
    protected function shouldForceReloadForSyncReason(?string $reason): bool {
        return in_array((string)$reason, [
            'register_managed_plan_activate',
            'install_takeover',
            'install_takeover_release',
        ], true);
    }

    /**
     * 渲染 nginx http 级别的全局调优配置。
     *
     * @return string 可直接写入 nginx include 文件的全局配置内容。
     */
    protected function renderGlobalConfig(): string {
        $disableLogs = (bool)($this->gateway->serverConfig()['gateway_nginx_disable_global_logs'] ?? true);
        $proxyTimeouts = $this->resolveNginxProxyTimeouts();
        $proxyConnectTimeout = $proxyTimeouts['connect_timeout'];
        $proxyIoTimeout = $proxyTimeouts['business_io_timeout'];
        $clientIoTimeout = $proxyTimeouts['client_io_timeout'];
        $buffering = (bool)($this->gateway->serverConfig()['gateway_nginx_proxy_buffering'] ?? true);
        $variablesHashBucketSize = $this->resolveVariablesHashBucketSize();
        $variablesHashMaxSize = $this->resolveVariablesHashMaxSize();
        $existing = $this->existingHttpDirectives();

        $lines = [
            '# generated by SCF Gateway',
            '# global http-level tuning',
        ];

        if ($disableLogs) {
            $this->appendDirectiveIfMissing($lines, $existing, 'access_log', 'access_log off;');
            $this->appendDirectiveIfMissing($lines, $existing, 'error_log', 'error_log /dev/null crit;');
        }

        $this->appendDirectiveIfMissing($lines, $existing, 'server_tokens', 'server_tokens off;');
        $this->appendDirectiveIfMissing($lines, $existing, 'sendfile', 'sendfile on;');
        $this->appendDirectiveIfMissing($lines, $existing, 'tcp_nopush', 'tcp_nopush on;');
        $this->appendDirectiveIfMissing($lines, $existing, 'tcp_nodelay', 'tcp_nodelay on;');
        $this->appendDirectiveIfMissing($lines, $existing, 'keepalive_timeout', 'keepalive_timeout 30;');
        $this->appendDirectiveIfMissing($lines, $existing, 'keepalive_requests', 'keepalive_requests 10000;');
        $this->appendDirectiveIfMissing($lines, $existing, 'reset_timedout_connection', 'reset_timedout_connection on;');
        $this->appendDirectiveIfMissing($lines, $existing, 'types_hash_max_size', 'types_hash_max_size 4096;');
        // Gateway 生成的 map/proxy 变量会随 app/role/port 扩展，默认 64 在部分机器上
        // 容易触发 "could not build variables_hash"。这里提供稳定的全局兜底。
        $this->appendDirectiveIfMissing($lines, $existing, 'variables_hash_bucket_size', 'variables_hash_bucket_size ' . $variablesHashBucketSize . ';');
        $this->appendDirectiveIfMissing($lines, $existing, 'variables_hash_max_size', 'variables_hash_max_size ' . $variablesHashMaxSize . ';');
        $this->appendDirectiveIfMissing($lines, $existing, 'proxy_http_version', 'proxy_http_version 1.1;');
        $this->appendDirectiveIfMissing($lines, $existing, 'proxy_request_buffering', 'proxy_request_buffering on;');
        $this->appendDirectiveIfMissing($lines, $existing, 'proxy_buffering', 'proxy_buffering ' . ($buffering ? 'on' : 'off') . ';');
        $this->appendDirectiveIfMissing($lines, $existing, 'proxy_socket_keepalive', 'proxy_socket_keepalive on;');
        $this->appendDirectiveIfMissing($lines, $existing, 'proxy_connect_timeout', 'proxy_connect_timeout ' . $proxyConnectTimeout . 's;');
        $this->appendDirectiveIfMissing($lines, $existing, 'proxy_read_timeout', 'proxy_read_timeout ' . $proxyIoTimeout . 's;');
        $this->appendDirectiveIfMissing($lines, $existing, 'proxy_send_timeout', 'proxy_send_timeout ' . $proxyIoTimeout . 's;');
        $this->appendDirectiveIfMissing($lines, $existing, 'client_body_timeout', 'client_body_timeout ' . $clientIoTimeout . 's;');
        $this->appendDirectiveIfMissing($lines, $existing, 'send_timeout', 'send_timeout ' . $clientIoTimeout . 's;');
        $lines[] = '';

        return implode("\n", $lines);
    }

    /**
     * 渲染 upstream 定义。
     *
     * 业务流量永远指向 active generation，控制面流量则单独走本机 gateway。
     *
     * @return string upstream 配置块内容。
     */
    protected function renderUpstreamConfig(): string {
        $activeInstances = $this->activeInstances();
        $businessServers = $this->renderBusinessUpstreamServers($activeInstances);
        $controlHost = $this->gateway->internalControlHost();
        $controlPort = $this->gateway->controlPort();
        $businessName = $this->businessUpstreamName();
        $controlName = $this->controlUpstreamName();
        $businessKeepalive = $this->resolveBusinessUpstreamKeepalive();
        $controlKeepalive = $this->resolveControlUpstreamKeepalive();

        return implode("\n", [
            '# generated by SCF Gateway',
            '# do not edit manually',
            '# app=' . APP_DIR_NAME . ', role=' . SERVER_ROLE . ', port=' . $this->gateway->businessPort(),
            '',
            'upstream ' . $businessName . ' {',
            $businessServers,
            '    keepalive ' . $businessKeepalive . ';',
            '}',
            '',
            'upstream ' . $controlName . ' {',
            '    server ' . $controlHost . ':' . $controlPort . ' max_fails=1 fail_timeout=2s;',
            '    keepalive ' . $controlKeepalive . ';',
            '}',
            '',
        ]);
    }

    /**
     * 渲染 server block。
     *
     * 这里把 `/dashboard.socket`、`/_gateway` 和 `/~` 统一映射到控制面，
     * 其余路径继续走业务 upstream。
     *
     * @return string nginx server block 内容。
     */
    protected function renderServerConfig(): string {
        $serverName = $this->gateway->resolvedGatewayNginxServerName();
        $listenPort = $this->gateway->businessPort();
        $businessName = $this->businessUpstreamName();
        $controlName = $this->controlUpstreamName();
        $defaultUpstream = $this->shouldRouteBusinessTrafficToControlPlane() ? $controlName : $businessName;
        $upgradeMapVar = '$' . $this->safeToken('scf_gateway_connection_upgrade_' . APP_DIR_NAME . '_' . SERVER_ROLE . '_' . $listenPort);
        $clientMaxBodySize = $this->clientMaxBodySizeDirectiveValue();
        $proxyTimeouts = $this->resolveNginxProxyTimeouts();
        $businessIoTimeout = $proxyTimeouts['business_io_timeout'];
        $controlIoTimeout = $proxyTimeouts['control_io_timeout'];
        $dashboardSocketIoTimeout = $proxyTimeouts['dashboard_socket_io_timeout'];
        $realIpHeader = trim((string)($this->gateway->serverConfig()['gateway_nginx_real_ip_header'] ?? 'X-Forwarded-For'));
        $realIpRecursive = (bool)($this->gateway->serverConfig()['gateway_nginx_real_ip_recursive'] ?? true);
        $trustedProxies = $this->trustedRealIpProxies();
        $realIpLines = [];
        if ($realIpHeader !== '' && $trustedProxies) {
            $realIpLines[] = '    real_ip_header ' . $realIpHeader . ';';
            foreach ($trustedProxies as $proxy) {
                $realIpLines[] = '    set_real_ip_from ' . $proxy . ';';
            }
            $realIpLines[] = '    real_ip_recursive ' . ($realIpRecursive ? 'on' : 'off') . ';';
            $realIpLines[] = '';
        }

        $staticLocationLines = $this->renderStaticLocationBlocks();
        if ($staticLocationLines) {
            $staticLocationLines[] = '';
        }

        return implode("\n", array_merge([
            'map $http_upgrade ' . $upgradeMapVar . ' {',
            "    default upgrade;",
            "    '' '';",
            '}',
            '',
            'server {',
            '    listen ' . $listenPort . ';',
            '    server_name ' . $serverName . ';',
            // Gateway 入口最终仍由应用 HTTP 服务接收请求体，因此这里把 nginx 的
            // body 限制对齐到应用 server 配置，避免入口层和业务进程对“大请求”
            // 的判定不一致，出现 nginx 已拦截或后端已断开其中一层先失败的情况。
            '    client_max_body_size ' . $clientMaxBodySize . ';',
            // gateway 往往监听在 9580 这类内部端口，外层再由公网域名反代接管。
            // 一旦 nginx 在这个 server 块里自己生成绝对重定向（目录补斜杠、规范化等），
            // 浏览器就会被带到 `host:9580`。这里统一关闭绝对跳转和端口拼接，让任何
            // 内部重定向都保持相对路径，不暴露内部监听端口。
            '    absolute_redirect off;',
            '    port_in_redirect off;',
            '',
        ], $realIpLines, [
            // `/dashboard.socket` 和 `/_gateway` 都属于控制面，必须绕过业务 upstream。
            '    location ~ /dashboard\\.socket$ {',
            '        proxy_http_version 1.1;',
            '        proxy_set_header Host $host;',
            '        proxy_set_header Upgrade $http_upgrade;',
            '        proxy_set_header Connection ' . $upgradeMapVar . ';',
            '        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;',
            '        proxy_set_header X-Forwarded-Host $host;',
            '        proxy_set_header X-Forwarded-Port $server_port;',
            '        proxy_set_header X-Forwarded-Proto $scheme;',
            '        proxy_set_header X-Real-IP $remote_addr;',
            '        proxy_read_timeout ' . $dashboardSocketIoTimeout . 's;',
            '        proxy_send_timeout ' . $dashboardSocketIoTimeout . 's;',
            '        proxy_pass http://' . $controlName . ';',
            '    }',
            '',
            '    location ^~ /_gateway/ {',
            '        proxy_http_version 1.1;',
            '        proxy_set_header Host $host;',
            '        proxy_set_header X-Real-IP $remote_addr;',
            '        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;',
            '        proxy_set_header X-Forwarded-Host $host;',
            '        proxy_set_header X-Forwarded-Port $server_port;',
            '        proxy_set_header X-Forwarded-Proto $scheme;',
            '        proxy_read_timeout ' . $controlIoTimeout . 's;',
            '        proxy_send_timeout ' . $controlIoTimeout . 's;',
            '        proxy_pass http://' . $controlName . ';',
            '    }',
            '',
            '    location ^~ /~ {',
            '        proxy_http_version 1.1;',
            '        proxy_set_header Host $host;',
            '        proxy_set_header X-Real-IP $remote_addr;',
            '        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;',
            '        proxy_set_header X-Forwarded-Host $host;',
            '        proxy_set_header X-Forwarded-Port $server_port;',
            '        proxy_set_header X-Forwarded-Proto $scheme;',
            '        proxy_read_timeout ' . $controlIoTimeout . 's;',
            '        proxy_send_timeout ' . $controlIoTimeout . 's;',
            '        proxy_pass http://' . $controlName . ';',
            '    }',
            '',
        ], $staticLocationLines, [
            '    location / {',
            '        proxy_http_version 1.1;',
            '        proxy_set_header Upgrade $http_upgrade;',
            '        proxy_set_header Connection ' . $upgradeMapVar . ';',
            '        proxy_set_header Host $host;',
            '        proxy_set_header X-Real-IP $remote_addr;',
            '        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;',
            '        proxy_set_header X-Forwarded-Host $host;',
            '        proxy_set_header X-Forwarded-Port $server_port;',
            '        proxy_set_header X-Forwarded-Proto $scheme;',
            '        proxy_read_timeout ' . $businessIoTimeout . 's;',
            '        proxy_send_timeout ' . $businessIoTimeout . 's;',
            '        proxy_pass http://' . $defaultUpstream . ';',
            '    }',
            '}',
            '',
        ]));
    }

    /**
     * 解析业务 upstream keepalive 连接池容量。
     *
     * 默认值会按 `max_connection` 与 nginx worker 进程数动态推导，避免在
     * “worker 数较多 + 固定 keepalive”场景下把 upstream 长连接池堆到超出
     * 业务面承载能力。
     *
     * @return int
     */
    protected function resolveBusinessUpstreamKeepalive(): int {
        $configured = (int)($this->gateway->serverConfig()['gateway_nginx_upstream_keepalive'] ?? 0);
        if ($configured > 0) {
            return $this->normalizeKeepaliveSize($configured, 8, 512);
        }

        $maxConnection = max(256, (int)($this->gateway->serverConfig()['max_connection'] ?? 4096));
        $workerProcesses = $this->resolveNginxWorkerProcessCount();
        $budgetRatio = (float)($this->gateway->serverConfig()['gateway_nginx_keepalive_capacity_ratio'] ?? 0.5);
        $budgetRatio = max(0.1, min(0.9, $budgetRatio));
        $keepaliveBudget = max(8, (int)floor($maxConnection * $budgetRatio));
        $perWorker = max(8, (int)floor($keepaliveBudget / max(1, $workerProcesses)));
        return $this->normalizeKeepaliveSize($perWorker, 8, 512);
    }

    /**
     * 解析控制面 upstream keepalive 容量。
     *
     * @return int
     */
    protected function resolveControlUpstreamKeepalive(): int {
        $configured = (int)($this->gateway->serverConfig()['gateway_nginx_control_keepalive'] ?? 16);
        return $this->normalizeKeepaliveSize($configured, 4, 128);
    }

    /**
     * 统一解析 nginx 代理超时配置。
     *
     * @return array{connect_timeout:int,business_io_timeout:int,client_io_timeout:int,control_io_timeout:int,dashboard_socket_io_timeout:int}
     */
    protected function resolveNginxProxyTimeouts(): array {
        $connectTimeout = max(1, (int)($this->gateway->serverConfig()['gateway_nginx_proxy_connect_timeout'] ?? 3));
        // 保持全局默认向后兼容：未显式配置时仍沿用旧值；应用可通过配置覆盖成更短的超时。
        $businessIoTimeout = max(60, (int)($this->gateway->serverConfig()['gateway_nginx_proxy_io_timeout'] ?? 31536000));
        $clientIoTimeout = max(30, (int)($this->gateway->serverConfig()['gateway_nginx_client_io_timeout'] ?? $businessIoTimeout));
        $controlIoTimeout = max(10, (int)($this->gateway->serverConfig()['gateway_nginx_control_io_timeout'] ?? min(120, $businessIoTimeout)));
        $dashboardSocketIoTimeout = max(60, (int)($this->gateway->serverConfig()['gateway_nginx_dashboard_socket_io_timeout'] ?? $businessIoTimeout));
        return [
            'connect_timeout' => $connectTimeout,
            'business_io_timeout' => $businessIoTimeout,
            'client_io_timeout' => $clientIoTimeout,
            'control_io_timeout' => $controlIoTimeout,
            'dashboard_socket_io_timeout' => $dashboardSocketIoTimeout,
        ];
    }

    /**
     * 解析 nginx variables_hash_bucket_size。
     *
     * Nginx bucket_size 对齐更偏好 2 的幂，且过小会在 `nginx -t` 阶段直接失败。
     * 这里统一做最小值和幂次归一，降低环境差异带来的配置抖动。
     *
     * @return int
     */
    protected function resolveVariablesHashBucketSize(): int {
        $configured = (int)($this->gateway->serverConfig()['gateway_nginx_variables_hash_bucket_size'] ?? 128);
        $target = max(64, $configured);
        $normalized = 64;
        while ($normalized < $target && $normalized < 4096) {
            $normalized <<= 1;
        }
        return min(4096, $normalized);
    }

    /**
     * 解析 nginx variables_hash_max_size。
     *
     * @return int
     */
    protected function resolveVariablesHashMaxSize(): int {
        $configured = (int)($this->gateway->serverConfig()['gateway_nginx_variables_hash_max_size'] ?? 2048);
        return max(1024, min(65536, $configured));
    }

    /**
     * 归一化 keepalive 数值边界。
     *
     * @param int $value
     * @param int $min
     * @param int $max
     * @return int
     */
    protected function normalizeKeepaliveSize(int $value, int $min, int $max): int {
        return max($min, min($max, $value));
    }

    /**
     * 推导 nginx worker 进程数。
     *
     * 优先读取显式配置，其次解析 nginx 主配置中的 `worker_processes`。
     * 配置为 `auto` 时按本机 CPU 数回退。
     *
     * @return int
     */
    protected function resolveNginxWorkerProcessCount(): int {
        $configured = (int)($this->gateway->serverConfig()['gateway_nginx_worker_processes'] ?? 0);
        if ($configured > 0) {
            return $configured;
        }
        $confPath = trim((string)($this->nginxRuntimeMeta()['conf_path'] ?? ''));
        if ($confPath === '' || !is_file($confPath)) {
            return 1;
        }
        $content = (string)@file_get_contents($confPath);
        if ($content === '' || !preg_match('/^\\s*worker_processes\\s+([^;]+);/m', $content, $matches)) {
            return 1;
        }
        $rawValue = trim((string)($matches[1] ?? ''));
        if ($rawValue === '') {
            return 1;
        }
        if (strcasecmp($rawValue, 'auto') === 0) {
            $cpuNum = function_exists('swoole_cpu_num') ? (int)swoole_cpu_num() : 0;
            return max(1, $cpuNum);
        }
        return max(1, (int)$rawValue);
    }

    /**
     * 判断业务入口是否应暂时回切到 gateway 控制面。
     *
     * 应用尚未安装完成时，外部入口不能继续指向一个并不存在的 business upstream。
     * 此时 nginx 需要先把业务入口绑定回 gateway 自身，由控制面承接安装页与安装流程，
     * 等业务实例真正 ready 后再切回 active upstream。
     *
     * @return bool
     */
    protected function shouldRouteBusinessTrafficToControlPlane(): bool {
        if (!App::isReady()) {
            return true;
        }
        return (bool)(Runtime::instance()->get(Key::RUNTIME_GATEWAY_INSTALL_TAKEOVER) ?? false);
    }

    /**
     * 生成示例 server 配置，供手工启用或调试参考。
     *
     * @return string 示例配置内容。
     */
    protected function renderExampleServerConfig(): string {
        return "# example only; rename to .conf when ready to enable\n" . $this->renderServerConfig();
    }

    /**
     * 把 active upstream 列表翻译成 nginx upstream server 行。
     *
     * @param array $instances 当前 active generation 的实例列表。
     * @return string upstream 内的 server 声明文本。
     */
    protected function renderBusinessUpstreamServers(array $instances): string {
        if (!$instances) {
            return '    server 127.0.0.1:9 down;';
        }

        // nginx 只消费 active generation 的可用实例，不保留旧代。
        $lines = [];
        foreach ($instances as $instance) {
            $host = (string)($instance['host'] ?? '127.0.0.1');
            $port = (int)($instance['port'] ?? 0);
            if ($port <= 0) {
                continue;
            }
            $weight = max(1, (int)($instance['weight'] ?? 1));
            $lines[] = '    server ' . $host . ':' . $port . ' weight=' . $weight . ' max_fails=1 fail_timeout=2s;';
        }

        return $lines ? implode("\n", $lines) : '    server 127.0.0.1:9 down;';
    }

    /**
     * 为静态资源路由生成额外的 location block。
     *
     * 这里不仅要让 gateway 入口把 `/cp` 这类前端目录直接交给 nginx 本地文件系统，
     * 还要避免 nginx 在“目录缺少尾随斜杠”时按监听端口生成绝对重定向。
     *
     * 业务入口通常监听在 9580 等内部端口，外层再由域名 nginx/SLB 接管。如果让
     * nginx 自己为目录补斜杠，浏览器会收到类似 `https://host:9580/cp/` 的绝对
     * Location，导致用户被错误地带到内部端口。因此目录型静态入口要由我们显式
     * 返回相对跳转，只补路径，不暴露内部端口。
     *
     * @return array nginx location block 行数组。
     */
    protected function renderStaticLocationBlocks(): array {
        $locations = (array)($this->gateway->serverConfig()['static_handler_locations'] ?? []);
        if (!$locations) {
            return [];
        }

        $publicRoot = rtrim(APP_PATH, '/') . '/public';
        $blocks = [];
        foreach ($locations as $location) {
            $path = trim((string)$location);
            if ($path === '' || $path[0] !== '/') {
                continue;
            }
            if ($this->gateway->shouldRoutePathToControlPlane($path)) {
                continue;
            }

            // 静态资源路由只负责本地文件命中，不参与 upstream 切流。
            if ($this->looksLikeStaticFile($path)) {
                $disableCache = $this->shouldDisableStaticCache($path);
                $blocks[] = '    location = ' . $path . ' {';
                $blocks[] = '        root ' . $publicRoot . ';';
                $blocks[] = '        access_log off;';
                $blocks[] = '        log_not_found off;';
                if ($disableCache) {
                    $blocks[] = '        etag off;';
                    $blocks[] = '        if_modified_since off;';
                    $blocks[] = '        expires -1;';
                    $blocks[] = '        add_header Cache-Control "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0" always;';
                    $blocks[] = '        add_header Pragma "no-cache" always;';
                }
                $blocks[] = '        try_files $uri =404;';
                $blocks[] = '    }';
                continue;
            }

            $normalized = rtrim($path, '/');
            if ($normalized === '') {
                $normalized = '/';
            }
            $blocks[] = '    location = ' . $normalized . ' {';
            $blocks[] = '        root ' . $publicRoot . ';';
            $blocks[] = '        access_log off;';
            $blocks[] = '        log_not_found off;';
            $blocks[] = '        etag off;';
            $blocks[] = '        if_modified_since off;';
            $blocks[] = '        expires -1;';
            $blocks[] = '        add_header Cache-Control "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0" always;';
            $blocks[] = '        add_header Pragma "no-cache" always;';
            // 静态目录入口直接命中目录下 index.html，不再让 nginx 走“补尾斜杠”
            // 的默认目录规范化流程。那条流程会按当前监听端口生成绝对 Location，
            // 从而把 9580 之类的内部端口暴露给浏览器。
            $blocks[] = '        try_files $uri/index.html =404;';
            $blocks[] = '    }';
            $blocks[] = '';
            $blocks[] = '    location = ' . $normalized . '/ {';
            $blocks[] = '        root ' . $publicRoot . ';';
            $blocks[] = '        access_log off;';
            $blocks[] = '        log_not_found off;';
            $blocks[] = '        etag off;';
            $blocks[] = '        if_modified_since off;';
            $blocks[] = '        expires -1;';
            $blocks[] = '        add_header Cache-Control "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0" always;';
            $blocks[] = '        add_header Pragma "no-cache" always;';
            // `/cp/` 这种带尾斜杠的目录首页必须显式命中 index.html。
            // 如果把它交给下面的前缀 location，再依赖 nginx 的目录/index 推断，
            // 不同环境下可能落成 404 或命中其它规则，表现就是“有时能开有时 404”。
            $blocks[] = '        try_files $uri/index.html =404;';
            $blocks[] = '    }';
            $blocks[] = '';
            $blocks[] = '    location ' . $normalized . '/ {';
            $blocks[] = '        root ' . $publicRoot . ';';
            $blocks[] = '        access_log off;';
            $blocks[] = '        log_not_found off;';
            $blocks[] = '        try_files $uri $uri/ =404;';
            $blocks[] = '    }';
        }

        return $blocks ? array_merge($blocks, ['']) : [];
    }

    /**
     * 判断路由是否更像静态文件而非目录前缀。
     *
     * @param string $path nginx location 路径。
     * @return bool 命中带扩展名的静态资源时返回 true。
     */
    protected function looksLikeStaticFile(string $path): bool {
        $basename = basename($path);
        return $basename !== '' && str_contains($basename, '.');
    }

    /**
     * 计算写入 nginx `client_max_body_size` 的值。
     *
     * gateway 入口最终转发给同一套应用 HTTP 服务处理，因此这里直接复用应用
     * `server.php` 的 `package_max_length` 作为请求体上限。这样可以让 nginx 与
     * 后端 Swoole 对允许的最大请求体保持一致，避免两层限制不一致导致排障困难。
     *
     * @return string nginx 可接受的大小文本，默认返回字节数；0 表示不限制。
     */
    protected function clientMaxBodySizeDirectiveValue(): string {
        $packageMaxLength = (int)($this->gateway->serverConfig()['package_max_length'] ?? 10 * 1024 * 1024);
        if ($packageMaxLength <= 0) {
            return '0';
        }

        return (string)$packageMaxLength;
    }

    /**
     * 以人类可读的方式格式化字节数，便于启动日志快速阅读。
     *
     * @param int $bytes 原始字节数。
     * @return string 格式化后的大小文本。
     */
    protected function formatBytes(int $bytes): string {
        if ($bytes <= 0) {
            return '0 B';
        }

        $units = ['B', 'KB', 'MB', 'GB', 'TB'];
        $value = (float)$bytes;
        $unitIndex = 0;
        while ($value >= 1024 && $unitIndex < count($units) - 1) {
            $value /= 1024;
            $unitIndex++;
        }

        $precision = $value >= 100 || $unitIndex === 0 ? 0 : 2;
        return number_format($value, $precision, '.', '') . ' ' . $units[$unitIndex];
    }

    /**
     * 判断一条静态路径是否应关闭缓存。
     *
     * 目录首页和明确声明的 HTML/TXT 入口通常承载的是后台面板或校验文件，内容变更
     * 后希望立即生效；而 `/asset/*` 这类资源目录应继续保留 nginx 默认缓存语义。
     *
     * @param string $path 静态路径。
     * @return bool
     */
    protected function shouldDisableStaticCache(string $path): bool {
        $extension = strtolower(pathinfo($path, PATHINFO_EXTENSION));
        return in_array($extension, ['html', 'htm', 'txt', 'json', 'webmanifest'], true);
    }

    /**
     * 读取当前 active generation 下可用于对外承载流量的实例。
     *
     * @return array 过滤后的 active 实例列表。
     */
    protected function activeInstances(): array {
        $state = $this->instanceManager->snapshot();
        $activeVersion = (string)($state['active_version'] ?? '');
        if ($activeVersion === '') {
            return [];
        }
        $instances = (array)($state['generations'][$activeVersion]['instances'] ?? []);
        return array_values(array_filter($instances, static function (array $instance) {
            return in_array((string)($instance['status'] ?? 'offline'), ['active', 'prepared'], true)
                && (int)($instance['port'] ?? 0) > 0;
        }));
    }

    /**
     * upstream 名称需要稳定且可作为 nginx 标识符。
     *
     * @return string 业务 upstream 名称。
     */
    protected function businessUpstreamName(): string {
        return $this->safeToken('scf_gateway_business_' . APP_DIR_NAME . '_' . SERVER_ROLE . '_' . $this->gateway->businessPort());
    }

    /**
     * 控制面 upstream 名称。
     *
     * @return string 控制面 upstream 名称。
     */
    protected function controlUpstreamName(): string {
        return $this->safeToken('scf_gateway_control_' . APP_DIR_NAME . '_' . SERVER_ROLE . '_' . $this->gateway->businessPort());
    }

    /**
     * 生成的业务 upstream 配置文件路径。
     *
     * @return string upstream 配置文件路径。
     */
    protected function generatedUpstreamFile(): string {
        return rtrim($this->confDir(), '/') . '/' . $this->fileBaseName() . '.conf';
    }

    /**
     * 生成的 nginx 全局配置文件路径。
     *
     * @return string 全局配置文件路径。
     */
    protected function generatedGlobalFile(): string {
        return rtrim($this->confDir(), '/') . '/scf_gateway.global.conf';
    }

    /**
     * 生成的 server block 文件路径。
     *
     * @return string server 配置文件路径。
     */
    protected function generatedServerFile(): string {
        return rtrim($this->confDir(), '/') . '/' . $this->fileBaseName() . '.server.conf';
    }

    /**
     * 示例 server 配置，便于手工启用或调试。
     *
     * @return string 示例配置文件路径。
     */
    protected function generatedExampleServerFile(): string {
        return rtrim($this->confDir(), '/') . '/' . $this->fileBaseName() . '.server.example';
    }

    /**
     * nginx 配置输出目录。
     *
     * 默认以当前 nginx 主配置推导出的 include 目录为准；只有在运维显式指定
     * `gateway_nginx_conf_dir` 时，才允许用配置值覆盖自动探测结果，专门处理
     * 多个 include 目录同时存在且无法靠语义唯一判定的环境。
     *
     * @return string 实际使用的 nginx conf.d/servers 目录。
     */
    protected function confDir(): string {
        return rtrim((string)($this->nginxRuntimeMeta()['conf_dir'] ?? '/etc/nginx/conf.d'), '/');
    }

    /**
     * nginx 可执行文件路径。
     *
     * @return string nginx 可执行文件路径。
     */
    protected function nginxBin(): string {
        $configured = trim((string)($this->gateway->serverConfig()['gateway_nginx_bin'] ?? ''));
        if ($configured !== '') {
            return $configured;
        }
        return (string)($this->nginxRuntimeMeta()['bin'] ?? 'nginx');
    }

    /**
     * 生成配置文件的基础名。
     *
     * 约束：同一台机器上，同一个业务入口端口只允许存在一套 gateway nginx
     * 转发配置文件。因此基础名只收敛到端口维度，确保“新 app/新进程接管同端口”
     * 时直接覆盖旧文件，避免同端口并存导致入口命中不确定。
     *
     * @return string 配置文件基名。
     */
    protected function fileBaseName(): string {
        return $this->safeToken('scf_upsteam_' . (int)$this->gateway->businessPort());
    }

    /**
     * 清理与当前 gateway 端口冲突的历史 managed 配置文件。
     *
     * 历史版本文件名可能包含 app/role（例如 `scf_gateway_{app}_{role}_{port}`），
     * 也可能是当前端口收敛命名（`scf_upsteam_{port}`）。只要文件解析出的端口与
     * 当前 businessPort 一致、且不是当前基础名，就视作冲突并删除。
     *
     * @return bool 本轮是否实际清理了冲突文件
     */
    protected function cleanupConflictingManagedConfigFiles(): bool {
        $confDir = $this->confDir();
        if (!is_dir($confDir)) {
            return false;
        }

        $currentBase = $this->fileBaseName();
        $port = (int)$this->gateway->businessPort();
        if ($port <= 0) {
            return false;
        }

        $patterns = [
            $confDir . '/scf_gateway_*.server.conf',
            $confDir . '/scf_gateway_*.upstreams.conf',
            $confDir . '/scf_gateway_*.server.example',
            $confDir . '/scf_upsteam_*.conf',
            $confDir . '/scf_upsteam_*.server.conf',
            $confDir . '/scf_upsteam_*.server.example',
        ];

        $removed = [];
        foreach ($patterns as $pattern) {
            $matches = glob($pattern) ?: [];
            foreach ($matches as $path) {
                if (!is_file($path)) {
                    continue;
                }
                $basename = basename($path);
                $filePort = $this->extractManagedConfigPortFromBasename($basename);
                if ($filePort !== $port) {
                    continue;
                }
                if (str_starts_with($basename, $currentBase . '.')) {
                    continue;
                }
                if (@unlink($path)) {
                    $removed[] = $basename;
                }
            }
        }

        if ($removed) {
            Console::warning(
                '【Gateway】检测到并清理同端口历史nginx配置冲突: port=' . $port
                . ', files=' . implode(' | ', array_values(array_unique($removed)))
            );
            return true;
        }
        return false;
    }

    /**
     * 从 managed 配置文件名中提取业务端口。
     *
     * 兼容两种命名：
     * - 新版：`scf_upsteam_{port}.conf`
     * - 旧版：`scf_gateway_{app}_{role}_{port}.server.conf`
     *
     * @param string $basename 文件名（不含路径）
     * @return int 解析成功返回端口，失败返回 0
     */
    protected function extractManagedConfigPortFromBasename(string $basename): int {
        $basename = trim($basename);
        if ($basename === '') {
            return 0;
        }

        if (preg_match('/^scf_gateway_(\d+)\.(?:server\.conf|upstreams\.conf|server\.example)$/', $basename, $matches)) {
            return max(0, (int)($matches[1] ?? 0));
        }

        if (preg_match('/^scf_upsteam_(\d+)\.(?:conf|server\.conf|server\.example)$/', $basename, $matches)) {
            return max(0, (int)($matches[1] ?? 0));
        }

        if (preg_match('/^scf_gateway_.+_(\d+)\.(?:server\.conf|upstreams\.conf|server\.example)$/', $basename, $matches)) {
            return max(0, (int)($matches[1] ?? 0));
        }

        return 0;
    }

    /**
     * 内容变化时才写盘，减少无意义的 reload。
     *
     * @param string $path 目标文件路径。
     * @param string $content 需要写入的配置内容。
     * @return bool 实际发生写入时返回 true。
     * @throws RuntimeException 当目录创建或写盘失败时抛出。
     */
    protected function writeIfChanged(string $path, string $content): bool {
        $dir = dirname($path);
        if (!is_dir($dir) && !@mkdir($dir, 0777, true) && !is_dir($dir)) {
            throw new RuntimeException('创建nginx配置目录失败: ' . $dir);
        }
        $existing = is_file($path) ? (string)file_get_contents($path) : null;
        if ($existing === $content) {
            return false;
        }
        if (@file_put_contents($path, $content) === false) {
            throw new RuntimeException('写入nginx配置失败: ' . $path);
        }
        return true;
    }

    /**
     * 执行 `nginx -t`，确保新配置可被当前运行环境接受。
     *
     * @return void
     * @throws RuntimeException 当 nginx 配置检测失败时抛出。
     */
    protected function testNginxConfig(): void {
        $bin = $this->nginxBin();
        $command = escapeshellarg($bin) . ' -t';
        $confPath = (string)($this->nginxRuntimeMeta()['conf_path'] ?? '');
        if ($confPath !== '') {
            $command .= ' -c ' . escapeshellarg($confPath);
        }
        $command .= ' 2>&1';
        exec($command, $output, $code);
        if ($code !== 0) {
            throw new RuntimeException("nginx 配置检测失败:\n" . implode("\n", $output));
        }
    }

    /**
     * 重载已经运行的 nginx。
     *
     * @return void
     * @throws RuntimeException 当 nginx reload 失败时抛出。
     */
    protected function reloadNginx(): void {
        $bin = $this->nginxBin();
        $command = escapeshellarg($bin) . ' -s reload';
        $confPath = (string)($this->nginxRuntimeMeta()['conf_path'] ?? '');
        if ($confPath !== '') {
            $command .= ' -c ' . escapeshellarg($confPath);
        }
        $command .= ' 2>&1';
        exec($command, $output, $code);
        if ($code !== 0) {
            throw new RuntimeException("nginx 重载失败:\n" . implode("\n", $output));
        }
    }

    /**
     * 在 nginx 尚未运行时启动它。
     *
     * @return void
     * @throws RuntimeException 当 nginx 启动失败时抛出。
     */
    protected function startNginx(): void {
        $bin = $this->nginxBin();
        $command = escapeshellarg($bin);
        $confPath = (string)($this->nginxRuntimeMeta()['conf_path'] ?? '');
        if ($confPath !== '') {
            $command .= ' -c ' . escapeshellarg($confPath);
        }
        $command .= ' 2>&1';
        exec($command, $output, $code);
        if ($code !== 0) {
            throw new RuntimeException("nginx 启动失败:\n" . implode("\n", $output));
        }
    }

    /**
     * 以 reload 优先、start 兜底的方式让配置生效。
     *
     * @return void
     */
    protected function reloadOrStartNginx(): void {
        if ($this->isNginxRunning()) {
            $this->reloadNginx();
            return;
        }
        $this->startNginx();
    }

    /**
     * 判断 nginx 主进程是否存在。
     *
     * @return bool nginx 进程可访问时返回 true。
     */
    protected function isNginxRunning(): bool {
        $pidPath = (string)($this->nginxRuntimeMeta()['pid_path'] ?? '');
        if ($pidPath === '') {
            $pidPath = '/run/nginx.pid';
        }
        $pid = is_file($pidPath) ? (int)trim((string)file_get_contents($pidPath)) : 0;
        if ($pid <= 0) {
            return false;
        }
        return function_exists('posix_kill') ? @posix_kill($pid, 0) : is_dir('/proc/' . $pid);
    }

    /**
     * 决定 sync 后是否真的执行 nginx reload/start。
     *
     * @return bool 当前配置要求 nginx 同步生效时返回 true。
     */
    protected function shouldReloadNginx(): bool {
        return $this->gateway->nginxProxyModeEnabled()
            || (bool)($this->gateway->serverConfig()['gateway_nginx_reload_on_sync'] ?? false);
    }

    /**
     * 判断正式 server 配置是否已经存在。
     *
     * @return bool 正式 server 配置文件存在时返回 true。
     */
    protected function nginxServerFileExists(): bool {
        return is_file($this->generatedServerFile());
    }

    /**
     * 生成适合写入 nginx 配置的标识符。
     *
     * @param string $value 原始字符串。
     * @return string 只包含 nginx 可接受字符的标识符。
     */
    protected function safeToken(string $value): string {
        $value = preg_replace('/[^a-zA-Z0-9_]+/', '_', $value) ?: 'scf_gateway';
        return trim($value, '_');
    }

    /**
     * 解析 real_ip 可信代理白名单。
     *
     * @return array 可信代理地址列表。
     */
    protected function trustedRealIpProxies(): array {
        $configured = $this->gateway->serverConfig()['gateway_nginx_trusted_proxies'] ?? ['127.0.0.1', '::1'];
        if (is_string($configured)) {
            $configured = array_map('trim', explode(',', $configured));
        }
        if (!is_array($configured)) {
            return ['127.0.0.1', '::1'];
        }
        $trusted = [];
        foreach ($configured as $proxy) {
            $proxy = trim((string)$proxy);
            if ($proxy !== '') {
                $trusted[] = $proxy;
            }
        }
        return $trusted ?: ['127.0.0.1', '::1'];
    }

    /**
     * 获取 nginx 运行时元数据。
     *
     * @return array nginx bin / conf / pid 等运行时信息。
     */
    protected function nginxRuntimeMeta(): array {
        if ($this->nginxRuntimeMeta !== null) {
            return $this->nginxRuntimeMeta;
        }

        $meta = $this->detectRuntimeMeta();
        // runtime meta 缓存只作为调试与观测记录，不再参与目录决策。
        // conf_dir 必须来自当前 nginx.conf 的 fresh 解析结果，避免历史缓存把
        // 错误目录长期固化；如果本次解析失败，应直接报错而不是悄悄回退。
        $this->writeRuntimeMetaCache($meta);
        $this->runtimeMetaLoadedFromCache = false;
        return $this->nginxRuntimeMeta = $meta;
    }

    /**
     * 从 `nginx -V` 推导当前环境下的 bin / conf / pid 路径。
     *
     * @return array 运行时元数据。
     */
    protected function detectRuntimeMeta(): array {
        $running = $this->detectRunningNginxMeta();
        $bin = $running['bin'] !== '' ? $running['bin'] : $this->detectNginxBinary();
        $meta = [
            'bin' => $bin,
            'conf_path' => (string)($running['conf_path'] ?? ''),
            'pid_path' => '',
            'conf_dir' => '',
        ];

        $command = escapeshellarg($bin) . ' -V 2>&1';
        exec($command, $output, $code);
        if ($code === 0 || $output) {
            $versionInfo = implode("\n", $output);
            if ($meta['conf_path'] === '') {
                $meta['conf_path'] = $this->extractNginxBuildArg($versionInfo, 'conf-path');
            }
            $meta['pid_path'] = $this->extractNginxBuildArg($versionInfo, 'pid-path');
        }

        // 双 nginx / 面板改造环境里，http include 目录可能同时存在 conf.d 与
        // sites-enabled。此时如果运维已经明确指定输出目录，应优先尊重显式配置，
        // 避免自动探测在“多个都像候选”的情况下直接中断整条切流链路。
        $configuredConfDir = $this->configuredNginxConfDir((string)$meta['conf_path']);
        if ($configuredConfDir !== '') {
            $meta['conf_dir'] = $configuredConfDir;
            return $meta;
        }

        $meta['conf_dir'] = $this->detectNginxConfDir((string)$meta['conf_path']);
        return $meta;
    }

    /**
     * 解析业务显式指定的 nginx 配置输出目录。
     *
     * `gateway_nginx_conf_dir` 是给运维留的最终裁决开关，专门用于“主配置里存在
     * 多个 http include 候选目录，但当前机器只能确定其中一个才是真正承载业务
     * server/upstream 的目录”这类场景。留空时仍走自动探测；填写后允许用相对
     * nginx.conf 所在目录的相对路径，但仍必须能被当前 `http {}` include 链命中。
     * 这样可以避免把配置写进一个 nginx 根本不会加载的目录里。
     *
     * @param string $confPath nginx 主配置路径。
     * @return string 显式指定的输出目录；未配置时返回空字符串。
     */
    protected function configuredNginxConfDir(string $confPath): string {
        $configured = trim((string)($this->gateway->serverConfig()['gateway_nginx_conf_dir'] ?? ''));
        if ($configured === '') {
            return '';
        }

        if (!str_starts_with($configured, '/')) {
            $baseDir = $confPath !== '' ? dirname($confPath) : '';
            $configured = $baseDir !== ''
                ? rtrim($baseDir, '/') . '/' . ltrim($configured, '/')
                : $configured;
        }

        $configured = rtrim($configured, '/');
        $includeMeta = $this->inspectNginxHttpIncludes($confPath);
        $candidates = $includeMeta['wildcard_candidates'];
        if (in_array($configured, $candidates, true)) {
            return $configured;
        }

        throw new RuntimeException(
            "gateway_nginx_conf_dir 未被 nginx http include 链引用: {$configured}"
            . (!empty($candidates) ? "\nhttp include candidates: " . implode(' | ', $candidates) : '')
            . (!empty($includeMeta['declared_includes']) ? "\nhttp declared includes: " . implode(' | ', $includeMeta['declared_includes']) : '')
        );
    }

    /**
     * nginx 运行时元数据缓存文件路径。
     *
     * @return string 缓存文件路径。
     */
    protected function runtimeMetaCacheFile(): string {
        $dir = defined('APP_UPDATE_DIR') ? APP_UPDATE_DIR : (APP_PATH . '/update');
        return rtrim($dir, '/') . '/gateway_nginx_runtime_' . SERVER_ROLE . '_' . $this->gateway->businessPort() . '.json';
    }

    /**
     * 读取 nginx 运行时元数据缓存。
     *
     * @return array|null 缓存可用时返回元数据数组，否则返回 null。
     */
    protected function loadRuntimeMetaCache(): ?array {
        $path = $this->runtimeMetaCacheFile();
        if (!is_file($path)) {
            return null;
        }
        $decoded = json_decode((string)file_get_contents($path), true);
        return is_array($decoded) ? $decoded : null;
    }

    /**
     * 写回 nginx 运行时元数据缓存。
     *
     * @param array $meta 要持久化的运行时元数据。
     * @return void
     */
    protected function writeRuntimeMetaCache(array $meta): void {
        $path = $this->runtimeMetaCacheFile();
        $dir = dirname($path);
        if (!is_dir($dir) && !@mkdir($dir, 0777, true) && !is_dir($dir)) {
            return;
        }
        @file_put_contents($path, json_encode([
            'bin' => (string)($meta['bin'] ?? ''),
            'conf_path' => (string)($meta['conf_path'] ?? ''),
            'pid_path' => (string)($meta['pid_path'] ?? ''),
            'conf_dir' => (string)($meta['conf_dir'] ?? ''),
            'updated_at' => time(),
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT));
    }

    /**
     * 判断缓存下来的 runtime meta 是否仍可用。
     *
     * @param array|null $meta 待检查的运行时元数据。
     * @return bool 可以复用时返回 true。
     */
    protected function isUsableRuntimeMeta(?array $meta): bool {
        if (!is_array($meta)) {
            return false;
        }
        $bin = trim((string)($meta['bin'] ?? ''));
        $confDir = trim((string)($meta['conf_dir'] ?? ''));
        $confPath = trim((string)($meta['conf_path'] ?? ''));
        if ($bin === '' || $confDir === '') {
            return false;
        }
        if (str_starts_with($bin, '/') && !is_file($bin)) {
            return false;
        }
        if ($confPath !== '' && str_starts_with($confPath, '/') && !is_file($confPath)) {
            return false;
        }
        if (!is_dir($confDir)) {
            return false;
        }
        return true;
    }

    /**
     * 探测 nginx 可执行文件。
     *
     * @return string 可执行文件路径或回退命令名。
     */
    protected function detectNginxBinary(): string {
        $candidates = [];
        $fromPath = trim((string)shell_exec('command -v nginx 2>/dev/null'));
        if ($fromPath !== '') {
            $candidates[] = $fromPath;
        }
        $candidates = array_merge($candidates, [
            '/usr/sbin/nginx',
            '/usr/local/sbin/nginx',
            '/usr/local/openresty/nginx/sbin/nginx',
            '/opt/homebrew/opt/nginx/bin/nginx',
        ]);
        foreach (array_unique($candidates) as $candidate) {
            if ($candidate !== '' && is_file($candidate) && is_executable($candidate)) {
                return $candidate;
            }
        }
        return $fromPath !== '' ? $fromPath : 'nginx';
    }

    /**
     * 优先从当前正在运行的 nginx master 进程提取 bin 与 conf 路径。
     *
     * 线上环境经常同时存在“系统 nginx”和“面板/自定义 nginx”两套安装。
     * 如果仅依赖 `command -v nginx`，很容易拿到错误的二进制并进一步解析出
     * 错误的 `conf-path/conf-dir`。这里优先跟随正在运行的 master 进程，确保
     * gateway 写入的是那套实际对外提供服务的 nginx 配置目录。
     *
     * @return array{bin:string, conf_path:string} 运行中 nginx 的关键元数据。
     */
    protected function detectRunningNginxMeta(): array {
        $commands = [
            'ps -eo command= 2>/dev/null',
            'ps ax -o command= 2>/dev/null',
        ];
        foreach ($commands as $command) {
            $output = shell_exec($command);
            if (!is_string($output) || trim($output) === '') {
                continue;
            }
            foreach (preg_split('/\r?\n/', trim($output)) as $line) {
                $line = trim((string)$line);
                if ($line === '' || !str_contains($line, 'nginx: master process')) {
                    continue;
                }
                $meta = $this->parseRunningNginxMasterCommand($line);
                if ($meta['bin'] !== '' || $meta['conf_path'] !== '') {
                    return $meta;
                }
            }
        }
        return ['bin' => '', 'conf_path' => ''];
    }

    /**
     * 解析 nginx master 进程命令行，提取真实 bin 和 `-c` 指定的主配置路径。
     *
     * @param string $line `ps` 输出中的单行命令文本。
     * @return array{bin:string, conf_path:string} 解析结果。
     */
    protected function parseRunningNginxMasterCommand(string $line): array {
        if (!preg_match('/nginx:\s+master process\s+(.+)$/', $line, $matches)) {
            return ['bin' => '', 'conf_path' => ''];
        }
        $commandLine = trim((string)$matches[1]);
        if ($commandLine === '') {
            return ['bin' => '', 'conf_path' => ''];
        }

        $parts = preg_split('/\s+/', $commandLine) ?: [];
        $bin = trim((string)($parts[0] ?? ''));
        if ($bin !== '' && !str_starts_with($bin, '/')) {
            $resolved = trim((string)shell_exec('command -v ' . escapeshellarg($bin) . ' 2>/dev/null'));
            if ($resolved !== '') {
                $bin = $resolved;
            }
        }

        $confPath = '';
        if (preg_match('/(?:^|\s)-c\s+([^\s]+)/', $commandLine, $confMatches)) {
            $confPath = trim((string)$confMatches[1], "\"'");
        }

        return [
            'bin' => $bin,
            'conf_path' => $confPath,
        ];
    }

    /**
     * 从 `nginx -V` 输出里提取构建参数。
     *
     * @param string $versionInfo `nginx -V` 原始输出。
     * @param string $key 构建参数名。
     * @return string 提取到的参数值，未命中时返回空字符串。
     */
    protected function extractNginxBuildArg(string $versionInfo, string $key): string {
        if (preg_match('/--' . preg_quote($key, '/') . '=([^\\s]+)/', $versionInfo, $matches)) {
            return trim((string)$matches[1], "\"'");
        }
        return '';
    }

    /**
     * 从 nginx 主配置的 http include 链里推导配置输出目录。
     *
     * gateway 生成的是 `upstream/server/http-level` 配置，只能放在 `http {}` 上下文
     * 下被 include 的目录里。像 Debian 系常见的 `modules-enabled/*.conf` 这类顶层
     * include 虽然也出现在 nginx.conf 中，但并不处于 http 上下文，不能承载
     * `server_tokens` / `upstream` / `server` 等指令。
     *
     * 因此这里必须先定位 `http {}` 配置块，再只从该块内部的 include 指令里挑选
     * 目录通配路径；没有明确命中时直接报错，不允许擅自回退。
     *
     * @param string $confPath nginx 主配置文件路径。
     * @return string 可用的 nginx 配置目录。
     * @throws RuntimeException 当无法从 nginx.conf 的 http include 链中明确推导目录时抛出。
     */
    protected function detectNginxConfDir(string $confPath): string {
        $includeMeta = $this->inspectNginxHttpIncludes($confPath);
        $declaredIncludes = $includeMeta['declared_includes'];
        $wildcardCandidates = $includeMeta['wildcard_candidates'];

        $selected = $this->pickManagedIncludeDir($wildcardCandidates);
        if ($selected !== '') {
            return $selected;
        }

        if (!$declaredIncludes) {
            throw new RuntimeException("nginx 主配置未声明可用的 include 目录: {$confPath}");
        }

        throw new RuntimeException(
            "nginx 主配置 include 未命中现有目录: " . implode(' | ', $declaredIncludes)
        );
    }

    /**
     * 读取 nginx 主配置里 http 上下文声明的 include 目录候选。
     *
     * 这一步只负责抽取 `http {}` 内的 include 原文与目录通配候选，供自动探测
     * 和显式配置校验复用，避免两条路径各自解析导致规则漂移。
     *
     * @param string $confPath nginx 主配置文件路径。
     * @return array{declared_includes: array<int, string>, wildcard_candidates: array<int, string>}
     * @throws RuntimeException 当主配置不存在或未找到 http 块时抛出。
     */
    protected function inspectNginxHttpIncludes(string $confPath): array {
        if ($confPath === '') {
            throw new RuntimeException('无法从 nginx -V 中解析 conf-path，不能确定 nginx 配置输出目录');
        }
        if (!is_file($confPath)) {
            throw new RuntimeException("nginx 主配置不存在，无法确定配置输出目录: {$confPath}");
        }

        $confDir = dirname($confPath);
        $content = (string)file_get_contents($confPath);
        $httpBlock = $this->extractHttpBlock($content);
        if ($httpBlock === '') {
            throw new RuntimeException("nginx 主配置未找到 http 配置块: {$confPath}");
        }

        $declaredIncludes = [];
        $wildcardCandidates = [];
        if (preg_match_all('/^\\s*include\\s+([^;]+);/m', $httpBlock, $matches)) {
            foreach ($matches[1] as $include) {
                $include = trim((string)$include, "\"' ");
                if ($include === '') {
                    continue;
                }
                if (!str_starts_with($include, '/')) {
                    $include = $confDir . '/' . $include;
                }
                $declaredIncludes[] = $include;
                $candidate = $this->extractManagedIncludeDir($include);
                if ($candidate !== '') {
                    $wildcardCandidates[] = rtrim($candidate, '/');
                }
            }
        }

        return [
            'declared_includes' => array_values(array_unique($declaredIncludes)),
            'wildcard_candidates' => array_values(array_unique($wildcardCandidates)),
        ];
    }

    /**
     * 提取 nginx 主配置里的 `http { ... }` 原始块内容。
     *
     * 这里只做轻量级括号扫描，不尝试完整解析 nginx 语法。我们的目标只是把
     * 顶层 `http` 上下文裁剪出来，避免把 `events`、`stream`、`modules-enabled`
     * 这类非 http include 误判为 gateway 配置目录。
     *
     * @param string $content nginx 主配置原始文本。
     * @return string `http {}` 内部内容；未找到时返回空字符串。
     */
    protected function extractHttpBlock(string $content): string {
        if (!preg_match('/\\bhttp\\s*\\{/', $content, $matches, PREG_OFFSET_CAPTURE)) {
            return '';
        }

        $matchText = (string)($matches[0][0] ?? '');
        $matchOffset = (int)($matches[0][1] ?? -1);
        if ($matchOffset < 0) {
            return '';
        }

        $openBracePos = strpos($matchText, '{');
        if ($openBracePos === false) {
            return '';
        }

        $blockStart = $matchOffset + $openBracePos;
        $length = strlen($content);
        $depth = 0;
        for ($i = $blockStart; $i < $length; $i++) {
            $char = $content[$i];
            if ($char === '{') {
                $depth++;
                continue;
            }
            if ($char !== '}') {
                continue;
            }
            $depth--;
            if ($depth === 0) {
                return substr($content, $blockStart + 1, $i - $blockStart - 1);
            }
        }

        return '';
    }

    /**
     * 从单条 include 指令里提取适合承载 gateway 配置的目录。
     *
     * 这里只识别“目录通配 include”，例如 `servers/*`、`conf.d/*.conf` 这类。
     * 像 `mime.types` 这种单文件 include 不属于承载扩展配置的目录，必须跳过。
     * 对通配 include 不要求目录预先存在，因为 gateway 后续写盘时会负责创建。
     *
     * @param string $include 已归一化成绝对路径的 include 文本。
     * @return string 命中时返回目标目录，否则返回空字符串。
     */
    protected function extractManagedIncludeDir(string $include): string {
        if (!str_contains($include, '*')) {
            return '';
        }

        $directory = rtrim(dirname($include), '/');
        if ($directory === '' || $directory === '.') {
            return '';
        }

        return $directory;
    }

    /**
     * 从 http include 候选目录里挑选最适合承载 gateway 配置的目录。
     *
     * 先优先沿用当前 app 已经落过 gateway 配置的目录，避免历史运行中已经选定
     * 的目录在一次重启后突然切换，导致旧目录残留文件与新目录新文件同时被加载。
     *
     * 如果当前 app 还没有历史文件，则按固定优先级挑选语义上最像 server/vhost
     * 扩展目录的候选，例如 `servers`、`conf.d`、`vhosts`。这能让
     * `/etc/nginx/conf.d` 与 `/etc/nginx/sites-enabled` 同时存在时仍有稳定结果，
     * 而不是把“两个都合法”误判成错误。
     *
     * @param array<int, string> $candidates 候选目录列表。
     * @return string
     */
    protected function pickManagedIncludeDir(array $candidates): string {
        $candidates = array_values(array_unique(array_filter(array_map(static fn($item) => rtrim((string)$item, '/'), $candidates))));
        if (!$candidates) {
            return '';
        }
        if (count($candidates) === 1) {
            return $candidates[0];
        }

        $existingManaged = array_values(array_filter($candidates, fn(string $candidate): bool => $this->candidateContainsManagedFiles($candidate)));
        if (count($existingManaged) === 1) {
            return $existingManaged[0];
        }

        $preferredNames = ['servers', 'conf.d', 'vhosts', 'sites-enabled', 'http.d'];
        foreach ($preferredNames as $preferredName) {
            foreach ($candidates as $candidate) {
                if (strtolower(basename($candidate)) === $preferredName) {
                    return $candidate;
                }
            }
        }

        sort($candidates, SORT_STRING);
        return $candidates[0];
    }

    /**
     * 判断某个候选目录里是否已经存在当前 app 的 gateway 配置文件。
     *
     * 这里只检查 app 维度的 upstream/server/example 文件，不把全局文件
     * `scf_gateway.global.conf` 作为判据，避免多个应用共享全局文件时把目录误判成
     * “当前 app 已在使用”。
     *
     * @param string $candidate 候选目录。
     * @return bool 当前 app 的 gateway 文件已经存在时返回 true。
     */
    protected function candidateContainsManagedFiles(string $candidate): bool {
        $candidate = rtrim($candidate, '/');
        if ($candidate === '') {
            return false;
        }

        $baseName = $this->fileBaseName();
        $paths = [
            $candidate . '/' . $baseName . '.conf',
            $candidate . '/' . $baseName . '.server.conf',
            $candidate . '/' . $baseName . '.server.example',
        ];
        foreach ($paths as $path) {
            if (is_file($path)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 读取当前 nginx 主配置里已存在的 http 级指令名。
     *
     * @return array 以指令名为键的存在性表。
     */
    protected function existingHttpDirectives(): array {
        $confPath = (string)($this->nginxRuntimeMeta()['conf_path'] ?? '');
        if ($confPath === '' || !is_file($confPath)) {
            return [];
        }
        $content = (string)file_get_contents($confPath);
        $found = [];
        preg_match_all('/^\\s*([a-z_]+)\\s+.+;/mi', $content, $matches);
        foreach ($matches[1] ?? [] as $name) {
            $name = strtolower(trim((string)$name));
            if ($name !== '') {
                $found[$name] = true;
            }
        }
        return $found;
    }


    /**
     * 仅在目标指令不存在时追加一行，避免重复写入 nginx 主配置。
     *
     * @param array $lines 输出行数组，按引用修改。
     * @param array $existing 已存在的指令名集合。
     * @param string $name 需要检测的指令名。
     * @param string $directive 要追加的完整指令文本。
     * @return void
     */
    protected function appendDirectiveIfMissing(array &$lines, array $existing, string $name, string $directive): void {
        if (!isset($existing[strtolower($name)])) {
            $lines[] = $directive;
        }
    }

    /**
     * 标记一次 runtime meta 是否已经被当前进程观察并用于生成配置。
     *
     * @param array $meta 当前 runtime meta。
     * @return bool 当签名发生变化时返回 true。
     */
    protected function markRuntimeMetaObserved(array $meta): bool {
        if ($this->runtimeMetaLoadedFromCache) {
            return false;
        }
        $signature = md5(json_encode([
            'bin' => (string)($meta['bin'] ?? ''),
            'conf_path' => (string)($meta['conf_path'] ?? ''),
            'conf_dir' => (string)($meta['conf_dir'] ?? ''),
            'pid_path' => (string)($meta['pid_path'] ?? ''),
        ], JSON_UNESCAPED_SLASHES));
        if ($this->lastObservedRuntimeMetaSignature === $signature) {
            return false;
        }
        $this->lastObservedRuntimeMetaSignature = $signature;
        return true;
    }
}
