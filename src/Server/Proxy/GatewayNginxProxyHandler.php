<?php

namespace Scf\Server\Proxy;

use RuntimeException;
use Scf\Core\App;
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
        $configChanged = $globalChanged || $upstreamChanged || $serverChanged || $exampleChanged;
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
        $proxyConnectTimeout = max(1, (int)($this->gateway->serverConfig()['gateway_nginx_proxy_connect_timeout'] ?? 3));
        $proxyIoTimeout = max(60, (int)($this->gateway->serverConfig()['gateway_nginx_proxy_io_timeout'] ?? 31536000));
        $clientIoTimeout = max(60, (int)($this->gateway->serverConfig()['gateway_nginx_client_io_timeout'] ?? $proxyIoTimeout));
        $buffering = (bool)($this->gateway->serverConfig()['gateway_nginx_proxy_buffering'] ?? true);
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

        return implode("\n", [
            '# generated by SCF Gateway',
            '# do not edit manually',
            '# app=' . APP_DIR_NAME . ', role=' . SERVER_ROLE . ', port=' . $this->gateway->businessPort(),
            '',
            'upstream ' . $businessName . ' {',
            $businessServers,
            '    keepalive 128;',
            '}',
            '',
            'upstream ' . $controlName . ' {',
            '    server ' . $controlHost . ':' . $controlPort . ' max_fails=1 fail_timeout=2s;',
            '    keepalive 16;',
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
        $serverName = (string)($this->gateway->serverConfig()['gateway_nginx_server_name'] ?? '_');
        $listenPort = $this->gateway->businessPort();
        $businessName = $this->businessUpstreamName();
        $controlName = $this->controlUpstreamName();
        $defaultUpstream = $this->shouldRouteBusinessTrafficToControlPlane() ? $controlName : $businessName;
        $upgradeMapVar = '$' . $this->safeToken('scf_gateway_connection_upgrade_' . APP_DIR_NAME . '_' . SERVER_ROLE . '_' . $listenPort);
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
            '        proxy_pass http://' . $defaultUpstream . ';',
            '    }',
            '}',
            '',
        ]));
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
        return rtrim($this->confDir(), '/') . '/' . $this->fileBaseName() . '.upstreams.conf';
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
     * 这里必须以当前 nginx 主配置推导出的 include 目录为准，而不是应用配置。
     * 否则同一套框架在不同机器上会被业务配置硬编码到错误目录，出现“文件已生成，
     * 但 nginx 实际没有 include 这份配置”的问题。
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
     * @return string 配置文件基名。
     */
    protected function fileBaseName(): string {
        return $this->safeToken('scf_gateway_' . APP_DIR_NAME . '_' . SERVER_ROLE . '_' . $this->gateway->businessPort());
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

        $meta['conf_dir'] = $this->detectNginxConfDir((string)$meta['conf_path']);
        return $meta;
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
     * 这里不再按“第一个通配 include”直接返回，而是优先选择语义上明显用于
     * server/vhost 扩展配置的目录，例如 `servers`、`conf.d`、`vhosts`。
     * 如果候选不唯一且无法明确判断，就直接报错，避免继续擅自写错目录。
     *
     * @param array<int, string> $candidates 候选目录列表。
     * @return string
     * @throws RuntimeException 当目录候选存在歧义时抛出。
     */
    protected function pickManagedIncludeDir(array $candidates): string {
        $candidates = array_values(array_unique(array_filter(array_map(static fn($item) => rtrim((string)$item, '/'), $candidates))));
        if (!$candidates) {
            return '';
        }
        if (count($candidates) === 1) {
            return $candidates[0];
        }

        $preferredNames = ['servers', 'conf.d', 'vhosts', 'sites-enabled', 'http.d'];
        $preferred = array_values(array_filter($candidates, static function (string $candidate) use ($preferredNames) {
            return in_array(strtolower(basename($candidate)), $preferredNames, true);
        }));
        if (count($preferred) === 1) {
            return $preferred[0];
        }

        throw new RuntimeException(
            'nginx 主配置 include 目录存在歧义: ' . implode(' | ', $candidates)
        );
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
