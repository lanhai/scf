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

        // “配置文件已经写好了，但 nginx 进程没起来”时，不能因为内容没变化就跳过。
        // 首次部署、手工停止 nginx、机器重启后 gateway 先起来等场景下，都需要在
        // 当前配置未变化的前提下重新执行 `nginx -t` 和 `nginx start`。
        if ($configChanged || $shouldEnsureRunning) {
            $this->testNginxConfig();
            $tested = true;
            if ($this->shouldReloadNginx()) {
                $this->reloadOrStartNginx();
                $reloaded = true;
            }
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
            '',
        ], $realIpLines, [
            // `/dashboard.socket` 和 `/_gateway` 都属于控制面，必须绕过业务 upstream。
            '    location = /dashboard.socket {',
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
                $blocks[] = '    location = ' . $path . ' {';
                $blocks[] = '        root ' . $publicRoot . ';';
                $blocks[] = '        access_log off;';
                $blocks[] = '        log_not_found off;';
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
            $blocks[] = '        try_files $uri $uri/ =404;';
            $blocks[] = '    }';
            $blocks[] = '';
            $blocks[] = '    location ^~ ' . $normalized . '/ {';
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
     * @return string 实际使用的 nginx conf.d/servers 目录。
     */
    protected function confDir(): string {
        $configured = trim((string)($this->gateway->serverConfig()['gateway_nginx_conf_dir'] ?? ''));
        if ($configured !== '') {
            return rtrim($configured, '/');
        }
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
     * 获取 nginx 运行时元数据，并优先复用缓存。
     *
     * @return array nginx bin / conf / pid 等运行时信息。
     */
    protected function nginxRuntimeMeta(): array {
        if ($this->nginxRuntimeMeta !== null) {
            return $this->nginxRuntimeMeta;
        }

        $cached = $this->loadRuntimeMetaCache();
        $meta = $this->detectRuntimeMeta();
        if ($this->isUsableRuntimeMeta($meta)) {
            // nginx conf 目录必须以当前 nginx.conf 的 include 链为准。
            // 这里即使存在旧缓存，也优先重新跑一遍 `nginx -V` + 主配置解析，
            // 避免历史上探测到的 conf.d/servers 目录在后续部署中被永久复用。
            $this->writeRuntimeMetaCache($meta);
            $this->runtimeMetaLoadedFromCache = false;
            return $this->nginxRuntimeMeta = $meta;
        }

        // 仅当 fresh 探测失败时才回退旧缓存，保证 nginx 命令暂时不可用时仍有
        // 兜底，但正常情况下不会让过期的 conf_dir 长期污染后续配置输出。
        if ($this->isUsableRuntimeMeta($cached)) {
            $this->runtimeMetaLoadedFromCache = true;
            return $this->nginxRuntimeMeta = $cached;
        }

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
        $bin = $this->detectNginxBinary();
        $meta = [
            'bin' => $bin,
            'conf_path' => '',
            'pid_path' => '',
            'conf_dir' => '',
        ];

        $command = escapeshellarg($bin) . ' -V 2>&1';
        exec($command, $output, $code);
        if ($code === 0 || $output) {
            $versionInfo = implode("\n", $output);
            $meta['conf_path'] = $this->extractNginxBuildArg($versionInfo, 'conf-path');
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
     * 推导 nginx 配置目录，兼容不同安装布局。
     *
     * @param string $confPath nginx 主配置文件路径。
     * @return string 可用的 nginx 配置目录。
     */
    protected function detectNginxConfDir(string $confPath): string {
        $confDir = $confPath !== '' ? dirname($confPath) : '/etc/nginx';
        $candidates = [];
        if ($confPath !== '' && is_file($confPath)) {
            $content = (string)file_get_contents($confPath);
            if (preg_match_all('/^\\s*include\\s+([^;]+);/m', $content, $matches)) {
                foreach ($matches[1] as $include) {
                    $include = trim((string)$include, "\"' ");
                    if ($include === '') {
                        continue;
                    }
                    if (!str_starts_with($include, '/')) {
                        $include = $confDir . '/' . $include;
                    }
                    $candidates[] = str_contains($include, '*') ? dirname($include) : $include;
                }
            }
        }

        $candidates = array_merge($candidates, [
            $confDir . '/conf.d',
            $confDir . '/servers',
            '/etc/nginx/conf.d',
            '/usr/local/openresty/nginx/conf/conf.d',
            '/opt/homebrew/etc/nginx/servers',
        ]);

        foreach (array_unique($candidates) as $candidate) {
            if (is_dir($candidate)) {
                return rtrim($candidate, '/');
            }
        }

        return rtrim($confDir . '/conf.d', '/');
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
