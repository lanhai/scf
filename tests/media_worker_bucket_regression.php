<?php
declare(strict_types=1);

/**
 * MediaWorker 活跃任务桶回归检查脚本。
 *
 * 这个脚本不依赖 CP 页面或 HTTP 接口，而是直接检查 Redis 里的任务桶不变量：
 * 1. bucket hash 里的任务是否都符合该桶声明的 queue_type/status；
 * 2. 排序 zset 的 member 数量是否与 bucket hash 一致；
 * 3. 排序 zset 里是否存在指向不存在 hash field 的死 member；
 * 4. 活跃桶是否出现“卡片计数有值，但桶内没有有效任务”的情况。
 *
 * 目的不是修数据，而是把“列表为空/卡片暴涨”这种问题还原成明确的存储层证据，
 * 方便后续只改真正写脏 bucket 的入口。
 *
 * 用法：
 * php scf/tests/media_worker_bucket_regression.php
 * php scf/tests/media_worker_bucket_regression.php --config=app --host=127.0.0.1 --port=6379 --auth=xxx --appid=mtvideo-Ir9X2E
 */

$options = getopt('', [
    'config::',
    'host::',
    'port::',
    'auth::',
    'db::',
    'appid::',
    'bucket::',
    'help::',
]);

if (isset($options['help'])) {
    fwrite(STDOUT, "Usage: php scf/tests/media_worker_bucket_regression.php [--config=dev|app] [--host=HOST] [--port=6379] [--auth=PWD] [--db=0] [--appid=APPID] [--bucket=name]\n");
    exit(0);
}

$root = dirname(__DIR__, 2);
$appsJsonFile = $root . '/apps/apps.json';
$defaultConfigName = (string)($options['config'] ?? 'dev');
$configFile = $root . '/apps/mtvideo/src/config/' . ($defaultConfigName === 'app' ? 'app.php' : 'app_dev.php');

if (!is_file($configFile)) {
    fwrite(STDERR, "配置文件不存在: {$configFile}\n");
    exit(2);
}

$config = require $configFile;
$redisServer = (array)($config['cache']['redis']['servers']['main'] ?? []);
$appId = trim((string)($options['appid'] ?? resolveMtvideoAppId($appsJsonFile)));
$host = trim((string)($options['host'] ?? ($redisServer['host'] ?? '127.0.0.1')));
$port = max(1, (int)($options['port'] ?? ($redisServer['port'] ?? 6379)));
$auth = (string)($options['auth'] ?? ($redisServer['auth'] ?? ''));
$dbIndex = max(0, (int)($options['db'] ?? ($redisServer['db_index'] ?? 0)));
$bucketFilter = trim((string)($options['bucket'] ?? ''));

if ($appId === '') {
    fwrite(STDERR, "无法解析 mtvideo appid，请显式传 --appid\n");
    exit(2);
}

$redis = new Redis();
try {
    $redis->connect($host, $port, 2.0);
    if ($auth !== '') {
        $redis->auth($auth);
    }
    $redis->select($dbIndex);
} catch (Throwable $throwable) {
    fwrite(STDERR, "Redis 连接失败: " . $throwable->getMessage() . "\n");
    exit(2);
}

$bucketRules = [
    'download_queued' => ['queue_type' => 'download', 'status' => 'queued'],
    'download_pending_ack' => ['queue_type' => 'download', 'status' => 'pending'],
    'download_processing' => ['queue_type' => 'download', 'status' => 'processing'],
    'download_failed' => ['queue_type' => 'download', 'status' => 'failed'],
    'upload_queued' => ['queue_type' => 'upload', 'status' => 'queued'],
    'upload_processing' => ['queue_type' => 'upload', 'status' => 'processing'],
    'upload_failed' => ['queue_type' => 'upload', 'status' => 'failed'],
];

if ($bucketFilter !== '') {
    $bucketRules = array_intersect_key($bucketRules, [$bucketFilter => true]);
    if (!$bucketRules) {
        fwrite(STDERR, "未知 bucket: {$bucketFilter}\n");
        exit(2);
    }
}

$failures = 0;
$reports = [];
foreach ($bucketRules as $bucket => $rule) {
    $reports[] = auditBucket($redis, $appId, $bucket, $rule);
}

foreach ($reports as $report) {
    $header = sprintf(
        "[%s] hash=%d valid=%d invalid=%d order=%d dead_members=%d",
        $report['bucket'],
        $report['hash_count'],
        $report['valid_count'],
        $report['invalid_count'],
        $report['order_count'],
        $report['dead_member_count']
    );
    fwrite(STDOUT, $header . PHP_EOL);
    if ($report['invalid_count'] > 0) {
        fwrite(STDOUT, "  invalid sample: " . json_encode($report['invalid_samples'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    }
    if ($report['dead_member_count'] > 0) {
        fwrite(STDOUT, "  dead order members: " . json_encode($report['dead_member_samples'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    }
    if ($report['hash_count'] !== $report['order_count']) {
        fwrite(STDOUT, "  mismatch: hash_count != order_count" . PHP_EOL);
    }
    if ($report['hash_count'] > 0 && $report['valid_count'] === 0) {
        fwrite(STDOUT, "  mismatch: bucket has rows but no valid tasks" . PHP_EOL);
    }
    if (
        $report['invalid_count'] > 0
        || $report['dead_member_count'] > 0
        || $report['hash_count'] !== $report['order_count']
        || ($report['hash_count'] > 0 && $report['valid_count'] === 0)
    ) {
        $failures++;
    }
}

exit($failures > 0 ? 1 : 0);

/**
 * 解析 mtvideo 的 appid。
 *
 * @param string $appsJsonFile apps.json 路径。
 * @return string
 */
function resolveMtvideoAppId(string $appsJsonFile): string {
    if (!is_file($appsJsonFile)) {
        return '';
    }
    $apps = json_decode((string)file_get_contents($appsJsonFile), true);
    if (!is_array($apps)) {
        return '';
    }
    foreach ($apps as $app) {
        if (!is_array($app)) {
            continue;
        }
        if (trim((string)($app['app_path'] ?? '')) !== 'mtvideo') {
            continue;
        }
        return trim((string)($app['appid'] ?? ''));
    }
    return '';
}

/**
 * 检查单个活跃 bucket 的存储不变量。
 *
 * @param Redis $redis Redis 连接。
 * @param string $appId 应用前缀。
 * @param string $bucket bucket 名称。
 * @param array<string, string> $rule 期望的 queue_type/status。
 * @return array<string, mixed>
 */
function auditBucket(Redis $redis, string $appId, string $bucket, array $rule): array {
    $bucketKey = buildRedisKey($appId, 'MEDIA_WORKER_TASK_BUCKET_' . strtoupper($bucket));
    $orderKey = $bucketKey . '_ORDER';
    $hashCount = (int)$redis->hLen($bucketKey);
    $orderCount = (int)$redis->zCard($orderKey);
    $validCount = 0;
    $invalidCount = 0;
    $invalidSamples = [];

    $iterator = null;
    while (($items = $redis->hScan($bucketKey, $iterator, '*', 200)) !== false) {
        foreach ($items as $taskHashKey => $rawTask) {
            $task = decodeRedisTask($rawTask);
            if (!is_array($task)) {
                $invalidCount++;
                appendSample($invalidSamples, [
                    'task_key' => (string)$taskHashKey,
                    'reason' => 'decode_failed',
                ]);
                continue;
            }
            $queueType = trim((string)($task['queue_type'] ?? $task['task_type'] ?? ''));
            $status = trim((string)($task['status'] ?? ''));
            if ($queueType !== $rule['queue_type'] || $status !== $rule['status']) {
                $invalidCount++;
                appendSample($invalidSamples, [
                    'task_key' => (string)$taskHashKey,
                    'video_id' => trim((string)($task['video_id'] ?? '')),
                    'queue_type' => $queueType,
                    'status' => $status,
                ]);
                continue;
            }
            $validCount++;
        }
    }

    $deadMemberCount = 0;
    $deadMemberSamples = [];
    $members = $redis->zRange($orderKey, 0, 499);
    if (is_array($members)) {
        foreach ($members as $member) {
            $member = trim((string)$member);
            if ($member === '') {
                continue;
            }
            if (!$redis->hExists($bucketKey, $member)) {
                $deadMemberCount++;
                appendSample($deadMemberSamples, $member);
            }
        }
    }

    return [
        'bucket' => $bucket,
        'bucket_key' => $bucketKey,
        'order_key' => $orderKey,
        'hash_count' => $hashCount,
        'order_count' => $orderCount,
        'valid_count' => $validCount,
        'invalid_count' => $invalidCount,
        'invalid_samples' => $invalidSamples,
        'dead_member_count' => $deadMemberCount,
        'dead_member_samples' => $deadMemberSamples,
    ];
}

/**
 * 构建带 app 前缀的 Redis key。
 *
 * @param string $appId 应用前缀。
 * @param string $key 原始 key。
 * @return string
 */
function buildRedisKey(string $appId, string $key): string {
    return trim($appId) === '' ? $key : $appId . ':' . $key;
}

/**
 * 把 Redis 中存储的 JSON 任务快照恢复成数组。
 *
 * @param mixed $rawTask 原始值。
 * @return array<string, mixed>|null
 */
function decodeRedisTask(mixed $rawTask): ?array {
    if (is_array($rawTask)) {
        return $rawTask;
    }
    if (!is_string($rawTask) || trim($rawTask) === '') {
        return null;
    }
    $decoded = json_decode($rawTask, true);
    return is_array($decoded) ? $decoded : null;
}

/**
 * 限制样本输出数量，避免检查报告过长。
 *
 * @param array<int, mixed> $samples 样本集合。
 * @param mixed $value 样本值。
 * @return void
 */
function appendSample(array &$samples, mixed $value): void {
    if (count($samples) >= 5) {
        return;
    }
    $samples[] = $value;
}
