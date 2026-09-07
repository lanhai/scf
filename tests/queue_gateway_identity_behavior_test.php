<?php
declare(strict_types=1);

namespace Scf\Core\Traits { trait Singleton {} }

namespace {
    require dirname(__DIR__) . '/src/Server/Task/RQueue.php';

    $root = sys_get_temp_dir() . '/scf-queue-identity-' . bin2hex(random_bytes(5));
    mkdir($root . '/var', 0700, true);
    define('SCF_ROOT', $root . '/scf');
    define('APP_DIR_NAME', 'mtvideo');
    define('SERVER_ROLE', 'master');
    define('SERVER_QUEUE_MANAGER_PID_FILE', $root . '/var/mtvideo_gateway_upstream_master_9581_queue_manager.pid');
    $path = $root . '/var/mtvideo_master_queue_manager.pid.worker.lock';
    $handle = null;
    function check(bool $condition, string $message): void {
        if (!$condition) throw new \RuntimeException($message);
    }
    try {
        check(\Scf\Server\Task\RQueue::gatewayWorkerLockOwner() === [], 'No consumer before the Gateway lock exists');
        check(!file_exists($path), 'Read-only probing must not create runtime files');
        $handle = fopen($path, 'w+');
        flock($handle, LOCK_EX | LOCK_NB);
        $owner = ['pid' => getmypid(), 'token' => 'gateway-generation-a', 'started_at' => time()];
        fwrite($handle, json_encode($owner));
        fflush($handle);
        $state = \Scf\Server\Task\RQueue::gatewayWorkerLockOwner();
        check($state['held'] && $state['token'] === $owner['token'], 'Upstream must read the live Gateway owner across different PID namespaces');
        check($state['pid'] === getmypid(), 'Identity must come from the actual held lock');
        flock($handle, LOCK_UN);
        check(\Scf\Server\Task\RQueue::gatewayWorkerLockOwner() === [], 'Released lock metadata must not identify a live worker');
        echo "PASS Gateway queue identity across process namespaces\n";
    } finally {
        if (is_resource($handle)) fclose($handle);
        if (file_exists($path)) unlink($path);
        rmdir($root . '/var');
        rmdir($root);
    }
}
