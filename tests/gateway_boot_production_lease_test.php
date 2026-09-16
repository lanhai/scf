<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/src/Command/Bootstrap/Support.php';
require_once dirname(__DIR__) . '/src/Command/Bootstrap/ServerRuntimePorts.php';
require_once dirname(__DIR__) . '/src/Server/Gateway/GatewayLease.php';

$root = sys_get_temp_dir() . '/scf-production-lease-' . bin2hex(random_bytes(5));
define('SCF_APPS_ROOT', $root);
define('APP_DIR_NAME', 'startup_fixture');
define('SERVER_ROLE', 'master');
define('SERVER_RUN_ENV', 'production');
define('APP_UPDATE_DIR', $root . '/startup_fixture/update');
mkdir(APP_UPDATE_DIR, 0700, true);
$previousEnv = getenv('APP_ENV');
putenv('APP_ENV');
try {
    $lease = ['app' => APP_DIR_NAME, 'env' => SERVER_RUN_ENV, 'role' => SERVER_ROLE,
        'gateway_port' => 9580, 'epoch' => 92, 'state' => 'stopped', 'expires_at' => time(),
        'meta' => ['gateway_pid' => 96009]];
    file_put_contents(\Scf\Server\Gateway\GatewayLease::leaseStateFile(9580, SERVER_ROLE), json_encode($lease));
    $args = ['boot', 'gateway', 'start', '-app=startup_fixture'];
    if (scf_gateway_startup_lease($args) !== $lease) {
        throw new RuntimeException('Default production boot must read the lease filename written by GatewayLease');
    }
    putenv('APP_ENV=dev');
    if (scf_boot_command_environment([...$args, '-env=production']) !== 'production'
        || scf_boot_command_environment([...$args, '-env=production', '-dev']) !== 'dev') {
        throw new RuntimeException('Environment priority must match Env::initialize: -dev, -env, APP_ENV, production');
    }
    echo "PASS production lease: real filename contract and environment precedence\n";
} finally {
    $previousEnv === false ? putenv('APP_ENV') : putenv('APP_ENV=' . $previousEnv);
    foreach (glob(APP_UPDATE_DIR . '/*') ?: [] as $file) unlink($file);
    rmdir(APP_UPDATE_DIR);
    rmdir(dirname(APP_UPDATE_DIR));
    rmdir($root);
}
