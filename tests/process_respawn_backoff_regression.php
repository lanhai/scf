<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/Server/ProcessRespawnBackoff.php';

use Scf\Server\ProcessRespawnBackoff;

function assertBackoff(bool $condition, string $message): void {
    if (!$condition) {
        fwrite(STDERR, "FAILED: {$message}\n");
        exit(1);
    }
}

$backoff = new ProcessRespawnBackoff();
$now = 1000;
foreach ([2, 4, 8, 16, 32, 60, 60] as $expectedDelay) {
    $delay = $backoff->recordStartFailure('worker', $now);
    assertBackoff($delay === $expectedDelay, "expected delay {$expectedDelay}, got {$delay}");
    assertBackoff(!$backoff->canStart('worker', $now), 'retry must be blocked before deadline');
    assertBackoff($backoff->canStart('worker', $now + $delay), 'retry must open at deadline');
    $now += $delay;
}

$shortLived = new ProcessRespawnBackoff();
$shortLived->recordStarted('worker', 2000);
assertBackoff($shortLived->recordExit('worker', 2005) === 2, 'short-lived worker must back off');

$stable = new ProcessRespawnBackoff();
$stable->recordStartFailure('worker', 3000);
$stable->recordStarted('worker', 3002);
$stable->markStable('worker', 3032);
assertBackoff((int)$stable->state('worker')['attempts'] === 0, 'stable worker must clear failure attempts');
assertBackoff($stable->recordExit('worker', 3033) === 0, 'stable worker exit must restart immediately');

$stormGuard = new ProcessRespawnBackoff();
$attempts = 0;
for ($second = 0; $second < 120; $second++) {
    if ($stormGuard->canStart('worker', $second)) {
        $stormGuard->recordStartFailure('worker', $second);
        $attempts++;
    }
}
assertBackoff($attempts <= 6, "120-second crash loop spawned {$attempts} attempts");

fwrite(STDOUT, "Process respawn backoff regression checks passed.\n");
